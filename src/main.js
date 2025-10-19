// src/main.js (ESM)
// Apify SDK + Crawlee (CheerioCrawler) + gotScraping (HTTP-based), ESM compatible.
// Includes: proxy rotation, session pool, robust start URL parsing, and resilient extractors
// for company, location, datePosted, employmentType, salary, description_html/text.

import { Actor } from 'apify';
import {
    CheerioCrawler,
    createCheerioRouter,
    Dataset,
    log,
    RequestQueue,
} from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

// -------------------- Helpers: text utils --------------------

const normText = (s) => (s || '').replace(/\u00A0/g, ' ').replace(/\s+/g, ' ').trim();

const pickFirstNonEmpty = (...vals) => {
    for (const v of vals) {
        if (v == null) continue;
        const t = typeof v === 'string' ? v : String(v);
        const n = normText(t);
        if (n) return n;
    }
    return null;
};

const cleanHtmlToText = (html) => {
    if (!html) return null;
    const $ = cheerioLoad(html);
    $('script, style, noscript, iframe, form, button, svg').remove();
    return normText($.root().text());
};

const stripAttrsKeepTags = (
    html,
    allowedTags = ['p', 'br', 'ul', 'ol', 'li', 'strong', 'em', 'b', 'i', 'a', 'h3', 'h4'],
) => {
    if (!html) return '';
    const $ = cheerioLoad(html, { decodeEntities: true });

    // Remove non-content & hidden elements
    $('script, style, noscript, iframe, form, button, svg, input, textarea, select').remove();
    $('[data-complaint-overlay], #complaint-popup-container, .popup, [aria-hidden="true"]').remove();
    $('[style*="display:none"], [hidden]').remove();

    // Keep only allowed tags; drop attributes except href on <a>
    $('*').each((_, el) => {
        const name = el.name || '';
        if (!allowedTags.includes(name)) {
            $(el).replaceWith($(el).contents());
            return;
        }
        const attribs = el.attribs || {};
        for (const attr of Object.keys(attribs)) {
            if (name === 'a' && attr === 'href') continue;
            $(el).removeAttr(attr);
        }
    });

    // Normalize whitespace
    $('body').find('*').contents().each((_, node) => {
        if (node.type === 'text' && node.data) node.data = normText(node.data);
    });

    const out = $('body').html() || '';
    return out.replace(/\s+\n/g, '\n').trim();
};

// -------------------- Helpers: parsing fields --------------------

const parseSpanishRelativeDateToISO = (s) => {
    const str = (s || '').toLowerCase();
    if (!str) return null;
    const now = new Date();

    // Direct ISO first
    const iso = str.match(/\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2}(?::\d{2})?)?/);
    if (iso) {
        const d = new Date(iso[0]);
        if (!isNaN(d)) return d.toISOString();
    }

    // "hace 3 días", "hace una hora", "hace 15 minutos"
    const rel = str.match(/hace\s+(\d+|una|un)\s+(minutos|minuto|horas|hora|días|día|semanas|semana|meses|mes|años|año)/i);
    if (rel) {
        const qtyRaw = rel[1];
        const unit = rel[2];
        const qty = (qtyRaw === 'una' || qtyRaw === 'un') ? 1 : Number(qtyRaw);

        const d = new Date(now);
        if (/minuto/.test(unit)) d.setMinutes(d.getMinutes() - qty);
        else if (/hora/.test(unit)) d.setHours(d.getHours() - qty);
        else if (/día/.test(unit)) d.setDate(d.getDate() - qty);
        else if (/semana/.test(unit)) d.setDate(d.getDate() - qty * 7);
        else if (/mes/.test(unit)) d.setMonth(d.getMonth() - qty);
        else if (/año/.test(unit)) d.setFullYear(d.getFullYear() - qty);

        return d.toISOString();
    }
    return null;
};

const extractLabeledValue = ($, labelRegexes) => {
    // 1) Chips / attributes
    let val = null;

    $('li, .box_attributes li, .attribute, .chip, .tag').each((_, li) => {
        const $li = $(li);
        const text = normText($li.text());
        if (!text) return;
        for (const re of labelRegexes) {
            if (re.test(text)) {
                const parts = text.split(':');
                if (parts.length > 1) val = normText(parts.slice(1).join(':'));
                else {
                    const spans = $li.find('span');
                    if (spans.length >= 2) val = normText($(spans[1]).text());
                    else val = text.replace(re, '').trim();
                }
                return false;
            }
        }
        if (val) return false;
    });
    if (val) return val;

    // 2) dt/dd pairs
    $('dt').each((_, dt) => {
        const label = normText($(dt).text());
        for (const re of labelRegexes) {
            if (re.test(label)) {
                const dd = $(dt).next('dd');
                if (dd.length) {
                    val = normText(dd.text());
                    return false;
                }
            }
        }
        if (val) return false;
    });

    return val;
};

const normalizeSalaryFromJsonLd = (baseSalary) => {
    if (!baseSalary) return null;
    const isMonetary = baseSalary['@type'] === 'MonetaryAmount';
    const monetary = isMonetary ? baseSalary : null;
    const value = monetary ? monetary.value : baseSalary.value || baseSalary;
    const currency = monetary?.currency || baseSalary.currency || null;

    let min = null, max = null, unitText = null, amount = null;

    if (value && typeof value === 'object') {
        min = value.minValue ?? null;
        max = value.maxValue ?? null;
        amount = value.value ?? null;
        unitText = value.unitText ?? baseSalary.unitText ?? null;
    } else if (typeof value === 'number') {
        amount = value;
    }

    const out = {};
    if (currency) out.salary_currency = currency;
    if (unitText) out.salary_period = unitText; // MONTH, HOUR, YEAR
    if (min != null) out.salary_min = min;
    if (max != null) out.salary_max = max;
    if (amount != null && min == null && max == null) out.salary_amount = amount;

    return Object.keys(out).length ? out : null;
};

const parseAllJsonLd = ($) => {
    const blocks = $('script[type="application/ld+json"]');
    const jobPostings = [];
    blocks.each((_, el) => {
        const raw = $(el).contents().text();
        if (!raw) return;
        try {
            const data = JSON.parse(raw);
            const items = Array.isArray(data) ? data : [data];
            for (const item of items) {
                const type = item['@type'] || item.type;
                const isJob = (Array.isArray(type) && type.includes('JobPosting')) || type === 'JobPosting';
                if (isJob) jobPostings.push(item);
            }
        } catch {
            // ignore malformed JSON
        }
    });
    return jobPostings[0] || null;
};

// -------------------- Hardened company / location / description helpers --------------------

// Company cleanup: keep only the clean name, drop ratings, popups, and legalese
const cleanCompanyName = (raw) => {
    let s = normText(raw);
    if (!s) return null;

    const STOP_TOKENS = [
        'seguir', 'volver', 'información básica', 'política de privacidad',
        'responsable', 'finalidad', 'legitimación', 'destinatarios', 'derechos',
        '¡no te pierdas', 'recibe notificaciones', 'formato incorrecto', 'contraseña incorrecta',
        'acepto las condiciones', 'ver detalle legal',
    ];

    s = s.split('\n')[0].split('|')[0].split('·')[0];
    s = s.replace(/\b\d[\d.,]{0,3}\b/g, '').trim();

    for (const tok of STOP_TOKENS) {
        const idx = s.toLowerCase().indexOf(tok);
        if (idx > 0) {
            s = s.slice(0, idx).trim();
        }
    }

    s = s.replace(/[•·|]+$/g, '').replace(/\s{2,}/g, ' ').trim();
    if (s.length > 80) s = s.slice(0, 80).trim();

    return s || null;
};

// Spanish-first DOM company selectors
const pickCompanyFromDom = ($) => {
    // Schema.org / microdata
    let c =
        $('[itemprop="hiringOrganization"] [itemprop="name"]').first().text() ||
        $('[itemscope][itemtype*="Organization"] [itemprop="name"]').first().text();
    if ((c = cleanCompanyName(c))) return c;

    // Empresa profile links / anchors
    const candidates = [
        'a[href*="/empresas/"]',
        'a[href*="/empresa/"]',
        '.box_header a[href*="/empresa"]',
        'a:contains("Ver más sobre la empresa")',
        'a:contains("Ver más sobre la compañía")',
    ];
    for (const sel of candidates) {
        const t = $(sel).first().text();
        const cleaned = cleanCompanyName(t);
        if (cleaned) return cleaned;
    }

    // Header near rating (as fallback)
    {
        const t = $('.box_header .fc_base, .box_header .fc_base a, .box_header .fc_base span')
            .first().text();
        const cleaned = cleanCompanyName(t);
        if (cleaned) return cleaned;
    }

    {
        const t = $('.box_header a, .box_company a, [class*="company"] a').first().text();
        const cleaned = cleanCompanyName(t);
        if (cleaned) return cleaned;
    }

    return null;
};

// Safer location extraction: microdata first, then chips, then cleaned fallback
const pickLocation = ($, fallbackText) => {
    const locality = $('[itemprop="jobLocation"] [itemprop="addressLocality"]').first().text();
    const region   = $('[itemprop="jobLocation"] [itemprop="addressRegion"]').first().text();
    const country  = $('[itemprop="jobLocation"] [itemprop="addressCountry"]').first().text();
    const parts = [locality, region, country].map(normText).filter(Boolean);
    if (parts.length) return parts.join(', ');

    const chip = extractLabeledValue($, [/ubicaci[oó]n/i, /ciudad/i, /estado/i, /localidad/i]);
    let loc = pickFirstNonEmpty(chip, fallbackText);
    if (!loc) return null;

    loc = loc
        .replace(/Publicado.*$/i, '')
        .replace(/Postular.*/i, '')
        .replace(/Ver detalle legal.*/i, '')
        .replace(/\s{2,}/g, ' ')
        .trim();

    loc = loc.split('|')[0].split('·')[0].trim();
    if (loc.length > 120) loc = loc.slice(0, 120).trim();
    return loc || null;
};

// Description extraction (Spanish-first) with sanitization and minimum content length
const pickDescriptionHtml = ($) => {
    const candidates = [
        '[itemprop="description"]',
        '.box_detail [itemprop="description"]',
        '.box_detail .box_section',
        '.box_detail article',
        '#offer-body',
        '.oferta-detalle, .descripcion-oferta',
        '.descripcion, .description, #description',
        '.job_desc',
    ];
    for (const sel of candidates) {
        const el = $(sel).first();
        if (el && el.length) {
            const textLen = normText(el.text()).length;
            if (textLen > 40) {
                return stripAttrsKeepTags(el.html());
            }
        }
    }
    const broad = $('.box_detail, .oferta, main').first();
    if (broad && broad.length) {
        const html = stripAttrsKeepTags(broad.html());
        const txt = cleanHtmlToText(html);
        if (normText(txt).length > 40) return html;
    }
    return null;
};

// -------------------- Extraction core --------------------

const extractFromJsonLd = ($) => {
    const item = parseAllJsonLd($);
    if (!item) return {};
    // Location(s)
    let location = null;
    const jl = item.jobLocation;
    if (jl) {
        const arr = Array.isArray(jl) ? jl : [jl];
        const parts = [];
        for (const j of arr) {
            const addr = j?.address || {};
            const piece = [addr.addressLocality, addr.addressRegion, addr.addressCountry]
                .map(normText).filter(Boolean).join(', ');
            if (piece) parts.push(piece);
        }
        location = parts.filter(Boolean).join(' | ') || null;
    }
    const salaryObj = normalizeSalaryFromJsonLd(item.baseSalary);
    const employmentType = item.employmentType
        ? (Array.isArray(item.employmentType) ? item.employmentType : [item.employmentType]).map(normText).filter(Boolean)
        : null;

    return {
        title: item.title || item.name || null,
        company: item.hiringOrganization?.name || null,
        datePosted: item.datePosted || null,
        description_raw: item.description || null, // can be HTML or text
        location,
        salary_struct: salaryObj,
        employmentType,
    };
};

const extractJobDetail = ($, url) => {
    const jsonLd = extractFromJsonLd($);

    // --- Title & company ---
    // Title: Use specific Computrabajo class
    const title = pickFirstNonEmpty(
        jsonLd.title,
        $('.title_offer.fs21.fwB.lh1_2').first().text(),
        $('h1, .box_title h1, [class*="title"]').first().text(),
    );

    // Company: Use specific Computrabajo class, fallback to other methods
    let company = jsonLd.company 
        || $('a.dIB.mr10').first().text()
        || pickCompanyFromDom($);
    company = cleanCompanyName(company);

    // --- Location ---
    let location = pickLocation($,
        $('.fs16.mb5').first().text()
        || $('.box_header p, [class*="location"], nav.breadcrumb').first().text()
    );

    // --- Salary ---
    let salary_struct = jsonLd.salary_struct || null;
    let salary_text = null;
    if (!salary_struct) {
        const scraped = extractLabeledValue($, [/salario/i, /sueldo/i, /compensaci[oó]n/i]);
        if (scraped) salary_text = normText(scraped);
    }

    // --- Employment type ---
    // Use specific Computrabajo class for employment type
    let employmentType = jsonLd.employmentType || null;
    if (!employmentType) {
        const dFlexText = $('.dFlex.mb10').first().text();
        if (dFlexText) {
            employmentType = normText(dFlexText);
        } else {
            const tipoContrato = extractLabeledValue($, [/tipo de contrato/i, /contrato/i]);
            const jornada = extractLabeledValue($, [/jornada/i, /horario/i, /modalidad/i]);
            const types = [tipoContrato, jornada].map(normText).filter(Boolean);
            if (types.length) employmentType = Array.from(new Set(types));
        }
    }

    // --- Date posted ---
    // Use specific Computrabajo class for date posted
    let datePosted = jsonLd.datePosted || null;
    if (!datePosted) {
        const dateText = $('.fc_aux.fs13.mtB').first().text();
        if (dateText) {
            const iso = parseSpanishRelativeDateToISO(dateText);
            datePosted = iso || normText(dateText) || null;
        } else {
            const rel = extractLabeledValue($, [/publicado/i, /fecha de publicaci[oó]n/i]);
            const iso = parseSpanishRelativeDateToISO(rel);
            datePosted = iso || null;
        }
    }

    // --- Description ---
    // Use specific Computrabajo class for description, keep HTML formatting
    let description_html = null;
    let description_text = null;

    if (jsonLd.description_raw && normText(jsonLd.description_raw).length > 40) {
        const sanitized = stripAttrsKeepTags(jsonLd.description_raw);
        if (sanitized && normText(cleanHtmlToText(sanitized)).length > 40) {
            description_html = sanitized;
            description_text = cleanHtmlToText(sanitized);
        }
    }
    if (!description_html) {
        // Try specific class first
        let descEl = $('.fs16.t_word_wrap').first();
        let html = descEl.length ? descEl.html() : null;
        
        if (!html) {
            html = pickDescriptionHtml($);
        }
        
        if (html) {
            // Remove hidden elements and popups
            const $ = cheerioLoad(html);
            $('[data-offers-grid-detail-container-error], .hide, [data-complaint-overlay], .popup, #complaint-popup-container').remove();
            description_html = $.root().html();
            description_text = cleanHtmlToText(description_html);
        }
    }

    const job = {
        url,
        source: 'computrabajo.com',
        title: title || null,
        company: company || null,
        location: location || null,
        datePosted: datePosted || null,
        description_html: description_html || null,
        description_text: description_text || null,
        employmentType: employmentType || null,
    };

    if (salary_struct) Object.assign(job, salary_struct);
    else if (salary_text) job.salary_text = salary_text;

    // Guard: CSS-only capture
    if (job.description_html && /{.*}/.test(job.description_html) && !/<(p|ul|li|a|strong|em|br|h3|h4)/i.test(job.description_html)) {
        job.description_html = null;
        job.description_text = null;
    }

    return job;
};

// -------------------- Start URL normalization --------------------

/**
 * Accept a wide variety of inputs:
 * - { startUrls: [{ url }, ...] }
 * - { startUrls: ["https://...", ...] }
 * - { startUrl: "https://..." }
 * - { urls: ["https://...", ...] } or { urls: "https://...\nhttps://..." }
 * - { requests: [{ url }, ...] } or { requests: ["https://...", ...] }
 */
const normalizeStartRequests = (input) => {
    const out = [];

    const pushUrl = (u) => {
        const url = String(u || '').trim();
        if (!url) return;
        try {
            const _ = new URL(url);
            out.push({ url });
        } catch { /* ignore invalid */ }
    };

    const pushMaybeArray = (val) => {
        if (!val) return;
        if (Array.isArray(val)) {
            for (const item of val) {
                if (typeof item === 'string') pushUrl(item);
                else if (item && typeof item === 'object' && item.url) pushUrl(item.url);
            }
        } else if (typeof val === 'string') {
            val.split(/\r?\n|,/).forEach(pushUrl);
        } else if (val && typeof val === 'object' && val.url) {
            pushUrl(val.url);
        }
    };

    pushMaybeArray(input.startUrls);
    pushMaybeArray(input.startUrl);
    pushMaybeArray(input.urls);
    pushMaybeArray(input.requests);
    pushMaybeArray(input.sources);

    return out;
};

// -------------------- Router --------------------

// Global state for tracking results
let totalJobsSaved = 0;
let maxResultsDesired = 1000; // Default, will be overridden by input

const router = createCheerioRouter();

router.addDefaultHandler(async ({ $, request, log, enqueueLinks }) => {
    const isDetail = /\/oferta-|\/job\/|\/empleo\/|\/vacante\//i.test(request.url);

    if (isDetail) {
        log.info(`Detail page: ${request.url}`);
        const job = extractJobDetail($, request.url);
        if (job && job.title) {
            await Dataset.pushData(job);
            totalJobsSaved++;
            log.info(`[${totalJobsSaved}/${maxResultsDesired}] Saved: ${job.title}`);
        }
        return;
    }

    log.info(`Listing page: ${request.url} (${totalJobsSaved}/${maxResultsDesired} saved)`);

    // Only enqueue more detail links if we haven't reached the limit
    if (totalJobsSaved < maxResultsDesired) {
        // Detail links (several patterns to catch template variants)
        await enqueueLinks({
            selector: [
                'a[href*="/oferta-"]',
                'a[href*="/ofertas-"]',
                'a[href*="/vacante-"]',
                'a[href*="/job/"]',
                'a.js-o-link',
                'a[href*="/empleo/"]',
                'a[href*="/trabajo-"]',
            ].join(','),
            label: 'DETAIL',
            transformRequestFunction: (req) => {
                try {
                    const u = new URL(req.url);
                    u.hash = '';
                    ['utm_source', 'utm_medium', 'utm_campaign', 'gclid', 'fbclid'].forEach((k) => u.searchParams.delete(k));
                    req.url = u.toString();
                } catch { /* noop */ }
                return req;
            },
        });

        // Pagination - but only if we still need more results
        if (totalJobsSaved < maxResultsDesired) {
            await enqueueLinks({
                selector: 'a[href*="page="], .pagination a, a.next, a[rel="next"]',
                forefront: false,
            });
        }
    }
});

router.addHandler('DETAIL', async ({ $, request, log }) => {
    // Skip if we've already saved enough
    if (totalJobsSaved >= maxResultsDesired) {
        log.info(`Limit reached (${totalJobsSaved}/${maxResultsDesired}), skipping ${request.url}`);
        return;
    }

    log.info(`Detail (labeled) page: ${request.url}`);
    const job = extractJobDetail($, request.url);
    if (job && job.title) {
        await Dataset.pushData(job);
        totalJobsSaved++;
        log.info(`[${totalJobsSaved}/${maxResultsDesired}] Saved: ${job.title}`);
    }
});

// -------------------- Main --------------------

await Actor.main(async () => {
    const input = await Actor.getInput() || {};
    const {
        maxRequestsPerCrawl = 1000,
        maxConcurrency = 10,
        proxy = { useApifyProxy: true }, // customize groups in input
        requestHandlerTimeoutSecs = 45,
        maxRequestRetries = 2, // crawler-level retries
        results_wanted = 50, // Number of job results desired
    } = input;

    // Set global results limit
    maxResultsDesired = Math.max(1, Math.floor(Number(results_wanted) || 50));
    totalJobsSaved = 0;
    log.info(`Target: ${maxResultsDesired} jobs`);

    // Normalize & validate start requests
    const startRequests = normalizeStartRequests(input);
    log.info(`Loaded ${startRequests.length} start URL(s).`);
    if (startRequests.length === 0) {
        throw new Error('No valid start URLs found in input. Provide startUrls (array of {url} or strings), or startUrl/urls/requests.');
    }

    // Proxy rotation
    const proxyConfiguration = await Actor.createProxyConfiguration(proxy);

    // Queue & seed requests
    const requestQueue = await RequestQueue.open();
    for (const r of startRequests) await requestQueue.addRequest(r);

    // Crawler
    const crawler = new CheerioCrawler({
        requestQueue,
        maxRequestsPerCrawl,
        maxConcurrency,
        requestHandler: router,
        proxyConfiguration,

        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: Math.max(8, maxConcurrency * 3),
            // Don't pass invalid keys here; maxRequestRetries is set at crawler level.
        },

        // Gentle header tweaks; Crawlee/gotScraping already does a lot
        preNavigationHooks: [
            async ({ request }) => {
                request.headers['accept-language'] = request.headers['accept-language'] || 'es-ES,es;q=0.9,en;q=0.8';
                request.headers['sec-fetch-site'] = 'same-origin';
                request.headers['sec-fetch-mode'] = 'navigate';
                request.headers['sec-fetch-dest'] = 'document';
            },
        ],

        failedRequestHandler: async ({ request, error, session }) => {
            log.warning(`Request failed: ${request.url} :: ${error?.message || error}`);
            if (session) session.retire();
        },

        requestHandlerTimeoutSecs,
        maxRequestRetries,
    });

    log.info('Starting crawler...');
    await crawler.run();
    log.info(`✓ Crawler finished. Saved ${totalJobsSaved}/${maxResultsDesired} jobs.`);
});
