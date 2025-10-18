// src/main.js (ESM)
// Apify SDK + Crawlee (CheerioCrawler) + gotScraping (HTTP-based), ESM compatible.

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

// Description extraction with sanitization
const extractDescription = ($) => {
    const candidates = [
        '.box_detail [itemprop="description"]',
        '.box_detail .box_section',
        '.box_detail article',
        '[class*="job-description"]',
        '.job_desc',
        '#offer-body',
        '.description, #description',
    ];
    for (const sel of candidates) {
        const el = $(sel).first();
        if (el && el.length && normText(el.text()).length > 20) {
            const html = stripAttrsKeepTags(el.html());
            if (html) return { description_html: html, description_text: cleanHtmlToText(html) };
        }
    }
    const fallback = $('.box_detail').first();
    if (fallback && fallback.length) {
        const html = stripAttrsKeepTags(fallback.html());
        if (html) return { description_html: html, description_text: cleanHtmlToText(html) };
    }
    return { description_html: null, description_text: null };
};

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

    // Title & company
    const title = pickFirstNonEmpty(
        jsonLd.title,
        $('h1, .box_title h1, [class*="title"]').first().text(),
    );
    const company = pickFirstNonEmpty(
        jsonLd.company,
        $('.box_header .fc_base a, .box_header a, [class*="company"], [itemprop="hiringOrganization"] a')
            .first().text(),
    );

    // Location
    let location = pickFirstNonEmpty(
        jsonLd.location,
        extractLabeledValue($, [/ubicaci[oó]n/i, /ciudad/i, /estado/i, /localidad/i]),
        $('.box_header p, [class*="location"], nav.breadcrumb').first().text(),
    );
    if (location) {
        location = location.replace(/Publicado.*$/i, '').replace(/\s+\|\s+$/, '').trim();
    }

    // Salary
    let salary_struct = jsonLd.salary_struct || null;
    let salary_text = null;
    if (!salary_struct) {
        const scraped = extractLabeledValue($, [/salario/i, /sueldo/i, /compensaci[oó]n/i]);
        if (scraped) salary_text = scraped;
    }

    // Employment type
    let employmentType = jsonLd.employmentType || null;
    if (!employmentType) {
        const tipoContrato = extractLabeledValue($, [/tipo de contrato/i, /contrato/i]);
        const jornada = extractLabeledValue($, [/jornada/i, /horario/i, /modalidad/i]);
        const types = [tipoContrato, jornada].map(normText).filter(Boolean);
        if (types.length) employmentType = Array.from(new Set(types));
    }

    // Date posted
    let datePosted = jsonLd.datePosted || null;
    if (!datePosted) {
        const rel = extractLabeledValue($, [/publicado/i, /fecha de publicaci[oó]n/i]);
        const iso = parseSpanishRelativeDateToISO(rel);
        datePosted = iso || null;
    }

    // Description
    let description_html = null;
    let description_text = null;

    if (jsonLd.description_raw && normText(jsonLd.description_raw).length > 20) {
        const sanitized = stripAttrsKeepTags(jsonLd.description_raw);
        if (sanitized && normText(cleanHtmlToText(sanitized)).length > 20) {
            description_html = sanitized;
            description_text = cleanHtmlToText(sanitized);
        }
    }
    if (!description_html) {
        const desc = extractDescription($);
        description_html = desc.description_html;
        description_text = desc.description_text;
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

    if (salary_struct) {
        Object.assign(job, salary_struct);
    } else if (salary_text) {
        job.salary_text = salary_text;
    }

    // Guard: CSS-only capture
    if (job.description_html && /{.*}/.test(job.description_html) && !/<(p|ul|li|a|strong|em|br|h3|h4)/i.test(job.description_html)) {
        job.description_html = null;
        job.description_text = null;
    }

    return job;
};

// -------------------- Start URL normalization --------------------

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

const router = createCheerioRouter();

router.addDefaultHandler(async ({ $, request, log, enqueueLinks }) => {
    const isDetail = /\/oferta-|\/job\/|\/empleo\/|\/vacante\//i.test(request.url);

    if (isDetail) {
        log.info(`Detail page: ${request.url}`);
        const job = extractJobDetail($, request.url);
        await Dataset.pushData(job);
        return;
    }

    log.info(`Listing page: ${request.url}`);

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

    await enqueueLinks({
        selector: 'a[href*="page="], .pagination a, a.next, a[rel="next"]',
        forefront: false,
    });
});

router.addHandler('DETAIL', async ({ $, request, log }) => {
    log.info(`Detail (labeled) page: ${request.url}`);
    const job = extractJobDetail($, request.url);
    await Dataset.pushData(job);
});

// -------------------- Main --------------------

await Actor.main(async () => {
    const input = await Actor.getInput() || {};
    const {
        maxRequestsPerCrawl = 1000,
        maxConcurrency = 10,
        proxy = { useApifyProxy: true }, // customize groups in input
        requestHandlerTimeoutSecs = 45,
        maxRequestRetries = 2, // crawler-level retries (OK)
    } = input;

    // Normalize & validate start requests
    const startRequests = normalizeStartRequests(input);
    log.info(`Loaded ${startRequests.length} start URL(s).`);
    if (startRequests.length === 0) {
        throw new Error('No valid start URLs found in input. Provide startUrls (array of {url} or strings), or startUrl/urls/requests.');
    }

    // Proxy rotation
    const proxyConfiguration = await Actor.createProxyConfiguration(proxy);

    const requestQueue = await RequestQueue.open();
    for (const r of startRequests) await requestQueue.addRequest(r);

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
            // IMPORTANT: do not pass invalid keys here (e.g., maxRequestRetries) — it belongs at crawler level.
        },

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
        maxRequestRetries, // valid here
    });

    log.info('Starting crawler...');
    await crawler.run();
    log.info('Crawler finished.');
});
