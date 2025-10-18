// main.js
// Apify SDK + Crawlee + modern HTTP-based scraping (gotScraping)
// Fully compatible, with proxy rotation, sessions, and robust extraction for job details.

const { Actor } = require('apify');
const {
    CheerioCrawler,
    createCheerioRouter,
    Dataset,
    log,
    RequestQueue,
} = require('crawlee');
const cheerio = require('cheerio');

// -------------------- Helpers --------------------

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
    const $ = cheerio.load(html);
    // remove any remaining non-content tags just in case
    $('script, style, noscript, iframe, form, button, svg').remove();
    return normText($.root().text());
};

const stripAttrsKeepTags = (html, allowedTags = ['p', 'br', 'ul', 'ol', 'li', 'strong', 'em', 'b', 'i', 'a', 'h3', 'h4']) => {
    if (!html) return '';
    const $ = cheerio.load(html, { decodeEntities: true });

    // Remove trashy/non-content and hidden elements
    $('script, style, noscript, iframe, form, button, svg, input, textarea, select').remove();
    $('[data-complaint-overlay], #complaint-popup-container, .popup, [aria-hidden="true"]').remove();
    // Some templates put junk in .hide (beware not to remove the entire description if they use "hide" in class names)
    // We'll specifically target hidden nodes, not generic .hide text containers:
    $('[style*="display:none"], [hidden]').remove();

    // Unwrap unknown tags; keep a/href only
    $('*').each((_, el) => {
        const name = el.name || '';
        if (!allowedTags.includes(name)) {
            $(el).replaceWith($(el).contents());
            return;
        }
        // Drop attributes except href on <a>
        const attribs = el.attribs || {};
        for (const attr of Object.keys(attribs)) {
            if (name === 'a' && attr === 'href') continue;
            $(el).removeAttr(attr);
        }
    });

    // Normalize whitespace
    $('body').find('*').contents().each((_, node) => {
        if (node.type === 'text' && node.data) {
            node.data = normText(node.data);
        }
    });

    const out = $('body').html() || '';
    return out.replace(/\s+\n/g, '\n').trim();
};

// Parse Spanish relative dates like "Publicado hace 2 días", "hace una hora", or ISO-looking strings.
const parseSpanishRelativeDateToISO = (s) => {
    const str = (s || '').toLowerCase();
    if (!str) return null;
    const now = new Date();

    // Try direct ISO-ish dates first
    const iso = str.match(/\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2}(?::\d{2})?)?/);
    if (iso) {
        const d = new Date(iso[0]);
        if (!isNaN(d)) return d.toISOString();
    }

    // Patterns: "hace 3 días", "hace una hora", "Publicado hace 15 minutos"
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
    // 1) Common list/chips
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
                    if (spans.length >= 2) {
                        val = normText($(spans[1]).text());
                    } else {
                        val = text.replace(re, '').trim();
                    }
                }
                return false; // break inner loop
            }
        }
        if (val) return false; // break outer loop
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
    if (unitText) out.salary_period = unitText; // e.g., MONTH, HOUR, YEAR
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

// Try to focus on real description containers; then sanitize.
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
            if (html) {
                return { description_html: html, description_text: cleanHtmlToText(html) };
            }
        }
    }
    // Fallback to a bigger container, but sanitize hard
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
        description_raw: item.description || null, // sometimes HTML, sometimes text
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
        $('h1, .box_title h1, [class*="title"]').first().text()
    );
    const company = pickFirstNonEmpty(
        jsonLd.company,
        $('.box_header .fc_base a, .box_header a, [class*="company"], [itemprop="hiringOrganization"] a')
            .first().text()
    );

    // Location
    let location = pickFirstNonEmpty(
        jsonLd.location,
        extractLabeledValue($, [/ubicaci[oó]n/i, /ciudad/i, /estado/i, /localidad/i]),
        $('.box_header p, [class*="location"], nav.breadcrumb').first().text()
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

    // If JSON-LD has description, try to use it if it looks like HTML and has content
    if (jsonLd.description_raw && normText(jsonLd.description_raw).length > 20) {
        // Sometimes JSON-LD description includes tags; sanitize either way
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

    return job;
};

// -------------------- Router --------------------

const router = createCheerioRouter();

router.addDefaultHandler(async ({ $, request, log, enqueueLinks, crawler }) => {
    // If this looks like a job detail page, handle directly.
    const isDetail = /\/oferta-|\/job\/|\/empleo\/|\/vacante\//i.test(request.url);
    if (isDetail) {
        log.info(`Detail page: ${request.url}`);
        const job = extractJobDetail($, request.url);

        // If description_html was polluted by CSS/selectors in the past, ensure it's clean now:
        if (job.description_html && /{.*}/.test(job.description_html) && !/<(p|ul|li|a|strong|em|br|h3|h4)/i.test(job.description_html)) {
            // Looks like only CSS text; nuke it
            job.description_html = null;
            job.description_text = null;
        }

        await Dataset.pushData(job);
        return;
    }

    // Otherwise assume it's a listing/search page. Enqueue detail links and pagination.
    log.info(`Listing page: ${request.url}`);

    // Detail link patterns (broad to catch variants)
    await enqueueLinks({
        selector: [
            'a[href*="/oferta-"]',
            'a[href*="/ofertas-"]',
            'a[href*="/vacante-"]',
            'a[href*="/job/"]',
            'a.js-o-link',
            'a[href*="/empleo/"]',
        ].join(','),
        label: 'DETAIL',
        transformRequestFunction: (req) => {
            // normalize to absolute URLs (Crawlee does this, but ensure tracking params removed)
            try {
                const u = new URL(req.url);
                u.hash = '';
                // you can strip marketing params if needed:
                ['utm_source', 'utm_medium', 'utm_campaign', 'gclid', 'fbclid'].forEach((k) => u.searchParams.delete(k));
                req.url = u.toString();
            } catch { /* ignore */ }
            return req;
        },
    });

    // Pagination links
    await enqueueLinks({
        selector: 'a[href*="page="], .pagination a, a.next, a[rel="next"]',
        forefront: false,
    });
});

router.addHandler('DETAIL', async ({ $, request, log }) => {
    log.info(`Detail (labeled) page: ${request.url}`);
    const job = extractJobDetail($, request.url);

    // Guard against CSS-only capture
    if (job.description_html && /{.*}/.test(job.description_html) && !/<(p|ul|li|a|strong|em|br|h3|h4)/i.test(job.description_html)) {
        job.description_html = null;
        job.description_text = null;
    }

    await Dataset.pushData(job);
});

// -------------------- Main --------------------

Actor.main(async () => {
    const input = await Actor.getInput() || {};
    const {
        startUrls = [],
        maxRequestsPerCrawl = 1000,
        maxConcurrency = 10,
        proxy = {}, // e.g., { groups: ['SHADER'] } or { proxyUrls: ['http://user:pass@host:port'] }
        requestTimeoutSecs = 45,
        navigationTimeoutSecs = 30,
    } = input;

    // Proxy rotation
    const proxyConfiguration = await Actor.createProxyConfiguration(proxy);

    const requestQueue = await RequestQueue.open();
    for (const s of startUrls) {
        if (!s || !s.url) continue;
        await requestQueue.addRequest({ url: s.url });
    }

    // CheerioCrawler uses gotScraping under the hood (modern HTTP-based scraping).
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
            sessionOptions: {
                maxRequestRetries: 2,
            },
        },
        // Stealth-ish defaults with modern headers via gotScraping; customize here if needed.
        preNavigationHooks: [
            async ({ request, session, proxies, crawler }) => {
                // You can tweak headers further if the target gets picky.
                request.headers['accept-language'] = request.headers['accept-language'] || 'es-ES,es;q=0.9,en;q=0.8';
                request.headers['sec-fetch-site'] = 'same-origin';
                request.headers['sec-fetch-mode'] = 'navigate';
                request.headers['sec-fetch-dest'] = 'document';
                // If site blocks aggressively, you can randomize UA:
                // request.headers['user-agent'] = gotScrapingApis.getRandomUserAgent(); // (Crawlee already sets modern UA)
            },
        ],
        // Retire session on common anti-bot signals
        failedRequestHandler: async ({ request, error, session }) => {
            log.warning(`Request ${request.url} failed too many times. Error: ${error?.message || error}`);
            if (session) session.retire();
        },
        additionalMimeTypes: ['text/html', 'application/xhtml+xml'],
        requestHandlerTimeoutSecs: requestTimeoutSecs,
        navigationTimeoutSecs,
        // Automatic retries are handled by Crawlee; consider lowering or raising if needed via maxRequestRetries at run-level
    });

    log.info('Starting crawler...');
    await crawler.run();
    log.info('Crawler finished.');
});
