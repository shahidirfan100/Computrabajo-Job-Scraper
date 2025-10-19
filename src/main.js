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
    // Try JSON-LD first for reliable structured data
    const jsonLd = extractFromJsonLd($);

    // ========== TITLE ==========
    let title = null;
    if (jsonLd.title) {
        title = normText(jsonLd.title);
    }
    if (!title) {
        // Multiple strategies for title - use contains class to handle dynamic classes
        const titleSelectors = [
            '[class*="title_offer"]',
            '.title_offer',
            'h1[class*="title"]',
            'h1.fs21',
            '.box_title h1',
            'h1',
        ];
        for (const sel of titleSelectors) {
            const el = $(sel).first();
            if (el.length) {
                const txt = normText(el.text());
                if (txt && txt.length > 3) {
                    title = txt;
                    break;
                }
            }
        }
    }

    // ========== COMPANY ==========
    let company = null;
    if (jsonLd.company) {
        company = cleanCompanyName(jsonLd.company);
    }
    if (!company) {
        // Multiple strategies - look for company in header area
        const companySelectors = [
            '.box_header a[href*="/empresas/"]',
            '.box_header a[href*="/empresa"]',
            'a.dIB.mr10',
            'a[class*="mr10"]',
            '.box_header .fc_base a',
            '[itemprop="hiringOrganization"] [itemprop="name"]',
        ];
        for (const sel of companySelectors) {
            const el = $(sel).first();
            if (el.length) {
                const txt = cleanCompanyName(el.text());
                if (txt && txt.length > 1) {
                    company = txt;
                    break;
                }
            }
        }
    }
    if (!company) {
        company = pickCompanyFromDom($);
    }

    // ========== LOCATION ==========
    let location = null;
    if (jsonLd.location) {
        location = normText(jsonLd.location);
    }
    if (!location) {
        // Multiple strategies for location
        const locationSelectors = [
            '.fs16.mb5',
            '[class*="mb5"]',
            '.box_header p.fs16',
            '.box_header p',
            '[itemprop="jobLocation"] [itemprop="addressLocality"]',
            'p:contains("Ubicación")',
        ];
        for (const sel of locationSelectors) {
            const el = $(sel).first();
            if (el.length) {
                let txt = normText(el.text());
                // Clean up
                txt = txt
                    .replace(/Ubicación:?/gi, '')
                    .replace(/\s*-\s*Publicado.*$/i, '')
                    .replace(/Ver mapa/gi, '')
                    .replace(/Postular.*/i, '')
                    .trim();
                if (txt && txt.length > 2) {
                    location = txt;
                    break;
                }
            }
        }
    }
    if (!location) {
        location = pickLocation($, null);
    }

    // ========== DATE POSTED ==========
    let datePosted = null;
    if (jsonLd.datePosted) {
        datePosted = jsonLd.datePosted;
    }
    if (!datePosted) {
        // Multiple strategies for date
        const dateSelectors = [
            '.fc_aux.fs13.mtB',
            '[class*="fc_aux"]',
            '.box_header .fc_aux',
            'p.fs13',
            'p:contains("Publicado")',
            'p:contains("hace")',
        ];
        for (const sel of dateSelectors) {
            const el = $(sel).first();
            if (el.length) {
                const txt = normText(el.text());
                if (txt && (txt.includes('hace') || txt.includes('Publicado'))) {
                    const iso = parseSpanishRelativeDateToISO(txt);
                    datePosted = iso || txt;
                    break;
                }
            }
        }
    }
    if (!datePosted) {
        const rel = extractLabeledValue($, [/publicado/i, /publicada/i, /fecha/i]);
        if (rel) {
            const iso = parseSpanishRelativeDateToISO(rel);
            datePosted = iso || rel;
        }
    }

    // ========== EMPLOYMENT TYPE ==========
    let employmentType = null;
    if (jsonLd.employmentType) {
        employmentType = Array.isArray(jsonLd.employmentType) 
            ? jsonLd.employmentType.filter(Boolean).join(', ')
            : jsonLd.employmentType;
    }
    if (!employmentType) {
        // Multiple strategies
        const typeSelectors = [
            '.dFlex.mb10',
            '[class*="dFlex"]',
            'p:contains("Jornada")',
            'p:contains("Tiempo completo")',
            'p:contains("Tiempo parcial")',
        ];
        for (const sel of typeSelectors) {
            const el = $(sel).first();
            if (el.length) {
                let txt = normText(el.text());
                txt = txt.replace(/^(Jornada|Tipo de contrato|Modalidad):\s*/i, '').trim();
                if (txt && txt.length > 3) {
                    employmentType = txt;
                    break;
                }
            }
        }
    }
    if (!employmentType) {
        const jornada = extractLabeledValue($, [/jornada/i, /tipo de contrato/i, /modalidad/i]);
        if (jornada) employmentType = jornada;
    }

    // ========== SALARY ==========
    let salary_struct = jsonLd.salary_struct || null;
    let salary_text = null;
    if (!salary_struct) {
        const scraped = extractLabeledValue($, [/salario/i, /sueldo/i, /compensaci[oó]n/i, /remuneraci[oó]n/i]);
        if (scraped) {
            salary_text = normText(scraped);
        }
    }

    // ========== DESCRIPTION ==========
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
        // Multiple strategies for description
        const descSelectors = [
            '[class*="t_word_wrap"]',
            '.fs16.t_word_wrap',
            '[itemprop="description"]',
            '.box_detail',
            '.box_section',
            'article.oferta',
            '#offer-body',
        ];
        
        for (const sel of descSelectors) {
            const descEl = $(sel).first();
            if (descEl.length) {
                let html = descEl.html();
                if (html && normText(cheerioLoad(html).text()).length > 40) {
                    // Remove hidden/unwanted elements
                    const $temp = cheerioLoad(html);
                    $temp('.hide, [data-offers-grid-detail-container-error], [data-complaint-overlay], .popup, #complaint-popup-container, [aria-hidden="true"]').remove();
                    $temp('script, style, noscript, iframe, form, button').remove();
                    
                    const cleaned = stripAttrsKeepTags($temp.root().html());
                    const txt = cleanHtmlToText(cleaned);
                    
                    if (cleaned && txt && normText(txt).length > 40) {
                        description_html = cleaned;
                        description_text = txt;
                        break;
                    }
                }
            }
        }
    }
    
    if (!description_html) {
        // Final fallback to pickDescriptionHtml
        const html = pickDescriptionHtml($);
        if (html) {
            description_html = html;
            description_text = cleanHtmlToText(html);
        }
    }

    // Guard: CSS-only capture (malformed HTML)
    if (description_html && /{.*}/.test(description_html) && !/<(p|ul|li|a|strong|em|br|h3|h4)/i.test(description_html)) {
        description_html = null;
        description_text = null;
    }

    // ========== BUILD JOB OBJECT ==========
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

    // Add salary fields
    if (salary_struct) {
        Object.assign(job, salary_struct);
    } else if (salary_text) {
        job.salary_text = salary_text;
    }

    // Debug logging to help troubleshoot
    log.debug(`Extraction results for ${url}:`);
    log.debug(`  - Title: ${title ? '✓' : '✗'} ${title ? `(${title.substring(0, 50)}...)` : ''}`);
    log.debug(`  - Company: ${company ? '✓' : '✗'} ${company || ''}`);
    log.debug(`  - Location: ${location ? '✓' : '✗'} ${location || ''}`);
    log.debug(`  - Date: ${datePosted ? '✓' : '✗'} ${datePosted || ''}`);
    log.debug(`  - Employment Type: ${employmentType ? '✓' : '✗'} ${employmentType || ''}`);
    log.debug(`  - Salary: ${(salary_text || salary_struct) ? '✓' : '✗'}`);
    log.debug(`  - Description: ${description_text ? '✓' : '✗'} ${description_text ? `(${description_text.substring(0, 50)}...)` : ''}`);

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
    // Check if we're being blocked or redirected
    const pageTitle = $('title').text().toLowerCase();
    const bodyText = $('body').text().toLowerCase();
    
    // Detect blocking/redirect pages
    const isBlockedOrRedirect = 
        pageTitle.includes('sign in') ||
        pageTitle.includes('iniciar sesión') ||
        bodyText.includes('continue with google') ||
        bodyText.includes('iniciar sesión') ||
        bodyText.includes('crear cuenta') ||
        $('input[type="password"]').length > 0 ||
        $('form[action*="login"]').length > 0;
    
    if (isBlockedOrRedirect) {
        log.warning(`⚠️ Blocked/redirected page detected: ${request.url}`);
        log.warning(`Page title: ${pageTitle}`);
        throw new Error('Blocked or redirected to login page - rotating session');
    }

    const isDetail = /\/oferta-|\/job\/|\/empleo\/|\/vacante\//i.test(request.url);

    if (isDetail) {
        // Skip if limit reached
        if (totalJobsSaved >= maxResultsDesired) {
            log.info(`✓ Limit reached (${totalJobsSaved}/${maxResultsDesired}), skipping ${request.url}`);
            return;
        }

        log.info(`[DETAIL] Processing: ${request.url}`);
        
        // Validate this is actually a job detail page
        const hasJobContent = 
            $('h1').length > 0 ||
            $('[class*="title"]').length > 0 ||
            $('script[type="application/ld+json"]').length > 0;
        
        if (!hasJobContent) {
            log.warning(`⚠️ No job content found on ${request.url} - might be blocked`);
            throw new Error('No job content detected - rotating session');
        }
        
        const job = extractJobDetail($, request.url);
        
        // Validate extracted data makes sense (not blocked/redirect page text)
        const hasValidData = job.title && 
            job.title.length < 200 &&
            !job.title.toLowerCase().includes('sign in') &&
            !job.title.toLowerCase().includes('job openings in') &&
            !job.title.toLowerCase().includes('crear cuenta') &&
            !job.title.toLowerCase().includes('iniciar sesión');
        
        if (!hasValidData) {
            log.warning(`⚠️ Invalid data extracted from ${request.url} - likely blocked`);
            log.warning(`Got title: ${job.title}`);
            throw new Error('Invalid data extracted - rotating session');
        }
        
        // Validate job data quality before saving
        if (job && job.title) {
            await Dataset.pushData(job);
            totalJobsSaved++;
            log.info(`✓ [${totalJobsSaved}/${maxResultsDesired}] Saved: "${job.title}" @ ${job.company || 'N/A'}`);
            
            // Log data quality info
            const fields = [];
            if (job.company) fields.push('company');
            if (job.location) fields.push('location');
            if (job.datePosted) fields.push('date');
            if (job.employmentType) fields.push('type');
            if (job.salary_text || job.salary_amount) fields.push('salary');
            if (job.description_text) fields.push('desc');
            log.info(`  Fields: [${fields.join(', ')}]`);
        } else {
            log.warning(`✗ No valid title found for ${request.url}, skipping`);
        }
        return;
    }

    log.info(`[LIST] Processing: ${request.url} (${totalJobsSaved}/${maxResultsDesired} saved)`);

    // Validate listing page
    if (isBlockedOrRedirect) {
        log.warning(`⚠️ Blocked on listing page: ${request.url}`);
        throw new Error('Blocked or redirected - rotating session');
    }

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
                selector: 'a[href*="page="], .pagination a, a.next, a[rel="next"], a.js-o-pager.next',
                forefront: false,
            });
        }
    } else {
        log.info(`✓ Target reached (${totalJobsSaved}/${maxResultsDesired}), stopping pagination`);
    }
});

router.addHandler('DETAIL', async ({ $, request, log }) => {
    // Skip if we've already saved enough
    if (totalJobsSaved >= maxResultsDesired) {
        log.info(`✓ Limit reached (${totalJobsSaved}/${maxResultsDesired}), skipping ${request.url}`);
        return;
    }

    // Check if we're being blocked or redirected
    const pageTitle = $('title').text().toLowerCase();
    const bodyText = $('body').text().toLowerCase();
    
    const isBlockedOrRedirect = 
        pageTitle.includes('sign in') ||
        pageTitle.includes('iniciar sesión') ||
        bodyText.includes('continue with google') ||
        bodyText.includes('iniciar sesión') ||
        bodyText.includes('crear cuenta') ||
        $('input[type="password"]').length > 0;
    
    if (isBlockedOrRedirect) {
        log.warning(`⚠️ Blocked/redirected (DETAIL labeled): ${request.url}`);
        throw new Error('Blocked or redirected to login page - rotating session');
    }

    log.info(`[DETAIL-LABELED] Processing: ${request.url}`);
    
    // Validate this is actually a job detail page
    const hasJobContent = 
        $('h1').length > 0 ||
        $('[class*="title"]').length > 0 ||
        $('script[type="application/ld+json"]').length > 0;
    
    if (!hasJobContent) {
        log.warning(`⚠️ No job content found on ${request.url}`);
        throw new Error('No job content detected - rotating session');
    }
    
    const job = extractJobDetail($, request.url);
    
    // Validate extracted data
    const hasValidData = job.title && 
        job.title.length < 200 &&
        !job.title.toLowerCase().includes('sign in') &&
        !job.title.toLowerCase().includes('job openings in') &&
        !job.title.toLowerCase().includes('crear cuenta');
    
    if (!hasValidData) {
        log.warning(`⚠️ Invalid data (DETAIL labeled): ${request.url}`);
        log.warning(`Got title: ${job.title}`);
        throw new Error('Invalid data extracted - rotating session');
    }
    
    // Validate job data quality before saving
    if (job && job.title) {
        await Dataset.pushData(job);
        totalJobsSaved++;
        log.info(`✓ [${totalJobsSaved}/${maxResultsDesired}] Saved: "${job.title}" @ ${job.company || 'N/A'}`);
        
        // Log data quality info
        const fields = [];
        if (job.company) fields.push('company');
        if (job.location) fields.push('location');
        if (job.datePosted) fields.push('date');
        if (job.employmentType) fields.push('type');
        if (job.salary_text || job.salary_amount) fields.push('salary');
        if (job.description_text) fields.push('desc');
        log.info(`  Fields: [${fields.join(', ')}]`);
    } else {
        log.warning(`✗ No valid title found for ${request.url}, skipping`);
    }
});

// -------------------- Main --------------------

await Actor.main(async () => {
    const input = await Actor.getInput() || {};
    const {
        maxRequestsPerCrawl = 1000,
        maxConcurrency = 3, // Lower concurrency to avoid blocking
        proxy = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] }, // Use residential proxies
        requestHandlerTimeoutSecs = 60, // Increased timeout
        maxRequestRetries = 3, // More retries for blocks
        results_wanted = 50,
    } = input;

    // Set global results limit
    maxResultsDesired = Math.max(1, Math.floor(Number(results_wanted) || 50));
    totalJobsSaved = 0;
    log.info(`🎯 Target: ${maxResultsDesired} jobs`);

    // Normalize & validate start requests
    const startRequests = normalizeStartRequests(input);
    log.info(`📋 Loaded ${startRequests.length} start URL(s).`);
    if (startRequests.length === 0) {
        throw new Error('No valid start URLs found in input. Provide startUrls (array of {url} or strings), or startUrl/urls/requests.');
    }

    // Proxy rotation
    const proxyConfiguration = await Actor.createProxyConfiguration(proxy);

    // Queue & seed requests
    const requestQueue = await RequestQueue.open();
    for (const r of startRequests) await requestQueue.addRequest(r);

    // Realistic user agents (recent browsers)
    const USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    ];

    const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

    // Crawler with enhanced anti-bot measures
    const crawler = new CheerioCrawler({
        requestQueue,
        maxRequestsPerCrawl,
        maxConcurrency,
        requestHandler: router,
        proxyConfiguration,

        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: 50,
            sessionOptions: {
                maxUsageCount: 10, // Rotate session after 10 requests
                maxErrorScore: 3, // Retire session after 3 errors
            },
        },

        // Enhanced request preparation with anti-bot headers
        preNavigationHooks: [
            async ({ request, session }) => {
                // Rotate user agent per session
                if (!session.userData.userAgent) {
                    session.userData.userAgent = getRandomUserAgent();
                }
                
                // Set realistic browser headers
                request.headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language': 'es-MX,es;q=0.9,en;q=0.8',
                    'accept-encoding': 'gzip, deflate, br',
                    'user-agent': session.userData.userAgent,
                    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'cache-control': 'max-age=0',
                };
                
                // Add referer for detail pages
                if (request.userData?.label === 'DETAIL') {
                    request.headers['referer'] = 'https://mx.computrabajo.com/';
                    request.headers['sec-fetch-site'] = 'same-origin';
                }
                
                // Random delay between requests (500-2000ms)
                const delay = 500 + Math.random() * 1500;
                await new Promise(resolve => setTimeout(resolve, delay));
            },
        ],

        failedRequestHandler: async ({ request, error, session, log }) => {
            log.warning(`❌ Request failed: ${request.url}`);
            log.warning(`Error: ${error?.message || error}`);
            
            // Retire session if blocked
            if (session && (
                error.message?.includes('Blocked') ||
                error.message?.includes('Invalid data') ||
                error.message?.includes('rotating session')
            )) {
                log.warning(`🔄 Retiring session due to blocking`);
                session.retire();
            }
        },

        requestHandlerTimeoutSecs,
        maxRequestRetries,
    });

    log.info('🚀 Starting crawler...');
    const startTime = Date.now();
    
    await crawler.run();
    
    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    log.info(`✅ Crawler finished. Saved ${totalJobsSaved}/${maxResultsDesired} jobs in ${elapsed} minutes.`);
});

