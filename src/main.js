// ======== Add these helpers (place near your other helpers) ========

// Safely parse *all* JSON-LD blocks and return the first JobPosting object found.
function parseAllJsonLd($) {
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
        } catch { /* ignore bad JSON */ }
    });

    return jobPostings[0] || null;
}

function normText(s) {
    return (s || '').replace(/\s+/g, ' ').replace(/\u00A0/g, ' ').trim();
}

function stripAttrsKeepTags(html, allowedTags = ['p','br','ul','ol','li','strong','em','b','i','a','h3','h4']) {
    if (!html) return '';
    const $ = cheerioLoad(html);

    // Remove trashy/non-content elements
    $('script, style, noscript, iframe, form, button, svg, input, textarea, select').remove();

    // Remove common overlays/popups/hidden sections
    $('[data-complaint-overlay], #complaint-popup-container, .hide, .popup, [aria-hidden="true"]').remove();

    // Remove inline styles & unwanted attributes
    $('*').each((_, el) => {
        if (!allowedTags.includes(el.name)) {
            // unwrap unknown tags but keep their text/children
            $(el).replaceWith($(el).contents());
            return;
        }
        // Keep href on <a>, drop everything else
        const attribs = el.attribs || {};
        Object.keys(attribs).forEach(name => {
            if (el.name === 'a' && name === 'href') return;
            $(el).removeAttr(name);
        });
    });

    // Collapse whitespace in text
    const textNodes = [];
    $('body').find('*').contents().each((_, node) => {
        if (node.type === 'text') node.data = normText(node.data);
    });

    return $('body').html().replace(/\s+\n/g, '\n').trim();
}

function pickFirstNonEmpty(...vals) {
    for (const v of vals) {
        const t = normText(typeof v === 'string' ? v : (v == null ? '' : String(v)));
        if (t) return t;
    }
    return null;
}

// Parse Spanish relative dates like "Publicado hace 2 días", "hace una hora"
function parseSpanishRelativeDateToISO(s) {
    const str = (s || '').toLowerCase();
    if (!str) return null;
    const now = new Date();

    // Try direct ISO-ish dates first
    const iso = str.match(/\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2}(?::\d{2})?)?/);
    if (iso) return new Date(iso[0]).toISOString();

    // Patterns: "hace 3 días", "hace una hora", "hace 15 minutos"
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
}

// Get label/value from definition/chip lists used on Computrabajo
function extractLabeledValue($, labelRegexes) {
    // Common places: attribute lists, dl/dt/dd pairs, info chips
    // 1) Attribute list items
    let val = null;

    // <li><span class="...">Salario</span><span> $15,000 - $20,000 </span></li>
    $('li, .box_attributes li, .attribute, .chip, .tag').each((_, li) => {
        const $li = $(li);
        const text = normText($li.text());
        if (!text) return;
        for (const re of labelRegexes) {
            if (re.test(text)) {
                // value is often in the trailing text after label
                const parts = text.split(':');
                if (parts.length > 1) val = normText(parts.slice(1).join(':'));
                else {
                    // try immediate span after label span
                    const spans = $li.find('span');
                    if (spans.length >= 2) {
                        val = normText($(spans[1]).text());
                    } else {
                        val = text.replace(re, '').trim();
                    }
                }
                if (val) return false; // break
            }
        }
        if (val) return false;
    });
    if (val) return val;

    // 2) dt/dd structures
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
}

function normalizeSalaryFromJsonLd(baseSalary) {
    if (!baseSalary) return null;
    // Can be MonetaryAmount or value object/number
    const monetary = baseSalary['@type'] === 'MonetaryAmount' ? baseSalary : null;
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
    if (unitText) out.salary_period = unitText; // e.g., "MONTH", "HOUR", "YEAR"
    if (min != null) out.salary_min = min;
    if (max != null) out.salary_max = max;
    if (amount != null && min == null && max == null) out.salary_amount = amount;

    return Object.keys(out).length ? out : null;
}

// ======== Replace your extractJsonLd with this ========
function extractJsonLd($) {
    const item = parseAllJsonLd($);
    if (!item) return null;

    // Flatten location(s)
    let loc = null;
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
        loc = parts.filter(Boolean).join(' | ') || null;
    }

    // Salary normalization
    const salaryObj = normalizeSalaryFromJsonLd(item.baseSalary);

    const employmentType = item.employmentType
        ? (Array.isArray(item.employmentType) ? item.employmentType : [item.employmentType]).map(normText).filter(Boolean)
        : null;

    return {
        title: item.title || item.name || null,
        company: item.hiringOrganization?.name || null,
        datePosted: item.datePosted || null,
        description: item.description || null,
        location: loc,
        salary: salaryObj,            // structured (min/max/currency/period/amount)
        employmentType,               // array of strings
    };
}

// ======== Replace your extractJobDetail with this ========
function extractJobDetail($, jobUrl) {
    const jsonLd = extractJsonLd($) || {};

    // --- Title & company ---
    const title = pickFirstNonEmpty(
        jsonLd.title,
        $('h1, .box_title h1, [class*="title"]').first().text()
    );

    const company = pickFirstNonEmpty(
        jsonLd.company,
        $('.box_header .fc_base a, .box_header a, [class*="company"], [itemprop="hiringOrganization"] a')
            .first().text()
    );

    // --- Location ---
    let location = pickFirstNonEmpty(
        jsonLd.location,
        extractLabeledValue($, [/ubicaci[oó]n/i, /ciudad/i, /estado/i, /localidad/i]),
        // fallbacks from header/breadcrumbs
        $('.box_header p, [class*="location"], nav.breadcrumb').first().text()
    );
    if (location) {
        // Clean “Publicado …” or other chips that get concatenated
        location = location.replace(/Publicado.*$/i, '').replace(/\s+\|\s+$/, '').trim();
    }

    // --- Salary ---
    let salary_text = null;
    let salary_struct = jsonLd.salary || null;
    if (!salary_struct) {
        const scraped = extractLabeledValue($, [/salario/i, /sueldo/i, /compensaci[oó]n/i]);
        if (scraped) salary_text = scraped;
    }

    // --- Employment Type ---
    let employmentType = jsonLd.employmentType || null;
    if (!employmentType) {
        const tipoContrato = extractLabeledValue($, [/tipo de contrato/i, /contrato/i]);
        const jornada = extractLabeledValue($, [/jornada/i, /horario/i, /modalidad/i]);
        const types = [tipoContrato, jornada].map(normText).filter(Boolean);
        employmentType = types.length ? Array.from(new Set(types)) : null;
    }

    // --- Date Posted ---
    let datePosted = jsonLd.datePosted || null;
    if (!datePosted) {
        const rel = extractLabeledValue($, [/publicado/i, /fecha de publicaci[oó]n/i]);
        const iso = parseSpanishRelativeDateToISO(rel);
        datePosted = iso || null;
    }

    // --- Description (HTML + text) ---
    // Prefer a specific content container to avoid modals/overlays/tooltips
    const descriptionCandidates = [
        '.box_detail [itemprop="description"]',
        '.box_detail .box_section, .box_detail article',
        '[class*="job-description"]',
        '.job_desc',
        '#offer-body'
    ];
    let descriptionEl = null;
    for (const sel of descriptionCandidates) {
        const el = $(sel).first();
        if (el && el.length && normText(el.text()).length > 20) { descriptionEl = el; break; }
    }
    if (!descriptionEl) {
        // fallback to the whole .box_detail, but we’ll sanitize it hard
        descriptionEl = $('.box_detail').first();
    }
    let description_html = descriptionEl && descriptionEl.length ? stripAttrsKeepTags(descriptionEl.html()) : null;
    let description_text = description_html ? cleanHtmlToText(description_html) : null;

    // Build the final record
    const job = {
        title: title || null,
        company: company || null,
        location: location || null,
        url: jobUrl,
        source: 'computrabajo.com',

        // Description
        description_html: description_html || null,
        description_text: description_text || null,

        // Date posted
        datePosted: datePosted || null,

        // Employment type (array if multiple)
        employmentType: employmentType || null,
    };

    // Salary fields
    if (salary_struct) {
        Object.assign(job, salary_struct);
    } else if (salary_text) {
        job.salary_text = salary_text;
    }

    return job;
}
