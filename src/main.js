/**
 * Computrabajo Mexico Job Scraper - Production Actor for Apify
 * Scrapes job listings from mx.computrabajo.com with stealth & pagination
 */
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            startUrl = '',
            keyword = 'asesor-de-ventas',
            location = '',
            posted_date = 'anytime',
            collectDetails = true,
            results_wanted = 50,
            max_pages = 10,
            proxyConfiguration,
            cookies = '',
            cookiesJson = '',
        } = input;

        // Internal defaults (not exposed in schema)
        const stealthMode = true;
        const minRequestDelay = 500;
        const maxRequestDelay = 1500;
        const MAX_CONCURRENCY = 3;

        const MAX_RESULTS = Math.max(1, Math.floor(Number(results_wanted) || 50));
        const MAX_PAGES = Math.max(1, Math.floor(Number(max_pages) || 10));


        // Parse custom headers safely
        let parsedCustomHeaders = {};
        
        // ============================================
        // STEALTH USER AGENTS & HEADERS
        // ============================================
        const USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        ];

        const ACCEPT_LANGUAGES = [
            'es-MX,es;q=0.9,en;q=0.8',
            'es-MX,es;q=0.9',
            'es;q=0.9,es-MX;q=0.8,en;q=0.7',
        ];

        const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
        const getRandomAcceptLang = () => ACCEPT_LANGUAGES[Math.floor(Math.random() * ACCEPT_LANGUAGES.length)];
        const getStealthDelay = () => {
            if (!stealthMode) return 0;
            const minD = Math.max(0, Number(minRequestDelay) || 0);
            const maxD = Math.max(minD, Number(maxRequestDelay) || minD);
            return Math.floor(minD + Math.random() * (maxD - minD + 1));
        };


        // URL utilities
        const toAbsoluteUrl = (href, base = 'https://mx.computrabajo.com') => {
            if (!href || typeof href !== 'string') return null;
            try { return new URL(href, base).href; } catch { return null; }
        };

        const cleanHtmlToText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript, [class*="animating"], [class*="hide"], [id*="complaint"], [data-complaint-overlay]').remove();
            let text = $.root().text().replace(/\s+/g, ' ').trim();
            // Remove common error/popup patterns
            text = text.replace(/Error al realizar.*?minutos\./gi, '');
            text = text.replace(/Por qué quieres reportar.*?privacidad/gi, '');
            text = text.replace(/Gracias por tu denuncia.*?empresa\./gi, '');
            text = text.replace(/¿Por qué quieres reportar.*?elecci[óo]n/gi, '');
            return text.replace(/\s+/g, ' ').trim();
        };        const buildSearchUrl = (searchKeyword) => {
            if (!searchKeyword || typeof searchKeyword !== 'string') {
                return 'https://mx.computrabajo.com/trabajo-de-asesor-de-ventas';
            }
            const normalized = searchKeyword.trim().toLowerCase()
                .replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').replace(/-+/g, '-');
            return `https://mx.computrabajo.com/trabajo-de-${normalized}`;
        };

        // Initialize URLs (priority: startUrl > built keyword URL)
        const initialUrls = [];
        if (typeof startUrl === 'string' && startUrl.trim().length > 0) {
            initialUrls.push(startUrl.trim());
        } else {
            // For now build URL from keyword; location and posted_date are informational inputs
            initialUrls.push(buildSearchUrl(keyword));
        }

        log.info(`Starting with ${initialUrls.length} URL(s): maxResults=${MAX_RESULTS}, maxPages=${MAX_PAGES}`);

        // Parse cookies if provided
        let parsedCookies = null;
        if (cookiesJson && typeof cookiesJson === 'string') {
            try {
                parsedCookies = JSON.parse(cookiesJson);
            } catch (e) {
                log.warning('cookiesJson is not valid JSON, ignoring');
                parsedCookies = null;
            }
        }

        const proxyConfig = proxyConfiguration 
            ? await Actor.createProxyConfiguration({ ...proxyConfiguration })
            : await Actor.createProxyConfiguration({ useApifyProxy: true, apifyProxyGroups: ['AUTO'] });

        // Tracking state
        let totalSaved = 0;
        const urlsVisited = new Set();
        const jobUrlsEnqueued = new Set();

        // Extract data from JSON-LD schema
        function extractJsonLd($) {
            const jsonLdScript = $('script[type="application/ld+json"]').html();
            if (!jsonLdScript) return null;
            try {
                const data = JSON.parse(jsonLdScript);
                const items = Array.isArray(data) ? data : [data];
                for (const item of items) {
                    const itemType = item['@type'] || item.type;
                    if (itemType === 'JobPosting' || (Array.isArray(itemType) && itemType.includes('JobPosting'))) {
                        // Extract salary range (baseSalary can be object with currency, value, unitText)
                        let salary = null;
                        if (item.baseSalary) {
                            if (typeof item.baseSalary === 'string') {
                                salary = item.baseSalary;
                            } else if (item.baseSalary.currency && item.baseSalary.value) {
                                salary = `${item.baseSalary.currency} ${item.baseSalary.value}`;
                            } else if (item.baseSalary.value) {
                                salary = String(item.baseSalary.value);
                            }
                        }
                        
                        return {
                            title: item.title || item.name || null,
                            company: item.hiringOrganization?.name || null,
                            datePosted: item.datePosted || null,
                            description: item.description || null,
                            location: item.jobLocation?.address?.addressLocality || item.jobLocation?.address?.addressRegion || null,
                            salary: salary,
                            employmentType: item.employmentType || null,
                        };
                    }
                }
            } catch (e) { /* silent */ }
            return null;
        }

        // Find job listing links on page
        function findJobLinks($, pageUrl) {
            const links = new Set();
            // Primary selector for Computrabajo job cards
            $('.js-o-link.fc_base, .js-o-link, [data-js-job-link], a[href*="/trabajo-de-"]').each((_, el) => {
                const href = $(el).attr('href');
                if (!href) return;
                if (/\/trabajo-de-/i.test(href)) {
                    const absUrl = toAbsoluteUrl(href, pageUrl);
                    if (absUrl && !links.has(absUrl)) links.add(absUrl);
                }
            });
            return [...links];
        }

        // Find next page link
        function findNextPage($, pageUrl) {
            // Try multiple pagination selectors
            const selectors = ['a.js-o-pager.next', 'a[rel="next"]', 'a[aria-label*="siguiente"]', 'a.pagination__next'];
            for (const sel of selectors) {
                const href = $(sel).first().attr('href');
                if (href) {
                    const nextUrl = toAbsoluteUrl(href, pageUrl);
                    if (nextUrl) return nextUrl;
                }
            }
            return null;
        }

        // Extract job detail data
        function extractJobDetail($, jobUrl) {
            const jsonLd = extractJsonLd($) || {};

            // Fallback selectors for each field
            const title = jsonLd.title || $('h1, .box_title h1, [class*="title"]').first().text().trim() || null;
            const company = jsonLd.company 
                || $('a[href*="/empresa/"], [class*="company"], .box_header .fc_base a').first().text().trim() 
                || null;
            
            // Extract location: try JSON-LD, then specific Computrabajo selectors
            let location = jsonLd.location || null;
            if (!location) {
                const locEl = $('.box_header p, [class*="location"], [class*="ciudad"], [class*="ubication"]').first().text().trim();
                location = locEl && locEl.length > 0 ? locEl : null;
                // If still not found, search in structured data attributes
                if (!location) {
                    const attr = $('[data-location], [data-city], [data-ubicacion]').first().text().trim();
                    location = attr && attr.length > 0 ? attr : null;
                }
            }
            
            // Extract salary: try JSON-LD, then HTML selectors
            let salary = jsonLd.salary || null;
            if (!salary) {
                const salEl = $('[class*="salary"], [class*="sueldo"], [class*="precio"], [data-salary]').first().text().trim();
                salary = salEl && salEl.length > 0 ? salEl : null;
            }
            
            // Extract employmentType: try JSON-LD, then HTML selectors
            let employmentType = jsonLd.employmentType || null;
            if (!employmentType) {
                const empEl = $('[class*="employment"], [class*="contract"], [class*="tipo-contrato"], [data-employment-type]').first().text().trim();
                employmentType = empEl && empEl.length > 0 ? empEl : null;
            }
            
            // Extract datePosted: try JSON-LD, then HTML selectors (look for date patterns)
            let datePosted = jsonLd.datePosted || null;
            if (!datePosted) {
                const dateEl = $('[class*="date"], [class*="fecha"], [class*="posted"], [data-date], time').first().text().trim();
                datePosted = dateEl && dateEl.length > 0 ? dateEl : null;
            }

            let description = jsonLd.description;
            if (!description) {
                const descEl = $('.box_detail, [class*="job-description"], .job_desc, [class*="offer-detail"], [class*="descripcion"]').first();
                description = descEl.length ? descEl.html() : null;
                // Remove error containers and loading placeholders from description HTML
                if (description) {
                    const tempDom = cheerioLoad(description);
                    tempDom('[class*="animating"], [class*="hide"], [data-offers-grid-detail-container-error], [data-complaint-overlay], [id*="complaint"], [id*="complaint-popup"]').remove();
                    description = tempDom.html();
                }
            }

            return {
                title,
                company,
                location,
                salary,
                employmentType,
                datePosted,
                description_html: description,
                description_text: description ? cleanHtmlToText(description) : null,
                url: jobUrl,
                source: 'computrabajo.com',
            };
        }


        // Helper to build per-request headers
        const buildHeaders = () => {
            const headers = {
                'User-Agent': getRandomUserAgent(),
                'Accept-Language': getRandomAcceptLang(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
            };
            // Attach cookies if provided (raw or parsed)
            if (typeof cookies === 'string' && cookies.trim().length > 0) {
                headers['Cookie'] = cookies.trim();
            } else if (parsedCookies) {
                try {
                    if (Array.isArray(parsedCookies)) {
                        headers['Cookie'] = parsedCookies.map(c => `${c.name}=${c.value}`).join('; ');
                    } else if (typeof parsedCookies === 'object') {
                        headers['Cookie'] = Object.entries(parsedCookies).map(([k, v]) => `${k}=${v}`).join('; ');
                    }
                } catch (e) { /* ignore */ }
            }
            return headers;
        };
        

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConfig,
            maxRequestRetries: 3,
            useSessionPool: true,
            sessionPoolOptions: { maxPoolSize: 100 },
            maxConcurrency: MAX_CONCURRENCY,
            minConcurrency: Math.max(1, Math.floor(MAX_CONCURRENCY / 2)),
            requestHandlerTimeoutSecs: 60,
            ignoreSslErrors: true,
            // Note: headers are applied per-request via initial requests and enqueueLinks transformRequestFunction
            async requestHandler({ request, $, enqueueLinks, log: logger }) {
                const label = request.userData?.label || 'LIST';
                const pageNum = request.userData?.pageNum || 1;

                urlsVisited.add(request.url);

                try {
                    if (label === 'LIST') {
                        logger.info(`[LIST p${pageNum}] ${request.url}`);
                        const jobLinks = findJobLinks($, request.url);
                        logger.info(`Found ${jobLinks.length} jobs on page ${pageNum}`);

                        if (collectDetails && jobLinks.length) {
                            const remaining = MAX_RESULTS - totalSaved;
                            const toQueue = jobLinks
                                .slice(0, remaining)
                                .filter(url => !jobUrlsEnqueued.has(url));

                            if (toQueue.length) {
                                await enqueueLinks({
                                    urls: toQueue,
                                    userData: { label: 'DETAIL' },
                                    transformRequestFunction: (req) => {
                                        req.headers = { ...(req.headers || {}), ...buildHeaders() };
                                        if (stealthMode && totalSaved > 0) {
                                            const delay = getStealthDelay();
                                            if (delay > 0) return new Promise(res => setTimeout(() => res(req), delay));
                                        }
                                        return req;
                                    }
                                });
                                toQueue.forEach(url => jobUrlsEnqueued.add(url));
                                logger.info(`Queued ${toQueue.length} detail pages`);
                            }
                        } else if (!collectDetails && jobLinks.length) {
                            for (const url of jobLinks.slice(0, MAX_RESULTS - totalSaved)) {
                                await Dataset.pushData({ url, title: null, company: null, location: null, description_html: null, description_text: null });
                                totalSaved++;
                            }
                        }

                        // Handle pagination
                        if (totalSaved < MAX_RESULTS && pageNum < MAX_PAGES) {
                            const nextUrl = findNextPage($, request.url);
                            if (nextUrl && !urlsVisited.has(nextUrl)) {
                                await enqueueLinks({
                                    urls: [nextUrl],
                                    userData: { label: 'LIST', pageNum: pageNum + 1 },
                                    transformRequestFunction: (req) => {
                                        req.headers = { ...(req.headers || {}), ...buildHeaders() };
                                        return req;
                                    }
                                });
                                logger.info(`Next page queued: ${nextUrl}`);
                            }
                        }
                        return;
                    }

                    if (label === 'DETAIL') {
                        if (totalSaved >= MAX_RESULTS) return;
                        logger.info(`[DETAIL] ${request.url}`);

                        const job = extractJobDetail($, request.url);
                        if (job.title) {
                            await Dataset.pushData(job);
                            totalSaved++;
                            logger.info(`[${totalSaved}/${MAX_RESULTS}] ${job.title} @ ${job.company}`);
                        } else {
                            logger.warning(`No title found for ${request.url}`);
                        }
                    }
                } catch (err) {
                    logger.error(`Error: ${err.message}`);
                }
            },
            errorHandler: async ({ request, error, log: logger }) => {
                logger.warning(`Failed: ${request.url} - ${error.message}`);
            },
        });

        log.info('Starting Computrabajo scraper...');
        const startTime = Date.now();

        // Prepare initial requests with headers
        const initialRequests = initialUrls.map(url => ({
            url,
            userData: { label: 'LIST', pageNum: 1 },
            headers: buildHeaders(),
        }));

        await crawler.run(initialRequests);

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        log.info(`✓ Saved ${totalSaved}/${MAX_RESULTS} jobs in ${elapsed}s | URLs: ${urlsVisited.size}`);

    } catch (err) {
        log.error(`Fatal: ${err.message}`);
        throw err;
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    console.error('Unhandled error:', err);
    process.exit(1);
});
