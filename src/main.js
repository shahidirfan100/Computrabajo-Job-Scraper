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
            startUrls = [],
            startUrlsText = '',
            keyword = 'asesor-de-ventas',
            maxResults: MAX_RESULTS_RAW = 100,
            maxPages: MAX_PAGES_RAW = 20,
            collectDetails = true,
            stealthMode = true,
            proxyConfiguration,
            requestDelayMs: BASE_DELAY_MS = 2000,
            maxConcurrency: MAX_CONCURRENCY_RAW = 3,
        } = input;

        const MAX_RESULTS = Math.max(1, Number.isFinite(+MAX_RESULTS_RAW) ? Math.floor(+MAX_RESULTS_RAW) : 100);
        const MAX_PAGES = Math.max(1, Number.isFinite(+MAX_PAGES_RAW) ? Math.floor(+MAX_PAGES_RAW) : 20);
        const MAX_CONCURRENCY = Math.max(1, Math.min(10, Number.isFinite(+MAX_CONCURRENCY_RAW) ? Math.floor(+MAX_CONCURRENCY_RAW) : 3));


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
            const variance = BASE_DELAY_MS * 0.4;
            return Math.floor(BASE_DELAY_MS - variance + Math.random() * (variance * 2));
        };


        // URL utilities
        const toAbsoluteUrl = (href, base = 'https://mx.computrabajo.com') => {
            if (!href || typeof href !== 'string') return null;
            try { return new URL(href, base).href; } catch { return null; }
        };

        const cleanHtmlToText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript').remove();
            return $.root().text().replace(/\s+/g, ' ').trim();
        };

        const buildSearchUrl = (searchKeyword) => {
            if (!searchKeyword || typeof searchKeyword !== 'string') {
                return 'https://mx.computrabajo.com/trabajo-de-asesor-de-ventas';
            }
            const normalized = searchKeyword.trim().toLowerCase()
                .replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').replace(/-+/g, '-');
            return `https://mx.computrabajo.com/trabajo-de-${normalized}`;
        };

        // Initialize URLs (priority: startUrlsText > startUrls array > built keyword URL)
        const initialUrls = [];
        if (typeof startUrlsText === 'string' && startUrlsText.trim().length > 0) {
            const parsed = startUrlsText.split(/[\r\n]+/).map(s => s.trim()).filter(Boolean);
            initialUrls.push(...parsed);
        } else if (Array.isArray(startUrls) && startUrls.length > 0) {
            initialUrls.push(...startUrls.filter(u => typeof u === 'string' && u.length > 0));
        }
        if (initialUrls.length === 0) {
            initialUrls.push(buildSearchUrl(keyword));
        }

        log.info(`Starting with ${initialUrls.length} URL(s): maxResults=${MAX_RESULTS}, maxPages=${MAX_PAGES}`);

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
                        return {
                            title: item.title || item.name || null,
                            company: item.hiringOrganization?.name || null,
                            datePosted: item.datePosted || null,
                            description: item.description || null,
                            location: item.jobLocation?.address?.addressLocality || item.jobLocation?.address?.addressRegion || null,
                            salary: item.baseSalary?.value || null,
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
                || $('.fc_base a, [class*="company"]').first().text().trim() 
                || null;
            const location = jsonLd.location 
                || $('.box_header p, [class*="location"]').first().text().trim()
                || null;

            let description = jsonLd.description;
            if (!description) {
                const descEl = $('.box_detail, [class*="job-description"], .job_desc').first();
                description = descEl.length ? descEl.html() : null;
            }

            return {
                title,
                company,
                location,
                description_html: description,
                description_text: description ? cleanHtmlToText(description) : null,
                url: jobUrl,
                datePosted: jsonLd.datePosted || null,
                salary: jsonLd.salary || null,
                employmentType: jsonLd.employmentType || null,
                source: 'computrabajo.com',
            };
        }


        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConfig,
            maxRequestRetries: 3,
            useSessionPool: true,
            sessionPoolOptions: { maxPoolSize: 100 },
            maxConcurrency: MAX_CONCURRENCY,
            minConcurrency: Math.max(1, Math.floor(MAX_CONCURRENCY / 2)),
            requestHandlerTimeoutSecs: 60,
            ignoreSslErrors: true,
            prepareRequest: async ({ request }) => {
                // Add stealth headers
                request.headers = request.headers || {};
                request.headers['User-Agent'] = getRandomUserAgent();
                request.headers['Accept-Language'] = getRandomAcceptLang();
                request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8';
                request.headers['Accept-Encoding'] = 'gzip, deflate';
                request.headers['DNT'] = '1';
                request.headers['Connection'] = 'keep-alive';
                request.headers['Upgrade-Insecure-Requests'] = '1';
                request.headers['Sec-Fetch-Dest'] = 'document';
                request.headers['Sec-Fetch-Mode'] = 'navigate';
                request.headers['Sec-Fetch-Site'] = 'none';

                // Add random delay
                if (stealthMode && totalSaved > 0) {
                    const delay = getStealthDelay();
                    if (delay > 0) await new Promise(r => setTimeout(r, delay));
                }
            },
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

        await crawler.run(initialUrls.map(url => ({ url, userData: { label: 'LIST', pageNum: 1 } })));

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
