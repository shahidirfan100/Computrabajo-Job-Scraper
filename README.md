# Computrabajo Mexico Job Scraper# Computrabajo Mexico Job Scraper



**Production-ready Apify actor** for scraping job listings from https://mx.computrabajo.comThis Apify actor scrapes job listings from Computrabajo Mexico (https://mx.computrabajo.com) using Crawlee's CheerioCrawler and gotScraping. It extracts both HTML and text job descriptions with structured data from JSON-LD schema.org/JobPosting markup.



## Features## Features



- ✅ Scrapes job listings and detail pages- 🚀 Scrapes Computrabajo Mexico job listings and detail pages (no browser required)

- ✅ JSON-LD data extraction with HTML fallback- 📊 Prefers structured data (JSON-LD) where available, falls back to HTML parsing

- ✅ Pagination support- 📄 Saves both HTML and plain text descriptions for each job

- ✅ Stealth headers and delays- 🔄 Handles pagination until the requested number of results is reached

- ✅ Proxy rotation- ⚡ Lightweight, fully Apify-compatible (no local dependencies required)

- ✅ Session management- 🛡️ Built-in rate-limiting and anti-bot measures (randomized delays, session pools, proxies)

- ✅ Duplicate prevention- 🕵️ Advanced stealth features: browser fingerprint rotation, header spoofing, proxy rotation



## Input Parameters## Input



- **startUrls** (array): Computrabajo URLs to scrapeThe actor accepts the following input fields (all optional unless noted):

- **keyword** (string): Job category (e.g., "asesor-de-ventas")

- **maxResults** (integer): Max jobs to collect (default: 100)- `startUrls` (array) — Array of Computrabajo Mexico URLs to start scraping from. Example: `["https://mx.computrabajo.com/trabajo-de-asesor-de-ventas"]`. If empty, uses keyword parameter.

- **maxPages** (integer): Max pages to visit (default: 20)- `keyword` (string) — Job category keyword to search for. Converted to URL format: `/trabajo-de-{keyword}`. Examples: `"asesor-de-ventas"`, `"marketing"`, `"atencion-al-cliente"`. Default: `"asesor-de-ventas"`.

- **collectDetails** (boolean): Scrape full descriptions (default: true)- `maxResults` (integer) — Maximum number of job listings to collect from all pages. Default: 100.

- **stealthMode** (boolean): Enable anti-detection (default: true)- `maxPages` (integer) — Maximum number of pagination pages to visit. Safety limit to prevent excessive scraping. Default: 20.

- **requestDelayMs** (integer): Delay between requests (default: 2000)- `collectDetails` (boolean) — If true, the actor opens each job detail page to extract full HTML and text descriptions. Increases scraping time but provides richer data. Default: true.

- **maxConcurrency** (integer): Parallel requests (default: 3)- `stealthMode` (boolean) — If true, uses advanced anti-detection techniques: randomized delays, header rotation, session management, and intelligent caching. Default: true.

- **proxyConfiguration** (object): Proxy settings (optional)- `proxyConfiguration` (object) — Proxy settings for request rotation. Use Apify Proxy (recommended) or custom proxies. Essential for large-scale scraping.

- `requestDelayMs` (integer) — Base delay between requests in milliseconds. In stealth mode, adds randomization (±50%). Range: 500-5000ms. Default: 2000ms.

## Output- `maxConcurrency` (integer) — Maximum number of parallel requests. Lower values (2-3) are stealthier; higher values (5-10) are faster but riskier. Default: 3.

- `customHeaders` (string) — Optional JSON object of custom HTTP headers. Example: `{"Accept-Language": "es-MX"}`.

Each job record contains:

```json### Example Input

{

  "title": "Job Title",```json

  "company": "Company Name",{

  "location": "City",  "startUrls": ["https://mx.computrabajo.com/trabajo-de-asesor-de-ventas"],

  "description_html": "<p>...</p>",  "maxResults": 50,

  "description_text": "Plain text",  "maxPages": 5,

  "url": "https://mx.computrabajo.com/...",  "collectDetails": true,

  "datePosted": "2025-10-18",  "stealthMode": true,

  "salary": "15000 MXN",  "proxyConfiguration": {

  "employmentType": "Full-time",    "useApifyProxy": true,

  "source": "computrabajo.com"    "apifyProxyGroups": ["AUTO"]

}  }

```}

```

## Usage

Or with keyword only:

```json

{```json

  "startUrls": ["https://mx.computrabajo.com/trabajo-de-asesor-de-ventas"],{

  "maxResults": 50,  "keyword": "asesor-de-ventas",

  "maxPages": 3,  "maxResults": 100,

  "collectDetails": true,  "stealthMode": true

  "stealthMode": true,}

  "proxyConfiguration": {```

    "useApifyProxy": true,

    "apifyProxyGroups": ["AUTO"]## Output

  }

}Each item saved to the dataset follows this structure:

```

```json

## CSS Selectors{

  "title": "Asesor de Ventas",

- **Job Links**: `.js-o-link.fc_base`, `a[href*="/trabajo-de-"]`  "company": "Empresa XYZ",

- **Pagination**: `a.js-o-pager.next`, `a[rel="next"]`  "location": "Ciudad de México",

- **Title**: `h1`  "description_html": "<p>Buscamos asesor de ventas...</p>",

- **Company**: `.fc_base a`  "description_text": "Buscamos asesor de ventas...",

- **Location**: `.box_header p`  "url": "https://mx.computrabajo.com/trabajo-de-asesor-de-ventas/12345",

- **Description**: `.box_detail`  "datePosted": "2025-10-18",

- **JSON-LD**: `script[type="application/ld+json"]`  "hiringOrganization": "Empresa XYZ",

  "salary": "15000 MXN"

## Performance}

```

- 10 jobs: ~30-45 seconds

- 50 jobs: ~2-3 minutes## How It Works

- 100 jobs: ~5-10 minutes

1. **Listing Page Parsing**: The actor starts from the provided URL(s) and extracts job links using the `.js-o-link.fc_base` selector.

## Stealth Features2. **Pagination**: Detects and follows the `a.js-o-pager.next` link to process multiple pages.

3. **Detail Page Parsing**: For each job link, it extracts:

✓ User-Agent rotation (5 profiles)     - **JSON-LD first**: Parses `<script type="application/ld+json">` for structured JobPosting data.

✓ Accept-Language randomization     - **HTML fallback**: If JSON-LD is incomplete, extracts from HTML tags (h1, .fc_base, .box_header, .box_detail).

✓ Request delay variation (±40%)  4. **Text Cleaning**: Converts HTML descriptions to plain text by removing scripts, styles, and extra whitespace.

✓ Session persistence  5. **Anti-Bot Safety**:

✓ IP rotation (via Apify Proxy)     - Randomized delays (1–4 seconds) between requests

✓ Security headers     - Rotated User-Agent headers

✓ Automatic retry logic   - Session pool for cookie/session management

   - Apify Proxy support for large-scale runs

## Deploy to Apify

## Advanced Stealth Features

```bash

apify pushThe scraper includes production-grade anti-detection measures:

```

- **Header Rotation**: Randomized User-Agent, Accept-Language, and Referer headers

## Support- **Request Delays**: Configurable base delays with ±50% randomization

- **Session Management**: Persistent session pools for cookie handling

For issues, check Apify logs for detailed error messages or adjust parameters.- **Proxy Rotation**: Automatic IP rotation via Apify Proxy

- **Browser Fingerprint Spoofing**: Sec-CH-UA, DNT, and other browser signals
- **Intelligent Pagination**: Detects and follows pagination links automatically
- **Duplicate Prevention**: Tracks visited URLs to avoid re-scraping

## Architecture

- **Technology Stack**: Crawlee CheerioCrawler + gotScraping (no browser automation)
- **HTTP Library**: Got-scraping for advanced HTTP control
- **Parsing**: Cheerio (lightweight jQuery-like DOM parser)
- **Platform**: 100% Apify-native, no local dependencies required

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Actor blocked after few requests | Increase `requestDelayMs` to 3000-5000, enable `stealthMode`, use Apify Proxy |
| Missing job descriptions | Enable `collectDetails: true` and ensure Computrabajo detail pages are accessible |
| Pagination stops early | Increase `maxPages` or verify pagination selectors in main.js |
| Low results compared to website | Check `maxResults` limit and verify job listing selectors |
| Proxy errors | Ensure `proxyConfiguration.useApifyProxy: true` and sufficient Apify credits |

## CSS Selectors Used

If Computrabajo changes their markup, update these selectors in `src/main.js`:

| Element | Selector | Fallback |
|---------|----------|----------|
| Job Links | `.js-o-link.fc_base` | `a[href*="/trabajo-de-"]` |
| Next Page | `a.js-o-pager.next` | `a[rel="next"]` |
| Job Title | `h1` | `[class*="title"]` |
| Company | `.fc_base a` | `[class*="company"]` |
| Location | `.box_header p` | `[class*="location"]` |
| Description | `.box_detail` | `[class*="job-description"]` |

## Support

For issues or updates to Computrabajo selectors, ensure the actor version is up-to-date and check the Apify logs for detailed error messages.