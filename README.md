# Retail Analyzer

Scrape product listings from multiple luxury/resale marketplaces and load the data into Snowflake for cross-marketplace analysis.

**Supported Marketplaces:**

| Marketplace | Scraping Method | Snowflake Schema | API Port |
|---|---|---|---|
| Google Shopping | Playwright (DOM + panel links) | `GOOGLE_SHOPPING` | 8000 |
| Grailed | `grailed_api` Python package (Algolia) | `GRAILED` | 8001 |
| Vestiaire Collective | Playwright (React SPA) | `VESTIAIRE` | 8002 |
| Rebag | Playwright (JS-rendered) | `REBAG` | 8003 |
| Farfetch | Playwright (bot-protected) | `FARFETCH` | 8004 |
| Fashionphile | Playwright (search page) | `FASHIONPHILE` | 8005 |
| 2nd Street USA | Shopify JSON + Playwright fallback | `SECOND_STREET` | 8006 |

```
retail_analyzer/
├── shared/                      # Shared Snowflake connection module
│   ├── __init__.py
│   └── snowflake.py             # get_connection(), check_env(), run_setup()
├── src/                         # Google Shopping scraper
│   ├── scraper.py
│   └── loader.py
├── api/                         # Google Shopping API + Celery
│   ├── server.py
│   ├── tasks.py
│   └── celeryconfig.py
├── grailed/                     # Grailed scraper service
│   ├── scraper.py               # Uses grailed_api package
│   ├── loader.py
│   ├── server.py
│   ├── tasks.py
│   ├── celeryconfig.py
│   ├── sql/setup.sql
│   └── Dockerfile
├── vestiaire/                   # Vestiaire Collective scraper service
│   ├── scraper.py               # Playwright-based
│   ├── loader.py
│   ├── server.py
│   ├── tasks.py
│   ├── celeryconfig.py
│   ├── sql/setup.sql
│   └── Dockerfile
├── rebag/                       # Rebag scraper service
│   ├── scraper.py
│   ├── loader.py
│   ├── server.py
│   ├── tasks.py
│   ├── celeryconfig.py
│   ├── sql/setup.sql
│   └── Dockerfile
├── farfetch/                    # Farfetch scraper service
│   ├── scraper.py
│   ├── loader.py
│   ├── server.py
│   ├── tasks.py
│   ├── celeryconfig.py
│   ├── sql/setup.sql
│   └── Dockerfile
├── fashionphile/                # Fashionphile scraper service
│   ├── scraper.py
│   ├── loader.py
│   ├── server.py
│   ├── tasks.py
│   ├── celeryconfig.py
│   ├── sql/setup.sql
│   └── Dockerfile
├── secondstreet/                # 2nd Street USA scraper service
│   ├── scraper.py               # Shopify JSON fast path + Playwright fallback
│   ├── loader.py
│   ├── server.py
│   ├── tasks.py
│   ├── celeryconfig.py
│   ├── sql/setup.sql
│   └── Dockerfile
├── sql/
│   ├── setup.sql                # Google Shopping DDL
│   ├── load.sql
│   └── migrations/
├── output/                      # Local scraper results (gitignored)
├── run_pipeline.py              # CLI: scrape Google Shopping + load to Snowflake
├── Dockerfile                   # Google Shopping container
├── docker-compose.yml           # Full stack: redis + 7 APIs + 6 workers
├── requirements.txt             # Python dependencies
├── .env.example                 # Env var template
└── .env.sh                      # Snowflake credentials for local dev (gitignored)
```

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Google Shopping Scraper (Standalone)

Run the scraper standalone without Snowflake:

```bash
python -m src.scraper "wireless headphones"
python -m src.scraper "gaming monitor" --pages 2 --output output/results.json
```

### Scraper CLI Options

| Option | Description | Default |
|---|---|---|
| `query` | Search term (required) | -- |
| `--pages` | Number of result pages | 1 |
| `--output`, `-o` | Output file (.json or .csv) | -- |
| `--details` | Click each product for merchant links (slower) | off |
| `--country` | Country code for localized results | us |
| `--language` | Language code | en |
| `--delay-min` | Min delay between pages (seconds) | 1.5 |
| `--delay-max` | Max delay between pages (seconds) | 3.5 |
| `--headless` | Run browser without visible window (may trigger CAPTCHA) | off |

## Full Pipeline (Scrape + Snowflake)

### Snowflake Prerequisites

1. Generate an RSA key pair:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/snowflake_rsa_key.p8 -nocrypt
openssl rsa -in ~/.ssh/snowflake_rsa_key.p8 -pubout -out ~/.ssh/snowflake_rsa_key.pub
```

2. Register the public key with your Snowflake user:

```sql
ALTER USER your_username SET RSA_PUBLIC_KEY='<contents of rsa_key.pub without header/footer>';
```

3. Configure environment variables (edit `.env.sh` then `source .env.sh`):

```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PRIVATE_KEY_PATH="$HOME/.ssh/snowflake_rsa_key.p8"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
# optional:
export SNOWFLAKE_ROLE="SYSADMIN"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your_passphrase"
```

### First Run (creates database, tables, stages)

```bash
source .env.sh
python run_pipeline.py "mechanical keyboard" --setup
```

### Subsequent Runs

```bash
python run_pipeline.py "wireless headphones"
python run_pipeline.py "running shoes" --pages 2
```

### Load Existing Results (skip scraping)

```bash
python run_pipeline.py "mechanical keyboard" --skip-scrape
```

### Pipeline CLI Options

| Option | Description | Default |
|---|---|---|
| `query` | Search term (required) | -- |
| `--pages` | Number of result pages to scrape | 1 |
| `--country` | Country code | us |
| `--language` | Language code | en |
| `--headless` | Run browser headless (may trigger CAPTCHA) | off |
| `--setup` | Run `sql/setup.sql` DDL first (first-time setup) | off |
| `--skip-scrape` | Skip scraping, load existing `output/results.json` | off |

### What the Pipeline Does

1. Scrapes Google Shopping for your query
2. Extracts merchant links for each product (clicks each card to capture the retailer URL)
3. Drops any products that don't have a valid merchant link
4. Saves `results.json` and product images to `output/`
5. Connects to Snowflake via RSA key-pair authentication
6. Uploads JSON to `@SCRAPE_DATA` and images to `@PRODUCT_IMAGES`
7. Upserts the search query into `SEARCH_QUERIES`, creates a `SCRAPE_RUNS` record, loads products into `PRODUCTS`, and links them via `PRODUCT_QUERIES`

### Querying Your Data

```sql
-- Google Shopping
SELECT * FROM RETAIL_ANALYZER.GOOGLE_SHOPPING.PRODUCTS_ANALYSIS
WHERE RUN_ID = <your_run_id> ORDER BY PRICE_NUMERIC;

-- Grailed
SELECT * FROM RETAIL_ANALYZER.GRAILED.PRODUCTS_ANALYSIS
WHERE RUN_ID = <your_run_id> ORDER BY PRICE_NUMERIC;

-- Cross-marketplace comparison (example)
SELECT 'Google Shopping' AS source, TITLE, PRICE_NUMERIC
FROM RETAIL_ANALYZER.GOOGLE_SHOPPING.PRODUCTS WHERE TITLE ILIKE '%louis vuitton%'
UNION ALL
SELECT 'Grailed', TITLE, PRICE_NUMERIC
FROM RETAIL_ANALYZER.GRAILED.PRODUCTS WHERE TITLE ILIKE '%louis vuitton%'
ORDER BY PRICE_NUMERIC;
```

## Output Fields

Each product includes (varies by marketplace):

- **title** -- Product name
- **price** -- Current listed price
- **original_price** -- Original price before discount (if on sale)
- **discount** -- Discount badge (e.g. "28% OFF")
- **link** -- Product URL (always captured; products without a link are excluded)
- **image_url** / **image_path** -- Product thumbnail
- **designer** / **brand** -- Brand or designer name (marketplace-specific)
- **condition** -- Item condition (Grailed, Vestiaire, Rebag, Fashionphile, 2nd Street)
- **category**, **color**, **material**, **size_info** -- Additional metadata (where available)
- **seller** / **rating** / **reviews** / **shipping** -- Google Shopping specific

## Docker Compose (Multi-Marketplace)

Run the full stack: 7 marketplace APIs, 6 Celery workers, and Redis.

Workers run Chromium inside a virtual framebuffer (Xvfb) so marketplace sites see a headed browser, avoiding bot detection. Grailed uses an API-only approach (no Playwright needed).

### Quick Start

1. Copy the env template and fill in your Snowflake credentials:

```bash
cp .env.example .env
# edit .env with your values
```

2. Start all services:

```bash
docker compose up -d --build
```

3. Start only specific marketplaces:

```bash
docker compose up -d redis api worker grailed-api grailed-worker
```

### API Endpoints

Every marketplace service exposes the same three endpoints:

**Trigger a scrape:**

```bash
# Google Shopping (port 8000)
curl -X POST http://localhost:8000/scrape \
  -H "Content-Type: application/json" \
  -d '{"query": "louis vuitton infrarouge", "pages": 1}'

# Grailed (port 8001)
curl -X POST http://localhost:8001/scrape \
  -H "Content-Type: application/json" \
  -d '{"query": "louis vuitton infrarouge", "pages": 1}'

# Vestiaire (port 8002)
curl -X POST http://localhost:8002/scrape \
  -H "Content-Type: application/json" \
  -d '{"query": "louis vuitton infrarouge"}'

# Rebag (8003), Farfetch (8004), Fashionphile (8005), 2nd Street (8006) -- same pattern
```

**Check scrape status:**

```bash
curl http://localhost:8001/scrape/<task_id>
```

**Health check:**

```bash
curl http://localhost:8001/health
```

### Services

| Service | Port | Description |
|---|---|---|
| `redis` | 6379 | Message broker |
| `api` | 8000 | Google Shopping API |
| `worker` | -- | Google Shopping Celery worker |
| `grailed-api` | 8001 | Grailed API |
| `grailed-worker` | -- | Grailed Celery worker |
| `vestiaire-api` | 8002 | Vestiaire Collective API |
| `vestiaire-worker` | -- | Vestiaire Celery worker |
| `rebag-api` | 8003 | Rebag API |
| `rebag-worker` | -- | Rebag Celery worker |
| `farfetch-api` | 8004 | Farfetch API |
| `farfetch-worker` | -- | Farfetch Celery worker |
| `fashionphile-api` | 8005 | Fashionphile API |
| `fashionphile-worker` | -- | Fashionphile Celery worker |
| `secondstreet-api` | 8006 | 2nd Street USA API |
| `secondstreet-worker` | -- | 2nd Street Celery worker |

### Snowflake Schemas

Each marketplace has its own schema inside the `RETAIL_ANALYZER` database:

| Schema | Tables |
|---|---|
| `GOOGLE_SHOPPING` | SEARCH_QUERIES, SCRAPE_RUNS, PRODUCTS, PRODUCT_QUERIES |
| `GRAILED` | SEARCH_QUERIES, SCRAPE_RUNS, PRODUCTS, PRODUCT_QUERIES |
| `VESTIAIRE` | SEARCH_QUERIES, SCRAPE_RUNS, PRODUCTS, PRODUCT_QUERIES |
| `REBAG` | SEARCH_QUERIES, SCRAPE_RUNS, PRODUCTS, PRODUCT_QUERIES |
| `FARFETCH` | SEARCH_QUERIES, SCRAPE_RUNS, PRODUCTS, PRODUCT_QUERIES |
| `FASHIONPHILE` | SEARCH_QUERIES, SCRAPE_RUNS, PRODUCTS, PRODUCT_QUERIES |
| `SECOND_STREET` | SEARCH_QUERIES, SCRAPE_RUNS, PRODUCTS, PRODUCT_QUERIES |

Each schema also has `PRODUCT_IMAGES` and `SCRAPE_DATA` stages, a `JSON_FORMAT` file format, and a `PRODUCTS_ANALYSIS` view.

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | required |
| `SNOWFLAKE_USER` | Snowflake username | required |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Path to RSA private key on host | required |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | required |
| `SNOWFLAKE_ROLE` | Snowflake role | optional |
| `REDIS_URL` | Redis connection URL | `redis://redis:6379/0` |

## Notes

- A visible browser window will briefly appear during local scraping (intentional -- headed mode bypasses bot detection). Docker containers use a virtual framebuffer (Xvfb) for the same effect without a display.
- Products without a valid link are automatically excluded from results and Snowflake.
- The Google Shopping scraper auto-detects product card CSS selectors with fallback heuristics.
- Grailed uses its Algolia-backed API via the `grailed_api` package (no browser needed).
- 2nd Street USA tries the Shopify JSON search endpoint first for speed, falling back to Playwright if unavailable.
- The first run for each marketplace may be slower as it sets up the browser profile.
- If you get CAPTCHA pages, wait a few minutes and try again with higher delays.
