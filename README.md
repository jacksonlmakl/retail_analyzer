# Retail Analyzer

Scrape Google Shopping product data and load it into Snowflake for analysis.

```
retail_analyzer/
├── src/
│   ├── scraper.py              # Google Shopping scraper (Playwright)
│   └── loader.py               # Snowflake connection & data loading
├── api/
│   ├── server.py               # FastAPI endpoints (POST /scrape, POST /verify)
│   ├── tasks.py                # Celery tasks (scrape_and_load, verify_products)
│   └── celeryconfig.py         # Celery + Beat schedule config
├── dags/
│   └── verify_products_dag.py  # Airflow DAG for scheduled verification
├── sql/
│   ├── setup.sql               # DDL: database, tables, stages, views
│   ├── load.sql                # Reference: manual SnowSQL load commands
│   └── migrations/             # Incremental schema changes
├── output/                     # Scraper results (gitignored)
│   ├── results.json
│   └── images/
├── run_pipeline.py             # CLI entry point: scrape + load
├── verify_products.py          # CLI: verify listings are still live
├── Dockerfile                  # Container image for api, worker, beat
├── Dockerfile.airflow          # Lightweight container for Airflow
├── docker-compose.yml          # Full stack: redis, api, worker, beat, airflow
├── requirements.txt            # Python deps for scraper/api/worker
├── requirements-airflow.txt    # Python deps for Airflow container
├── .env.example                # Template for environment variables
└── .env.sh                     # Snowflake credentials for local dev (gitignored)
```

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Scraper Only

Run the scraper standalone without Snowflake:

```bash
python -m src.scraper "wireless headphones"
python -m src.scraper "gaming monitor" --pages 2 --output output/results.json
python -m src.scraper "running shoes" --output output/results.csv
```

### Scraper CLI Options

| Option | Description | Default |
|---|---|---|
| `query` | Search term (required) | — |
| `--pages` | Number of result pages | 1 |
| `--output`, `-o` | Output file (.json or .csv) | — |
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
| `query` | Search term (required) | — |
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
SELECT * FROM RETAIL_ANALYZER.GOOGLE_SHOPPING.PRODUCTS_ANALYSIS
WHERE RUN_ID = <your_run_id>
ORDER BY PRICE_NUMERIC;
```

## Verify Product Listings

Check whether previously scraped products are still listed on Google Shopping. Updates changed fields (price, seller, etc.) and marks missing products as inactive.

**If upgrading an existing database**, run the migrations in order:

```bash
# In Snowflake, run:
# sql/migrations/001_add_verification_columns.sql
# sql/migrations/002_add_search_query_column.sql
# sql/migrations/003_normalize_search_queries.sql
```

If you started fresh with `--setup`, all tables are already included.

### Usage

```bash
python verify_products.py                    # verify all active products
python verify_products.py --run-id 101       # verify only products from a specific run
python verify_products.py --query "gucci"    # verify products whose search query contains "gucci"
python verify_products.py --dry-run          # preview changes without writing to Snowflake
```

### Verification CLI Options

| Option | Description | Default |
|---|---|---|
| `--run-id` | Only verify products from this scrape run ID | all |
| `--query` | Filter by search query (substring match) | all |
| `--headless` | Run browser headless (may trigger CAPTCHA) | off |
| `--dry-run` | Show what would change without writing to Snowflake | off |

### How It Works

1. Fetches all active products from Snowflake, grouped by their original search query
2. Re-runs each search query through the Google Shopping scraper
3. Fuzzy-matches stored products against fresh results by title (80% similarity threshold)
4. For matched products: updates any changed fields (price, seller, rating, etc.) and sets `LAST_VERIFIED_AT`
5. For unmatched products: double-checks the merchant link before marking `IS_ACTIVE = FALSE`
6. Prints a summary of what changed

### Querying Inactive Products

```sql
SELECT TITLE, PRICE, SELLER, LAST_VERIFIED_AT
FROM RETAIL_ANALYZER.GOOGLE_SHOPPING.PRODUCTS
WHERE IS_ACTIVE = FALSE
ORDER BY LAST_VERIFIED_AT DESC;
```

## Output Fields

Each product includes:

- **title** — Product name
- **price** — Current listed price
- **original_price** — Original price before discount (if on sale)
- **discount** — Discount badge (e.g. "28% OFF")
- **seller** — Store/merchant name
- **rating** — Star rating (e.g. "4.8/5")
- **reviews** — Review count
- **shipping** — Shipping/delivery info
- **link** — Merchant URL (always captured; products without a link are excluded)
- **image_url** — Product thumbnail URL
- **image_path** — Local path to saved thumbnail

## Docker Compose (API + Workers + Airflow)

Run the full stack in containers: a FastAPI scrape API, Celery workers for parallel scraping, Celery Beat for scheduled verification, and Airflow for monitoring.

The stack uses two separate Docker images:
- `Dockerfile` — scraper, API, workers, and beat (Playwright, Chromium, Snowflake connector)
- `Dockerfile.airflow` — Airflow only (lightweight, no browser dependencies)

Workers run Chromium inside a virtual framebuffer (Xvfb) so Google Shopping sees a headed browser, avoiding bot detection.

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

3. Scale workers for parallel scraping:

```bash
docker compose up -d --scale worker=5
```

### API Endpoints

**Trigger a scrape:**

```bash
curl -X POST http://localhost:8000/scrape \
  -H "Content-Type: application/json" \
  -d '{"query": "mechanical keyboard", "pages": 2}'
# Returns: {"task_id": "abc-123", "status": "queued"}
```

**Check scrape status:**

```bash
curl http://localhost:8000/scrape/abc-123
# Returns: {"task_id": "abc-123", "status": "SUCCESS", "result": {"run_id": 42, "products_loaded": 30}}
```

**Trigger verification:**

```bash
curl -X POST http://localhost:8000/verify \
  -H "Content-Type: application/json" \
  -d '{"max_stale_days": 7}'
```

**Check verification status:**

```bash
curl http://localhost:8000/verify/def-456
```

**Health check:**

```bash
curl http://localhost:8000/health
```

### Airflow UI

Airflow runs on http://localhost:8080. The admin password is auto-generated on first startup — check the airflow container logs for the line:

```
Simple auth manager | Password for user 'admin': <generated_password>
```

The `verify_products` DAG runs daily and calls the API's `/verify` endpoint over HTTP. This means Airflow and the API can be deployed to separate infrastructure (different cloud providers, etc.) — just set `RETAIL_API_URL` in the Airflow environment.

### Services

| Service | Port | Image | Description |
|---|---|---|---|
| `api` | 8000 | `Dockerfile` | FastAPI server for on-demand scraping |
| `worker` | — | `Dockerfile` | Celery workers (Playwright + Xvfb + Snowflake) |
| `beat` | — | `Dockerfile` | Celery Beat scheduler (periodic verification) |
| `airflow` | 8080 | `Dockerfile.airflow` | Airflow scheduler + web UI |
| `redis` | 6379 | `redis:7-alpine` | Message broker |

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | required |
| `SNOWFLAKE_USER` | Snowflake username | required |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Path to RSA private key on host | required |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | required |
| `SNOWFLAKE_ROLE` | Snowflake role | optional |
| `REDIS_URL` | Redis connection URL | `redis://redis:6379/0` |
| `RETAIL_API_URL` | API URL for Airflow to call (override for cross-cloud) | `http://api:8000` |
| `VERIFY_INTERVAL_HOURS` | Hours between auto-verification runs (Celery Beat) | `24` |
| `VERIFY_STALE_DAYS` | Days before a query is considered stale | `7` |

## Notes

- A visible browser window will briefly appear during local scraping (intentional — headed mode bypasses Google's bot detection). Docker containers use a virtual framebuffer (Xvfb) for the same effect without a display.
- Products without a valid merchant link are automatically excluded from results and Snowflake.
- The scraper auto-detects Google Shopping's product card CSS selectors, with fallback heuristics if Google rotates class names.
- The first run may be slower as it sets up the browser profile.
- If you get CAPTCHA pages, wait a few minutes and try again with higher delays.
