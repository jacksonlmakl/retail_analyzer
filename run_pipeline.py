#!/usr/bin/env python3
"""
Pipeline: Scrape Google Shopping -> Load into Snowflake

Runs the scraper, saves results to output/, then uploads everything
to Snowflake using RSA key-pair authentication.

Required env vars:
    SNOWFLAKE_ACCOUNT              Account identifier (e.g. xy12345.us-east-1)
    SNOWFLAKE_USER                 Username
    SNOWFLAKE_PRIVATE_KEY_PATH     Path to RSA private key (.pem)
    SNOWFLAKE_WAREHOUSE            Warehouse name

Optional env vars:
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE   Passphrase for the private key
    SNOWFLAKE_ROLE                     Role to assume
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from src.scraper import (
    GoogleShoppingScraper,
    save_product_images,
    save_to_json,
    print_products,
)
from src.loader import (
    DATABASE,
    SCHEMA,
    get_connection,
    run_setup,
    upload_and_load,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"


async def run_scraper(query, pages, country, language, headless):
    """Run the Google Shopping scraper and return the product list."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results_path = OUTPUT_DIR / "results.json"

    print(f"\n[*] Searching Google Shopping for: '{query}'")
    print(f"[*] Country: {country} | Language: {language} | Pages: {pages}\n")

    async with GoogleShoppingScraper(
        country=country,
        language=language,
        headless=headless,
    ) as scraper:
        products = await scraper.search(query, max_pages=pages)

        if products:
            save_product_images(products, str(results_path))
            save_to_json(products, str(results_path))
            print_products(products)

    return products


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Google Shopping and load results into Snowflake.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
environment variables (key-pair auth):
  SNOWFLAKE_ACCOUNT                 Account identifier
  SNOWFLAKE_USER                    Username
  SNOWFLAKE_PRIVATE_KEY_PATH        Path to RSA private key (.pem)
  SNOWFLAKE_WAREHOUSE               Warehouse name
  SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  (optional) Key passphrase
  SNOWFLAKE_ROLE                    (optional) Role to assume
""",
    )
    parser.add_argument(
        "query",
        help="Product search query (e.g. 'wireless headphones')",
    )
    parser.add_argument(
        "--pages", type=int, default=1,
        help="Result pages to scrape (default: 1)",
    )
    parser.add_argument("--country", default="us", help="Country code (default: us)")
    parser.add_argument("--language", default="en", help="Language code (default: en)")
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser without visible window (may trigger CAPTCHA)",
    )
    parser.add_argument(
        "--setup", action="store_true",
        help="Run sql/setup.sql DDL before loading (first-time setup)",
    )
    parser.add_argument(
        "--skip-scrape", action="store_true",
        help="Skip scraping; load existing output/results.json instead",
    )

    args = parser.parse_args()

    results_path = OUTPUT_DIR / "results.json"
    images_dir = OUTPUT_DIR / "images"

    # ---- Phase 1: Scrape ----
    if not args.skip_scrape:
        products = asyncio.run(
            run_scraper(
                args.query, args.pages, args.country,
                args.language, args.headless,
            )
        )
        if not products:
            print("[!] No products found — nothing to load.")
            sys.exit(1)
    else:
        if not results_path.exists():
            print(f"[!] {results_path} not found. Run the scraper first.")
            sys.exit(1)
        with open(results_path) as f:
            count = len(json.load(f))
        print(f"[*] Skipping scrape — using existing {results_path} ({count} products)")

    # ---- Phase 2: Load into Snowflake ----
    print("\n" + "=" * 60)
    print("  LOADING INTO SNOWFLAKE")
    print("=" * 60 + "\n")

    conn = get_connection()
    try:
        if args.setup:
            run_setup(conn)

        run_id, loaded = upload_and_load(
            conn, args.query, str(results_path), images_dir,
            args.country, args.language,
        )

        print(f"\n{'=' * 60}")
        print(f"  Pipeline complete!")
        print(f"  Snowflake run ID : {run_id}")
        print(f"  Products loaded  : {loaded}")
        print(f"{'=' * 60}")
        print(f"\nQuery your data:")
        print(f"  SELECT * FROM {DATABASE}.{SCHEMA}.PRODUCTS_ANALYSIS")
        print(f"  WHERE RUN_ID = {run_id};")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
