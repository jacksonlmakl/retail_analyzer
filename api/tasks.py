"""Celery tasks for scraping and verification."""

import asyncio
import tempfile
from pathlib import Path

from celery import Celery

app = Celery("retail_analyzer")
app.config_from_object("api.celeryconfig")

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


@app.task(bind=True, name="api.tasks.scrape_and_load")
def scrape_and_load(self, query, pages=1, country="us", language="en"):
    """Scrape Google Shopping and load results into Snowflake."""
    from src.scraper import GoogleShoppingScraper, save_product_images, save_to_json
    from src.loader import get_connection, upload_and_load

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        results_path = tmp_path / "results.json"
        images_dir = tmp_path / "images"

        async def _scrape():
            async with GoogleShoppingScraper(
                country=country,
                language=language,
                headless=False,
            ) as scraper:
                products = await scraper.search(query, max_pages=pages)
                if products:
                    save_product_images(products, str(results_path))
                    save_to_json(products, str(results_path))
                return products

        products = asyncio.run(_scrape())

        if not products:
            return {"run_id": None, "products_loaded": 0, "error": "No products found"}

        conn = get_connection()
        try:
            run_id, loaded = upload_and_load(
                conn, query, str(results_path), str(images_dir),
                country, language,
            )
        finally:
            conn.close()

    return {"run_id": run_id, "products_loaded": loaded}


@app.task(bind=True, name="api.tasks.verify_products")
def verify_products(self, query_filter=None, max_stale_days=7):
    """Verify stale product listings are still live on Google Shopping.

    If query_filter is None, automatically finds all queries with products
    not verified in max_stale_days.
    """
    from src.loader import get_connection, fetch_stale_queries
    from verify_products import run_verification

    if query_filter is None and max_stale_days is not None:
        conn = get_connection()
        try:
            stale_queries = fetch_stale_queries(conn, max_stale_days=max_stale_days)
        finally:
            conn.close()

        if not stale_queries:
            return {"message": "No stale queries found", "queries_checked": 0}

        results = []
        for q in stale_queries:
            summary = run_verification(query_filter=q, headless=False)
            results.append({"query": q, **summary})

        return {
            "queries_checked": len(stale_queries),
            "results": results,
        }

    summary = run_verification(query_filter=query_filter, headless=False)
    return summary
