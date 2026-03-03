"""Celery tasks for Farfetch scraping."""

import asyncio
import tempfile
from pathlib import Path

from celery import Celery

app = Celery("farfetch")
app.config_from_object("farfetch.celeryconfig")


@app.task(bind=True, name="farfetch.tasks.scrape_and_load")
def scrape_and_load(self, query, pages=1, country="us", language="en"):
    from farfetch.scraper import FarfetchScraper, save_product_images, save_to_json
    from farfetch.loader import get_connection, upload_and_load

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        results_path = tmp_path / "results.json"
        images_dir = tmp_path / "images"

        async def _scrape():
            async with FarfetchScraper(headless=False) as scraper:
                return await scraper.search(query, max_pages=pages)

        products = asyncio.run(_scrape())

        if not products:
            return {"run_id": None, "products_loaded": 0, "error": "No products found"}

        save_product_images(products, str(results_path))
        save_to_json(products, str(results_path))

        conn = get_connection()
        try:
            run_id, loaded = upload_and_load(conn, query, str(results_path), str(images_dir), country, language)
        finally:
            conn.close()

    return {"run_id": run_id, "products_loaded": loaded}
