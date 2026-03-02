"""Celery tasks for 2nd Street USA scraping."""

import tempfile
from pathlib import Path

from celery import Celery

app = Celery("secondstreet")
app.config_from_object("secondstreet.celeryconfig")


@app.task(bind=True, name="secondstreet.tasks.scrape_and_load")
def scrape_and_load(self, query, pages=1, country="us", language="en"):
    from secondstreet.scraper import search, save_product_images, save_to_json
    from secondstreet.loader import get_connection, upload_and_load

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        results_path = tmp_path / "results.json"
        images_dir = tmp_path / "images"

        products = search(query, max_pages=pages, headless=False)
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
