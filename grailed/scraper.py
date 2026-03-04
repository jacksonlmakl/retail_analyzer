"""Grailed scraper — uses the grailed_api package (Algolia-backed)."""

import json
import os
import re
import requests as req_lib
from dataclasses import dataclass, field, asdict
from pathlib import Path

from grailed_api import GrailedAPIClient

PROXY_URL = os.environ.get("PROXY_URL")
_PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

CONDITION_MAP = {
    "is_new": "New/Never Worn",
    "is_gently_used": "Gently Used",
    "is_used": "Used",
    "is_very_worn": "Very Worn",
    "is_not_specified": "Not Specified",
}

MIME_TO_EXT = {
    "image/webp": ".webp",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
}


@dataclass
class Product:
    title: str = ""
    price: str = ""
    original_price: str = ""
    discount: str = ""
    link: str = ""
    image_url: str = ""
    image_path: str = ""
    designer: str = ""
    condition: str = ""
    size_info: str = ""
    seller_location: str = ""
    department: str = ""
    category: str = ""


def _relaxed_queries(query: str) -> list[str]:
    """Generate progressively shorter query variants for Algolia AND-matching."""
    words = query.strip().split()
    variants = [query]
    while len(words) > 2:
        words = words[:-1]
        variants.append(" ".join(words))
    return variants


def search(query: str, max_pages: int = 1, hits_per_page: int = 40) -> list[Product]:
    """Search Grailed and return Product list. Only products with links are kept.

    If the full query returns zero results, progressively shorter variants
    are tried (dropping trailing words) until results are found.
    """
    client = GrailedAPIClient()
    all_products: list[Product] = []

    effective_query = query
    for variant in _relaxed_queries(query):
        test = client.find_products(
            query_search=variant, sold=False, hits_per_page=5, page=0,
        )
        if test:
            effective_query = variant
            break
        print(f"[*] Grailed: '{variant}' returned 0 results, relaxing query...")
    else:
        print(f"[*] Grailed: no results for any query variant")
        return all_products

    if effective_query != query:
        print(f"[*] Grailed: using relaxed query '{effective_query}'")

    for page_num in range(max_pages):
        print(f"[*] Grailed page {page_num + 1} for '{effective_query}' ...")
        raw = client.find_products(
            query_search=effective_query,
            sold=False,
            hits_per_page=hits_per_page,
            page=page_num,
        )
        if not raw:
            print(f"[*] No more results on page {page_num}")
            break

        for item in raw:
            product = _parse(item)
            if product and product.link:
                all_products.append(product)

        print(f"[+] Page {page_num}: {len(raw)} raw -> {len(all_products)} total with links")

    print(f"\n[+] Total Grailed products: {len(all_products)}")
    return all_products


def _parse(item: dict) -> Product | None:
    title = item.get("title", "").strip()
    if not title:
        return None

    listing_id = item.get("id")
    if not listing_id:
        return None

    price = item.get("price", 0)
    price_drops = item.get("price_drops", [])
    original_price = price_drops[0] if price_drops else None

    discount = ""
    if original_price and original_price > price:
        pct = round((1 - price / original_price) * 100)
        discount = f"{pct}% off"

    condition_raw = item.get("condition", "")
    condition = CONDITION_MAP.get(condition_raw, condition_raw.replace("is_", "").replace("_", " ").title())

    cover = item.get("cover_photo") or {}
    image_url = cover.get("url", "")

    category_path = item.get("category_path", "")
    category = category_path.replace(".", " > ") if category_path else ""

    return Product(
        title=title,
        price=f"${price:,.2f}" if price else "",
        original_price=f"${original_price:,.2f}" if original_price else "",
        discount=discount,
        link=f"https://www.grailed.com/listings/{listing_id}",
        image_url=image_url,
        designer=item.get("designer_names", ""),
        condition=condition,
        size_info=item.get("size", ""),
        seller_location=item.get("location", ""),
        department=item.get("department", ""),
        category=category,
    )


def _slugify(text: str, max_len: int = 60) -> str:
    slug = re.sub(r'[^\w\s-]', '', text.lower())
    slug = re.sub(r'[\s_-]+', '_', slug).strip('_')
    return slug[:max_len]


def save_product_images(products: list[Product], output_path: str):
    output_path = Path(output_path)
    images_dir = output_path.parent / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    session = req_lib.Session()
    if _PROXIES:
        session.proxies.update(_PROXIES)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.grailed.com/",
        "Accept": "image/webp,image/*,*/*",
    })

    saved = 0
    for i, p in enumerate(products):
        if not p.image_url or not p.image_url.startswith("http"):
            continue
        idx = f"{i + 1:03d}"
        slug = _slugify(p.title)
        base = f"{idx}_{slug}" if slug else idx
        try:
            resp = session.get(p.image_url, timeout=15)
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "image/jpeg")
            ext = MIME_TO_EXT.get(ct.split(";")[0].strip(), ".jpg")
            fp = images_dir / f"{base}{ext}"
            fp.write_bytes(resp.content)
            p.image_path = f"images/{fp.name}"
            saved += 1
        except Exception as e:
            print(f"  [!] Image #{i+1}: {e}")

    print(f"[+] Saved {saved} images to {images_dir}/")


def save_to_json(products: list[Product], filepath: str):
    data = [asdict(p) for p in products]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[+] Saved {len(products)} products to {filepath}")
