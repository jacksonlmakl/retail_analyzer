"""Vestiaire Collective scraper — direct search API + cloudscraper for Cloudflare bypass."""

import json
import os
import re
import uuid
import cloudscraper
from dataclasses import dataclass, field, asdict
from pathlib import Path

BASE_URL = "https://us.vestiairecollective.com"
SEARCH_API = "https://search.vestiairecollective.com/v1/product/search"
IMAGE_CDN = "https://images.vestiairecollective.com/images/resized/w=600,q=75,f=auto"

PROXY_URL = os.environ.get("PROXY_URL")
_PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

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
    material: str = ""
    category: str = ""
    color: str = ""


_SEARCH_FIELDS = [
    "name", "description", "brand", "model", "country", "price", "discount",
    "link", "sold", "likes", "pictures", "colors", "size", "stock",
    "universeId", "createdAt", "condition", "categoryLvl0",
]

_SEARCH_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/search/",
    "x-usecase": "plpStandard",
}


def search(query: str, max_pages: int = 1) -> list[Product]:
    """Search Vestiaire via the internal search API."""
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "desktop": True}
    )
    if _PROXIES:
        session.proxies.update(_PROXIES)

    session_id = str(uuid.uuid4())
    device_id = str(uuid.uuid4())
    all_products: list[Product] = []
    per_page = 60

    for page_num in range(max_pages):
        offset = page_num * per_page
        payload = {
            "pagination": {"offset": offset, "limit": per_page},
            "fields": _SEARCH_FIELDS,
            "q": query,
            "sortBy": "relevance",
            "filters": {},
            "locale": {"country": "US", "currency": "USD", "language": "us", "sizeType": "US"},
        }
        headers = {
            **_SEARCH_HEADERS,
            "x-search-session-id": session_id,
            "x-search-query-id": str(uuid.uuid4()),
            "x-deviceid": device_id,
        }

        print(f"[*] Vestiaire API page {page_num + 1} (offset={offset})")
        try:
            resp = session.post(SEARCH_API, json=payload, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[!] Vestiaire API error: {e}")
            break

        items = data.get("items", [])
        if not items:
            print(f"[*] No more results")
            break

        for item in items:
            p = _parse_item(item)
            if p and p.link:
                all_products.append(p)

        total = data.get("paginationStats", {}).get("total", 0)
        print(f"[+] Page {page_num + 1}: {len(items)} items (total available: {total})")

        if offset + per_page >= total:
            break

    print(f"\n[+] Total Vestiaire products: {len(all_products)}")
    return all_products


def _parse_item(item: dict) -> Product | None:
    name = item.get("name", "").strip()
    if not name:
        return None

    brand = item.get("brand", {})
    brand_name = brand.get("name", "") if isinstance(brand, dict) else str(brand)

    price_obj = item.get("price", {})
    cents = price_obj.get("cents", 0)
    currency = price_obj.get("currency", "USD")
    price = f"${cents / 100:,.2f}" if currency == "USD" else f"{cents / 100:,.2f} {currency}"

    discount_obj = item.get("discount", {})
    discount = ""
    if isinstance(discount_obj, dict) and discount_obj.get("percentage"):
        discount = f"{discount_obj['percentage']}% off"
        orig_cents = discount_obj.get("originalPrice", {}).get("cents", 0)
        if orig_cents:
            original_price = f"${orig_cents / 100:,.2f}" if currency == "USD" else f"{orig_cents / 100:,.2f} {currency}"
        else:
            original_price = ""
    else:
        original_price = ""

    link_path = item.get("link", "")
    link = f"{BASE_URL}{link_path}" if link_path and not link_path.startswith("http") else link_path

    pics = item.get("pictures", [])
    image_url = ""
    if pics:
        img_path = pics[0] if isinstance(pics[0], str) else pics[0].get("path", "")
        if img_path:
            image_url = f"{IMAGE_CDN}{img_path}" if not img_path.startswith("http") else img_path

    colors = item.get("colors", {})
    color = ""
    if isinstance(colors, dict):
        all_colors = colors.get("all", [])
        if all_colors and isinstance(all_colors[0], dict):
            color = all_colors[0].get("name", "")

    size_obj = item.get("size", {})
    size_info = size_obj.get("label", "") if isinstance(size_obj, dict) else ""

    condition_val = item.get("condition", "")
    condition = ""
    if isinstance(condition_val, dict):
        condition = condition_val.get("description", condition_val.get("name", ""))
    elif isinstance(condition_val, str):
        condition = condition_val

    category = ""
    cat_val = item.get("categoryLvl0", "")
    if isinstance(cat_val, dict):
        category = cat_val.get("name", "")
    elif isinstance(cat_val, str):
        category = cat_val

    return Product(
        title=name,
        price=price,
        original_price=original_price,
        discount=discount,
        link=link,
        image_url=image_url,
        designer=brand_name,
        condition=condition,
        size_info=size_info,
        color=color,
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

    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "desktop": True}
    )
    if _PROXIES:
        session.proxies.update(_PROXIES)
    session.headers.update({
        "Referer": BASE_URL + "/",
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
            resp = session.get(p.image_url, timeout=10)
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
