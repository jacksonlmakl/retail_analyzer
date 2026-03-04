"""Rebag scraper — Shopify JSON API (no browser needed)."""

import json
import os
import re
import requests
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urlencode

BASE_URL = "https://shop.rebag.com"
PROXY_URL = os.environ.get("PROXY_URL")
_PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

MIME_TO_EXT = {
    "image/webp": ".webp",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
}

_SESSION_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

_KNOWN_CATEGORIES = {
    "handbags", "accessories", "wallets", "shoes", "jewelry",
    "watches", "belts", "scarves", "clothing", "sunglasses",
}

_KNOWN_MATERIALS = [
    "monogram canvas", "damier ebene", "damier azur", "damier",
    "epi leather", "empreinte leather", "vernis leather", "taiga leather",
    "saffiano leather", "caviar leather", "lambskin leather",
    "canvas", "leather", "suede", "nylon", "denim", "tweed", "silk",
    "patent leather", "exotic leather", "crocodile", "python",
]

_CONDITION_TAGS = {
    "excellent": "Excellent",
    "very-good": "Very Good",
    "good": "Good",
    "fair": "Fair",
    "pristine": "Pristine",
    "gently-used": "Gently Used",
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
    category: str = ""
    color: str = ""
    material: str = ""


def search(query: str, max_pages: int = 1) -> list[Product]:
    """Search Rebag via the Shopify JSON suggest API."""
    session = requests.Session()
    session.headers.update(_SESSION_HEADERS)
    if _PROXIES:
        session.proxies.update(_PROXIES)

    all_products: list[Product] = []

    for page_num in range(1, max_pages + 1):
        params = {
            "q": query,
            "resources[type]": "product",
            "resources[limit]": 24,
            "page": page_num,
        }
        url = f"{BASE_URL}/search/suggest.json?{urlencode(params)}"
        print(f"[*] Rebag Shopify JSON page {page_num}: {url}")

        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[!] Rebag API error: {e}")
            break

        raw_products = data.get("resources", {}).get("results", {}).get("products", [])
        if not raw_products:
            print(f"[*] No more results on page {page_num}")
            break

        for item in raw_products:
            p = _parse_shopify_product(item)
            if p and p.link:
                all_products.append(p)

        print(f"[+] Page {page_num}: {len(raw_products)} raw -> {len(all_products)} total")

    print(f"\n[+] Total Rebag products: {len(all_products)}")
    return all_products


def _parse_shopify_product(item: dict) -> Product | None:
    title = item.get("title", "").strip()
    if not title:
        return None

    handle = item.get("handle", "")
    link = f"{BASE_URL}/products/{handle}" if handle else item.get("url", "")
    if link and not link.startswith("http"):
        link = f"{BASE_URL}{link}"
    if not link:
        return None

    price_raw = item.get("price", "")
    price = ""
    if price_raw:
        try:
            price = f"${float(price_raw):,.2f}"
        except (ValueError, TypeError):
            price = str(price_raw)

    compare_raw = item.get("compare_at_price_max", "") or item.get("compare_at_price_min", "")
    original_price = ""
    if compare_raw:
        try:
            val = float(compare_raw)
            if val > 0:
                original_price = f"${val:,.2f}"
        except (ValueError, TypeError):
            pass

    image_url = item.get("image", "") or ""
    fi = item.get("featured_image", {})
    if not image_url and isinstance(fi, dict):
        image_url = fi.get("url", "")
    if image_url and image_url.startswith("//"):
        image_url = f"https:{image_url}"

    vendor = item.get("vendor", "")
    product_type = item.get("type", "")

    tags = item.get("tags", [])
    condition, color, material, category = _parse_tags(tags)

    if not category and product_type:
        category = product_type

    if not category:
        _extract_category_from_handle(handle)

    if not material:
        _extract_material_from_title(title)

    discount = ""
    if original_price and price:
        try:
            orig_val = float(original_price.replace("$", "").replace(",", ""))
            price_val = float(price.replace("$", "").replace(",", ""))
            if orig_val > price_val > 0:
                pct = round((1 - price_val / orig_val) * 100)
                discount = f"{pct}% off"
        except (ValueError, TypeError):
            pass

    return Product(
        title=title,
        price=price,
        original_price=original_price,
        discount=discount,
        link=link,
        image_url=image_url,
        designer=vendor,
        condition=condition,
        category=category,
        color=color,
        material=material,
    )


def _parse_tags(tags: list) -> tuple[str, str, str, str]:
    """Extract condition, color, material, and category from Shopify product tags."""
    condition = ""
    color = ""
    material = ""
    category = ""

    for tag in tags:
        tag_lower = tag.lower().strip()

        if tag_lower.startswith("bc-filter-exterior-color-"):
            color = tag.split("-")[-1].title()
        elif tag_lower.startswith("bc-filter-condition-"):
            raw_cond = tag.split("bc-filter-condition-", 1)[-1]
            condition = raw_cond.replace("-", " ").title()
        elif tag_lower.startswith("bc-filter-material-"):
            material = tag.split("bc-filter-material-", 1)[-1].replace("-", " ").title()
        elif tag_lower.startswith("bc-filter-category-"):
            category = tag.split("bc-filter-category-", 1)[-1].replace("-", " ").title()

    return condition, color, material, category


def _extract_category_from_handle(handle: str) -> str:
    slug_lower = handle.lower()
    for cat in _KNOWN_CATEGORIES:
        if slug_lower.startswith(cat + "-"):
            return cat.title()
    return ""


def _extract_material_from_title(title: str) -> str:
    title_lower = title.lower()
    for mat in _KNOWN_MATERIALS:
        if mat in title_lower:
            return mat.title()
    return ""


def _slugify(text: str, max_len: int = 60) -> str:
    slug = re.sub(r'[^\w\s-]', '', text.lower())
    slug = re.sub(r'[\s_-]+', '_', slug).strip('_')
    return slug[:max_len]


def save_product_images(products: list[Product], output_path: str):
    output_path = Path(output_path)
    images_dir = output_path.parent / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    if _PROXIES:
        session.proxies.update(_PROXIES)
    session.headers.update({
        "User-Agent": _SESSION_HEADERS["User-Agent"],
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
