"""Fashionphile scraper — Shopify JSON API (no browser needed)."""

import json
import re
import requests
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urlencode

BASE_URL = "https://www.fashionphile.com"

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

_CONDITION_KEYWORDS = [
    "new", "excellent", "very good", "good", "shows wear",
    "gently used", "fair", "pristine",
]

_KNOWN_CATEGORIES = {
    "handbags", "wallets", "accessories", "shoes", "jewelry",
    "watches", "belts", "scarves", "clothing", "sunglasses",
    "backpack", "tote", "clutch", "crossbody", "shoulder-bag",
}

_KNOWN_MATERIALS = [
    "monogram canvas", "damier ebene", "damier azur", "damier",
    "epi leather", "empreinte leather", "vernis leather",
    "saffiano leather", "caviar leather", "lambskin",
    "canvas", "leather", "suede", "nylon", "denim", "tweed", "silk",
    "patent leather", "exotic", "crocodile", "python",
]

_KNOWN_COLORS = [
    "black", "white", "brown", "red", "blue", "green", "pink",
    "grey", "gray", "beige", "navy", "purple", "orange", "yellow",
    "gold", "silver", "burgundy", "camel", "cream", "multicolor",
]


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
    """Search Fashionphile via the Shopify JSON suggest API."""
    session = requests.Session()
    session.headers.update(_SESSION_HEADERS)

    all_products: list[Product] = []

    for page_num in range(1, max_pages + 1):
        params = {
            "q": query,
            "resources[type]": "product",
            "resources[limit]": 24,
            "page": page_num,
        }
        url = f"{BASE_URL}/search/suggest.json?{urlencode(params)}"
        print(f"[*] Fashionphile Shopify JSON page {page_num}: {url}")

        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[!] Fashionphile API error: {e}")
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

    print(f"\n[+] Total Fashionphile products: {len(all_products)}")
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

    body = item.get("body", "") or ""
    condition = _extract_condition_from_body(body)

    category = product_type
    color = ""
    material = ""

    if handle:
        slug_lower = handle.lower()
        for mat in _KNOWN_MATERIALS:
            if mat.replace(" ", "-") in slug_lower:
                material = mat.title()
                break
        for c in _KNOWN_COLORS:
            if f"-{c}-" in slug_lower or slug_lower.endswith(f"-{c}"):
                color = c.title()
                break

    if not color:
        color = _extract_color_from_title(title)

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


def _extract_condition_from_body(body: str) -> str:
    body_lower = body.lower()
    for kw in _CONDITION_KEYWORDS:
        idx = body_lower.find(kw)
        if idx != -1:
            snippet = body[max(0, idx - 10):idx + len(kw) + 20]
            if "condition" in snippet.lower() or idx < 100:
                return kw.title()
    return ""


def _extract_color_from_title(title: str) -> str:
    title_lower = title.lower()
    for color in _KNOWN_COLORS:
        if f" {color} " in f" {title_lower} ":
            return color.title()
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
