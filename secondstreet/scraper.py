"""2nd Street USA scraper — Shopify JSON fast path with Playwright fallback."""

import asyncio
import json
import os
import re
import requests as req_lib
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import quote_plus, urlencode

from playwright.async_api import async_playwright

PROFILE_DIR = Path.home() / ".cache" / "secondstreet_scraper" / "browser_profile"
PROXY_URL = os.environ.get("PROXY_URL")

def _parse_proxy(url):
    """Parse http://user:pass@host:port into Playwright proxy dict."""
    if not url:
        return None
    from urllib.parse import urlparse
    p = urlparse(url)
    proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        proxy["username"] = p.username
    if p.password:
        proxy["password"] = p.password
    return proxy
_PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
BASE_URL = "https://ec.2ndstreetusa.com"

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
    brand: str = ""
    condition: str = ""
    category: str = ""
    color: str = ""
    size_info: str = ""


def _try_shopify_json(query: str, max_pages: int = 1) -> list[Product] | None:
    """Attempt to use Shopify's search/suggest JSON endpoint (fast, no browser)."""
    products: list[Product] = []

    for page_num in range(1, max_pages + 1):
        params = {
            "q": query,
            "resources[type]": "product",
            "resources[limit]": 24,
            "page": page_num,
        }
        url = f"{BASE_URL}/search/suggest.json?{urlencode(params)}"
        print(f"[*] 2nd Street Shopify JSON page {page_num}: {url}")

        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            print(f"[!] Shopify JSON failed: {e}")
            return None

        resource_products = (
            data.get("resources", {}).get("results", {}).get("products", [])
        )
        if not resource_products:
            break

        for item in resource_products:
            p = _parse_shopify_product(item)
            if p and p.link:
                products.append(p)

        print(f"[+] Shopify JSON page {page_num}: {len(resource_products)} raw items")

    if not products:
        return None

    print(f"[+] Shopify JSON total: {len(products)} products")
    return products


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

    compare_raw = item.get("compare_at_price", "") or item.get("compare_at_price_max", "")
    original_price = ""
    if compare_raw:
        try:
            val = float(compare_raw)
            if val > 0:
                original_price = f"${val:,.2f}"
        except (ValueError, TypeError):
            pass

    image_url = item.get("image", "") or item.get("featured_image", {}).get("url", "")
    if image_url and image_url.startswith("//"):
        image_url = f"https:{image_url}"

    vendor = item.get("vendor", "")
    category = item.get("type", "") or item.get("product_type", "")

    body_html = item.get("body", "") or item.get("body_html", "")
    condition, color, material, size_info = _parse_body_html(body_html)

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
        brand=vendor,
        condition=condition,
        category=category,
        color=color,
        size_info=size_info,
    )


def _parse_body_html(html: str) -> tuple[str, str, str, str]:
    """Extract condition, color, material, and size from the product body HTML table."""
    if not html:
        return "", "", "", ""

    condition = ""
    color = ""
    material = ""
    size_info = ""

    flat = re.sub(r'\s+', ' ', html)

    def _extract_table_value(label: str) -> str:
        pattern = rf'{label}\s*:?\s*(?:</\w+>\s*)*(?:</\w+>\s*)*</td>\s*<td[^>]*>\s*(?:<[^>]*>\s*)*([^<]+)'
        m = re.search(pattern, flat, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return ""

    raw_cond = _extract_table_value("Condition")
    if raw_cond:
        raw_cond = re.sub(r'^\[.\]\s*', '', raw_cond).strip()
        raw_cond = raw_cond.replace("Used_", "Used - ").replace("Used:", "Used -").replace("Used_ ", "Used - ")
        raw_cond = re.sub(r'\s+', ' ', raw_cond).strip()
        if raw_cond and len(raw_cond) > 1:
            condition = raw_cond

    color = _extract_table_value("Color")
    material = _extract_table_value("Material")
    size_info = _extract_table_value("Measurements?")

    return condition, color, material, size_info


def _extract_color_from_title(title: str) -> str:
    """Fall back to extracting color abbreviation from the title slash-format."""
    colors = {
        "BLK": "Black", "WHT": "White", "BRN": "Brown", "RED": "Red",
        "BLU": "Blue", "GRN": "Green", "PNK": "Pink", "GRY": "Gray",
        "BGE": "Beige", "NVY": "Navy", "ORG": "Orange", "YLW": "Yellow",
        "PRP": "Purple", "GLD": "Gold", "SLV": "Silver",
    }
    parts = [p.strip() for p in title.split("/") if p.strip()]
    for part in reversed(parts):
        if part.upper() in colors:
            return colors[part.upper()]
    return ""


class SecondStreetScraper:
    """Playwright fallback scraper for 2nd Street USA."""

    def __init__(self, headless=False):
        self.headless = headless
        self._playwright = None
        self._context = None

    async def start(self):
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        self._playwright = await async_playwright().start()
        launch_opts = dict(
            user_data_dir=str(PROFILE_DIR),
            headless=self.headless,
            viewport={"width": 1280, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        proxy = _parse_proxy(PROXY_URL)
        if proxy:
            launch_opts["proxy"] = proxy
        self._context = await self._playwright.chromium.launch_persistent_context(**launch_opts)

    async def stop(self):
        if self._context:
            await self._context.close()
        if self._playwright:
            await self._playwright.stop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.stop()

    async def search(self, query: str, max_pages: int = 1) -> list[Product]:
        all_products: list[Product] = []
        page = self._context.pages[0] if self._context.pages else await self._context.new_page()

        for page_num in range(1, max_pages + 1):
            url = f"{BASE_URL}/pages/search-results-page?q={quote_plus(query)}"
            if page_num > 1:
                url += f"&page={page_num}"

            print(f"[*] 2nd Street Playwright page {page_num}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(800)

            products = await self._extract_products(page)
            if not products:
                print(f"[*] No results on page {page_num}")
                break

            with_links = [p for p in products if p.link]
            dropped = len(products) - len(with_links)
            if dropped:
                print(f"[*] Dropped {dropped} products without links")
            all_products.extend(with_links)
            print(f"[+] Page {page_num}: {len(with_links)} products with links")

        print(f"\n[+] Total 2nd Street products: {len(all_products)}")
        return all_products

    async def _extract_products(self, page) -> list[Product]:
        raw = await page.evaluate("""() => {
            const results = [];
            const seen = new Set();

            // 2nd Street product links
            const cards = document.querySelectorAll(
                'a[href*="/products/"], .product-card a[href], .snize-product a[href]'
            );

            cards.forEach(card => {
                const href = card.getAttribute('href') || '';
                if (!href || seen.has(href) || !href.includes('/products/')) return;
                seen.add(href);

                const container = card.closest('.snize-product') || card.closest('.product-card') || card.parentElement?.parentElement || card;
                const text = (container.innerText || '').trim();
                const img = container.querySelector('img');
                results.push({
                    href: href,
                    text: text,
                    imgSrc: img ? (img.src || img.dataset.src || '') : ''
                });
            });

            return results;
        }""")

        products = []
        for item in raw:
            href = item.get("href", "")
            link = href if href.startswith("http") else f"{BASE_URL}{href}"
            p = self._parse_card(item.get("text", ""), link)
            if p:
                p.image_url = item.get("imgSrc", "")
                products.append(p)
        return products

    def _parse_card(self, text: str, link: str) -> Product | None:
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        if not lines:
            return None

        p = Product(link=link)

        for line in lines:
            price_m = re.search(r'\$[\d,.]+', line)
            if price_m:
                if not p.price:
                    p.price = price_m.group()
                elif not p.original_price:
                    p.original_price = price_m.group()
                continue

            if not p.title and len(line) > 3 and not line.startswith("$"):
                p.title = line
                continue

            if not p.brand and len(line) < 40:
                p.brand = line
                continue

        return p if p.title else None


def search(query: str, max_pages: int = 1, headless: bool = False) -> list[Product]:
    """Try Shopify JSON first, fall back to Playwright DOM scraping."""
    products = _try_shopify_json(query, max_pages)
    if products:
        return products

    print("[*] Shopify JSON unavailable, falling back to Playwright ...")

    async def _pw():
        async with SecondStreetScraper(headless=headless) as scraper:
            return await scraper.search(query, max_pages)

    return asyncio.run(_pw())


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
