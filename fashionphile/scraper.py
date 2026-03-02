"""Fashionphile scraper — Playwright-based search."""

import asyncio
import json
import re
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import quote_plus

from playwright.async_api import async_playwright

PROFILE_DIR = Path.home() / ".cache" / "fashionphile_scraper" / "browser_profile"
BASE_URL = "https://www.fashionphile.com"

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
    category: str = ""
    color: str = ""
    material: str = ""


class FashionphileScraper:
    def __init__(self, headless=False):
        self.headless = headless
        self._playwright = None
        self._context = None

    async def start(self):
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        self._playwright = await async_playwright().start()
        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=self.headless,
            viewport={"width": 1280, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )

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
            url = f"{BASE_URL}/search?q={quote_plus(query)}"
            if page_num > 1:
                url += f"&page={page_num}"

            print(f"[*] Fashionphile page {page_num}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)

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

        print(f"\n[+] Total Fashionphile products: {len(all_products)}")
        return all_products

    async def _extract_products(self, page) -> list[Product]:
        raw = await page.evaluate("""() => {
            function bestImgSrc(el) {
                if (!el) return '';
                const img = el.querySelector('img.ais-hit--picture, img.fp-injected-primary-image, img');
                if (!img) return '';
                const cur = img.currentSrc || '';
                if (cur.startsWith('http')) return cur;
                if (img.src && img.src.startsWith('http')) return img.src;
                if (img.dataset.src) return img.dataset.src;
                return '';
            }

            const results = [];
            const seen = new Set();

            // Fashionphile uses Shopify + Algolia: cards are .fp-algolia-product-card
            const cards = document.querySelectorAll(
                '.fp-algolia-product-card, .product-card-wrapper'
            );

            cards.forEach(card => {
                const a = card.querySelector('a[href*="/products/"]');
                if (!a) return;
                const href = a.getAttribute('href') || '';
                if (!href || seen.has(href)) return;
                seen.add(href);

                const text = (card.innerText || '').trim();
                const imgSrc = bestImgSrc(card);
                results.push({ href, text, imgSrc });
            });

            // Fallback: direct link selection
            if (results.length === 0) {
                document.querySelectorAll('a[href*="/products/"]').forEach(a => {
                    const href = a.getAttribute('href') || '';
                    if (!href || seen.has(href)) return;
                    seen.add(href);
                    const container = a.closest('.fp-algolia-product-card, .card-wrapper, [class*="product-card"]')
                        || a.parentElement?.parentElement || a;
                    const text = (container.innerText || '').trim();
                    if (text.length > 5) {
                        const imgSrc = bestImgSrc(container) || bestImgSrc(a);
                        results.push({ href, text, imgSrc });
                    }
                });
            }

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

    _CONDITION_KEYWORDS = [
        "new", "excellent", "very good", "good", "shows wear",
        "gently used", "fair", "pristine",
    ]

    def _parse_card(self, text: str, link: str) -> Product | None:
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        if not lines:
            return None

        p = Product(link=link)

        for line in lines:
            if re.match(r'^\d+$', line):
                continue

            price_m = re.search(r'\$[\d,.]+', line)
            if price_m:
                if not p.price:
                    p.price = price_m.group()
                elif not p.original_price:
                    p.original_price = price_m.group()
                continue

            line_l = line.lower()
            if any(kw in line_l for kw in self._CONDITION_KEYWORDS):
                if not p.condition:
                    p.condition = line
                    continue

            if line_l in ("add to wishlist", "wishlist", "sale", "sold"):
                continue

            if not p.designer and len(line) < 50 and line.isupper():
                p.designer = line
                continue

            if not p.title and len(line) > 3:
                p.title = line
                continue

        if not p.title and p.designer:
            p.title = p.designer

        self._extract_from_url(link, p)

        if p.price and p.original_price:
            try:
                cur = float(p.price.replace("$", "").replace(",", ""))
                orig = float(p.original_price.replace("$", "").replace(",", ""))
                if orig > cur:
                    pct = round((1 - cur / orig) * 100)
                    p.discount = f"{pct}% off"
            except ValueError:
                pass

        return p if p.title else None

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

    def _extract_from_url(self, link: str, p: Product):
        slug = link.rstrip("/").rsplit("/", 1)[-1] if "/products/" in link else ""
        if not slug:
            return
        slug_lower = slug.lower()

        for cat in self._KNOWN_CATEGORIES:
            if cat in slug_lower:
                p.category = cat.replace("-", " ").title()
                break

        for mat in self._KNOWN_MATERIALS:
            if mat.replace(" ", "-") in slug_lower:
                p.material = mat.title()
                break

        for color in self._KNOWN_COLORS:
            if f"-{color}-" in slug_lower or slug_lower.startswith(color + "-"):
                p.color = color.title()
                break


def _slugify(text: str, max_len: int = 60) -> str:
    slug = re.sub(r'[^\w\s-]', '', text.lower())
    slug = re.sub(r'[\s_-]+', '_', slug).strip('_')
    return slug[:max_len]


def save_product_images(products: list[Product], output_path: str):
    output_path = Path(output_path)
    images_dir = output_path.parent / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    for i, p in enumerate(products):
        if not p.image_url or not p.image_url.startswith("http"):
            continue
        idx = f"{i + 1:03d}"
        slug = _slugify(p.title)
        base = f"{idx}_{slug}" if slug else idx
        try:
            req = urllib.request.Request(p.image_url, headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": BASE_URL + "/",
                "Accept": "image/webp,image/*,*/*",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                ct = resp.headers.get("Content-Type", "image/jpeg")
                ext = MIME_TO_EXT.get(ct.split(";")[0].strip(), ".jpg")
                fp = images_dir / f"{base}{ext}"
                fp.write_bytes(resp.read())
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
