"""Vestiaire Collective scraper — Playwright with stealth + cloudscraper image downloads."""

import asyncio
import json
import re
import cloudscraper
from dataclasses import dataclass, field, asdict
from pathlib import Path
from urllib.parse import quote_plus

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

PROFILE_DIR = Path.home() / ".cache" / "vestiaire_scraper" / "browser_profile"
BASE_URL = "https://us.vestiairecollective.com"

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


class VestiaireScraper:
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
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        await Stealth().apply_stealth_async(self._context)

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
            url = f"{BASE_URL}/search/?q={quote_plus(query)}"
            if page_num > 1:
                url += f"&page={page_num}"

            print(f"[*] Vestiaire page {page_num}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)

            try:
                cookie_btn = page.locator("#popin_tc_privacy_button_2, button:has-text('Accept')")
                if await cookie_btn.count() > 0:
                    await cookie_btn.first.click()
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

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

        print(f"\n[+] Total Vestiaire products: {len(all_products)}")
        return all_products

    async def _extract_products(self, page) -> list[Product]:
        raw = await page.evaluate("""() => {
            const results = [];
            const seen = new Set();

            function bestImgSrc(el) {
                if (!el) return '';
                const img = el.querySelector('img');
                if (!img) return '';
                let url = '';
                if (img.src && img.src.startsWith('http')) url = img.src;
                else if (img.currentSrc && img.currentSrc.startsWith('http')) url = img.currentSrc;
                else if (img.dataset.src) url = img.dataset.src;
                if (url) {
                    url = url.replace(/w=64/, 'w=600').replace(/w=96/, 'w=600')
                             .replace(/w=128/, 'w=600').replace(/w=256/, 'w=600');
                }
                return url;
            }

            function tryAdd(a) {
                const href = a.getAttribute('href') || '';
                if (!href || seen.has(href)) return;
                if (!href.match(/\\/[\\w-]+-\\d+\\.shtml/) && !href.includes('/product/')) return;
                seen.add(href);
                const text = (a.innerText || '').trim();
                if (text.length < 5) return;
                let imgSrc = bestImgSrc(a);
                if (!imgSrc) {
                    const container = a.closest('[class*="product"]') || a.parentElement;
                    imgSrc = bestImgSrc(container);
                }
                results.push({href, text, imgSrc});
            }

            document.querySelectorAll('a[data-testid="product-card"]').forEach(tryAdd);
            if (results.length === 0) {
                document.querySelectorAll('a[href]').forEach(tryAdd);
            }
            return results;
        }""")

        products = []
        for item in raw:
            href = item.get("href", "")
            link = href if href.startswith("http") else f"{BASE_URL}{href}"
            p = self._parse_card(item.get("text", ""), link)
            if p:
                img = item.get("imgSrc", "")
                if img and not img.startswith("http"):
                    img = f"https://images.vestiairecollective.com/images/resized/w=600,q=75,f=auto,{img}"
                p.image_url = img
                products.append(p)

        return products

    def _parse_card(self, text: str, link: str) -> Product | None:
        lines = [ln.strip().replace("\xa0", " ").strip() for ln in text.split("\n") if ln.strip()]
        if not lines:
            return None

        p = Product(link=link)

        for line in lines:
            line_l = line.lower()

            if line_l in ("united states", "france", "italy", "spain", "germany", "united kingdom"):
                continue

            price_m = re.search(r'\$[\d,.]+', line)
            if price_m:
                if not p.price:
                    p.price = price_m.group()
                elif not p.original_price:
                    p.original_price = price_m.group()
                continue

            if any(kw in line_l for kw in ["like new", "good condition", "fair condition", "never worn", "very good"]):
                p.condition = line
                continue

            size_m = re.match(r'^(\d+\s+(?:FR|EU|US|UK|IT)|(?:XXS|XS|S|M|L|XL|XXL|XXXL)\s+International)', line)
            if size_m:
                p.size_info = size_m.group().strip()
                continue

            if not p.designer and len(line) < 60 and not line.startswith("$"):
                p.designer = line
                continue

            if not p.title and len(line) > 3:
                p.title = line
                continue

        if not p.title and p.designer:
            p.title = p.designer

        if not p.title:
            return None

        self._extract_from_url(link, p)
        return p

    def _extract_from_url(self, link: str, p: Product):
        path = link.split("vestiairecollective.com")[-1] if "vestiairecollective.com" in link else ""
        if not path:
            return

        parts = [s for s in path.strip("/").split("/") if s]
        if len(parts) >= 2:
            p.category = parts[1].replace("-", " ").title()

        filename = parts[-1] if parts else ""
        slug = filename.rsplit("-", 1)[0] if filename.endswith(".shtml") else filename

        known_colors = [
            "black", "white", "brown", "red", "blue", "green", "pink",
            "grey", "gray", "beige", "navy", "purple", "orange", "yellow",
            "gold", "silver", "burgundy", "camel", "khaki", "ecru", "multicolour",
        ]
        known_materials = [
            "leather", "canvas", "cloth", "cotton", "silk", "wool",
            "synthetic", "denim", "suede", "patent leather", "tweed",
            "cashmere", "polyester", "nylon", "linen", "velvet",
        ]

        slug_lower = slug.lower()
        for color in known_colors:
            if slug_lower.startswith(color + "-"):
                p.color = color.title()
                break

        for mat in known_materials:
            if mat.replace(" ", "-") in slug_lower:
                p.material = mat.title()
                break


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
