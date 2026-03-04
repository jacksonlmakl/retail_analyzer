"""Farfetch scraper — Playwright-based with bot-protection handling."""

import asyncio
import json
import os
import re
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import quote_plus

import requests as req_lib

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

PROFILE_DIR = Path.home() / ".cache" / "farfetch_scraper" / "browser_profile"
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
BASE_URL = "https://www.farfetch.com"

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
    pre_owned: bool = False
    category: str = ""
    color: str = ""


class FarfetchScraper:
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
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        proxy = _parse_proxy(PROXY_URL)
        if proxy:
            launch_opts["proxy"] = proxy
        self._context = await self._playwright.chromium.launch_persistent_context(**launch_opts)
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
            url = f"{BASE_URL}/shopping/women/search/items.aspx?q={quote_plus(query)}&page={page_num}"

            print(f"[*] Farfetch page {page_num}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            # Dismiss cookie popup
            try:
                cookie_btn = page.locator("button[data-testid='cookie-banner-accept'], button:has-text('Accept')")
                if await cookie_btn.count() > 0:
                    await cookie_btn.first.click()
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

            for _ in range(4):
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

        print(f"\n[+] Total Farfetch products: {len(all_products)}")
        return all_products

    async def download_images(self, products: list[Product], images_dir: Path):
        """Download product images via full browser navigation (bypasses CDN bot detection)."""
        images_dir.mkdir(parents=True, exist_ok=True)
        dl_page = await self._context.new_page()
        await dl_page.set_extra_http_headers({
            "Referer": BASE_URL + "/",
            "Accept": "image/webp,image/*,*/*",
        })
        saved = 0
        for i, p in enumerate(products):
            if not p.image_url or not p.image_url.startswith("http"):
                continue
            try:
                resp = await dl_page.goto(p.image_url, timeout=15000, wait_until="load")
                if resp and resp.ok:
                    ct = resp.headers.get("content-type", "image/jpeg")
                    ext = MIME_TO_EXT.get(ct.split(";")[0].strip(), ".jpg")
                    slug = _slugify(p.title)
                    fname = f"{i+1:03d}_{slug}{ext}" if slug else f"{i+1:03d}{ext}"
                    fp = images_dir / fname
                    fp.write_bytes(await resp.body())
                    p.image_path = f"images/{fp.name}"
                    saved += 1
                elif resp:
                    print(f"  [!] Image #{i+1}: HTTP {resp.status}")
            except Exception as e:
                print(f"  [!] Image #{i+1}: {e}")
        await dl_page.close()
        print(f"[+] Saved {saved} images to {images_dir}/")

    async def _extract_products(self, page) -> list[Product]:
        raw = await page.evaluate("""() => {
            function bestImgSrc(el) {
                if (!el) return '';
                const img = el.querySelector('img');
                if (!img) return '';
                const cur = img.currentSrc || '';
                if (cur.startsWith('http')) return cur;
                if (img.src && img.src.startsWith('http')) return img.src;
                if (img.dataset.src) return img.dataset.src;
                return '';
            }

            const results = [];
            const seen = new Set();

            const cards = document.querySelectorAll(
                'a[href*="/shopping/"][href*="/items-"], a[data-component="ProductCardLink"]'
            );

            cards.forEach(card => {
                const href = card.getAttribute('href') || '';
                if (!href || seen.has(href)) return;
                seen.add(href);

                const container = card.closest('[data-component="ProductCard"]') || card.parentElement?.parentElement || card;
                const text = (container.innerText || '').trim();
                let imgSrc = bestImgSrc(container);
                if (!imgSrc) imgSrc = bestImgSrc(card);
                results.push({ href, text, imgSrc });
            });

            if (results.length === 0) {
                document.querySelectorAll('a[href]').forEach(a => {
                    const href = a.getAttribute('href') || '';
                    if (href.match(/\\/shopping\\/.*item/) && !seen.has(href)) {
                        seen.add(href);
                        const container = a.closest('[data-component="ProductCard"]') || a.parentElement?.parentElement || a;
                        const text = (container.innerText || '').trim();
                        if (text.length > 5) {
                            const imgSrc = bestImgSrc(container) || bestImgSrc(a);
                            results.push({ href, text, imgSrc });
                        }
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

    def _parse_card(self, text: str, link: str) -> Product | None:
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        if not lines:
            return None

        p = Product(link=link)

        if "pre-owned" in text.lower():
            p.pre_owned = True

        for line in lines:
            line_l = line.lower()

            price_m = re.search(r'\$[\d,.]+', line)
            if price_m:
                if not p.price:
                    p.price = price_m.group()
                elif not p.original_price:
                    p.original_price = price_m.group()
                continue

            pct_m = re.search(r'(\d+)%\s*off', line_l)
            if pct_m:
                p.discount = f"{pct_m.group(1)}% off"
                continue

            if not p.designer and len(line) < 50 and not line.startswith("$"):
                p.designer = line
                continue

            if not p.title and len(line) > 3:
                p.title = line
                continue

        if not p.title and p.designer:
            p.title = p.designer

        return p if p.title else None


def _slugify(text: str, max_len: int = 60) -> str:
    slug = re.sub(r'[^\w\s-]', '', text.lower())
    slug = re.sub(r'[\s_-]+', '_', slug).strip('_')
    return slug[:max_len]


def save_product_images(products: list[Product], output_path: str, max_seconds: int = 300):
    import time

    output_path = Path(output_path)
    images_dir = output_path.parent / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    session = req_lib.Session()
    if PROXY_URL:
        session.proxies.update({"http": PROXY_URL, "https": PROXY_URL})
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": BASE_URL + "/",
        "Accept": "image/webp,image/*,*/*",
    })

    saved = 0
    start = time.monotonic()
    for i, p in enumerate(products):
        if time.monotonic() - start > max_seconds:
            print(f"  [!] Image download budget ({max_seconds}s) exceeded, skipping remaining")
            break
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
