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
            // Try __NEXT_DATA__ first
            const ndScript = document.querySelector('script#__NEXT_DATA__');
            if (ndScript) {
                try {
                    const nd = JSON.parse(ndScript.textContent);
                    const pp = nd?.props?.pageProps;
                    const items = pp?.listingItems || pp?.products || pp?.initialData?.products || [];
                    if (items.length > 0) {
                        return { source: 'next_data', items };
                    }
                } catch(e) {}
            }

            // Try JSON-LD structured data
            const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
            for (const s of ldScripts) {
                try {
                    const ld = JSON.parse(s.textContent);
                    if (ld['@type'] === 'ItemList' && ld.itemListElement?.length > 0) {
                        return { source: 'json_ld', items: ld.itemListElement };
                    }
                } catch(e) {}
            }

            // Fallback: DOM scraping with better image extraction
            function bestImgSrc(el) {
                if (!el) return '';
                for (const img of el.querySelectorAll('img')) {
                    const srcset = img.getAttribute('srcset') || '';
                    if (srcset) {
                        const best = srcset.split(',').pop().trim().split(' ')[0];
                        if (best.startsWith('http')) return best;
                    }
                    const src = img.currentSrc || img.src || img.dataset.src || '';
                    if (src.startsWith('http')) return src;
                }
                return '';
            }

            const results = [];
            const seen = new Set();
            const cards = document.querySelectorAll(
                'a[href*="/shopping/"][href*="/items-"], a[data-component="ProductCardLink"], a[href*="/shopping/"][href*="item-"]'
            );
            cards.forEach(card => {
                const href = card.getAttribute('href') || '';
                if (!href || seen.has(href)) return;
                seen.add(href);
                const container = card.closest('[data-component="ProductCard"]')
                               || card.closest('[class*="ProductCard"]')
                               || card.parentElement?.parentElement || card;
                const text = (container.innerText || '').trim();
                const imgSrc = bestImgSrc(container) || bestImgSrc(card);
                if (text.length > 3) results.push({ href, text, imgSrc });
            });

            if (results.length === 0) {
                document.querySelectorAll('a[href]').forEach(a => {
                    const href = a.getAttribute('href') || '';
                    if ((href.includes('/shopping/') && href.includes('item')) && !seen.has(href)) {
                        seen.add(href);
                        const container = a.closest('[data-component="ProductCard"]') || a.parentElement?.parentElement || a;
                        const text = (container.innerText || '').trim();
                        if (text.length > 3) {
                            const imgSrc = bestImgSrc(container) || bestImgSrc(a);
                            results.push({ href, text, imgSrc });
                        }
                    }
                });
            }
            return { source: 'dom', items: results };
        }""")

        if not raw or not raw.get("items"):
            return []

        source = raw["source"]
        items = raw["items"]

        if source == "next_data":
            return self._parse_next_data(items)
        if source == "json_ld":
            return self._parse_json_ld(items)
        return self._parse_dom_cards(items)

    def _parse_next_data(self, items: list[dict]) -> list[Product]:
        products = []
        for item in items:
            try:
                price_data = item.get("priceInfo", item.get("price", {}))
                price = ""
                if isinstance(price_data, dict):
                    price = price_data.get("finalPrice", price_data.get("formattedValue", ""))
                    if isinstance(price, (int, float)):
                        price = f"${price:.2f}"
                elif isinstance(price_data, (int, float)):
                    price = f"${price_data:.2f}"

                images = item.get("images", item.get("image", []))
                image_url = ""
                if isinstance(images, list) and images:
                    first = images[0]
                    image_url = first.get("url", first.get("src", "")) if isinstance(first, dict) else str(first)
                elif isinstance(images, str):
                    image_url = images

                p = Product(
                    title=item.get("shortDescription", item.get("name", item.get("title", ""))),
                    price=str(price),
                    link=item.get("url", ""),
                    image_url=image_url,
                    designer=item.get("brand", {}).get("name", "") if isinstance(item.get("brand"), dict) else str(item.get("brand", "")),
                    pre_owned="pre-owned" in str(item).lower(),
                )
                if p.link and not p.link.startswith("http"):
                    p.link = f"{BASE_URL}{p.link}"
                if p.title and p.link:
                    products.append(p)
            except Exception as e:
                print(f"  [!] Farfetch next_data parse error: {e}")
        return products

    def _parse_json_ld(self, items: list[dict]) -> list[Product]:
        products = []
        for entry in items:
            item = entry.get("item", entry)
            try:
                offers = item.get("offers", {})
                if not isinstance(offers, dict):
                    offers = {}
                price_val = offers.get("price", "")
                currency = offers.get("priceCurrency", "USD")
                if isinstance(price_val, (int, float)):
                    price_str = f"${price_val:,.2f}" if currency == "USD" else f"{price_val:,.2f} {currency}"
                else:
                    price_str = f"${price_val}" if currency == "USD" and price_val else str(price_val)

                link = item.get("url", "") or offers.get("url", "")

                images = item.get("image", "")
                if isinstance(images, list):
                    image_url = images[0] if images else ""
                else:
                    image_url = images

                brand = item.get("brand", {})
                designer = brand.get("name", "") if isinstance(brand, dict) else str(brand)

                p = Product(
                    title=item.get("name", ""),
                    price=price_str,
                    link=link,
                    image_url=image_url,
                    designer=designer,
                    pre_owned="pre-owned" in (item.get("name", "") + " " + designer).lower(),
                )
                if p.link and not p.link.startswith("http"):
                    p.link = f"{BASE_URL}{p.link}"
                if p.title and p.link:
                    products.append(p)
            except Exception as e:
                print(f"  [!] Farfetch json_ld parse error: {e}")
        return products

    def _parse_dom_cards(self, items: list[dict]) -> list[Product]:
        products = []
        for item in items:
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

            price_m = re.search(r'[\$€£¥][\d,.]+|[\d,.]+\s*(?:USD|EUR|GBP)', line)
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

            if not p.designer and len(line) < 50 and not re.match(r'^[\$€£]', line):
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


def save_product_images(products: list[Product], output_path: str, max_seconds: int = 600):
    import time

    output_path = Path(output_path)
    images_dir = output_path.parent / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    session = req_lib.Session()
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
        downloaded = False
        for attempt in range(3):
            try:
                resp = session.get(p.image_url, timeout=15)
                resp.raise_for_status()
                ct = resp.headers.get("Content-Type", "image/jpeg")
                ext = MIME_TO_EXT.get(ct.split(";")[0].strip(), ".jpg")
                fp = images_dir / f"{base}{ext}"
                fp.write_bytes(resp.content)
                p.image_path = f"images/{fp.name}"
                saved += 1
                downloaded = True
                break
            except Exception:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
        if not downloaded:
            print(f"  [!] Image #{i+1}: failed after 3 attempts")
        time.sleep(0.3)

    print(f"[+] Saved {saved} images to {images_dir}/")


def save_to_json(products: list[Product], filepath: str):
    data = [asdict(p) for p in products]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[+] Saved {len(products)} products to {filepath}")
