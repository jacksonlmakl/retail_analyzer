"""
Google Shopping Scraper

Search for products on Google Shopping and scrape product details
including title, price, seller, rating, and product links.

Uses Playwright with a persistent browser profile to handle Google's
JS-rendered pages and anti-bot detection.
"""

import asyncio
import base64
import random
import argparse
import json
import csv
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from urllib.parse import urlencode

import requests as req_lib

from playwright.async_api import async_playwright
from playwright_stealth import Stealth


PROFILE_DIR = Path.home() / ".cache" / "google_shopping_scraper" / "browser_profile"


@dataclass
class Product:
    title: str = ""
    price: str = ""
    original_price: str = ""
    discount: str = ""
    seller: str = ""
    rating: str = ""
    reviews: str = ""
    link: str = ""
    image_url: str = ""
    image_path: str = ""
    shipping: str = ""
    extras: dict = field(default_factory=dict)


class GoogleShoppingScraper:
    """Scrapes product listings and details from Google Shopping."""

    BASE_URL = "https://www.google.com/search"

    def __init__(self, country="us", language="en", delay_range=(1.5, 3.5), headless=False):
        self.country = country
        self.language = language
        self.delay_range = delay_range
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

    def _build_search_url(self, query, page=0):
        params = {
            "q": query,
            "tbm": "shop",
            "hl": self.language,
            "gl": self.country,
        }
        if page > 0:
            params["start"] = page * 60
        return f"{self.BASE_URL}?{urlencode(params)}"

    async def search(self, query, max_pages=1):
        """
        Search Google Shopping and return a list of Product results.

        Args:
            query: The search term (e.g. "wireless headphones").
            max_pages: Number of result pages to scrape (default 1).

        Returns:
            List of Product dataclass instances.
        """
        all_products = []

        for page_num in range(max_pages):
            if page_num > 0:
                delay = random.uniform(*self.delay_range)
                print(f"[*] Waiting {delay:.1f}s...")
                await asyncio.sleep(delay)

            url = self._build_search_url(query, page_num)
            print(f"[*] Fetching page {page_num + 1}: {url}")

            page = self._context.pages[0] if self._context.pages else await self._context.new_page()

            try:
                if page_num == 0:
                    await page.goto("https://www.google.com/", wait_until="networkidle", timeout=15000)
                    await page.wait_for_timeout(1000)

                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(3000)

                if "sorry" in page.url:
                    print("[!] Google returned a CAPTCHA page. Try again later or increase delays.")
                    break

                # Dismiss consent banner if present
                try:
                    consent = page.locator("button:has-text('Accept all'), button:has-text('I agree')")
                    if await consent.count() > 0:
                        await consent.first.click()
                        await page.wait_for_timeout(1000)
                except Exception:
                    pass

                products = await self._extract_products(page)
                if not products:
                    print(f"[*] No results found on page {page_num + 1}.")
                    break

                print(f"[+] Found {len(products)} products on page {page_num + 1}")
                await self._extract_links(page, products)
                with_links = [p for p in products if p.link]
                dropped = len(products) - len(with_links)
                if dropped:
                    print(f"[*] Dropped {dropped} products without links")
                all_products.extend(with_links)

            except Exception as e:
                print(f"[!] Error on page {page_num + 1}: {e}")
                break

        print(f"\n[+] Total products scraped: {len(all_products)}")
        return all_products

    async def _extract_products(self, page):
        """Extract product data from rendered Google Shopping results using JS evaluation."""
        raw = await page.evaluate("""() => {
            // Try known selectors first, then auto-detect
            const KNOWN_SELECTORS = ['div.gXGikb', 'div.MUWJ8c'];
            let cards = [];
            for (const sel of KNOWN_SELECTORS) {
                cards = document.querySelectorAll(sel);
                if (cards.length > 0) break;
            }

            // Auto-detect: find repeating div class that contains price-like text
            if (cards.length === 0) {
                const freq = {};
                document.querySelectorAll('div[class]').forEach(d => {
                    const t = (d.innerText || '').trim();
                    if (t.includes('$') && t.length > 20 && t.length < 2000) {
                        const cls = d.className;
                        if (!freq[cls]) freq[cls] = [];
                        freq[cls].push(d);
                    }
                });
                let best = null, bestCount = 0;
                for (const [cls, divs] of Object.entries(freq)) {
                    // Pick the class with highest count in a reasonable range
                    if (divs.length > bestCount && divs.length >= 5) {
                        // Prefer the most specific (smallest text) variant
                        if (!best || divs[0].innerText.length < best[0].innerText.length) {
                            best = divs;
                            bestCount = divs.length;
                        }
                    }
                }
                if (best) cards = best;
            }

            if (cards.length === 0) return [];

            const results = [];
            const seen = new Set();

            for (const card of cards) {
                const text = (card.innerText || '').trim();
                if (!text || seen.has(text) || text.includes('About this result')) continue;
                seen.add(text);

                const arias = [];
                card.querySelectorAll('[aria-label]').forEach(el => {
                    arias.push(el.getAttribute('aria-label'));
                });
                const ariaText = arias.join(' | ');

                const img = card.querySelector('img[src]');
                const imgSrc = img ? img.src : '';
                const imgSrcset = img ? (img.getAttribute('srcset') || '') : '';

                results.push({text, ariaText, imgSrc, imgSrcset});
            }
            return results;
        }""")

        products = []
        for item in raw:
            product = self._parse_card(item["text"], item["ariaText"])
            if not product:
                continue
            if item["imgSrc"]:
                product.image_url = item["imgSrc"]
            elif item["imgSrcset"]:
                product.image_url = item["imgSrcset"].split()[0]
            products.append(product)
        return products

    _PANEL_SELLER_LINKS_JS = """() => {
        const SKIP = ['google.com','gstatic.com','googleapis.com','youtube.com'];
        const results = [];
        for (const a of document.querySelectorAll('a[href]')) {
            const h = a.href || '';
            const text = (a.innerText || '').trim();
            if (!h.startsWith('http') || SKIP.some(d => h.includes(d))) continue;
            if (text.includes('$') && text.includes('\\n') && text.length > 15) {
                results.push({href: h, seller: text.split('\\n')[0].trim()});
            }
        }
        return results;
    }"""

    @staticmethod
    def _seller_domain_hint(seller):
        """Turn a seller name like 'Neiman Marcus' into a domain fragment like 'neimanmarcus'."""
        if not seller:
            return None
        s = seller.lower().strip()
        s = re.sub(r'\.(com|net|org|co\.uk)$', '', s)
        s = re.sub(r'\s+(us|uk|eu|ca|au|de|fr|it|es)$', '', s)
        s = re.sub(r'[^a-z0-9]', '', s)
        return s if len(s) >= 3 else None

    def _pick_seller_link(self, seller_links, seller):
        """From a list of {href, seller} dicts (panel seller rows), pick the
        one that best matches the product's seller.

        Priority: domain match > seller text contained in row > first row.
        Domain matching is most reliable because panel row text often has
        badges like 'Best price' or 'Most popular' prepended.
        """
        if not seller_links:
            return None

        hint = self._seller_domain_hint(seller)
        seller_lower = (seller or "").lower().strip()

        # 1) Match by seller domain hint (most reliable)
        if hint:
            for sl in seller_links:
                domain = re.split(r'[/?#]', sl["href"].split("://")[-1])[0].lower()
                domain_clean = domain.replace("www.", "").replace(".", "").replace("-", "")
                if hint in domain_clean:
                    return sl["href"].split("&srsltid=")[0]

        # 2) Seller name appears somewhere in the row text
        if seller_lower and len(seller_lower) >= 3:
            for sl in seller_links:
                if seller_lower in sl["seller"].lower():
                    return sl["href"].split("&srsltid=")[0]

        # 3) Fallback: first row (typically the "best price" or primary seller)
        return seller_links[0]["href"].split("&srsltid=")[0]

    async def _extract_links(self, page, products):
        """Extract merchant links by clicking each card and reading the panel.

        For each card: click to open the detail panel, find seller-row links
        (identified by multi-line text containing a price), and match the
        correct seller by name or domain.
        """
        card_selector = await page.evaluate("""() => {
            const KNOWN = ['div.gXGikb', 'div.MUWJ8c'];
            for (const sel of KNOWN) {
                if (document.querySelectorAll(sel).length > 0) return sel;
            }
            return null;
        }""")
        if not card_selector:
            return

        cards = page.locator(card_selector)
        total = await cards.count()

        unique_indices = []
        seen = set()
        for i in range(total):
            text = (await cards.nth(i).inner_text()).strip()
            if text and text not in seen and "About this result" not in text:
                seen.add(text)
                unique_indices.append(i)

        linked = 0
        for product_idx, card_idx in enumerate(unique_indices[:len(products)]):
            seller = products[product_idx].seller

            await cards.nth(card_idx).click()
            await page.wait_for_timeout(1500)

            seller_links = await page.evaluate(self._PANEL_SELLER_LINKS_JS)
            link = self._pick_seller_link(seller_links, seller)

            if link:
                products[product_idx].link = link
                linked += 1

            await page.keyboard.press("Escape")
            await page.wait_for_timeout(300)

        print(f"[+] Extracted links for {linked}/{len(products)} products")

    def _parse_card(self, text, aria_text):
        """
        Parse a product card's visible text and aria-labels into a Product.
        Returns None for non-product cards (e.g. "More" button).
        """
        product = Product()

        price_m = re.search(r'Current Price:\s*(\$[\d,.]+)', aria_text)
        if price_m:
            product.price = price_m.group(1)

        was_m = re.search(r'Was\s*(\$[\d,.]+)', aria_text)
        if was_m:
            product.original_price = was_m.group(1)

        rating_m = re.search(
            r'Rated\s+([\d.]+)\s+out of\s+(\d+)[.,]\s*([\d.]+\w*)\s*reviews?', aria_text
        )
        if rating_m:
            product.rating = f"{rating_m.group(1)}/{rating_m.group(2)}"
            product.reviews = rating_m.group(3)

        lines = [l.strip() for l in text.split("\n") if l.strip()]

        if len(lines) == 1 and product.price:
            lines = re.split(r'(?=\$\d)', lines[0], maxsplit=1)
            if len(lines) == 2:
                title_part = lines[0].strip()
                rest = lines[1]
                rest_parts = re.split(r'(\$[\d,.]+)', rest)
                lines = [title_part] + [p.strip() for p in rest_parts if p.strip()]

        if not lines:
            return None

        idx = 0

        if re.match(r'^\d+%\s*OFF', lines[0], re.IGNORECASE):
            product.discount = lines[0]
            idx = 1

        while idx < len(lines) and re.match(r'^(LOW PRICE|SALE|NEW|180|3D|360|AR)$', lines[idx], re.IGNORECASE):
            idx += 1

        title_parts = []
        while idx < len(lines) and not re.match(r'^[\$€£]\d', lines[idx]):
            title_parts.append(lines[idx])
            idx += 1
        product.title = " ".join(title_parts).strip()

        if product.title.lower() in ("more", "see more", "show more") and not product.price:
            return None

        if idx < len(lines) and re.match(r'^[\$€£]\d', lines[idx]):
            if not product.price:
                product.price = lines[idx]
            idx += 1

        if idx < len(lines) and re.match(r'^[\$€£]\d', lines[idx]):
            if not product.original_price:
                product.original_price = lines[idx]
            idx += 1

        if idx < len(lines) and re.match(r'^Usually\s+\$', lines[idx], re.IGNORECASE):
            if not product.original_price:
                m = re.search(r'\$[\d,.]+', lines[idx])
                if m:
                    product.original_price = m.group()
            idx += 1

        if idx < len(lines):
            candidate = lines[idx]
            if not candidate.startswith("&") and not candidate.startswith("\xa0"):
                product.seller = candidate
                idx += 1

        while idx < len(lines) and (
            lines[idx].startswith("&") or lines[idx].startswith("\xa0")
        ):
            idx += 1

        for line in lines[idx:]:
            self._parse_info_line(line, product)

        if product.price and product.title and product.price in product.title:
            parts = product.title.split(product.price, 1)
            product.title = parts[0].strip()
            if len(parts) > 1:
                remainder = parts[1].strip()
                if product.original_price and remainder.startswith(product.original_price):
                    remainder = remainder[len(product.original_price):].strip()
                self._parse_run_together_remainder(remainder, product)

        if product.seller:
            product.seller = self._clean_seller(product.seller)

        product.price = product.price.rstrip(".")
        product.original_price = product.original_price.rstrip(".")

        product.title = re.sub(
            r'^(LOW\s+PRICE|SALE|NEW)\s*',
            '', product.title, flags=re.IGNORECASE
        ).strip()
        product.title = re.sub(
            r'^(Nearby,?\s*\d*\s*mi|Also\s+nearby)\s*',
            '', product.title, flags=re.IGNORECASE
        ).strip()
        product.title = re.sub(r'^(180|3D|360|AR)\s*', '', product.title).strip()

        if not product.title:
            return None

        return product

    def _parse_info_line(self, line, product):
        """Parse a single info line for shipping or rating data."""
        line_lower = line.lower()
        if any(kw in line_lower for kw in ["deliver", "shipping", "free ", "day return"]):
            if not product.shipping:
                product.shipping = line
        elif re.match(r'[\d.]+\(', line) and not product.rating:
            r_match = re.match(r'([\d.]+)\(([\d,.]+\w*)\)', line)
            if r_match:
                product.rating = r_match.group(1)
                product.reviews = r_match.group(2)

    def _parse_run_together_remainder(self, text, product):
        """Parse the run-together text after the price in single-line cards."""
        if not text:
            return

        rating_m = re.search(r'(\d+\.\d+)\(([\d,.]+\w*)\)\s*$', text)
        if rating_m and not product.rating:
            product.rating = rating_m.group(1)
            product.reviews = rating_m.group(2)
            text = text[:rating_m.start()].strip()

        ship_m = re.search(r'(Free deliver\w*[^$]*?|Free shipping[^$]*?|\d+-day return\w*)', text, re.IGNORECASE)
        if ship_m and not product.shipping:
            product.shipping = ship_m.group(0).strip()
            text = text[:ship_m.start()].strip()

        if not product.seller and text:
            product.seller = self._clean_seller(text)

    def _clean_seller(self, seller):
        """Remove trailing junk from a seller string."""
        seller = re.split(
            r'(?:Free\s+deliver|Free\s+ship|\d+-day\s+return|\d+\.\d+\()',
            seller, maxsplit=1
        )[0]
        seller = re.sub(r'\s*&\s*more.*$', '', seller).strip()
        seller = seller.strip("\xa0").strip()
        return seller

    async def get_product_details(self, page, card_index):
        """
        Click on a product card to open the viewer and extract merchant links.

        Args:
            page: The Playwright page with search results loaded.
            card_index: Index of the card to click.

        Returns:
            Dict with merchant links and any extra info.
        """
        cards = page.locator("div.gXGikb, div.MUWJ8c")
        if card_index >= await cards.count():
            return {}

        card = cards.nth(card_index)
        await card.click()
        await page.wait_for_timeout(3000)

        details = await page.evaluate("""() => {
            const links = [];
            document.querySelectorAll('a[href]').forEach(a => {
                const href = a.href;
                const text = (a.innerText || '').trim();
                if (text.match(/\\$\\d/) && (href.includes('http') && !href.includes('google.com/search'))) {
                    links.push({
                        url: href,
                        text: text.substring(0, 200),
                    });
                }
            });
            return {merchant_links: links.slice(0, 10)};
        }""")

        await page.keyboard.press("Escape")
        await page.wait_for_timeout(500)

        return details


# ---------------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------------

MIME_TO_EXT = {
    "image/webp": ".webp",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/svg+xml": ".svg",
    "image/avif": ".avif",
}


def _slugify(text, max_len=60):
    slug = re.sub(r'[^\w\s-]', '', text.lower())
    slug = re.sub(r'[\s_-]+', '_', slug).strip('_')
    return slug[:max_len]


def save_product_images(products, output_path):
    """
    Decode and save product images to an images/ directory next to the output file.
    Sets each product's image_path to the relative path from the output directory.
    """
    output_path = Path(output_path)
    images_dir = output_path.parent / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    for i, product in enumerate(products):
        if not product.image_url:
            continue

        idx_str = f"{i + 1:03d}"
        slug = _slugify(product.title)
        filename_base = f"{idx_str}_{slug}" if slug else idx_str

        try:
            if product.image_url.startswith("data:"):
                filepath = _save_base64_image(product.image_url, images_dir, filename_base)
            elif product.image_url.startswith("http"):
                filepath = _download_image(product.image_url, images_dir, filename_base)
            else:
                continue

            if filepath:
                product.image_path = f"images/{filepath.name}"
                saved += 1
        except Exception as e:
            print(f"  [!] Failed to save image for product #{i+1}: {e}")

    print(f"[+] Saved {saved} product images to {images_dir}/")


def _save_base64_image(data_url, images_dir, filename_base):
    """Decode a data:image/...;base64,... URL and write it to disk."""
    match = re.match(r'data:(image/[\w+.-]+);base64,(.+)', data_url)
    if not match:
        return None

    mime_type = match.group(1)
    b64_data = match.group(2)
    ext = MIME_TO_EXT.get(mime_type, ".bin")

    filepath = images_dir / f"{filename_base}{ext}"
    filepath.write_bytes(base64.b64decode(b64_data))
    return filepath


_img_session = None

def _get_img_session():
    global _img_session
    if _img_session is None:
        _img_session = req_lib.Session()
        _img_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "image/webp,image/*,*/*",
        })
    return _img_session

def _download_image(url, images_dir, filename_base):
    """Download an image from a URL via a persistent session."""
    try:
        resp = _get_img_session().get(url, timeout=10)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "image/jpeg")
        ext = MIME_TO_EXT.get(content_type.split(";")[0].strip(), ".jpg")
        filepath = images_dir / f"{filename_base}{ext}"
        filepath.write_bytes(resp.content)
        return filepath
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def print_products(products):
    for i, p in enumerate(products, 1):
        print(f"\n{'='*60}")
        print(f"  Product #{i}")
        print(f"{'='*60}")
        print(f"  Title:    {p.title}")
        print(f"  Price:    {p.price}")
        if p.original_price:
            print(f"  Was:      {p.original_price}")
        if p.discount:
            print(f"  Discount: {p.discount}")
        print(f"  Seller:   {p.seller}")
        if p.rating:
            print(f"  Rating:   {p.rating}")
        if p.reviews:
            print(f"  Reviews:  {p.reviews}")
        if p.shipping:
            print(f"  Shipping: {p.shipping}")
        if p.image_path:
            print(f"  Image:    {p.image_path}")
        if p.link:
            print(f"  Link:     {p.link}")
        if p.extras:
            print(f"  Extras:   {json.dumps(p.extras, indent=4)}")


def save_to_json(products, filepath):
    data = [asdict(p) for p in products]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[+] Saved {len(products)} products to {filepath}")


def save_to_csv(products, filepath):
    if not products:
        print("[!] No products to save.")
        return
    fieldnames = [
        "title", "price", "original_price", "discount", "seller",
        "rating", "reviews", "link", "image_url", "image_path", "shipping",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for p in products:
            writer.writerow(asdict(p))
    print(f"[+] Saved {len(products)} products to {filepath}")


# ---------------------------------------------------------------------------
# Standalone CLI (python -m src.scraper "query")
# ---------------------------------------------------------------------------

async def async_main():
    parser = argparse.ArgumentParser(
        description="Search and scrape product details from Google Shopping."
    )
    parser.add_argument("query", help="Product search query (e.g. 'wireless headphones')")
    parser.add_argument("--pages", type=int, default=1, help="Number of result pages to scrape (default: 1)")
    parser.add_argument("--output", "-o", help="Output file path (.json or .csv)")
    parser.add_argument("--details", action="store_true", help="Click each product to get merchant links (slower)")
    parser.add_argument("--country", default="us", help="Country code for results (default: us)")
    parser.add_argument("--language", default="en", help="Language code (default: en)")
    parser.add_argument("--delay-min", type=float, default=1.5, help="Minimum delay between requests in seconds")
    parser.add_argument("--delay-max", type=float, default=3.5, help="Maximum delay between requests in seconds")
    parser.add_argument("--headless", action="store_true", help="Run browser without visible window (may trigger CAPTCHA)")

    args = parser.parse_args()

    print(f"\n[*] Searching Google Shopping for: '{args.query}'")
    print(f"[*] Country: {args.country} | Language: {args.language} | Pages: {args.pages}\n")

    async with GoogleShoppingScraper(
        country=args.country,
        language=args.language,
        delay_range=(args.delay_min, args.delay_max),
        headless=args.headless,
    ) as scraper:
        products = await scraper.search(args.query, max_pages=args.pages)

        if args.details and products:
            print(f"\n[*] Fetching merchant links for {len(products)} products...")
            page = scraper._context.pages[0]
            url = scraper._build_search_url(args.query)
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

            for i, product in enumerate(products):
                print(f"  [{i+1}/{len(products)}] {product.title[:50]}...")
                try:
                    details = await scraper.get_product_details(page, i)
                    product.extras = details
                    if details.get("merchant_links"):
                        product.link = details["merchant_links"][0]["url"]
                except Exception as e:
                    print(f"    [!] Failed: {e}")
                await asyncio.sleep(random.uniform(0.5, 1.5))

        if products:
            output_ref = args.output or "output/results.json"
            save_product_images(products, output_ref)

        print_products(products)

        if args.output:
            if args.output.endswith(".csv"):
                save_to_csv(products, args.output)
            else:
                save_to_json(products, args.output)

    return products


def main():
    return asyncio.run(async_main())


if __name__ == "__main__":
    main()
