#!/usr/bin/env python3
"""
Product Verification — check whether scraped products are still listed
on Google Shopping.

Strategy:
  1. Re-search Google Shopping by the original query, fuzzy-match titles.
  2. Any product flagged as inactive gets a second chance: if it has a
     merchant link, visit the link directly to confirm it's really gone
     before marking it inactive.

Requires the same SNOWFLAKE_* environment variables as run_pipeline.py.
"""

import argparse
import asyncio
import random
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher

from src.scraper import GoogleShoppingScraper, PROFILE_DIR
from src.loader import (
    get_connection,
    fetch_active_products,
    mark_inactive,
)

MATCH_THRESHOLD = 0.80

DEAD_PAGE_SIGNALS = [
    "page not found",
    "404",
    "no longer available",
    "item not found",
    "product not found",
    "this item is unavailable",
    "is no longer available",
    "has been removed",
    "we couldn't find",
    "we can't find",
    "doesn't exist",
    "does not exist",
    "sorry, this page",
    "this product is currently unavailable",
    "discontinued",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(text):
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s]', '', text)
    return re.sub(r'\s+', ' ', text)


def _price_to_float(price_str):
    if not price_str:
        return None
    cleaned = re.sub(r'[^\d.]', '', price_str)
    try:
        return float(cleaned)
    except ValueError:
        return None


def _find_best_match(stored_title, fresh_products):
    norm_stored = _normalize(stored_title)
    best_product = None
    best_score = 0

    for fp in fresh_products:
        score = SequenceMatcher(None, norm_stored, _normalize(fp.title)).ratio()
        if score > best_score:
            best_score = score
            best_product = fp

    return (best_product, best_score) if best_score >= MATCH_THRESHOLD else (None, best_score)


def _diff_fields(stored, fresh_product):
    changes = {}

    field_map = {
        "PRICE":          fresh_product.price,
        "SELLER":         fresh_product.seller,
        "RATING":         fresh_product.rating,
        "REVIEWS":        fresh_product.reviews,
        "SHIPPING":       fresh_product.shipping,
        "DISCOUNT":       fresh_product.discount,
        "ORIGINAL_PRICE": fresh_product.original_price,
    }

    for col, new_val in field_map.items():
        old_val = stored.get(col) or ""
        if new_val and new_val != old_val:
            changes[col] = new_val

    if "PRICE" in changes:
        new_numeric = _price_to_float(changes["PRICE"])
        if new_numeric is not None:
            changes["PRICE_NUMERIC"] = new_numeric
        changes["PRICE_UPDATED_AT"] = "CURRENT_TIMESTAMP()"

    if "ORIGINAL_PRICE" in changes:
        new_numeric = _price_to_float(changes["ORIGINAL_PRICE"])
        if new_numeric is not None:
            changes["ORIGINAL_PRICE_NUMERIC"] = new_numeric

    if "RATING" in changes:
        m = re.match(r'([\d.]+)', changes["RATING"])
        if m:
            try:
                changes["RATING_NUMERIC"] = float(m.group(1))
            except ValueError:
                pass

    return changes


# ---------------------------------------------------------------------------
# Link-based confirmation (second chance for "inactive" candidates)
# ---------------------------------------------------------------------------

async def _check_link(page, url):
    """Visit a merchant URL to see if the product page is still live."""
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
    except Exception:
        return False

    if resp is None or resp.status >= 400:
        return False

    await page.wait_for_timeout(2000)

    body_text = await page.evaluate(
        "() => document.body ? document.body.innerText.substring(0, 5000).toLowerCase() : ''"
    )

    for signal in DEAD_PAGE_SIGNALS:
        if signal in body_text:
            return False

    return True


async def confirm_inactive(candidates, page):
    """Double-check inactive candidates that have merchant links.

    Returns:
        (rescued_ids, confirmed_inactive_ids)
    """
    rescued = []
    confirmed = []

    for row in candidates:
        pid = row["PRODUCT_ID"]
        link = row.get("LINK")

        if not link:
            confirmed.append(pid)
            print(f"  [x] CONFIRMED INACTIVE #{pid} {row['TITLE'][:50]}  (no link to double-check)")
            continue

        print(f"  [?] Double-checking #{pid} via link...")
        print(f"      {link[:80]}")

        is_live = await _check_link(page, link)
        await asyncio.sleep(random.uniform(1.0, 2.0))

        if is_live:
            rescued.append(pid)
            print(f"  [+] RESCUED     #{pid} {row['TITLE'][:50]}  (link is still live)")
        else:
            confirmed.append(pid)
            print(f"  [x] CONFIRMED   #{pid} {row['TITLE'][:50]}  (link is dead)")

    return rescued, confirmed


# ---------------------------------------------------------------------------
# Main verification flow
# ---------------------------------------------------------------------------

async def verify(products_by_query, headless):
    """Search-based verification with link-based confirmation for inactives."""
    matched_updates = []
    inactive_candidates = []
    verified_ids = []

    # Phase 1: search-based matching
    async with GoogleShoppingScraper(headless=headless) as scraper:
        for query, stored_rows in products_by_query.items():
            print(f"\n[*] Re-searching: '{query}' ({len(stored_rows)} products to verify)")
            fresh = await scraper.search(query, max_pages=1)

            if not fresh:
                print(f"  [!] No results returned — skipping (won't mark inactive)")
                continue

            for row in stored_rows:
                pid = row["PRODUCT_ID"]
                verified_ids.append(pid)

                match, score = _find_best_match(row["TITLE"], fresh)

                if match:
                    changes = _diff_fields(row, match)
                    if changes:
                        matched_updates.append((pid, row["TITLE"], changes))
                        print(f"  [~] UPDATED  #{pid} {row['TITLE'][:50]}")
                        for col, val in changes.items():
                            if col not in ("PRICE_NUMERIC", "ORIGINAL_PRICE_NUMERIC",
                                           "RATING_NUMERIC", "PRICE_UPDATED_AT"):
                                print(f"               {col}: {row.get(col, '')} -> {val}")
                    else:
                        print(f"  [=] OK       #{pid} {row['TITLE'][:50]}")
                else:
                    inactive_candidates.append(row)
                    print(f"  [?] MAYBE INACTIVE #{pid} {row['TITLE'][:50]}  (score: {score:.2f})")

    # Phase 2: double-check candidates by visiting their merchant links
    rescued_ids = []
    confirmed_inactive_ids = []

    if inactive_candidates:
        linkable = [r for r in inactive_candidates if r.get("LINK")]
        unlinkable = [r for r in inactive_candidates if not r.get("LINK")]

        if linkable:
            print(f"\n[*] Double-checking {len(linkable)} candidate(s) via merchant links...")

            from playwright.async_api import async_playwright
            PROFILE_DIR.mkdir(parents=True, exist_ok=True)
            pw = await async_playwright().start()
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=headless,
                viewport={"width": 1280, "height": 900},
                args=["--disable-blink-features=AutomationControlled"],
            )
            try:
                page = ctx.pages[0] if ctx.pages else await ctx.new_page()
                rescued, confirmed = await confirm_inactive(linkable, page)
                rescued_ids.extend(rescued)
                confirmed_inactive_ids.extend(confirmed)
            finally:
                await ctx.close()
                await pw.stop()

        for row in unlinkable:
            confirmed_inactive_ids.append(row["PRODUCT_ID"])
            print(f"  [x] INACTIVE #{row['PRODUCT_ID']} {row['TITLE'][:50]}  (no link to verify)")
    else:
        print("\n[*] No inactive candidates to double-check.")

    return matched_updates, rescued_ids, confirmed_inactive_ids, verified_ids


def apply_updates(conn, matched_updates, rescued_ids, inactive_ids, verified_ids):
    """Write all changes back to Snowflake."""
    price_changed = 0

    for pid, title, changes in matched_updates:
        set_parts = []
        params = []
        for col, val in changes.items():
            if val == "CURRENT_TIMESTAMP()":
                set_parts.append(f"{col} = CURRENT_TIMESTAMP()")
            else:
                set_parts.append(f"{col} = %s")
                params.append(val)

        set_parts.append("LAST_VERIFIED_AT = CURRENT_TIMESTAMP()")
        params.append(pid)

        cur = conn.cursor()
        try:
            cur.execute("USE DATABASE RETAIL_ANALYZER")
            cur.execute("USE SCHEMA GOOGLE_SHOPPING")
            cur.execute(
                f"UPDATE PRODUCTS SET {', '.join(set_parts)} WHERE PRODUCT_ID = %s",
                params,
            )
        finally:
            cur.close()

        if "PRICE" in changes:
            price_changed += 1

    # Timestamp all verified-and-ok products (including rescued ones)
    ok_ids = rescued_ids + [
        pid for pid in verified_ids
        if pid not in inactive_ids
        and pid not in rescued_ids
        and pid not in [m[0] for m in matched_updates]
    ]
    if ok_ids:
        cur = conn.cursor()
        try:
            cur.execute("USE DATABASE RETAIL_ANALYZER")
            cur.execute("USE SCHEMA GOOGLE_SHOPPING")
            placeholders = ", ".join(["%s"] * len(ok_ids))
            cur.execute(
                f"UPDATE PRODUCTS SET LAST_VERIFIED_AT = CURRENT_TIMESTAMP() "
                f"WHERE PRODUCT_ID IN ({placeholders})",
                ok_ids,
            )
        finally:
            cur.close()

    mark_inactive(conn, inactive_ids)

    return price_changed


# ---------------------------------------------------------------------------
# Reusable entry point (called by CLI and Celery tasks)
# ---------------------------------------------------------------------------

def run_verification(
    run_id=None,
    query_filter=None,
    headless=True,
    dry_run=False,
):
    """Run the full verification flow and return a summary dict.

    Can be called programmatically (from Celery tasks) or from the CLI.
    """
    conn = get_connection()
    try:
        rows = fetch_active_products(conn, run_id=run_id, query_filter=query_filter)
    except Exception as e:
        print(f"[!] Failed to fetch products: {e}")
        conn.close()
        raise

    if not rows:
        print("[*] No active products found matching your filters.")
        conn.close()
        return {
            "products_checked": 0,
            "still_active": 0,
            "fields_updated": 0,
            "rescued": 0,
            "confirmed_dead": 0,
            "price_changes": 0,
        }

    products_by_query = defaultdict(list)
    for row in rows:
        products_by_query[row["QUERY_TEXT"]].append(row)

    print(f"[*] {len(rows)} active products across {len(products_by_query)} search queries")

    matched_updates, rescued_ids, inactive_ids, verified_ids = asyncio.run(
        verify(products_by_query, headless)
    )

    price_changed = 0
    if not dry_run:
        price_changed = apply_updates(
            conn, matched_updates, rescued_ids, inactive_ids, verified_ids,
        )

    conn.close()

    summary = {
        "products_checked": len(verified_ids),
        "still_active": len(verified_ids) - len(inactive_ids),
        "fields_updated": len(matched_updates),
        "rescued": len(rescued_ids),
        "confirmed_dead": len(inactive_ids),
        "price_changes": price_changed,
    }

    print(f"\n{'=' * 60}")
    print(f"  VERIFICATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Products checked : {summary['products_checked']}")
    print(f"  Still active     : {summary['still_active']}")
    print(f"  Fields updated   : {summary['fields_updated']}")
    print(f"  Rescued by link  : {summary['rescued']}")
    print(f"  Confirmed dead   : {summary['confirmed_dead']}")

    if dry_run:
        print(f"\n  [DRY RUN] No changes written to Snowflake.")
    else:
        print(f"  Price changes    : {summary['price_changes']}")
        print(f"\n  [+] Snowflake updated.")

    print(f"{'=' * 60}")
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Verify product listings are still live on Google Shopping.",
    )
    parser.add_argument(
        "--run-id", type=int, default=None,
        help="Only verify products from this scrape run ID",
    )
    parser.add_argument(
        "--query", default=None,
        help="Only verify products whose search query contains this text",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser without visible window (may trigger CAPTCHA)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would change without writing to Snowflake",
    )

    args = parser.parse_args()

    run_verification(
        run_id=args.run_id,
        query_filter=args.query,
        headless=args.headless,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
