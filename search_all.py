#!/usr/bin/env python3
"""
Search all marketplace APIs concurrently for a given query.

Reads API URLs from environment variables (or .env file).
Each marketplace is queried in parallel using threads.

Usage:
    python search_all.py "louis vuitton wallet"
    python search_all.py "gucci bag" --pages 2
    python search_all.py "prada shoes" --poll-interval 10 --timeout 300
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

MARKETPLACES = {
    "Google Shopping": "GOOGLE_SHOPPING_API_URL",
    "Grailed":         "GRAILED_API_URL",
    "Vestiaire":       "VESTIAIRE_API_URL",
    "Rebag":           "REBAG_API_URL",
    "Farfetch":        "FARFETCH_API_URL",
    "Fashionphile":    "FASHIONPHILE_API_URL",
    "2nd Street":      "SECONDSTREET_API_URL",
}


def _load_dotenv():
    """Minimal .env loader -- no external dependency required."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _get_api_url(env_var: str) -> str | None:
    url = os.environ.get(env_var)
    if not url:
        return None
    return url.rstrip("/")


def _submit_scrape(base_url: str, query: str, pages: int) -> str:
    resp = requests.post(
        f"{base_url}/scrape",
        json={"query": query, "pages": pages},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["task_id"]


def _poll_task(base_url: str, task_id: str, poll_interval: float, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = requests.get(f"{base_url}/scrape/{task_id}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "")
        if status in ("SUCCESS", "FAILURE"):
            return data
        time.sleep(poll_interval)
    return {"task_id": task_id, "status": "TIMEOUT"}


def _run_marketplace(name: str, base_url: str, query: str, pages: int,
                     poll_interval: float, timeout: float) -> dict:
    """Submit a scrape request and poll until completion. Returns a result dict."""
    try:
        health = requests.get(f"{base_url}/health", timeout=5)
        if health.status_code != 200:
            return {"marketplace": name, "status": "UNREACHABLE", "error": f"Health check returned {health.status_code}"}
    except requests.RequestException as e:
        return {"marketplace": name, "status": "UNREACHABLE", "error": str(e)}

    try:
        task_id = _submit_scrape(base_url, query, pages)
    except requests.RequestException as e:
        return {"marketplace": name, "status": "SUBMIT_FAILED", "error": str(e)}

    result = _poll_task(base_url, task_id, poll_interval, timeout)
    result["marketplace"] = name
    return result


def main():
    _load_dotenv()

    parser = argparse.ArgumentParser(
        description="Search all marketplace scrapers concurrently.",
    )
    parser.add_argument("query", help="Search query (e.g. 'louis vuitton wallet')")
    parser.add_argument("--pages", type=int, default=1, help="Pages to scrape per marketplace (default: 1)")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="Seconds between status polls (default: 5)")
    parser.add_argument("--timeout", type=float, default=300.0, help="Max seconds to wait per marketplace (default: 300)")
    args = parser.parse_args()

    apis: dict[str, str] = {}
    missing = []
    for name, env_var in MARKETPLACES.items():
        url = _get_api_url(env_var)
        if url:
            apis[name] = url
        else:
            missing.append(f"  {env_var} ({name})")

    if missing:
        print(f"[!] Missing API URL env vars (these marketplaces will be skipped):")
        for m in missing:
            print(m)
        print()

    if not apis:
        print("[!] No marketplace API URLs configured. Set them in .env or as environment variables.")
        sys.exit(1)

    print(f"[*] Query: \"{args.query}\"")
    print(f"[*] Pages: {args.pages}")
    print(f"[*] Marketplaces: {len(apis)}")
    print()

    results = []
    with ThreadPoolExecutor(max_workers=len(apis)) as pool:
        futures = {
            pool.submit(
                _run_marketplace, name, url, args.query, args.pages,
                args.poll_interval, args.timeout,
            ): name
            for name, url in apis.items()
        }

        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"marketplace": name, "status": "ERROR", "error": str(e)}
            results.append(result)

            status = result.get("status", "UNKNOWN")
            if status == "SUCCESS":
                r = result.get("result", {})
                loaded = r.get("products_loaded", 0)
                run_id = r.get("run_id")
                print(f"  [+] {name:20s}  SUCCESS  {loaded:>4} products  (run_id={run_id})")
            elif status == "UNREACHABLE":
                print(f"  [!] {name:20s}  UNREACHABLE  ({result.get('error', '')})")
            else:
                error = result.get("error", "")
                print(f"  [-] {name:20s}  {status:10s}  {error[:80]}")

    print()
    print("=" * 70)
    print(f"  {'Marketplace':20s}  {'Status':12s}  {'Products':>10s}  {'Run ID':>8s}")
    print("-" * 70)
    total = 0
    for r in sorted(results, key=lambda x: x.get("marketplace", "")):
        name = r.get("marketplace", "")
        status = r.get("status", "UNKNOWN")
        loaded = r.get("result", {}).get("products_loaded", "")
        run_id = r.get("result", {}).get("run_id", "")
        if status == "SUCCESS" and loaded:
            total += int(loaded)
        print(f"  {name:20s}  {status:12s}  {str(loaded):>10s}  {str(run_id):>8s}")
    print("-" * 70)
    print(f"  {'TOTAL':20s}  {'':12s}  {total:>10}  {'':>8s}")
    print("=" * 70)


if __name__ == "__main__":
    main()
