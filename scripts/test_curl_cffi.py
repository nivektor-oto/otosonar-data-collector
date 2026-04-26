"""Local sanity test for the curl_cffi-backed HttpFetcher.

Runs:
  - 5 brands × 2 pages on arabam.com (10 search fetches)
  - 10 detail page fetches (sampled from search results)
  - 2 sahibinden category fetches (will likely 403 — recorded, not fatal)

Reports:
  - per-host status counts
  - parser success rate
  - sample parsed listings
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.fetcher import HttpFetcher  # noqa: E402
from src.sources import REGISTRY  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s :: %(message)s")
log = logging.getLogger("test_curl_cffi")

ARABAM_BRANDS = [
    ("renault", "clio"),
    ("fiat", "egea"),
    ("ford", "focus"),
    ("volkswagen", "polo"),
    ("hyundai", "i20"),
]


async def main() -> None:
    arabam = REGISTRY["arabam"]
    sahibinden = REGISTRY["sahibinden"]

    stats = {
        "search_total": 0, "search_2xx": 0, "search_blocked": 0, "search_other": 0,
        "detail_total": 0, "detail_2xx": 0, "detail_blocked": 0, "detail_other": 0,
        "parser_ok": 0, "parser_fail": 0,
        "discovered_listings": 0,
        "sahibinden_total": 0, "sahibinden_2xx": 0, "sahibinden_blocked": 0,
    }
    sample_parsed = []
    detail_urls: list[str] = []

    async with HttpFetcher() as fetcher:
        # 1. arabam search pages
        for brand, model in ARABAM_BRANDS:
            for page in (1, 2):
                urls = arabam.search_urls(brand, model, page=page)
                for url in urls:
                    stats["search_total"] += 1
                    res = await fetcher.fetch(url)
                    log.info("search %s status=%s blocked=%s len=%s",
                             url, res.status, res.blocked,
                             len(res.html or ""))
                    if res.blocked:
                        stats["search_blocked"] += 1
                    elif res.status and 200 <= res.status < 300:
                        stats["search_2xx"] += 1
                        disc = arabam.parse_search_page(res.html or "", url)
                        stats["discovered_listings"] += len(disc.listing_urls)
                        # take up to 2 detail urls per page
                        for u in disc.listing_urls[:2]:
                            if u not in detail_urls:
                                detail_urls.append(u)
                    else:
                        stats["search_other"] += 1

        # 2. arabam details — sample 10
        for url in detail_urls[:10]:
            stats["detail_total"] += 1
            res = await fetcher.fetch(url)
            if res.blocked:
                stats["detail_blocked"] += 1
                log.warning("detail blocked: %s status=%s", url, res.status)
                continue
            if not (res.status and 200 <= res.status < 300):
                stats["detail_other"] += 1
                continue
            stats["detail_2xx"] += 1
            parsed = arabam.parse_detail_page(res.html or "", url)
            if parsed is None:
                stats["parser_fail"] += 1
                log.warning("parser fail: %s", url)
            else:
                stats["parser_ok"] += 1
                if len(sample_parsed) < 5:
                    sample_parsed.append({
                        "brand": parsed.brand, "model": parsed.model, "year": parsed.year,
                        "km": parsed.km, "price": parsed.priceTry, "photos": parsed.photoCount,
                        "damage": parsed.damageStatus,
                    })

        # 3. sahibinden — best-effort (expected 403)
        for brand, model in ARABAM_BRANDS[:2]:
            urls = sahibinden.search_urls(brand, model)
            for url in urls:
                stats["sahibinden_total"] += 1
                res = await fetcher.fetch(url)
                log.info("sahibinden search %s status=%s blocked=%s len=%s",
                         url, res.status, res.blocked, len(res.html or ""))
                if res.blocked:
                    stats["sahibinden_blocked"] += 1
                elif res.status and 200 <= res.status < 300:
                    stats["sahibinden_2xx"] += 1
                    disc = sahibinden.parse_search_page(res.html or "", url)
                    log.info("  parsed %d listings", len(disc.listing_urls))

    print("\n========== RESULTS ==========")
    for k, v in stats.items():
        print(f"  {k:25s} = {v}")
    print("\nSample parsed listings:")
    for s in sample_parsed:
        print(f"  {s}")


if __name__ == "__main__":
    asyncio.run(main())
