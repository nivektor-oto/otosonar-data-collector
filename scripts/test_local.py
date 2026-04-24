"""Quick local sanity test: fetch one arabam listing + verify parser.

Run with:
    DATABASE_URL=postgres://... python scripts/test_local.py

Goal: prove the pipeline end-to-end on a real URL before wiring up CI.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from src import persistence
from src.db import close_pool
from src.fetcher import StealthFetcher
from src.sources import REGISTRY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s :: %(message)s")
log = logging.getLogger("test_local")

TEST_URL = "https://www.arabam.com/ikinci-el/otomobil/renault-clio"


async def main() -> None:
    adapter = REGISTRY["arabam"]
    async with StealthFetcher() as fetcher:
        log.info("fetching %s", TEST_URL)
        search = await fetcher.fetch(TEST_URL)
        if not search.html:
            log.error("search failed: status=%s blocked=%s error=%s",
                      search.status, search.blocked, search.error)
            sys.exit(1)
        discovery = adapter.parse_search_page(search.html, TEST_URL)
        log.info("discovered %d listings; next_page=%s",
                 len(discovery.listing_urls), discovery.next_page_url)
        if not discovery.listing_urls:
            log.error("no listings parsed")
            sys.exit(1)

        detail_url = discovery.listing_urls[0]
        log.info("fetching detail: %s", detail_url)
        detail = await fetcher.fetch(detail_url)
        if not detail.html:
            log.error("detail failed: %s", detail.error or detail.status)
            sys.exit(1)

        parsed = adapter.parse_detail_page(detail.html, detail_url)
        if not parsed:
            log.error("parser returned None for %s", detail_url)
            sys.exit(1)
        log.info("parsed: brand=%s model=%s year=%s km=%s price=%s photos=%s damage=%s phone=%s",
                 parsed.brand, parsed.model, parsed.year, parsed.km, parsed.priceTry,
                 parsed.photoCount, parsed.damageStatus, parsed.sellerPhone)

        listing_id, is_new = await persistence.upsert_listing(parsed.to_dict())
        log.info("persisted: id=%s new=%s", listing_id, is_new)


async def _run() -> None:
    try:
        await main()
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(_run())
