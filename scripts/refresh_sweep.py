"""Freshness maintenance: queue refresh jobs, mark stale listings dropped, purge old.

Called from the GH Actions "maintenance" workflow. Separate from scraping so
scraper shards can stay focused on fetching and not contend for the same rows.
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from src import queue
from src.config import SETTINGS
from src.db import close_pool, connection

log = logging.getLogger("maintenance")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s :: %(message)s")


async def enqueue_refresh(source: str, limit: int) -> int:
    """Queue refresh jobs for the oldest still-active listings of a source."""
    async with connection() as conn:
        rows = await conn.fetch(
            """
            SELECT "sourceUrl"
            FROM "ScrapedListing"
            WHERE source = $1
              AND dropped = false
            ORDER BY "lastSeenAt" ASC
            LIMIT $2
            """,
            source, limit,
        )
    if not rows:
        return 0
    batch = [{
        "job_type": "refresh_listing",
        "source": source,
        "payload": {"url": r["sourceUrl"]},
        "priority": 3,
        "dedupe_key": f"refresh:{source}:{r['sourceUrl']}",
    } for r in rows]
    return await queue.enqueue_bulk(batch)


async def sweep(source: str) -> dict[str, int]:
    from src.persistence import sweep_stale, purge_ancient
    dropped = await sweep_stale(source, SETTINGS.dropped_days)
    purged = await purge_ancient(SETTINGS.purge_days)
    return {"dropped": dropped, "purged": purged}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="arabam")
    parser.add_argument("--refresh-limit", type=int, default=5000)
    args = parser.parse_args()

    queued = await enqueue_refresh(args.source, args.refresh_limit)
    log.info("queued %d refresh jobs for %s", queued, args.source)
    stats = await sweep(args.source)
    log.info("sweep: %s", stats)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        asyncio.run(close_pool())
