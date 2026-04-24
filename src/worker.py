"""Scraper worker entry point.

Invoked by GitHub Actions shards, or manually for local testing.

Flow:
  1. (optional) seed discovery jobs from catalog
  2. loop: claim pending jobs, execute, persist, mark done
  3. stop when queue depletes or wall-clock budget hits
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Optional

from . import persistence, queue
from .catalog import arabam_brand_model_pairs
from .config import SETTINGS
from .db import close_pool
from .fetcher import FetchResult, StealthFetcher
from .sources import REGISTRY, ParsedDiscovery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("worker")


async def seed_discovery_jobs(source: str, *, pages_per_combo: int = 2) -> int:
    if source not in REGISTRY:
        raise ValueError(f"unknown source: {source}")

    adapter = REGISTRY[source]
    seeded = 0
    rows: list[dict[str, Any]] = []
    pairs = arabam_brand_model_pairs() if source == "arabam" else []

    for brand, model in pairs:
        for page in range(1, pages_per_combo + 1):
            for url in adapter.search_urls(brand, model, page=page):
                dedupe_key = f"discover:{source}:{url}"
                rows.append({
                    "job_type": "discover_search",
                    "source": source,
                    "payload": {"url": url, "brand": brand, "model": model, "page": page},
                    "priority": max(1, 10 - page),
                    "dedupe_key": dedupe_key,
                })

    # Insert in batches so we don't blow out the wire
    for i in range(0, len(rows), 200):
        seeded += await queue.enqueue_bulk(rows[i:i + 200])
    log.info("seeded %d discovery jobs for %s", seeded, source)
    return seeded


async def _shard_allowed(payload: dict[str, Any], shard: int, shards: int) -> bool:
    """Simple hash-based shard filter for GH Actions matrix parallelism."""
    if shards <= 1:
        return True
    key = payload.get("url") or payload.get("brand") or ""
    bucket = hash(key) % shards
    return bucket == (shard - 1)


async def run(args: argparse.Namespace) -> None:
    source: Optional[str] = args.source
    budget_seconds = args.budget_seconds
    start = time.monotonic()

    metrics = {
        "fetch_count": 0, "success_count": 0, "error_count": 0,
        "new_listings": 0, "updated_listings": 0, "dropped_listings": 0,
        "status_breakdown": {},
    }

    # Reap stuck jobs from previous crashed shards
    reaped = await queue.reap_stuck(older_than_minutes=30)
    if reaped:
        log.info("reaped %d stuck running jobs", reaped)

    if args.seed:
        if not source:
            raise SystemExit("--seed requires --source")
        await seed_discovery_jobs(source, pages_per_combo=args.seed_pages)
        if args.seed_only:
            return

    run_id = await persistence.start_run(SETTINGS.worker_id, "shard", source or "any")

    async with StealthFetcher() as fetcher:
        while True:
            if time.monotonic() - start > budget_seconds:
                log.info("budget exhausted, stopping")
                break

            jobs = await queue.claim(
                worker_id=SETTINGS.worker_id,
                source=source,
                limit=SETTINGS.job_batch_size,
            )
            if not jobs:
                log.info("queue drained, stopping")
                break

            for job in jobs:
                if time.monotonic() - start > budget_seconds:
                    # re-queue gracefully: mark this one as pending again
                    await queue.mark_failed(job["id"], "budget-timeout", retry=True)
                    break
                try:
                    payload = dict(job["payload"]) if isinstance(job["payload"], dict) else json.loads(job["payload"])
                except (TypeError, ValueError):
                    payload = {}

                if not await _shard_allowed(payload, args.shard, args.shards):
                    # not ours — return to queue for another shard
                    await queue.mark_failed(job["id"], "shard-skip", retry=True)
                    continue

                await _handle_job(
                    fetcher=fetcher,
                    job_id=job["id"],
                    job_type=job["jobType"],
                    source=job["source"],
                    payload=payload,
                    metrics=metrics,
                )

    await persistence.finish_run(run_id, metrics)
    log.info("run complete: %s", metrics)


async def _handle_job(
    *,
    fetcher: StealthFetcher,
    job_id: str,
    job_type: str,
    source: str,
    payload: dict[str, Any],
    metrics: dict[str, Any],
) -> None:
    adapter = REGISTRY.get(source)
    if adapter is None:
        await queue.mark_failed(job_id, f"no-adapter:{source}", retry=False)
        return

    url = payload.get("url")
    if not url:
        await queue.mark_failed(job_id, "missing-url", retry=False)
        return

    result = await fetcher.fetch(url)
    metrics["fetch_count"] += 1
    status_key = str(result.status or "none")
    metrics["status_breakdown"][status_key] = metrics["status_breakdown"].get(status_key, 0) + 1

    if result.error or result.blocked:
        metrics["error_count"] += 1
        await queue.mark_failed(job_id, result.error or f"blocked:{result.status}", retry=True)
        if result.blocked:
            # cheap throttle after a block
            await asyncio.sleep(random.uniform(2.0, 5.0))
            await fetcher.rotate_identity()
        return

    html = result.html or ""
    try:
        if job_type == "discover_search":
            discovery = adapter.parse_search_page(html, url)
            await _enqueue_detail_jobs(source, discovery)
            # Also queue the next page if present
            if discovery.next_page_url:
                await queue.enqueue(
                    "discover_search",
                    source,
                    {"url": discovery.next_page_url,
                     "brand": payload.get("brand"),
                     "model": payload.get("model"),
                     "page": payload.get("page", 1) + 1},
                    priority=max(1, 8 - int(payload.get("page", 1))),
                    dedupe_key=f"discover:{source}:{discovery.next_page_url}",
                )
        elif job_type in ("fetch_detail", "refresh_listing"):
            parsed = adapter.parse_detail_page(html, url)
            if parsed is None:
                metrics["error_count"] += 1
                await queue.mark_failed(job_id, "parse-failed", retry=False)
                return
            _listing_id, is_new = await persistence.upsert_listing(parsed.to_dict())
            if is_new:
                metrics["new_listings"] += 1
            else:
                metrics["updated_listings"] += 1
        else:
            await queue.mark_failed(job_id, f"unknown-job-type:{job_type}", retry=False)
            return

        metrics["success_count"] += 1
        await queue.mark_done(job_id)
    except Exception as e:
        log.exception("job %s crashed", job_id)
        metrics["error_count"] += 1
        await queue.mark_failed(job_id, f"{type(e).__name__}: {e}"[:2000], retry=True)


async def _enqueue_detail_jobs(source: str, discovery: ParsedDiscovery) -> None:
    if not discovery.listing_urls:
        return
    rows = [{
        "job_type": "fetch_detail",
        "source": source,
        "payload": {"url": url},
        "priority": 5,
        "dedupe_key": f"detail:{source}:{url}",
    } for url in discovery.listing_urls]
    await queue.enqueue_bulk(rows)


def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=list(REGISTRY.keys()))
    parser.add_argument("--shard", type=int, default=1)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--budget-seconds", type=int, default=5 * 60 * 60)  # 5h default
    parser.add_argument("--seed", action="store_true", help="seed discovery jobs before running")
    parser.add_argument("--seed-pages", type=int, default=2)
    parser.add_argument("--seed-only", action="store_true", help="seed and exit")
    return parser


async def _entrypoint(args: argparse.Namespace) -> None:
    try:
        await run(args)
    finally:
        await close_pool()


def main() -> None:
    parser = _build_argparser()
    args = parser.parse_args()
    asyncio.run(_entrypoint(args))


if __name__ == "__main__":
    main()
