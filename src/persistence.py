"""Upsert scraped listings into Neon with history snapshots.

Dedup strategy:
  - UNIQUE(source, sourceId) when both are set
  - Fallback: sourceUrl is unique
  - Cross-source match handled separately by dedupe.py
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from .db import connection, json_encode


async def upsert_listing(data: dict[str, Any]) -> tuple[str, bool]:
    """Insert or update a ScrapedListing row and record history.

    Returns (listingId, is_new).

    Expected keys:
        source, sourceUrl, sourceId?, brand, model, year, km, priceTry,
        location?, title?, description?, photoCount?, damageStatus?,
        extras? (list), sellerPhone?, sellerName?
    """
    async with connection() as conn:
        async with conn.transaction():
            now = datetime.utcnow()
            row = await conn.fetchrow(
                """
                INSERT INTO "ScrapedListing" (
                    id, source, "sourceUrl", "sourceId",
                    brand, model, year, km, "priceTry",
                    location, title, description, "photoCount",
                    "damageStatus", extras, "sellerPhone", "sellerName",
                    "firstSeenAt", "lastSeenAt", "scrapedAt", dropped
                ) VALUES (
                    gen_random_uuid()::text, $1, $2, $3,
                    $4, $5, $6, $7, $8,
                    $9, $10, $11, $12,
                    $13, $14, $15, $16,
                    $17, $17, $17, false
                )
                ON CONFLICT ("sourceUrl") DO UPDATE SET
                    "priceTry" = EXCLUDED."priceTry",
                    km = EXCLUDED.km,
                    title = COALESCE(EXCLUDED.title, "ScrapedListing".title),
                    description = COALESCE(EXCLUDED.description, "ScrapedListing".description),
                    "photoCount" = COALESCE(EXCLUDED."photoCount", "ScrapedListing"."photoCount"),
                    "damageStatus" = COALESCE(EXCLUDED."damageStatus", "ScrapedListing"."damageStatus"),
                    extras = CASE
                        WHEN array_length(EXCLUDED.extras, 1) > 0 THEN EXCLUDED.extras
                        ELSE "ScrapedListing".extras
                    END,
                    "sellerPhone" = COALESCE(EXCLUDED."sellerPhone", "ScrapedListing"."sellerPhone"),
                    "sellerName" = COALESCE(EXCLUDED."sellerName", "ScrapedListing"."sellerName"),
                    "sourceId" = COALESCE(EXCLUDED."sourceId", "ScrapedListing"."sourceId"),
                    location = COALESCE(EXCLUDED.location, "ScrapedListing".location),
                    "lastSeenAt" = EXCLUDED."lastSeenAt",
                    "scrapedAt" = EXCLUDED."scrapedAt",
                    dropped = false
                RETURNING id, (xmax = 0) AS inserted, "priceTry", km
                """,
                data["source"], data["sourceUrl"], data.get("sourceId"),
                data["brand"], data["model"], data["year"], data["km"], data["priceTry"],
                data.get("location"), data.get("title"), data.get("description"),
                data.get("photoCount"), data.get("damageStatus"),
                data.get("extras") or [], data.get("sellerPhone"), data.get("sellerName"),
                now,
            )
            listing_id = row["id"]
            is_new = bool(row["inserted"])

            # Record history point if price/km looks valid
            price = data.get("priceTry")
            km = data.get("km")
            if price is not None:
                await conn.execute(
                    """
                    INSERT INTO "ScrapedListingHistory"
                        (id, "listingId", "priceTry", km, status, "observedAt")
                    VALUES
                        (gen_random_uuid()::text, $1, $2, $3, 'active', $4)
                    """,
                    listing_id, price, km, now,
                )

            return listing_id, is_new


async def mark_dropped_urls(urls: list[str], source: str) -> int:
    if not urls:
        return 0
    async with connection() as conn:
        result = await conn.execute(
            """
            UPDATE "ScrapedListing"
            SET dropped = true,
                "lastSeenAt" = NOW()
            WHERE source = $1
              AND "sourceUrl" = ANY($2::text[])
              AND dropped = false
            """,
            source, urls,
        )
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0


async def sweep_stale(source: str, dropped_after_days: int) -> int:
    """Mark listings as dropped if not seen in N days."""
    async with connection() as conn:
        result = await conn.execute(
            """
            UPDATE "ScrapedListing"
            SET dropped = true
            WHERE source = $1
              AND dropped = false
              AND "lastSeenAt" < NOW() - ($2::int * INTERVAL '1 day')
            """,
            source, dropped_after_days,
        )
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0


async def purge_ancient(dropped_after_days: int) -> int:
    """Hard-delete listings that have been dropped for a long time."""
    async with connection() as conn:
        result = await conn.execute(
            """
            DELETE FROM "ScrapedListing"
            WHERE dropped = true
              AND "lastSeenAt" < NOW() - ($1::int * INTERVAL '1 day')
            """,
            dropped_after_days,
        )
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0


async def start_run(worker_id: str, worker_type: str, source: str) -> str:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO "ScraperRun" (id, "workerId", "workerType", source)
            VALUES (gen_random_uuid()::text, $1, $2, $3)
            RETURNING id
            """,
            worker_id, worker_type, source,
        )
        return row["id"]


async def finish_run(run_id: str, metrics: dict[str, Any]) -> None:
    async with connection() as conn:
        await conn.execute(
            """
            UPDATE "ScraperRun"
            SET "completedAt" = NOW(),
                "fetchCount" = $2,
                "successCount" = $3,
                "errorCount" = $4,
                "newListings" = $5,
                "updatedListings" = $6,
                "droppedListings" = $7,
                "proxyIp" = $8,
                "statusBreakdown" = $9::jsonb,
                notes = $10
            WHERE id = $1
            """,
            run_id,
            metrics.get("fetch_count", 0),
            metrics.get("success_count", 0),
            metrics.get("error_count", 0),
            metrics.get("new_listings", 0),
            metrics.get("updated_listings", 0),
            metrics.get("dropped_listings", 0),
            metrics.get("proxy_ip"),
            json_encode(metrics.get("status_breakdown", {})),
            metrics.get("notes"),
        )
