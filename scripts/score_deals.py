"""Compute deal scores for fresh listings.

Algorithm (v1):
  1. For each brand/model/year bucket, compute median price among comparable
     active listings with km within ±20% of the target.
  2. dealScore = (median - listing.price) / median, clipped to [-1, 1].
  3. If dealScore >= 0.15 AND emsalCount >= 5 AND confidence >= 0.5 →
     insert into DealAlert.
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from src.db import close_pool, connection

log = logging.getLogger("scoring")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s :: %(message)s")

DEAL_THRESHOLD = 0.15
MIN_EMSAL = 5


async def score_all(*, limit: int, lookback_hours: int) -> dict[str, int]:
    async with connection() as conn:
        # Pick fresh, unscored (or stale-score) listings
        candidates = await conn.fetch(
            """
            SELECT id, brand, model, year, km, "priceTry", location
            FROM "ScrapedListing"
            WHERE dropped = false
              AND "priceTry" IS NOT NULL
              AND km IS NOT NULL
              AND "lastSeenAt" > NOW() - ($1::int * INTERVAL '1 hour')
              AND (
                "dealScore" IS NULL
                OR "emsalCountAtScore" IS NULL
              )
            ORDER BY "lastSeenAt" DESC
            LIMIT $2
            """,
            lookback_hours, limit,
        )

        scored = 0
        alerts_created = 0
        for row in candidates:
            stats = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS n,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY "priceTry") AS median,
                    percentile_cont(0.25) WITHIN GROUP (ORDER BY "priceTry") AS p25,
                    percentile_cont(0.75) WITHIN GROUP (ORDER BY "priceTry") AS p75
                FROM "ScrapedListing"
                WHERE dropped = false
                  AND brand = $1
                  AND model = $2
                  AND year = $3
                  AND km IS NOT NULL
                  AND "priceTry" IS NOT NULL
                  AND km BETWEEN ($4 * 0.8)::int AND ($4 * 1.2)::int
                  AND id <> $5
                """,
                row["brand"], row["model"], row["year"], row["km"], row["id"],
            )
            emsal_count = int(stats["n"] or 0)
            median = float(stats["median"] or 0)
            if emsal_count < MIN_EMSAL or median <= 0:
                await conn.execute(
                    """
                    UPDATE "ScrapedListing"
                    SET "dealScore" = 0,
                        "marketPosition" = 'unknown',
                        "emsalCountAtScore" = $2
                    WHERE id = $1
                    """,
                    row["id"], emsal_count,
                )
                scored += 1
                continue

            price = float(row["priceTry"])
            deal_score = max(-1.0, min(1.0, (median - price) / median))
            position = (
                "under" if deal_score >= 0.10
                else "over" if deal_score <= -0.10
                else "market"
            )
            spread = float((stats["p75"] or median) - (stats["p25"] or median))
            confidence = _confidence(emsal_count, median, spread)

            await conn.execute(
                """
                UPDATE "ScrapedListing"
                SET "dealScore" = $2,
                    "marketPosition" = $3,
                    "emsalCountAtScore" = $4
                WHERE id = $1
                """,
                row["id"], deal_score, position, emsal_count,
            )
            scored += 1

            if deal_score >= DEAL_THRESHOLD and confidence >= 0.5:
                savings = int(median - price)
                await conn.execute(
                    """
                    INSERT INTO "DealAlert" (
                        id, "listingId", "dealScore", "marketMedian",
                        "marketP25", "listingPrice", savings,
                        "emsalCount", confidence, "brandModel", year, km
                    ) VALUES (
                        gen_random_uuid()::text, $1, $2, $3::int,
                        $4::int, $5, $6, $7, $8, $9, $10, $11
                    )
                    """,
                    row["id"], deal_score, int(median),
                    int(stats["p25"] or 0), int(row["priceTry"]), savings,
                    emsal_count, confidence,
                    f"{row['brand']} {row['model']}", row["year"], row["km"],
                )
                alerts_created += 1
        return {"scored": scored, "alerts": alerts_created}


def _confidence(n: int, median: float, spread: float) -> float:
    # Saturating confidence: more emsal + tighter spread = higher
    if median <= 0:
        return 0.0
    tightness = 1.0 - min(1.0, spread / max(1.0, median))  # 0..1
    size = min(1.0, (n - MIN_EMSAL + 1) / 25)  # saturates at ~30 emsal
    return round(0.4 * tightness + 0.6 * size, 3)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20000)
    parser.add_argument("--lookback-hours", type=int, default=48)
    args = parser.parse_args()

    stats = await score_all(limit=args.limit, lookback_hours=args.lookback_hours)
    log.info("scoring complete: %s", stats)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        asyncio.run(close_pool())
