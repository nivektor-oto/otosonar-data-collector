"""Postgres-based job queue for the scraper fleet.

Jobs live in ScraperJob. Workers claim with SELECT ... FOR UPDATE SKIP LOCKED
so multiple shards can pull in parallel without clobbering each other.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

import asyncpg

from .db import connection, json_encode


async def enqueue(
    job_type: str,
    source: str,
    payload: dict[str, Any],
    *,
    priority: int = 0,
    dedupe_key: Optional[str] = None,
    scheduled_at: Optional[datetime] = None,
) -> Optional[str]:
    """Add one job. Returns id or None if dedupe_key collision."""
    async with connection() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO "ScraperJob"
                    (id, "jobType", source, payload, priority, "dedupeKey", "scheduledAt")
                VALUES
                    (gen_random_uuid()::text, $1, $2, $3::jsonb, $4, $5, $6)
                RETURNING id
                """,
                job_type, source, json_encode(payload), priority, dedupe_key,
                scheduled_at or datetime.utcnow(),
            )
            return row["id"] if row else None
        except asyncpg.UniqueViolationError:
            return None


async def enqueue_bulk(rows: list[dict[str, Any]]) -> int:
    """Batch insert, skipping dedupe collisions. Returns inserted count."""
    if not rows:
        return 0
    async with connection() as conn:
        records = [
            (
                r["job_type"], r["source"], json_encode(r["payload"]),
                r.get("priority", 0), r.get("dedupe_key"),
                r.get("scheduled_at", datetime.utcnow()),
            )
            for r in rows
        ]
        result = await conn.executemany(
            """
            INSERT INTO "ScraperJob"
                (id, "jobType", source, payload, priority, "dedupeKey", "scheduledAt")
            VALUES
                (gen_random_uuid()::text, $1, $2, $3::jsonb, $4, $5, $6)
            ON CONFLICT ("dedupeKey") DO NOTHING
            """,
            records,
        )
        return len(records)


async def claim(worker_id: str, source: Optional[str], limit: int) -> list[asyncpg.Record]:
    """Atomically claim up to `limit` pending jobs. Returns claimed rows."""
    async with connection() as conn:
        source_filter = "AND source = $3" if source else ""
        params: list[Any] = [worker_id, limit]
        if source:
            params.append(source)
        rows = await conn.fetch(
            f"""
            WITH candidates AS (
                SELECT id
                FROM "ScraperJob"
                WHERE status = 'pending'
                  AND "scheduledAt" <= NOW()
                  AND attempts < "maxAttempts"
                  {source_filter}
                ORDER BY priority DESC, "scheduledAt" ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE "ScraperJob" j
            SET status = 'running',
                "startedAt" = NOW(),
                "workerId" = $1,
                attempts = attempts + 1
            FROM candidates c
            WHERE j.id = c.id
            RETURNING j.id, j."jobType", j.source, j.payload, j.priority, j.attempts
            """,
            *params,
        )
        return rows


async def mark_done(job_id: str) -> None:
    async with connection() as conn:
        await conn.execute(
            """
            UPDATE "ScraperJob"
            SET status = 'done', "completedAt" = NOW()
            WHERE id = $1
            """,
            job_id,
        )


async def mark_failed(job_id: str, error: str, *, retry: bool = True) -> None:
    """Mark failed. If retry=True and under maxAttempts, revert to pending with backoff."""
    async with connection() as conn:
        if retry:
            await conn.execute(
                """
                UPDATE "ScraperJob"
                SET status = CASE
                    WHEN attempts >= "maxAttempts" THEN 'failed'
                    ELSE 'pending'
                END,
                "lastError" = $2,
                "scheduledAt" = NOW() + (INTERVAL '1 minute' * (attempts * attempts * 2)),
                "workerId" = NULL,
                "startedAt" = NULL
                WHERE id = $1
                """,
                job_id, error[:2000],
            )
        else:
            await conn.execute(
                """
                UPDATE "ScraperJob"
                SET status = 'failed',
                    "completedAt" = NOW(),
                    "lastError" = $2
                WHERE id = $1
                """,
                job_id, error[:2000],
            )


async def reap_stuck(older_than_minutes: int = 30) -> int:
    """Jobs stuck 'running' past threshold get reverted to pending."""
    async with connection() as conn:
        result = await conn.execute(
            """
            UPDATE "ScraperJob"
            SET status = 'pending',
                "workerId" = NULL,
                "startedAt" = NULL,
                "lastError" = 'reaped-stuck-running'
            WHERE status = 'running'
              AND "startedAt" < NOW() - ($1::int * INTERVAL '1 minute')
              AND attempts < "maxAttempts"
            """,
            older_than_minutes,
        )
        # asyncpg executemany returns 'UPDATE n' string
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0


async def queue_depth(source: Optional[str] = None) -> dict[str, int]:
    async with connection() as conn:
        source_filter = "AND source = $1" if source else ""
        params = [source] if source else []
        rows = await conn.fetch(
            f"""
            SELECT status, COUNT(*) AS n
            FROM "ScraperJob"
            WHERE 1=1 {source_filter}
            GROUP BY status
            """,
            *params,
        )
        return {r["status"]: r["n"] for r in rows}
