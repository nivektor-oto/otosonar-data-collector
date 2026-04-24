import asyncpg
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from .config import SETTINGS

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            SETTINGS.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60,
            statement_cache_size=0,  # Neon pgbouncer uyumu
        )
    return _pool


@asynccontextmanager
async def connection() -> AsyncIterator[asyncpg.Connection]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def json_encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)
