import os
from dataclasses import dataclass, field

@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: os.environ["DATABASE_URL"])
    worker_id: str = field(default_factory=lambda: os.environ.get("WORKER_ID", "local-dev"))
    max_concurrent_fetches: int = int(os.environ.get("MAX_CONCURRENT", "4"))
    job_batch_size: int = int(os.environ.get("JOB_BATCH_SIZE", "50"))
    fetch_timeout_ms: int = int(os.environ.get("FETCH_TIMEOUT_MS", "25000"))
    fetch_delay_min_ms: int = int(os.environ.get("FETCH_DELAY_MIN_MS", "800"))
    fetch_delay_max_ms: int = int(os.environ.get("FETCH_DELAY_MAX_MS", "2500"))
    max_job_attempts: int = int(os.environ.get("MAX_JOB_ATTEMPTS", "3"))
    stale_days: int = int(os.environ.get("STALE_DAYS", "7"))
    dropped_days: int = int(os.environ.get("DROPPED_DAYS", "14"))
    purge_days: int = int(os.environ.get("PURGE_DAYS", "60"))

SETTINGS = Settings()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]
