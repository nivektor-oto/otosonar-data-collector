"""HTTP/Browser fetchers for the scraper.

Two backends:
  1. HttpFetcher (curl_cffi) — TLS-fingerprint-impersonating HTTP client.
     Fast, resource-light, gets past Cloudflare TLS-level bot checks.
     Default for arabam.com; passes 200 reliably (verified 2026-04-26).
  2. StealthFetcher (Playwright) — full Chromium for sites that need JS
     execution (Cloudflare turnstile, DataDome, anti-bot JS challenges).
     Reserved for sahibinden.com.

Selection rules live in `select_fetcher(source)`. Both expose the same
async context-manager + `.fetch()` contract returning `FetchResult`.
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Optional

from curl_cffi import requests as curl_requests
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeout,
    async_playwright,
)

from .config import SETTINGS, USER_AGENTS


STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['tr-TR', 'tr', 'en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
window.chrome = window.chrome || { runtime: {} };
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
        ? Promise.resolve({state: Notification.permission})
        : originalQuery(parameters)
);
"""


# curl_cffi browser fingerprints to rotate through. chrome124 has the
# strongest success rate against arabam.com per local probe; chrome131,
# chrome120 are fallbacks if a streak of 403s shows up.
CURL_IMPERSONATIONS = ["chrome124", "chrome131", "chrome120"]


@dataclass
class FetchResult:
    url: str
    status: Optional[int]
    html: Optional[str]
    final_url: Optional[str]
    blocked: bool = False
    error: Optional[str] = None


# --------------------------------------------------------------------------- #
# curl_cffi-based HTTP fetcher
# --------------------------------------------------------------------------- #

class HttpFetcher:
    """Async-compatible curl_cffi fetcher.

    curl_cffi itself is sync-only at runtime, so we hop to a thread pool via
    `asyncio.to_thread`. That keeps the worker's await-driven loop happy
    without blocking on each request.
    """

    def __init__(self, *, impersonate: Optional[str] = None) -> None:
        self._impersonate = impersonate or CURL_IMPERSONATIONS[0]
        self._session: Optional[curl_requests.Session] = None
        self._ua: Optional[str] = None

    async def __aenter__(self) -> "HttpFetcher":
        self._build_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if self._session is not None:
                self._session.close()
        except Exception:
            pass

    def _build_session(self) -> None:
        self._ua = random.choice(USER_AGENTS)
        self._session = curl_requests.Session(impersonate=self._impersonate)
        # curl_cffi assigns sensible defaults under impersonate; we still
        # nudge a few headers to look natural and TR-flavoured.
        self._session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
        })

    async def fetch(self, url: str, *, wait_selector: Optional[str] = None) -> FetchResult:
        assert self._session is not None
        # Polite jitter so we don't hammer one host
        delay_ms = random.randint(SETTINGS.fetch_delay_min_ms, SETTINGS.fetch_delay_max_ms)
        await asyncio.sleep(delay_ms / 1000)

        def _do() -> FetchResult:
            assert self._session is not None
            try:
                resp = self._session.get(
                    url,
                    timeout=SETTINGS.fetch_timeout_ms / 1000,
                    allow_redirects=True,
                    headers={
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                        "Sec-Fetch-User": "?1",
                    },
                )
                status = resp.status_code
                final_url = resp.url
                html = resp.text or ""
                blocked = _looks_blocked(html, status)
                return FetchResult(
                    url=url,
                    status=status,
                    html=None if blocked else html,
                    final_url=final_url,
                    blocked=blocked,
                )
            except Exception as e:
                return FetchResult(
                    url=url, status=None, html=None, final_url=None,
                    error=f"{type(e).__name__}: {e}",
                )

        return await asyncio.to_thread(_do)

    async def rotate_identity(self) -> None:
        """Cycle to the next impersonation profile + fresh session/cookies."""
        try:
            if self._session is not None:
                self._session.close()
        except Exception:
            pass
        # rotate impersonate
        try:
            idx = CURL_IMPERSONATIONS.index(self._impersonate)
        except ValueError:
            idx = -1
        self._impersonate = CURL_IMPERSONATIONS[(idx + 1) % len(CURL_IMPERSONATIONS)]
        self._build_session()


# --------------------------------------------------------------------------- #
# Playwright-based browser fetcher (Cloudflare turnstile, DataDome, etc.)
# --------------------------------------------------------------------------- #

class StealthFetcher:
    def __init__(self) -> None:
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._user_agent: Optional[str] = None

    async def __aenter__(self) -> "StealthFetcher":
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--lang=tr-TR",
            ],
        )
        await self._refresh_context()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        if self._playwright:
            await self._playwright.stop()

    async def _refresh_context(self) -> None:
        assert self._browser is not None
        if self._context is not None:
            try:
                await self._context.close()
            except Exception:
                pass
        self._user_agent = random.choice(USER_AGENTS)
        self._context = await self._browser.new_context(
            user_agent=self._user_agent,
            locale="tr-TR",
            timezone_id="Europe/Istanbul",
            viewport={"width": 1366, "height": 768},
            device_scale_factor=1,
            is_mobile="iPhone" in self._user_agent,
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        await self._context.add_init_script(STEALTH_INIT_SCRIPT)
        self._page = await self._context.new_page()

    async def fetch(self, url: str, *, wait_selector: Optional[str] = None) -> FetchResult:
        assert self._page is not None
        try:
            response = await self._page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=SETTINGS.fetch_timeout_ms,
            )
            status = response.status if response else None
            if wait_selector:
                try:
                    await self._page.wait_for_selector(wait_selector, timeout=5000)
                except PlaywrightTimeout:
                    pass

            # Human-ish pause
            delay_ms = random.randint(SETTINGS.fetch_delay_min_ms, SETTINGS.fetch_delay_max_ms)
            await asyncio.sleep(delay_ms / 1000)

            html = await self._page.content()
            final_url = self._page.url

            # Detect anti-bot walls
            blocked = _looks_blocked(html, status)

            return FetchResult(
                url=url,
                status=status,
                html=None if blocked else html,
                final_url=final_url,
                blocked=blocked,
            )
        except PlaywrightTimeout as e:
            return FetchResult(url=url, status=None, html=None, final_url=None,
                               error=f"timeout: {e}")
        except Exception as e:
            return FetchResult(url=url, status=None, html=None, final_url=None,
                               error=f"{type(e).__name__}: {e}")

    async def rotate_identity(self) -> None:
        """Drop context + UA, get a fresh one. Call on 403/429 streaks."""
        await self._refresh_context()


# --------------------------------------------------------------------------- #
# Selection
# --------------------------------------------------------------------------- #

# Sites that require JS execution to pass anti-bot. Anything not in this set
# defaults to the much-cheaper HttpFetcher (curl_cffi).
_BROWSER_REQUIRED_SOURCES = {"sahibinden"}


def select_fetcher(source: Optional[str]):
    """Return an *async-context-manager-compatible* fetcher for the source.

    Heuristic: only spin up a browser when the target needs one. arabam.com
    yields fine to curl_cffi with chrome124 impersonation.
    """
    if source in _BROWSER_REQUIRED_SOURCES:
        return StealthFetcher()
    return HttpFetcher()


_BLOCKED_MARKERS = (
    "cf-chl-",
    "just a moment...",
    "datadome",
    "checking your browser",
    "access denied",
    "captcha-delivery",
    "geo.captcha-delivery",
    "/cdn-cgi/challenge-platform/",
)


def _looks_blocked(html: str, status: Optional[int]) -> bool:
    if status in (403, 429, 503):
        return True
    if not html:
        return False
    lowered = html[:20000].lower()
    return any(marker.lower() in lowered for marker in _BLOCKED_MARKERS)
