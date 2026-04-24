"""Playwright-backed page fetcher with stealth touches.

Design goals:
  - Keep one browser across many fetches (launch cost matters in CI)
  - Rotate user agents per context
  - Randomised human-like delays between navigations
  - Gracefully degrade when anti-bot walls arrive
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Optional

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


@dataclass
class FetchResult:
    url: str
    status: Optional[int]
    html: Optional[str]
    final_url: Optional[str]
    blocked: bool = False
    error: Optional[str] = None


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


_BLOCKED_MARKERS = (
    "cf-chl-",
    "Just a moment...",
    "datadome",
    "Checking your browser",
    "Access denied",
    "captcha-delivery",
)


def _looks_blocked(html: str, status: Optional[int]) -> bool:
    if status in (403, 429, 503):
        return True
    if not html:
        return False
    lowered = html[:10000].lower()
    return any(marker.lower() in lowered for marker in _BLOCKED_MARKERS)
