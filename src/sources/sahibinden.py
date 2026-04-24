"""Sahibinden.com source adapter — currently PLACEHOLDER.

The site is behind Cloudflare Enterprise managed challenge. Direct HTTP
requests (even with real browser UAs) return 403. Scraping without a TR
residential proxy + cf_clearance cookie reuse is not viable.

This adapter is registered so the queue layer accepts sahibinden jobs, but
fetch attempts will mark them blocked until the pipeline is upgraded.
"""
from __future__ import annotations

from typing import Optional

from .base import ParsedDiscovery, ParsedListing, SourceAdapter

SITE_BASE = "https://www.sahibinden.com"


class SahibindenAdapter(SourceAdapter):
    name = "sahibinden"

    def search_urls(self, brand: str, model: Optional[str] = None, page: int = 1) -> list[str]:
        slug = brand.lower().replace(" ", "-")
        if model:
            slug = f"{slug}-{model.lower().replace(' ', '-')}"
        offset = (page - 1) * 50
        params = "?pagingSize=50"
        if offset:
            params = f"{params}&pagingOffset={offset}"
        return [f"{SITE_BASE}/{slug}{params}"]

    def sitemap_urls(self) -> list[str]:
        return [f"{SITE_BASE}/sitemap.xml"]

    def parse_search_page(self, html: str, base_url: str) -> ParsedDiscovery:
        # TODO: once residential proxy is wired, implement real parsing.
        return ParsedDiscovery(listing_urls=[], next_page_url=None)

    def parse_detail_page(self, html: str, url: str) -> Optional[ParsedListing]:
        # TODO: wire up once we can actually reach the site.
        return None
