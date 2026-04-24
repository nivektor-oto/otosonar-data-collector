from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ParsedDiscovery:
    """Output of parsing a listing search/category page."""
    listing_urls: list[str] = field(default_factory=list)
    next_page_url: Optional[str] = None


@dataclass
class ParsedListing:
    """Output of parsing a detail page. Field names match persistence upsert."""
    source: str
    sourceUrl: str
    sourceId: Optional[str]
    brand: str
    model: str
    year: int
    km: Optional[int]
    priceTry: Optional[int]
    location: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    photoCount: Optional[int] = None
    damageStatus: Optional[str] = None
    extras: list[str] = field(default_factory=list)
    sellerPhone: Optional[str] = None
    sellerName: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "sourceUrl": self.sourceUrl,
            "sourceId": self.sourceId,
            "brand": self.brand,
            "model": self.model,
            "year": self.year,
            "km": self.km,
            "priceTry": self.priceTry,
            "location": self.location,
            "title": self.title,
            "description": self.description,
            "photoCount": self.photoCount,
            "damageStatus": self.damageStatus,
            "extras": self.extras,
            "sellerPhone": self.sellerPhone,
            "sellerName": self.sellerName,
        }


class SourceAdapter(ABC):
    name: str = ""

    @abstractmethod
    def search_urls(self, brand: str, model: Optional[str] = None, page: int = 1) -> list[str]:
        """Build search/listing page URLs for a brand/model combo."""

    @abstractmethod
    def parse_search_page(self, html: str, base_url: str) -> ParsedDiscovery:
        """Extract listing detail URLs + next page."""

    @abstractmethod
    def parse_detail_page(self, html: str, url: str) -> Optional[ParsedListing]:
        """Extract structured listing from a detail page."""

    @abstractmethod
    def sitemap_urls(self) -> list[str]:
        """Root sitemap URLs (may reference sub-sitemaps)."""
