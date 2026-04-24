from .base import SourceAdapter, ParsedListing, ParsedDiscovery
from .arabam import ArabamAdapter
from .sahibinden import SahibindenAdapter

REGISTRY: dict[str, SourceAdapter] = {
    "arabam": ArabamAdapter(),
    "sahibinden": SahibindenAdapter(),
}

__all__ = ["REGISTRY", "SourceAdapter", "ParsedListing", "ParsedDiscovery"]
