"""Arabam.com source adapter.

Discovery: crawl /ikinci-el/otomobil/{brand}[-{model}]?page=N search pages.
Detail: extract window.productDetail JSON blob (richer than DOM selectors).

Anti-bot: negligible. Polite 1-2 req/sec with UA rotation is fine.
"""
from __future__ import annotations

import json
import re
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import ParsedDiscovery, ParsedListing, SourceAdapter

SITE_BASE = "https://www.arabam.com"

_PRODUCT_DETAIL_RE = re.compile(
    r"window\.productDetail\s*=\s*(\{.*?\})\s*;\s*window\.",
    re.DOTALL,
)

_NUMBER_RE = re.compile(r"[\d\.]+")


def _clean_int(text: str) -> Optional[int]:
    if not text:
        return None
    match = _NUMBER_RE.search(text.replace(",", ""))
    if not match:
        return None
    cleaned = match.group().replace(".", "")
    try:
        return int(cleaned)
    except ValueError:
        return None


class ArabamAdapter(SourceAdapter):
    name = "arabam"

    def search_urls(self, brand: str, model: Optional[str] = None, page: int = 1) -> list[str]:
        brand_slug = _slugify(brand)
        if model:
            path = f"/ikinci-el/otomobil/{brand_slug}-{_slugify(model)}"
        else:
            path = f"/ikinci-el/otomobil/{brand_slug}"
        if page > 1:
            return [f"{SITE_BASE}{path}?page={page}"]
        return [f"{SITE_BASE}{path}"]

    def sitemap_urls(self) -> list[str]:
        return [f"{SITE_BASE}/sitemap/sitemap.xml"]

    def parse_search_page(self, html: str, base_url: str) -> ParsedDiscovery:
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []

        for row in soup.select("tr.listing-list-item"):
            anchor = row.find("a", href=True)
            if not anchor:
                continue
            href = anchor["href"]
            if "/ilan/" not in href:
                continue
            full = urljoin(SITE_BASE, href.split("?")[0])
            if full not in urls:
                urls.append(full)

        next_page = None
        page_links = soup.select("a[href*='page=']")
        current = _current_page_from_url(base_url)
        for link in page_links:
            href = link.get("href", "")
            match = re.search(r"page=(\d+)", href)
            if match and int(match.group(1)) == current + 1:
                next_page = urljoin(SITE_BASE, href)
                break

        return ParsedDiscovery(listing_urls=urls, next_page_url=next_page)

    def parse_detail_page(self, html: str, url: str) -> Optional[ParsedListing]:
        blob = _extract_product_detail(html)
        if blob is None:
            return _parse_detail_fallback(html, url)
        return _listing_from_blob(blob, url)


def _slugify(text: str) -> str:
    text = text.strip().lower()
    replacements = {
        "ı": "i", "İ": "i", "ş": "s", "Ş": "s",
        "ğ": "g", "Ğ": "g", "ü": "u", "Ü": "u",
        "ö": "o", "Ö": "o", "ç": "c", "Ç": "c",
    }
    for tr, en in replacements.items():
        text = text.replace(tr, en)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def _current_page_from_url(url: str) -> int:
    match = re.search(r"page=(\d+)", url or "")
    return int(match.group(1)) if match else 1


def _extract_product_detail(html: str) -> Optional[dict]:
    """Parse window.productDetail = {...}; blob. The content may contain
    nested braces and escaped strings, so we use a brace-balancing scan."""
    needle = "window.productDetail"
    idx = html.find(needle)
    if idx < 0:
        return None
    eq = html.find("=", idx)
    if eq < 0:
        return None
    start = html.find("{", eq)
    if start < 0:
        return None

    depth = 0
    in_string = False
    escape = False
    end = -1
    for i in range(start, min(len(html), start + 2_000_000)):
        ch = html[i]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end <= start:
        return None

    raw = html[start:end]
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _listing_from_blob(blob: dict, url: str) -> Optional[ParsedListing]:
    advert_no = blob.get("AdvertNo") or blob.get("advertNo")
    if advert_no is None:
        return None
    source_id = str(advert_no)

    properties: dict[str, str] = {}
    for item in blob.get("Properties") or []:
        key = (item.get("Key") or item.get("Label") or "").strip().lower()
        value = item.get("Value")
        if key and value is not None:
            properties[key] = str(value)

    # Prefer explicit top-level fields where present; fall back to Properties.
    brand = (blob.get("BrandName")
             or properties.get("marka") or properties.get("brand") or "").strip()
    model = (properties.get("seri") or properties.get("model") or "").strip()
    year_raw = properties.get("yıl") or properties.get("yil") or ""
    km_raw = properties.get("kilometre") or properties.get("km") or ""

    year = _clean_int(year_raw) or 0
    km = _clean_int(km_raw)
    price = _clean_int(str(blob.get("Price") or blob.get("FormattedPrice") or ""))

    if not brand or not model or not year:
        return None

    location = _location_string(blob)
    damage_status = _derive_damage_status(blob, properties)
    extras = _derive_extras(blob)

    photo_count = 0
    photos = blob.get("Photos")
    if isinstance(photos, list):
        photo_count = len(photos)
    elif isinstance(photos, int):
        photo_count = photos

    phone = blob.get("MobilePhone") or None
    if isinstance(phone, dict):
        phone = phone.get("Number") or phone.get("PhoneNumber")
    if phone:
        phone = re.sub(r"\s+", "", str(phone))

    member = blob.get("Member") or {}
    seller_name = None
    if isinstance(member, dict):
        seller_name = member.get("FullName") or member.get("Name")

    description = blob.get("Description") or ""
    description_plain = _strip_html(description)[:5000] if description else None

    return ParsedListing(
        source="arabam",
        sourceUrl=_canonical_url(blob, url),
        sourceId=source_id,
        brand=brand,
        model=model,
        year=int(year),
        km=km,
        priceTry=price,
        location=location or None,
        title=blob.get("Title") or None,
        description=description_plain,
        photoCount=photo_count or None,
        damageStatus=damage_status,
        extras=extras,
        sellerPhone=phone,
        sellerName=seller_name,
    )


def _location_string(blob: dict) -> Optional[str]:
    """Arabam stores City/County as nested dicts. FullAddress is all-caps."""
    parts: list[str] = []
    for key in ("City", "County", "District"):
        node = blob.get(key)
        if isinstance(node, dict):
            name = node.get("Name")
            if name:
                parts.append(str(name))
        elif isinstance(node, str) and node:
            parts.append(node)
    if parts:
        return ", ".join(parts)
    full = blob.get("FullAddress")
    if isinstance(full, str) and full:
        # Title-case the first three path segments so we don't store yelling caps
        segs = [s.strip().title() for s in full.split("/")[:3] if s.strip()]
        return ", ".join(segs) if segs else full
    return None


def _canonical_url(blob: dict, fallback: str) -> str:
    canon = blob.get("Canonical") or blob.get("CanonicalUrl") or blob.get("Url")
    if not canon:
        return fallback
    if canon.startswith("http"):
        return canon.split("?")[0]
    return urljoin(SITE_BASE, canon).split("?")[0]


def _derive_damage_status(blob: dict, properties: dict[str, str]) -> Optional[str]:
    expertise = blob.get("Expertise") or {}
    details = expertise.get("ExpertiseDetails") or []
    if details:
        boyali = 0
        degisen = 0
        for d in details:
            vt = str(d.get("ValueText") or d.get("Status") or "").lower()
            if vt in ("painted", "boyalı", "boyali"):
                boyali += 1
            elif vt in ("changed", "değişen", "degisen", "swapped"):
                degisen += 1
        if degisen or boyali:
            parts = []
            if degisen:
                parts.append(f"{degisen} değişen")
            if boyali:
                parts.append(f"{boyali} boyalı")
            return ", ".join(parts)
        return "hasarsız"

    # Fallback to summary Properties fields
    summary = properties.get("boya-değişen") or properties.get("boya-degisen")
    if summary:
        return summary
    heavy = properties.get("ağır hasarlı") or properties.get("agir hasarli")
    if heavy and heavy.lower().strip() in ("evet", "var"):
        return "ağır hasarlı"
    return None


def _derive_extras(blob: dict) -> list[str]:
    features = blob.get("Features") or blob.get("Options") or []
    extras: list[str] = []
    if isinstance(features, list):
        for f in features:
            if isinstance(f, dict):
                name = f.get("Name") or f.get("Label")
                if name:
                    extras.append(str(name))
            elif isinstance(f, str):
                extras.append(f)
    return extras[:40]


_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    return re.sub(r"\s+", " ", _TAG_RE.sub(" ", text or "")).strip()


def _parse_detail_fallback(html: str, url: str) -> Optional[ParsedListing]:
    """Fallback when JSON blob is missing — scrape the DOM directly.

    We intentionally keep this conservative: only emit a listing if we can
    pin brand+model+year, otherwise drop so we don't write junk rows.
    """
    soup = BeautifulSoup(html, "lxml")
    props: dict[str, str] = {}
    for item in soup.select("div.property-item"):
        key_el = item.select_one(".property-key")
        val_el = item.select_one(".property-value")
        if key_el and val_el:
            props[key_el.get_text(strip=True).lower()] = val_el.get_text(strip=True)

    brand = props.get("marka") or ""
    model = props.get("model") or props.get("seri") or ""
    year = _clean_int(props.get("yıl") or props.get("yil") or "")
    km = _clean_int(props.get("km") or "")

    price_el = soup.select_one("div.product-price") or soup.select_one("span.listing-price")
    price = _clean_int(price_el.get_text(strip=True) if price_el else "")

    if not brand or not model or not year:
        return None

    advert_match = re.search(r"/(\d{7,10})(?:/|$)", url)
    source_id = advert_match.group(1) if advert_match else None

    return ParsedListing(
        source="arabam",
        sourceUrl=url.split("?")[0],
        sourceId=source_id,
        brand=brand,
        model=model,
        year=int(year),
        km=km,
        priceTry=price,
    )
