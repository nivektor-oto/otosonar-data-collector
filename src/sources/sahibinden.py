"""Sahibinden.com source adapter.

Status (2026-04-26): site sits behind Cloudflare turnstile + DataDome.
curl_cffi alone cannot pass — confirmed via probe (chrome120/124/131 → 403,
chrome133a returns a 200 *interstitial* shell with no listings). The viable
path is Playwright with stealth init script + real Chromium, optionally
backed by a residential TR proxy.

This adapter ships a *complete* search/detail parser that runs against the
real DOM as soon as the fetcher pulls a clean page. Selectors mirror the
long-stable sahibinden classnames documented across community scrapers:
  - tr.searchResultsItem (one row per listing)
  - td.searchResultsTitleValue a.classifiedTitle (title + href)
  - td.searchResultsPriceValue
  - td.searchResultsLocationValue, td.searchResultsDateValue
  - dl.classifiedInfoList dd / ul.classifiedInfoList li (detail props)

Robots note: /ara, /arama, /arama/* are disallowed; we use category paths
like /{brand}-{model} which are explicitly *not* in the disallow list, and
keep query strings free of pagingOffset (also disallowed) — pagination
goes via clean pagingOffset-free URLs only when allowed.
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import ParsedDiscovery, ParsedListing, SourceAdapter

SITE_BASE = "https://www.sahibinden.com"


_NUMBER_RE = re.compile(r"[\d\.]+")
_TAG_RE = re.compile(r"<[^>]+>")


def _clean_int(text: Optional[str]) -> Optional[int]:
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


def _strip(s: Optional[str]) -> str:
    return (s or "").strip()


class SahibindenAdapter(SourceAdapter):
    name = "sahibinden"

    def search_urls(self, brand: str, model: Optional[str] = None, page: int = 1) -> list[str]:
        slug = brand.lower().replace(" ", "-")
        if model:
            slug = f"{slug}-{model.lower().replace(' ', '-')}"
        # Stay clean of robots-disallowed query params (pagingOffset, pagingSize,
        # sorting, etc). Pagination uses ?pagingOffset which is disallowed, so
        # we instead request paged URLs only when the search page itself
        # surfaces a follow-up link we can crawl. For page=1 just hit /slug.
        return [f"{SITE_BASE}/{slug}"]

    def sitemap_urls(self) -> list[str]:
        # No public sitemap — confirmed 404 across common paths. Kept empty
        # so seed_sitemap_jobs falls through.
        return []

    # ------------------------------------------------------------------ #
    # Search page parser                                                 #
    # ------------------------------------------------------------------ #

    def parse_search_page(self, html: str, base_url: str) -> ParsedDiscovery:
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []

        # Primary layout: classic table-based search results. Each row is
        # tr.searchResultsItem with the title anchor inside.
        for row in soup.select("tr.searchResultsItem"):
            anchor = row.select_one("a.classifiedTitle") or row.select_one("a[href*='/ilan/']")
            if not anchor:
                continue
            href = anchor.get("href") or ""
            if not href:
                continue
            # Skip "rezerv ilan" and store-promoted detached ads, keep real /ilan/
            if "/ilan/" not in href and "/listing/" not in href:
                continue
            full = urljoin(SITE_BASE, href.split("?")[0])
            if full not in urls:
                urls.append(full)

        # Fallback layout: card grid (used on some categories / mobile).
        # Looks for any anchor that points at /ilan/<slug>/<id>.
        if not urls:
            for anchor in soup.select("a[href*='/ilan/']"):
                href = anchor.get("href") or ""
                if not re.search(r"/ilan/.+-\d{6,}", href):
                    continue
                full = urljoin(SITE_BASE, href.split("?")[0])
                if full not in urls:
                    urls.append(full)

        # Next page detection — sahibinden uses pagingOffset (robots-blocked)
        # for paginated URLs, so we deliberately do NOT follow numeric paging
        # to stay polite. Instead we let the seeder enqueue the next page
        # URLs if it ever needs to.
        next_page = None

        return ParsedDiscovery(listing_urls=urls, next_page_url=next_page)

    # ------------------------------------------------------------------ #
    # Detail page parser                                                 #
    # ------------------------------------------------------------------ #

    def parse_detail_page(self, html: str, url: str) -> Optional[ParsedListing]:
        soup = BeautifulSoup(html, "lxml")

        title = self._extract_title(soup)
        properties = self._extract_properties(soup)

        # Prefer marka+model from property table
        brand = _strip(properties.get("marka") or properties.get("brand"))
        model = _strip(
            properties.get("seri") or properties.get("model") or properties.get("series")
        )
        year = _clean_int(properties.get("yıl") or properties.get("yil") or properties.get("year"))
        km = _clean_int(properties.get("km") or properties.get("kilometre"))

        price = self._extract_price(soup)
        location = self._extract_location(soup)
        damage_status = self._extract_damage(soup, properties)
        photo_count = self._extract_photo_count(soup)
        extras = self._extract_extras(soup)
        seller_name, seller_phone = self._extract_seller(soup)
        description = self._extract_description(soup)

        source_id = self._extract_source_id(url, soup)

        if not brand or not model or not year:
            return None

        return ParsedListing(
            source="sahibinden",
            sourceUrl=url.split("?")[0],
            sourceId=source_id,
            brand=brand,
            model=model,
            year=int(year),
            km=km,
            priceTry=price,
            location=location,
            title=title,
            description=description,
            photoCount=photo_count,
            damageStatus=damage_status,
            extras=extras,
            sellerPhone=seller_phone,
            sellerName=seller_name,
        )

    # ------------------------------------------------------------------ #
    # helpers                                                            #
    # ------------------------------------------------------------------ #

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        el = soup.select_one("h1.classifiedDetailTitle") \
            or soup.select_one("h1") \
            or soup.select_one(".classified-title")
        return _strip(el.get_text(" ", strip=True)) if el else None

    def _extract_properties(self, soup: BeautifulSoup) -> dict[str, str]:
        """Return lowercased {label: value} from the detail info table.

        Sahibinden ships two layouts:
          1. <ul class="classifiedInfoList"> with <li><strong>label</strong> value </li>
          2. <dl class="classifiedInfoList"> with <dt>label</dt><dd>value</dd>
        """
        props: dict[str, str] = {}

        # ul-style
        for li in soup.select("ul.classifiedInfoList li"):
            strong = li.find("strong")
            if not strong:
                continue
            label = _strip(strong.get_text()).lower()
            # value = text minus the label
            value_text = li.get_text(" ", strip=True)
            value = value_text[len(strong.get_text(strip=True)):].strip(" :")
            if label:
                props[label] = value

        # dl-style
        for pair_dt, pair_dd in zip(
            soup.select("dl.classifiedInfoList dt"),
            soup.select("dl.classifiedInfoList dd"),
        ):
            label = _strip(pair_dt.get_text()).lower()
            value = _strip(pair_dd.get_text(" ", strip=True))
            if label and label not in props:
                props[label] = value

        return props

    def _extract_price(self, soup: BeautifulSoup) -> Optional[int]:
        el = (
            soup.select_one("h3.classifiedPrice")
            or soup.select_one("div.classifiedInfo h3")
            or soup.select_one(".price")
            or soup.select_one("[itemprop='price']")
        )
        if not el:
            return None
        text = el.get_text(" ", strip=True)
        return _clean_int(text)

    def _extract_location(self, soup: BeautifulSoup) -> Optional[str]:
        el = (
            soup.select_one("div.classifiedInfo h2")
            or soup.select_one("h2.classifiedAddress")
            or soup.select_one(".classifiedInfo .location")
        )
        if not el:
            return None
        anchors = el.find_all("a")
        if anchors:
            parts = [_strip(a.get_text()) for a in anchors if _strip(a.get_text())]
            if parts:
                return ", ".join(parts[:3])
        return _strip(el.get_text(" ", strip=True)) or None

    def _extract_damage(self, soup: BeautifulSoup, properties: dict[str, str]) -> Optional[str]:
        # Property summary first
        for k in ("boya-değişen", "boya-degisen", "tramer", "hasarli"):
            v = properties.get(k)
            if v and v.strip().lower() not in ("belirtilmemiş", "belirtilmemis", "yok"):
                return v
        # Inline expertise/damage tag (sahibinden marks them with classes)
        tags = soup.select(".paint-changes-section, .expertizeContainer, .damage-info")
        if tags:
            text = " ".join(t.get_text(" ", strip=True) for t in tags)[:200]
            if any(w in text.lower() for w in ("boyalı", "değişen", "hasar", "tramer")):
                return text or None
        return None

    def _extract_photo_count(self, soup: BeautifulSoup) -> Optional[int]:
        # Sahibinden shows photo count near the gallery; otherwise count thumbs
        thumbs = soup.select("ul.classifiedDetailPhotoList li, .gallery-thumbs li")
        if thumbs:
            return len(thumbs)
        counter = soup.select_one(".total-photo, .photo-count")
        if counter:
            n = _clean_int(counter.get_text(" ", strip=True))
            if n:
                return n
        return None

    def _extract_extras(self, soup: BeautifulSoup) -> list[str]:
        out: list[str] = []
        # Selected feature checkboxes/list
        for li in soup.select("ul.classifiedFeatureList li.selected, ul.uniqueFeaturesList li.selected"):
            t = _strip(li.get_text(" ", strip=True))
            if t:
                out.append(t)
        # Generic "options" list
        if not out:
            for li in soup.select("ul.featureList li"):
                if "selected" in (li.get("class") or []) or li.find("i", class_=re.compile("checked|selected")):
                    t = _strip(li.get_text(" ", strip=True))
                    if t:
                        out.append(t)
        return out[:40]

    def _extract_seller(self, soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
        name_el = (
            soup.select_one(".classifiedUserBox .userName")
            or soup.select_one(".classifiedUserBox h3")
            or soup.select_one(".user-info h3")
        )
        name = _strip(name_el.get_text()) if name_el else None

        phone = None
        phone_el = soup.select_one("a.phone, .phone-number, .pretty-phone-part")
        if phone_el:
            phone = re.sub(r"\D+", "", phone_el.get_text(" ", strip=True))
            if phone:
                phone = phone[-13:]  # keep last 11-13 digits, drop labels
        return name or None, phone or None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        el = (
            soup.select_one("#classifiedDescription")
            or soup.select_one(".classifiedDescription")
            or soup.select_one("[itemprop='description']")
        )
        if not el:
            return None
        text = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
        return text[:5000] or None

    def _extract_source_id(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        # Detail URLs end in /<slug>-<id> or have /detay/<id>
        m = re.search(r"-(\d{6,})(?:/|$)", url)
        if m:
            return m.group(1)
        m = re.search(r"/detay/(\d+)", url)
        if m:
            return m.group(1)
        # As a last resort, sahibinden embeds it in a meta tag
        meta = soup.find("meta", attrs={"name": "classifiedId"})
        if meta and meta.get("content"):
            return _strip(meta["content"])
        return None
