"""sahibinden.com cf_clearance cookie yenileme aracı (v2 — browser_cookie3).

Kullanım:
    cd /home/aller/Desktop/otosonar-data-collector
    .venv/bin/python scripts/refresh_sahibinden_cf.py

Akış:
    1. Sen normal tarayıcında (Firefox / Brave / Chrome) yeni sekme aç,
       https://www.sahibinden.com/kategori/otomobil sayfasına git.
    2. Eğer "I am human" Cloudflare kutusu çıkarsa tıkla. Sayfada gerçek
       araba ilanları görünene kadar bekle.
    3. Tarayıcı sekmesini KAPATMA. Terminale dön, Enter'a bas.
    4. Script Firefox/Chrome/Brave SQLite cookie store'larından sahibinden
       cookie'lerini okur, ~/.config/otosonar/sahibinden-cf.json'a yazar.

Neden bu yol: Playwright'ın açtığı tarayıcı çoğu zaman Cloudflare'i geçemez
(automation flag'leri). Senin günlük tarayıcın insan kullanım örüntüsüne
sahip olduğu için Cloudflare kolayca geçirir.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import browser_cookie3

CFG_DIR = Path.home() / ".config" / "otosonar"
OUT = CFG_DIR / "sahibinden-cf.json"

BROWSERS = ["firefox", "brave", "chrome", "chromium", "edge", "vivaldi", "opera"]
BROWSER_LABELS = {
    "firefox": "Firefox",
    "brave": "Brave",
    "chrome": "Google Chrome",
    "chromium": "Chromium",
    "edge": "Edge",
    "vivaldi": "Vivaldi",
    "opera": "Opera",
}

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0"
)


def collect_from_all_browsers() -> tuple[list[dict], list[str], str | None]:
    """Tüm yerel browser cookie store'larından sahibinden cookie'lerini topla."""
    all_cookies: dict[str, dict] = {}  # name → cookie (en yeni)
    found_in: list[str] = []
    errors: list[str] = []
    detected_ua: str | None = None

    for name in BROWSERS:
        fn = getattr(browser_cookie3, name, None)
        if fn is None:
            continue
        try:
            cj = fn(domain_name="sahibinden.com")
        except Exception as e:
            errors.append(f"{BROWSER_LABELS[name]}: {type(e).__name__}: {e}")
            continue

        cookies_here = list(cj)
        if not cookies_here:
            continue
        found_in.append(BROWSER_LABELS[name])
        # Firefox cookie değeri çoğunlukla en yeni → over-write
        for c in cookies_here:
            all_cookies[c.name] = {
                "name": c.name,
                "value": c.value,
                "domain": c.domain,
                "path": c.path or "/",
            }
        # UA bilgisi cookie store'da yok; firefox prefs.js okuyalım — basit yol:
        if detected_ua is None and name == "firefox":
            ua = _read_firefox_user_agent()
            if ua:
                detected_ua = ua

    return list(all_cookies.values()), found_in, errors[:5], detected_ua


def _read_firefox_user_agent() -> str | None:
    """Firefox prefs.js içinden general.useragent.override varsa al; yoksa
    sürüm numarasından default UA üret."""
    try:
        ff_root = Path.home() / ".mozilla" / "firefox"
        for prof in ff_root.glob("*.default*"):
            prefs = prof / "prefs.js"
            if not prefs.exists():
                continue
            for line in prefs.read_text(errors="ignore").splitlines():
                if "general.useragent.override" in line:
                    parts = line.split('"')
                    if len(parts) >= 4:
                        return parts[3]
    except Exception:
        pass
    return None


def main() -> int:
    CFG_DIR.mkdir(parents=True, exist_ok=True)

    print()
    print("=" * 72)
    print("SAHİBİNDEN COOKIE YENİLEME (v2 — kendi tarayıcından)")
    print("=" * 72)
    print()
    print("ADIM 1: Normal tarayıcında (Firefox/Brave/Chrome) yeni sekme aç,")
    print("        https://www.sahibinden.com/kategori/otomobil 'a git.")
    print("ADIM 2: 'I am human' kutusu çıkarsa tıkla. Sayfada gerçek arabalar")
    print("        görünene kadar bekle (5-10 sn).")
    print("ADIM 3: Sekmeyi KAPATMA. Buraya dön + Enter'a bas.")
    print()
    try:
        input("Hazırsan Enter: ")
    except KeyboardInterrupt:
        print("\niptal.")
        return 130

    cookies, found_in, errors, ua = collect_from_all_browsers()

    if not cookies:
        print()
        print("HATA: Hiçbir tarayıcıda sahibinden cookie bulunamadı.")
        if errors:
            print("Browser hataları:")
            for e in errors:
                print(f"  - {e}")
        print()
        print("İPUCU: Tarayıcında sayfayı açmadan önce kapatmış olabilirsin.")
        print("       Ya da kullandığın tarayıcı listede yok. Tekrar dene.")
        return 1

    has_cf = any(c["name"] == "cf_clearance" for c in cookies)
    payload = {
        "user_agent": ua or DEFAULT_UA,
        "cookies": cookies,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "host": "sahibinden.com",
        "has_cf_clearance": has_cf,
        "source_browsers": found_in,
    }

    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    os.chmod(OUT, 0o600)

    print()
    print(f"Yazıldı: {OUT}")
    print(f"  cookie sayısı   : {len(cookies)}")
    print(f"  cf_clearance var: {has_cf}")
    print(f"  kaynak browser  : {', '.join(found_in)}")
    print(f"  user-agent      : {(ua or DEFAULT_UA)[:80]}")
    print()
    if not has_cf:
        print("UYARI: cf_clearance cookie YOK — Cloudflare challenge geçilmemiş.")
        print("       Tarayıcıda sayfa gerçekten yüklendi mi? Tekrar dene.")
        return 2
    print("Hazır. Sonraki ~2 saat boyunca scraper sahibinden'i 200 alır.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
