"""sahibinden.com cf_clearance cookie yenileme aracı.

Kullanım:
    cd /home/aller/Desktop/otosonar-data-collector
    .venv/bin/python scripts/refresh_sahibinden_cf.py

Akış:
    1. Görünür (headed) Chromium açılır.
    2. sahibinden.com/kategori/otomobil sayfasına gider.
    3. Cloudflare turnstile / challenge varsa kullanıcı elle çözer (1 tık).
    4. Sayfada gerçek arama sonucu göründüğünde Enter'a bas.
    5. Script cookie'leri + user-agent'ı ~/.config/otosonar/sahibinden-cf.json
       dosyasına yazar (mode 600). Scraper bunu okuyup curl_cffi
       request'lerine inject eder.

Cookie ömrü genelde 30dk-2h. cron öncesi yaş kontrolü scrape_local.sh içinde.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

CFG_DIR = Path.home() / ".config" / "otosonar"
OUT = CFG_DIR / "sahibinden-cf.json"

STEALTH_INIT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['tr-TR', 'tr', 'en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
window.chrome = window.chrome || { runtime: {} };
"""

TARGET_URL = "https://www.sahibinden.com/kategori/otomobil"


def main() -> int:
    CFG_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[refresh-cf] hedef: {TARGET_URL}")
    print("[refresh-cf] Browser açılıyor...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--lang=tr-TR",
                "--start-maximized",
            ],
        )
        context = browser.new_context(
            locale="tr-TR",
            timezone_id="Europe/Istanbul",
            viewport={"width": 1366, "height": 768},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        context.add_init_script(STEALTH_INIT)
        page = context.new_page()
        try:
            page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as e:
            print(f"[refresh-cf] navigation hatası: {e}")

        print()
        print("=" * 70)
        print("ŞİMDİ:")
        print("  - Açılan tarayıcıda Cloudflare 'I am human' kutusu varsa tıkla.")
        print("  - Sayfada gerçek araba sonuçları görünürse hazırsın demektir.")
        print("  - Buradan terminale dönüp ENTER'a bas, cookie kaydedilecek.")
        print("=" * 70)
        try:
            input("[refresh-cf] Hazır olduğunda Enter: ")
        except KeyboardInterrupt:
            print("\n[refresh-cf] iptal edildi.")
            browser.close()
            return 130

        cookies = context.cookies()
        try:
            ua = page.evaluate("() => navigator.userAgent")
        except Exception:
            ua = (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )

        # Sahibinden cookie'lerini filtrele (alanı .sahibinden.com olanlar)
        cf_cookies = [c for c in cookies if "sahibinden" in c.get("domain", "")]
        has_cf_clearance = any(c.get("name") == "cf_clearance" for c in cf_cookies)

        payload = {
            "user_agent": ua,
            "cookies": cf_cookies,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "host": "sahibinden.com",
            "has_cf_clearance": has_cf_clearance,
        }

        OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        os.chmod(OUT, 0o600)

        print()
        print(f"[refresh-cf] Yazıldı: {OUT}")
        print(f"[refresh-cf] cookie sayısı: {len(cf_cookies)}")
        print(f"[refresh-cf] cf_clearance var mı: {has_cf_clearance}")
        if not has_cf_clearance:
            print("[refresh-cf] UYARI: cf_clearance cookie yok — challenge geçilmemiş olabilir.")

        browser.close()
        return 0 if has_cf_clearance else 2


if __name__ == "__main__":
    sys.exit(main())
