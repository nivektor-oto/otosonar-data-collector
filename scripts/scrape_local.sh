#!/usr/bin/env bash
# OtoSonar lokal scraper — GH Actions'ta arabam.com Cloudflare'i 403 verdi.
# Lokal TR IP'si curl_cffi chrome124 ile %100 200 dönüyor.
# Cron: */2 saat. Budget: 8 dakika. WORKER_ID benzersiz log için.
#
# sahibinden.com için cf_clearance cookie hibrit yolu (2026-04-26):
#   ~/.config/otosonar/sahibinden-cf.json varsa ve <2 saat eskiyse,
#   sahibinden'i de tara. Yoksa sadece arabam tara + TG uyarısı yolla
#   ("cookie yenile zamanı geldi").
set -e
cd /home/aller/Desktop/otosonar-data-collector
export DATABASE_URL="postgresql://neondb_owner:npg_TRh9HoKYy6Cu@ep-shiny-base-alrbs1ug-pooler.c-3.eu-central-1.aws.neon.tech/neondb?sslmode=require"
export WORKER_ID="local-cron-$(date +%H%M)"
export JOB_BATCH_SIZE="15"
export FETCH_DELAY_MIN_MS="500"
export FETCH_DELAY_MAX_MS="1500"

# arabam — her seferinde (curl_cffi yeterli)
.venv/bin/python -m src.worker --source arabam --budget-seconds 360 \
    >> /home/aller/Desktop/otosonar-data-collector/cron.log 2>&1

# sahibinden cookie yaşı kontrol (Python ile, taşınabilir)
COOKIE_OK=$(.venv/bin/python -c "
import json, sys
from datetime import datetime, timezone
from pathlib import Path
p = Path.home() / '.config/otosonar/sahibinden-cf.json'
if not p.exists(): print('NONE'); sys.exit(0)
try:
    d = json.loads(p.read_text())
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(d['saved_at'])).total_seconds()
    print('OK' if age <= 7200 and d.get('cookies') else 'STALE')
except Exception: print('ERR')
" 2>/dev/null || echo "ERR")

if [ "$COOKIE_OK" = "OK" ]; then
    echo "[$(date)] sahibinden cookie taze, scrape başlıyor" >> /home/aller/Desktop/otosonar-data-collector/cron.log
    .venv/bin/python -m src.worker --source sahibinden --budget-seconds 240 \
        >> /home/aller/Desktop/otosonar-data-collector/cron.log 2>&1 || true
else
    # cookie yok/eski → sadece günde 1 kez TG uyarısı yolla (rate-limit dosyası)
    LAST_NUDGE=/tmp/otosonar-cf-nudge.timestamp
    NOW=$(date +%s)
    LAST=$(cat $LAST_NUDGE 2>/dev/null || echo 0)
    DIFF=$((NOW - LAST))
    if [ $DIFF -gt 21600 ]; then  # 6 saat
        TOKEN="8600332658:AAEoz4qUmUPCYWZNFcKZOGi9HkAUJrfkX0E"
        CHAT="5748487741"
        TXT="🔔 sahibinden cookie yenilenmesi gerekiyor (durum: $COOKIE_OK). Açıkken: bash ~/refresh-sahibinden-cf.sh — 30sn"
        curl -sS -o /dev/null --max-time 8 \
            "https://api.telegram.org/bot$TOKEN/sendMessage" \
            --data-urlencode "chat_id=$CHAT" \
            --data-urlencode "text=$TXT" || true
        echo $NOW > $LAST_NUDGE
    fi
    echo "[$(date)] sahibinden cookie $COOKIE_OK → scrape atlandı" >> /home/aller/Desktop/otosonar-data-collector/cron.log
fi
