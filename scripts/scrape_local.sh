#!/usr/bin/env bash
# OtoSonar lokal scraper — GH Actions'ta arabam.com Cloudflare'i 403 verdi.
# Lokal TR IP'si curl_cffi chrome124 ile %100 200 dönüyor.
# Cron: */2 saat. Budget: 8 dakika. WORKER_ID benzersiz log için.
set -e
cd /home/aller/Desktop/otosonar-data-collector
export DATABASE_URL="postgresql://neondb_owner:npg_TRh9HoKYy6Cu@ep-shiny-base-alrbs1ug-pooler.c-3.eu-central-1.aws.neon.tech/neondb?sslmode=require"
export WORKER_ID="local-cron-$(date +%H%M)"
export JOB_BATCH_SIZE="15"
export FETCH_DELAY_MIN_MS="500"
export FETCH_DELAY_MAX_MS="1500"
exec .venv/bin/python -m src.worker --source arabam --budget-seconds 480 \
    >> /home/aller/Desktop/otosonar-data-collector/cron.log 2>&1
