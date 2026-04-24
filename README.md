# Market Data Collector

Türkiye ikinci el otomotiv pazarı için açık kaynak veri toplama aracı. Sahibinden.com, Arabam.com gibi halka açık ilan sitelerinden yapılandırılmış veri çıkarır, dedupe eder, zaman serisi olarak saklar.

## Mimari

- **Discovery:** sitemap + arama sayfası crawling ile yeni ilan keşfi
- **Fetcher:** Playwright (stealth modda) ile detay sayfa çekme
- **Parser:** HTML → yapılandırılmış dict
- **Dedupe:** kaynak içi ve cross-source duplicate tespit
- **Scoring:** emsal havuzuna göre fırsat skoru hesaplama
- **Persistence:** Neon Postgres (asyncpg)

## Çalıştırma

GitHub Actions üzerinden otomatik çalışır (her 2 saatte bir, 10 paralel shard). Manuel trigger için:

```bash
gh workflow run scrape.yml -f source=arabam -f shard=1 -f shards=10
```

## Yerel geliştirme

```bash
pip install -r requirements.txt
playwright install chromium
export DATABASE_URL="postgresql://..."
python -m src.worker --source arabam --shard 1 --shards 10
```

## Lisans

MIT
