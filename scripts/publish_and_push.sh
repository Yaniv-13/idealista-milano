#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Run scraper first (manual CAPTCHA may be required)
python3 idealista_search.py

python3 scripts/publish_latest.py

git add data/latest.json data/latest.meta.json
git commit -m "Update latest data" || true
git push
