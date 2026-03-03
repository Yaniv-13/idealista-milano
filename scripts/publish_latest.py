#!/usr/bin/env python3
import json
import os
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
CFG = ROOT / "config.yaml"

def latest_listing_file():
    files = sorted([p for p in DATA.glob("listings_*.json") if not p.name.endswith(".meta.json")])
    if not files:
        return None
    return files[-1]

def meta_for(listing_path: Path) -> Path:
    return listing_path.with_suffix(".meta.json")

def load_config():
    try:
        import yaml
        with open(CFG) as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

def main():
    DATA.mkdir(parents=True, exist_ok=True)
    latest = latest_listing_file()
    if not latest:
        raise SystemExit("No listings_*.json found in data/")

    latest_meta = meta_for(latest)
    (DATA / "latest.json").write_text(latest.read_text())

    scraped_at = datetime.fromtimestamp(latest.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    meta = {}
    if latest_meta.exists():
        try:
            meta = json.loads(latest_meta.read_text())
        except Exception:
            meta = {}
    if not meta:
        cfg = load_config()
        meta = {
            "filters": cfg.get("filters", {}),
            "commute": cfg.get("commute", {}),
        }
    meta["scraped_at"] = scraped_at
    (DATA / "latest.meta.json").write_text(json.dumps(meta, indent=2))

    print(f"Wrote {DATA / 'latest.json'}")
    print(f"Wrote {DATA / 'latest.meta.json'}")

if __name__ == "__main__":
    main()
