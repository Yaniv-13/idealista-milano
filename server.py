#!/usr/bin/env python3
"""
Idealista Milan — Web UI Server
Run: python3 server.py
Then open: http://localhost:5050
"""

import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

# ── Absolute paths (works regardless of cwd) ──────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_WEB  = os.path.join(_HERE, "web")
_DATA = os.path.join(_HERE, "data")
os.makedirs(_DATA, exist_ok=True)

# ── Import scraping logic ──────────────────────────────────────────────────
sys.path.insert(0, _HERE)
import idealista_search as scraper
from commute import get_all_commutes

app = Flask(__name__, static_folder=_HERE, static_url_path="")
CORS(app)

# ── Shared state ───────────────────────────────────────────────────────────
_state = {
    "status": "idle",
    "progress": "",
    "captcha_needed": False,
    "captcha_url": "",
}
_captcha_event = threading.Event()

def today_cache_path() -> Path:
    return Path(_DATA) / f"listings_{datetime.now().strftime('%Y-%m-%d')}.json"

def today_cache_meta_path() -> Path:
    return Path(_DATA) / f"listings_{datetime.now().strftime('%Y-%m-%d')}.meta.json"

def save_raw(listings: list):
    with open(today_cache_path(), "w") as f:
        json.dump(listings, f, indent=2)

def save_meta(cfg: dict):
    with open(today_cache_meta_path(), "w") as f:
        json.dump({
            "filters": cfg.get("filters", {}),
            "commute": cfg.get("commute", {}),
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }, f, indent=2)

def load_raw() -> list:
    p = today_cache_path()
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return []

def set_status(status, progress=""):
    _state["status"] = status
    _state["progress"] = progress

def wait_for_captcha_solve(url: str):
    _state["captcha_needed"] = True
    _state["captcha_url"] = url
    _captcha_event.clear()
    _captcha_event.wait(timeout=300)
    _state["captcha_needed"] = False
    _state["captcha_url"] = ""

def apply_filters(listings: list, filters: dict, destinations: list) -> list:
    result = listings
    if filters.get("max_price"):
        result = [l for l in result if l["price_eur"] <= filters["max_price"]]
    if filters.get("min_price"):
        result = [l for l in result if l["price_eur"] >= filters["min_price"]]
    if filters.get("min_rooms"):
        result = [l for l in result if l.get("rooms") and int(l["rooms"]) >= filters["min_rooms"]]
    if filters.get("min_sqm"):
        result = [l for l in result if not l.get("sqm") or int(l["sqm"]) >= filters["min_sqm"]]
    for dest in destinations:
        if not dest.get("filter"):
            continue
        max_min = dest.get("max_commute_minutes")
        label = dest["label"]
        if not max_min:
            continue
        def passes(l, label=label, max_min=max_min):
            if l.get("lat") is None:
                return True
            for c in l.get("commutes", []):
                if c["label"] != label:
                    continue
                if c.get("passes_filter") is None:
                    return True
                return c.get("passes_filter", True)
            return True
        result = [l for l in result if passes(l)]
    return result

def run_scrape(cfg: dict):
    try:
        set_status("scraping", "Opening browser...")
        urls = scraper.build_urls(cfg)
        commute_cfg = cfg.get("commute", {})
        gmaps_key = commute_cfg.get("google_maps_api_key", "")
        destinations = commute_cfg.get("destinations", [])
        dep_hour = commute_cfg.get("departure_hour", 9)
        include_bus = commute_cfg.get("include_bus", False)

        browser = scraper.get_browser()
        browser.get("https://www.idealista.it/en/")
        time.sleep(5)
        html = browser.page_source
        if any(s in html.lower() for s in ["fai scorrere", "slide to verify", "cf-challenge", "just a moment"]):
            wait_for_captcha_solve("https://www.idealista.it/en/")
            time.sleep(2)

        all_listings = []
        for i, entry in enumerate(urls):
            url = entry["url"]
            neighborhood = entry["neighborhood"]
            set_status("scraping", f"Scraping {neighborhood} ({i+1}/{len(urls)})...")
            soup = scraper.fetch_page(url)
            if soup == "CAPTCHA":
                wait_for_captcha_solve(url)
                soup = scraper.fetch_page(url)
            if not soup or soup == "CAPTCHA":
                continue
            listings = scraper.parse_listings(soup, neighborhood)
            if len(listings) == 0:
                wait_for_captcha_solve(url)
                soup = scraper.fetch_page(url)
                if soup and soup != "CAPTCHA":
                    listings = scraper.parse_listings(soup, neighborhood)
            # Fill missing baths/pets only when not present
            for l in listings:
                if l.get("bathrooms") and l.get("pets_allowed") is not None:
                    continue
                detail_soup = scraper.fetch_page(l.get("url", ""))
                if detail_soup == "CAPTCHA":
                    wait_for_captcha_solve(l.get("url", ""))
                    detail_soup = scraper.fetch_page(l.get("url", ""))
                if detail_soup and detail_soup != "CAPTCHA":
                    baths, pets = scraper.parse_listing_page_details(detail_soup)
                    if not l.get("bathrooms") and baths:
                        l["bathrooms"] = baths
                    if l.get("pets_allowed") is None and pets is not None:
                        l["pets_allowed"] = pets
                time.sleep(0.6)
            if "pets_allowed" in cfg.get("filters", {}):
                for l in listings:
                    l["pets_allowed"] = bool(cfg["filters"].get("pets_allowed"))
            all_listings.extend(listings)
            time.sleep(1.5)

        min_sqm = cfg["filters"].get("min_sqm")
        if min_sqm:
            all_listings = [l for l in all_listings if not l.get("sqm") or int(l["sqm"]) >= min_sqm]

        if all_listings and gmaps_key and destinations:
            set_status("geocoding", f"Geocoding {len(all_listings)} addresses...")
            for listing in all_listings:
                addr = listing.get("address", "")
                if addr:
                    lat, lng = scraper.geocode_address(addr)
                    listing["lat"] = lat
                    listing["lng"] = lng
                    time.sleep(1.1)

        if gmaps_key and destinations and all_listings:
            for j, listing in enumerate(all_listings):
                set_status("commuting", f"Commute {j+1}/{len(all_listings)}: {listing.get('neighborhood','')}")
                listing["commutes"] = get_all_commutes(listing, destinations, gmaps_key, dep_hour, include_bus)
        else:
            for listing in all_listings:
                listing["commutes"] = []

        save_raw(all_listings)
        save_meta(cfg)
        set_status("done", f"Found {len(all_listings)} listings")
        scraper.quit_browser()
    except Exception as e:
        set_status("error", str(e))
        scraper.quit_browser()


@app.route("/")
def index():
    return send_from_directory(_HERE, "index.html")

@app.route("/api/status")
def api_status():
    return jsonify(_state)

@app.route("/api/config")
def api_config():
    cfg = scraper.load_config(os.path.join(_HERE, "config.yaml"))
    return jsonify(cfg)

@app.route("/api/cache-info")
def api_cache_info():
    p = today_cache_path()
    if p.exists():
        listings = load_raw()
        meta = {}
        mp = today_cache_meta_path()
        if mp.exists():
            with open(mp) as f:
                meta = json.load(f)
        return jsonify({
            "has_cache": True,
            "count": len(listings),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "scrape_filters": meta.get("filters"),
            "scrape_commute": meta.get("commute"),
            "scraped_at": meta.get("scraped_at"),
        })
    return jsonify({"has_cache": False})

@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    if _state["status"] in ("scraping", "geocoding", "commuting"):
        return jsonify({"error": "Already running"}), 400
    cfg = scraper.load_config(os.path.join(_HERE, "config.yaml"))
    body = request.json or {}
    if "filters" in body:
        cfg["filters"].update(body["filters"])
    if "commute" in body:
        cfg["commute"].update(body["commute"])
    threading.Thread(target=run_scrape, args=(cfg,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/listings", methods=["POST"])
def api_listings():
    raw = load_raw()
    if not raw:
        return jsonify({"error": "No cache for today. Please scrape first."}), 404
    cfg = scraper.load_config(os.path.join(_HERE, "config.yaml"))
    body = request.json or {}
    if "pets_allowed" in cfg.get("filters", {}):
        for l in raw:
            l["pets_allowed"] = bool(cfg["filters"].get("pets_allowed"))
    filtered = apply_filters(raw, body.get("filters", {}), body.get("destinations", []))
    def sort_key(l):
        best = min((c["duration_min"] for c in l.get("commutes",[]) if c.get("duration_min") is not None), default=9999)
        return (best, l["price_eur"])
    filtered.sort(key=sort_key)
    return jsonify({"listings": filtered, "total": len(filtered), "cached": len(raw)})

@app.route("/api/captcha-solved", methods=["POST"])
def api_captcha_solved():
    _captcha_event.set()
    return jsonify({"ok": True})

if __name__ == "__main__":
    print(f"\n🏠 Idealista Milan Search — Web UI")
    print(f"   Open: http://localhost:5050")
    print(f"   Serving from: {_HERE}\n")
    app.run(host="0.0.0.0", port=5050, debug=False)
