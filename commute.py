"""
Commute time calculation via Google Maps Directions API.
Supports walking + transit (metro, tram, optionally bus).
"""

import re
import time
from typing import Optional

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

GMAPS_URL = "https://maps.googleapis.com/maps/api/directions/json"


def get_transit_commute(
    origin_lat: float,
    origin_lng: float,
    destination: dict,
    api_key: str,
    departure_hour: int = 9,
    include_bus: bool = False,
) -> dict:
    """
    Get commute time (walk + transit) from origin to a destination.

    Returns dict with:
        label           destination label
        duration_min    total door-to-door minutes (None if failed)
        walk_mins       total walking minutes across all legs
        summary         e.g. "28 min (M2 + M1)"
        passes_filter   True/False — only set if destination has filter:true
        walk_warning    True if any walk leg exceeds destination max_walk_minutes
    """
    label = destination["label"]
    max_commute = destination.get("max_commute_minutes")
    max_walk = destination.get("max_walk_minutes", 999)
    do_filter = destination.get("filter", False)

    result = {
        "label": label,
        "duration_min": None,
        "walk_mins": None,
        "summary": "N/A",
        "passes_filter": None,
        "walk_warning": False,
    }

    if not HAS_REQUESTS or not api_key:
        return result

    origin = f"{origin_lat},{origin_lng}"
    dest = (f"{destination['lat']},{destination['lng']}"
            if "lat" in destination and "lng" in destination
            else destination["address"])

    # Next Monday at departure_hour Milan time — always a future weekday
    import datetime, zoneinfo
    milan_tz = zoneinfo.ZoneInfo("Europe/Rome")
    now = datetime.datetime.now(milan_tz)
    days_until_monday = (7 - now.weekday()) % 7 or 7
    next_monday = now + datetime.timedelta(days=days_until_monday)
    departure = next_monday.replace(hour=departure_hour, minute=0, second=0, microsecond=0)
    departure_ts = int(departure.timestamp())

    transit_modes = ["subway", "tram"] + (["bus"] if include_bus else [])

    try:
        r = _requests.get(GMAPS_URL, params={
            "origin": origin,
            "destination": dest,
            "mode": "transit",
            "transit_mode": "|".join(transit_modes),
            "transit_routing_preference": "fewer_transfers",
            "departure_time": departure_ts,
            "key": api_key,
            "language": "en",
        }, timeout=15)
        data = r.json()

        if data.get("status") != "OK" or not data.get("routes"):
            status = data.get("status", "ERR")
            result["summary"] = f"No route ({status})"
            # Don't filter out listings where route lookup simply failed
            # Only filter on confirmed time violations, not API errors
            result["passes_filter"] = None
            return result

        leg = data["routes"][0]["legs"][0]
        total_min = round(leg["duration"]["value"] / 60)
        result["duration_min"] = total_min

        # Parse steps into full route
        walk_mins = 0
        transit_names = []
        max_single_walk = 0
        route_steps = []   # full step-by-step route for Excel tooltip/cell

        for step in leg.get("steps", []):
            mode = step.get("travel_mode", "")
            secs = step["duration"]["value"]
            mins = round(secs / 60) or 1

            if mode == "WALKING":
                walk_mins += secs / 60
                max_single_walk = max(max_single_walk, secs / 60)
                # Extract destination stop name from "Walk to <stop>" or "Head toward <stop>"
                instr = step.get("html_instructions", "Walk")
                instr = re.sub(r"<[^>]+>", " ", instr).strip()
                to_stop = re.sub(r"^(Walk to|Head toward|Head towards)\s*", "", instr, flags=re.I).strip()
                # Clean up ugly final destination strings like "P.za del Duomo, Milano MI, Italy"
                to_stop = re.sub(r",\s*(Milano\s*MI|Italy).*$", "", to_stop, flags=re.I).strip()
                to_stop = re.sub(r"(\s+M\d)+$", "", to_stop).strip()
                route_steps.append(f"🚶 {mins}min walk → {to_stop}")

            elif mode == "TRANSIT":
                td = step.get("transit_details", {})
                line = td.get("line", {})
                short = line.get("short_name") or line.get("name", "?")
                vehicle = line.get("vehicle", {}).get("type", "")
                vtype = {"SUBWAY": "Metro", "TRAM": "Tram", "BUS": "Bus"}.get(vehicle, vehicle.title())
                dep_stop = td.get("departure_stop", {}).get("name", "")
                arr_stop = td.get("arrival_stop", {}).get("name", "")
                # Strip trailing metro line refs from stop names e.g. "Missori M3" -> "Missori"
                dep_clean = re.sub(r"(\s+M\d)+$", "", dep_stop).strip()
                arr_clean = re.sub(r"(\s+M\d)+$", "", arr_stop).strip()
                num_stops = td.get("num_stops", "")
                stops_str = f" ({num_stops} stops)" if num_stops else ""
                transit_names.append(f"{vtype} {short}")
                icon = {"SUBWAY": "🚇", "TRAM": "🚊", "BUS": "🚌"}.get(vehicle, "🚌")
                route_steps.append(f"{icon} {vtype} {short}{stops_str}: {dep_clean} -> {arr_clean}")

        result["walk_mins"] = round(walk_mins)
        result["walk_warning"] = max_single_walk > max_walk
        result["route_steps"] = route_steps

        transit_str = " → ".join(transit_names) if transit_names else "transit"
        walk_flag = f" ⚠{round(max_single_walk)}min walk" if result["walk_warning"] else ""
        result["summary"] = f"{total_min}min via {transit_str}{walk_flag}"

        # Apply filter
        if do_filter:
            commute_ok = max_commute is None or total_min <= max_commute
            walk_ok = not result["walk_warning"]
            result["passes_filter"] = commute_ok and walk_ok

    except Exception as e:
        result["summary"] = f"Error: {e}"
        result["passes_filter"] = None  # keep on error, don't filter

    return result


def get_all_commutes(
    listing: dict,
    destinations: list,
    api_key: str,
    departure_hour: int = 9,
    include_bus: bool = False,
) -> list:
    """
    Calculate commute times from a listing to all destinations.
    Returns list of commute result dicts.
    """
    if not api_key or listing.get("lat") is None:
        return [{"label": d["label"], "duration_min": None, "walk_mins": None,
                 "summary": "No coords", "passes_filter": None, "walk_warning": False}
                for d in destinations]

    results = []
    for dest in destinations:
        r = get_transit_commute(
            listing["lat"], listing["lng"],
            dest, api_key, departure_hour, include_bus
        )
        results.append(r)
        time.sleep(0.1)

    return results
