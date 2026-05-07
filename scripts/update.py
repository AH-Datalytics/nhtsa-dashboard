#!/usr/bin/env python3
"""
NHTSA Vehicle Complaints automated update pipeline.
Scrapes complaint counts from api.nhtsa.gov, compresses to base64,
and embeds into the HTML dashboard template.
"""
import json
import gzip
import base64
import time
import os
import re
import sys
from datetime import datetime
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

DATA_FILE = os.path.join(PROJECT_DIR, "nhtsa_complaint_counts.json")
B64_FILE = os.path.join(PROJECT_DIR, "nhtsa_b64.txt")
INDEX_FILE = os.path.join(PROJECT_DIR, "index.html")

YEARS = list(range(2015, datetime.now().year + 1))
WORKERS = 8

_lock = threading.Lock()
_done = 0
_total = 0


def api(url, retries=3):
    """Call NHTSA API with retries."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 (NHTSA-Dashboard)"})
            resp = urlopen(req, timeout=20)
            return json.loads(resp.read())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None


def scrape_vehicle(make, model, year):
    """Get complaint counts for a single make/model/year."""
    global _done
    url = (
        f"https://api.nhtsa.gov/complaints/complaintsByVehicle?"
        f"make={make}&model={model}&modelYear={year}"
    )
    d = api(url)
    with _lock:
        _done += 1
        if _done % 500 == 0:
            print(f"  Progress: {_done}/{_total}", flush=True)
    if not d or not d.get("count"):
        return None
    results = d.get("results") or []
    crashes = sum(1 for c in results if c.get("crash"))
    fires = sum(1 for c in results if c.get("fire"))
    injuries = sum(c.get("numberOfInjuries", 0) for c in results)
    deaths = sum(c.get("numberOfDeaths", 0) for c in results)
    # filed year breakdown
    by_filed = {}
    for c in results:
        filed = c.get("dateComplaintFiled", "")
        if filed:
            fy = filed[:4]
            by_filed[fy] = by_filed.get(fy, 0) + 1
    return {
        "make": make,
        "model": model,
        "year": year,
        "count": d["count"],
        "crashes": crashes,
        "fires": fires,
        "injuries": injuries,
        "deaths": deaths,
        "by_filed_year": by_filed,
    }


def scrape_all():
    """Scrape all make/model/year combos from NHTSA API."""
    global _total, _done
    _done = 0

    print("Getting all makes and models...", flush=True)
    make_models = {}

    for year in YEARS:
        d = api(
            f"https://api.nhtsa.gov/products/vehicle/makes?"
            f"modelYear={year}&issueType=c"
        )
        if not d:
            continue
        for m in d.get("results", []):
            make = m["make"]
            if make not in make_models:
                make_models[make] = set()
            md = api(
                f"https://api.nhtsa.gov/products/vehicle/models?"
                f"modelYear={year}&make={make}&issueType=c"
            )
            if md:
                for mm in md.get("results", []):
                    make_models[make].add(mm["model"])
        total = sum(len(v) for v in make_models.values())
        print(f"  {year}: {total} make/model combos", flush=True)
        time.sleep(0.1)

    # Build work items
    work = []
    for make, models in make_models.items():
        for model in models:
            for year in YEARS:
                work.append((make, model, year))

    _total = len(work)
    print(f"\nScraping {_total} vehicle/year combos with {WORKERS} workers...", flush=True)

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(scrape_vehicle, make, model, year): (make, model, year)
            for make, model, year in work
        }
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                results.append(r)

    print(f"\nGot {len(results)} results with complaints", flush=True)
    return results


def build_json(results):
    """Build the aggregated JSON structure from raw results."""
    # Aggregate by make/model
    agg = {}
    for r in results:
        key = (r["make"], r["model"])
        if key not in agg:
            agg[key] = {
                "make": r["make"],
                "model": r["model"],
                "total": 0,
                "crashes": 0,
                "fires": 0,
                "injuries": 0,
                "deaths": 0,
                "by_year": {},
                "by_filed_year": {},
            }
        v = agg[key]
        v["total"] += r["count"]
        v["crashes"] += r["crashes"]
        v["fires"] += r["fires"]
        v["injuries"] += r["injuries"]
        v["deaths"] += r["deaths"]
        v["by_year"][r["year"]] = r["count"]
        for fy, cnt in r.get("by_filed_year", {}).items():
            v["by_filed_year"][fy] = v["by_filed_year"].get(fy, 0) + cnt

    vehicles = sorted(agg.values(), key=lambda v: -v["total"])

    # Detail records
    detail = []
    for r in results:
        detail.append({
            "make": r["make"],
            "model": r["model"],
            "year": r["year"],
            "count": r["count"],
            "crashes": r["crashes"],
            "fires": r["fires"],
            "injuries": r["injuries"],
            "deaths": r["deaths"],
        })

    # Filed year totals
    filed_years = {}
    for v in vehicles:
        for fy, cnt in v.get("by_filed_year", {}).items():
            filed_years[fy] = filed_years.get(fy, 0) + cnt

    data = {
        "scraped": datetime.now().strftime("%Y-%m-%d"),
        "years": YEARS,
        "filed_years": filed_years,
        "vehicles": vehicles,
        "detail": detail,
    }
    return data


def compress_to_b64(data):
    """Gzip compress JSON data and encode as base64."""
    raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
    compressed = gzip.compress(raw)
    return base64.b64encode(compressed).decode("ascii")


def embed_in_html(b64_data):
    """Replace the base64 data in index.html with new data."""
    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    # Find and replace the data between <script id="nhtsa-data"...> tags
    pattern = r'(<script\s+id="nhtsa-data"\s+type="text/plain">)\s*\S+\s*(</script>)'
    replacement = r'\1\n' + b64_data + r'\n\2'
    new_html, count = re.subn(pattern, replacement, html, count=1)

    if count == 0:
        print("ERROR: Could not find data placeholder in index.html")
        sys.exit(1)

    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        f.write(new_html)

    size_kb = len(new_html) / 1024
    print(f"Updated index.html ({size_kb:.0f} KB)")


def main():
    print("=" * 60)
    print("NHTSA Vehicle Complaints Automated Update")
    print("=" * 60)

    # Step 1: Scrape
    print("\n--- Step 1: Scraping NHTSA API ---")
    results = scrape_all()

    if len(results) < 100:
        print(f"ERROR: Only got {len(results)} results, expected thousands. Aborting.")
        sys.exit(1)

    # Step 2: Build JSON
    print("\n--- Step 2: Building JSON ---")
    data = build_json(results)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f)
    print(f"Saved {DATA_FILE} ({os.path.getsize(DATA_FILE) / 1024 / 1024:.1f} MB)")
    print(f"  {len(data['vehicles'])} vehicles, {len(data['detail'])} detail records")

    # Step 3: Compress
    print("\n--- Step 3: Compressing ---")
    b64 = compress_to_b64(data)
    with open(B64_FILE, "w") as f:
        f.write(b64)
    print(f"Saved {B64_FILE} ({len(b64) / 1024:.0f} KB)")

    # Step 4: Embed in HTML
    print("\n--- Step 4: Embedding in HTML ---")
    embed_in_html(b64)

    print("\nUpdate complete!")


if __name__ == "__main__":
    main()
