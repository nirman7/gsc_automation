#!/usr/bin/env python3
"""
gsc_to_airtable.py
Fetch previous-month SearchAtlas GSC Core-Report metrics,
write to local CSV, and (optionally) upsert into Airtable.
"""

import csv
import logging
import sys
from datetime import date, timedelta
from typing import Dict, List, Any

import requests
import pandas as pd
import tldextract
from pyairtable import Api

# ────────────── LOGGING SETUP ──────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ────────────── CONFIG – HARD CODED ──────────────

SA_JWT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoic2xpZGluZyIsImV4cCI6MTc2NjM0NDA1MiwianRpIjoiNzUxYzkxNDY2Y2UzNDEzOThiZGYzN2I0YWY5NGY0NmQiLCJyZWZyZXNoX2V4cCI6MTc2NjM0NDA1MiwidXNlcl9pZCI6MjQ2NjIsImN1c3RvbWVyIjp7ImlkIjoyMjQxMywiZW1haWwiOiJsaW5rZ3JhcGh0ZXN0dXNlckBnbWFpbC5jb20iLCJ0ZWFtX2lkcyI6W10sImlzX3N1YnNjcmliZXIiOnRydWUsInF1b3RhIjp7fSwicGxhbiI6Ikdyb3d0aCIsInNlYXRzIjoxLCJ0aW1lem9uZSI6IkFzaWEvS2FyYWNoaSIsImlzX3doaXRlbGFiZWwiOmZhbHNlLCJ3aGl0ZWxhYmVsX2RvbWFpbiI6bnVsbCwid2hpdGVsYWJlbF9vdHRvIjpudWxsLCJpc192ZW5kYXN0YV9jbGllbnQiOmZhbHNlLCJwaG9uZV9udW1iZXIiOiIrMTY0NjgyNDkwMjMiLCJjb21wYW55X25hbWUiOiJTZXJ2aWNlIFVzZXIgLSBEbyBOb3QgRGVsZXRlIiwibG9nbyI6Imh0dHBzOi8vc3RvcmFnZS5nb29nbGVhcGlzLmNvbS9saW5rZ3JhcGgtY3VzdG9tZXItbG9nby9Mb2dvX1NWRy5zdmc_WC1Hb29nLUFsZ29yaXRobT1HT09HNC1SU0EtU0hBMjU2JlgtR29vZy1DcmVkZW50aWFsPWdjcy1mdWxsLWFjY2VzcyU0MG9yZ2FuaWMtcnVsZXItMjA3MTIzLmlhbS5nc2VydmljZWFjY291bnQuY29tJTJGMjAyNTA2MjQlMkZhdXRvJTJGc3RvcmFnZSUyRmdvb2c0X3JlcXVlc3QmWC1Hb29nLURhdGU9MjAyNTA2MjRUMTkwNzMyWiZYLUdvb2ctRXhwaXJlcz04NjQwMCZYLUdvb2ctU2lnbmVkSGVhZGVycz1ob3N0JlgtR29vZy1TaWduYXR1cmU9MTVkMTcwNWExNTdmNDhiNTYxZDI2MWJkY2E3MjM2ODBkNGQ2MzAyYzM5MDNkZTA4NDg1ZWZhZDBhNjIwM2ViN2Y3ZTZlY2M4YTliMmJlZDNkNThiY2UwYjMzNThmNzIyMzIyZmE5M2MzZTA3NWYyNWNlNzNlODczNjJiODIyY2Y2NGI3MDAyODZiZDk1MmJhZDMwNGU3MGJhOGI2NjczYzU1NDZlOGJkMjE5YzBkYTA4ZTEzOWJmOWUyZWUyYmRlMTRmYWFjYTRiODBhYzAzMjA0NDlkYTY2ZWFiZGFhYzc1OGU5YTI3MWEzMzA0ZWQ2OTA1Mjg0MDZmZTU3Zjg0NzBlNTZiZDY2YjM4ZmQ4ZjM2Y2YwZDRhYWIwOWZmYzgwYjJjMTRmZDEyZWY1ZjI5ODFmYzkwYTUwMGJmYWEzYjhmODQ1MDM0NjgxYTJiOWE1NjdmNDUwZGUxYTczOWIzM2I3MWM1NTZmOTJkYmIyMDAyMjI1Y2VhZjA5NDdjYmI4YWE3NWY3YmU2ZTAyN2EyNDU5NDRjNmJjMTA4OTRhYTdmNmI3MmY4ZTA2YzJjOGJkYzg0M2FiOTczODc5N2FjZjdhYzg4MmYzZjlmZjkzMmE2YzFjYWQ5YWYzYTg5ZGFlMjE0MmUyYTBkYTUzOTA1YWI2NmMyMTU0NzI2ZWVlZDMiLCJzaG91bGRfY2hpbGRyZW5fcmVjZWl2ZV9lbWFpbHMiOnRydWUsInJlZ2lzdHJhdGlvbl9zb3VyY2UiOm51bGwsImFkZG9uX2lzX2FjdGl2ZSI6ZmFsc2UsInNlYXJjaGF0bGFzX2FwaV9rZXkiOiI1M2FiMDI4ZTRlMDZiOThhYzE4MmU1OGM4ODBiMjVkOSJ9LCJpc19zdGFmZiI6dHJ1ZSwiaXNfaW1wZXJzb25hdGVkIjp0cnVlLCJpbXBlcnNvbmF0ZWRfYnlfdXNlciI6MjU1NDU2LCJpbXBlcnNvbmF0ZWRfYnlfdXNlcl9vYmoiOnsidXNlcm5hbWUiOiJ5YWhpYS5tYXpvdXppQHNlYXJjaGF0bGFzLmNvbSIsImVtYWlsIjoieWFoaWEubWF6b3V6aUBzZWFyY2hhdGxhcy5jb20ifX0.u9_t1AJRP8GQSx6oLupu1SAXA0bq5oBbJFB0HljP18k"  # your full JWT token

# Airtable config
AIRTABLE_PAT = "pat9bJSI1M4bOQZc6.f9b19c9e872dfc6c022956fb63fb6e0e740d35f7ea9849f05fe03325470b8f83"
AIRTABLE_BASE_ID = "appX9LkZUX3KI7d3G"
AIRTABLE_TABLE = "LG_GSC"

TEST_MODE = False  # 🚀 Set to False when ready for Airtable upload

BASE_URL = "https://gsc.searchatlas.com/search-console/api/v2/core-reports"
HEADERS = {"Authorization": f"Bearer {SA_JWT}", "Content-Type": "application/json"}

DOMAINS_CSV_PATH = "domains.csv"
OUTPUT_CSV_PATH = "core_report_snapshot.csv"

# ────────────── PERIOD LOGIC ──────────────
def previous_month_ranges() -> Dict[str, str]:
    today = date.today()
    first_this_month = today.replace(day=1)
    last_prev = first_this_month - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    last_prev2 = first_prev - timedelta(days=1)
    first_prev2 = last_prev2.replace(day=1)
    return {
        "period1_start": first_prev2.isoformat(),
        "period1_end":   last_prev2.isoformat(),
        "period2_start": first_prev.isoformat(),
        "period2_end":   last_prev.isoformat(),
    }

PERIODS = previous_month_ranges()
log.info("Running snapshot | P1: %s→%s | P2: %s→%s",
    PERIODS["period1_start"], PERIODS["period1_end"],
    PERIODS["period2_start"], PERIODS["period2_end"])

# ────────────── FETCHING LOGIC ──────────────
def fetch_core_report(selected_property: str, timeout: int = 60) -> Dict[str, Any]:
    params = dict(selected_property=selected_property, **PERIODS)
    try:
        resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json() or {}
    except Exception as exc:
        log.warning("Fetch failed for %s: %s", selected_property, exc)
    return {}

def percent_change(old: float, new: float) -> Any:
    if old == 0:
        return None if new > 0 else 0.0  # Use None for undefined (∞) cases
    return round(((new - old) / old) * 100.0, 2)

def is_non_zero(metrics: Dict[str, Any]) -> bool:
    return any(v not in (0, 0.0, None, "nan", "NaN") for v in metrics.values())

def extract_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    clicks = payload.get("clicks", {}) or {}
    impr   = payload.get("total_impressions", {}) or {}
    rank   = payload.get("avg_rank_changes", {}) or {}
    kw     = payload.get("keyword_data", {}) or {}

    clicks_p1, clicks_p2 = clicks.get("previous", 0), clicks.get("current", 0)
    impr_p1,   impr_p2   = impr.get("previous", 0), impr.get("current", 0)
    rank_p1,   rank_p2   = rank.get("previous", 0), rank.get("current", 0)

    return {
        "Clicks P1": clicks_p1,
        "Clicks P2": clicks_p2,
        "% Δ Clicks": percent_change(clicks_p1, clicks_p2),
        "Impr P1": impr_p1,
        "Impr P2": impr_p2,
        "% Δ Impr": percent_change(impr_p1, impr_p2),
        "Avg Rank P1": round(rank_p1, 2),
        "Avg Rank P2": round(rank_p2, 2),
        "% Δ Rank": percent_change(rank_p1, rank_p2),
        "Improved KW": kw.get("improved_kw", 0),
        "Declined KW": kw.get("declined_kw", 0),
    }

def has_valid_response(payload: Dict[str, Any]) -> bool:
    """Check if the API response contains valid data structure, even if metrics are zero."""
    if not payload:
        return False
    
    # Check if we have the expected structure
    required_keys = ["clicks", "total_impressions", "avg_rank_changes", "keyword_data"]
    return any(key in payload for key in required_keys)

def get_empty_metrics() -> Dict[str, Any]:
    """Return a dictionary of empty/zero metrics."""
    return {
        "Clicks P1": 0,
        "Clicks P2": 0,
        "% Δ Clicks": 0.0,
        "Impr P1": 0,
        "Impr P2": 0,
        "% Δ Impr": 0.0,
        "Avg Rank P1": 0.0,
        "Avg Rank P2": 0.0,
        "% Δ Rank": 0.0,
        "Improved KW": 0,
        "Declined KW": 0,
    }

# ────────────── CSV INPUT ──────────────
def load_domains(path: str) -> List[Dict[str, str]]:
    df = pd.read_csv(path).rename(columns=str.lower).fillna("")
    required = {"deal name", "domain"}
    if not required.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {', '.join(required)}")
    return df.to_dict(orient="records")

def canonical_domain(raw: str) -> str:
    raw = raw.strip().lower()
    ext = tldextract.extract(raw)
    if not ext.domain:
        return ""
    return f"{ext.domain}.{ext.suffix}"

# ────────────── AIRTABLE UPSERT ──────────────
def upload_to_airtable(records: List[Dict[str, Any]]):
    from pyairtable import Api
    api = Api(AIRTABLE_PAT)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE)
    log.info("Fetching existing records for upsert …")

    # Diagnostic: List current Airtable field names
    log.info("🔎 Fetching Airtable table fields for verification…")
    try:
        records_sample = table.all(max_records=1)
        if records_sample:
            fields = records_sample[0]["fields"].keys()
            log.info("✅ Existing fields in Airtable table: %s", list(fields))
        else:
            log.info("⚠️ No records found to fetch field names.")
    except Exception as e:
        log.error("Error fetching Airtable table fields: %s", e)

    existing = {}
    for row in table.all(fields=["Domain", "Period 2 Start"]):
        fields = row.get("fields", {})
        domain = fields.get("Domain")
        period2_start = fields.get("Period 2 Start")
        if domain and period2_start:
            key = (domain.strip().lower(), period2_start)
            existing[key] = row["id"]

    log.info("Existing records fetched: %d", len(existing))

    created, updated = 0, 0

    for rec in records:
        domain = rec["Domain"].strip().lower()
        period2_start = rec["Period 2 Start"]
        key = (domain, period2_start)

        if key in existing:
            table.update(existing[key], rec)
            updated += 1
            log.info("🔄 Updated %s (%s)", domain, period2_start)
        else:
            table.create(rec)
            created += 1
            log.info("🆕 Created %s (%s)", domain, period2_start)

    log.info("✅ Airtable sync complete | Created: %d | Updated: %d", created, updated)


# ────────────── MAIN FLOW ──────────────
def main() -> None:
    if not SA_JWT:
        log.error("SEARCHATLAS_JWT is not set; aborting.")
        sys.exit(1)

    log.info("Reading domains from %s …", DOMAINS_CSV_PATH)
    rows = load_domains(DOMAINS_CSV_PATH)
    snapshots: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        raw_domain = str(row["domain"])
        deal_name  = str(row["deal name"])
        
        # Skip N/A domains
        if raw_domain.upper() == "N/A":
            log.info("[%02d/%02d] Skipping N/A domain for %s", idx, len(rows), deal_name)
            continue
            
        clean = canonical_domain(raw_domain)
        if not clean:
            log.warning("[%02d/%02d] Invalid domain format: %s", idx, len(rows), raw_domain)
            # Still add to snapshots with empty metrics
            snapshots.append({
                "Deal Name": deal_name,
                "Domain": raw_domain,
                "Selected Property": "",
                "Period 1 Start": PERIODS["period1_start"],
                "Period 1 End": PERIODS["period1_end"],
                "Period 2 Start": PERIODS["period2_start"],
                "Period 2 End": PERIODS["period2_end"],
                **get_empty_metrics(),
            })
            continue

        log.info("[%02d/%02d] %s – fetching …", idx, len(rows), clean)

        props = [
            f"https://{clean}/",
            f"https://{clean}",
            f"https://www.{clean}/",
            f"https://www.{clean}",
            f"sc-domain:{clean}",
        ]

        metrics = {}
        used_prop = ""
        api_success = False
        
        for prop in props:
            log.info("  Trying property: %s", prop)
            payload = fetch_core_report(prop)
            
            if has_valid_response(payload):
                api_success = True
                metrics = extract_metrics(payload)
                used_prop = prop
                log.info("  ✓ Success with property: %s", prop)
                break
            else:
                log.info("  ✗ No valid response from property: %s", prop)

        # If no API success, still record the domain with empty metrics
        if not api_success:
            log.warning("No valid API response for %s - recording with empty metrics", clean)
            metrics = get_empty_metrics()

        snapshots.append({
            "Deal Name": deal_name,
            "Domain": raw_domain,
            "Selected Property": used_prop,
            "Period 1 Start": PERIODS["period1_start"],
            "Period 1 End": PERIODS["period1_end"],
            "Period 2 Start": PERIODS["period2_start"],
            "Period 2 End": PERIODS["period2_end"],
            **metrics,
        })

    log.info("Collected data for %d domains", len(snapshots))

    if not snapshots:
        log.warning("No data collected – exiting.")
        return

    log.info("Writing snapshot to %s …", OUTPUT_CSV_PATH)
    fieldnames = list(snapshots[0].keys())
    with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(snapshots)

    if not TEST_MODE:
        log.info("Uploading to Airtable table '%s' …", AIRTABLE_TABLE)
        upload_to_airtable(snapshots)
    else:
        log.info("🚫 TEST MODE: skipping Airtable upload (CSV only).")

    log.info("✅ Finished: %d rows processed", len(snapshots))

# ────────────── ENTRYPOINT ──────────────
if __name__ == "__main__":
    main()
