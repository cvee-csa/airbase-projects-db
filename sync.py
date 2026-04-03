"""
Zendesk > Airtable Ticket Log Sync
===================================
Pulls recent tickets from the Zendesk IT-Operations-Projects group,
compares them against the CatherineAirbase Ticket Log in Airtable,
and creates or updates records to keep the two in sync.

Usage:
    python sync.py              # Normal sync
    python sync.py --cleanup    # Find and delete duplicate Ticket IDs

Environment variables required:
    ZENDESK_SUBDOMAIN   — e.g. "cloudsecurityalliance"
    ZENDESK_EMAIL       — agent email (e.g. you@company.org)
    ZENDESK_API_TOKEN   — Zendesk API token (Admin > Channels > API)
    AIRTABLE_PAT        — Airtable Personal Access Token
"""

import os
import sys
import time
import requests
from collections import defaultdict
from datetime import datetime, timezone

# ── Configuration ───────────────────────────────────────────────────────────

ZENDESK_SUBDOMAIN = os.environ.get("ZENDESK_SUBDOMAIN", "cloudsecurityalliance")
ZENDESK_EMAIL = os.environ["ZENDESK_EMAIL"]
ZENDESK_API_TOKEN = os.environ["ZENDESK_API_TOKEN"]
AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]

AIRTABLE_BASE_ID = "app4guQr5NCgNngnE"
TICKET_LOG_TABLE_ID = "tblRG0INg9CrAjss7"

# Airtable field IDs (CatherineAirbase → Ticket Log)
FIELDS = {
    "Ticket ID":     "fldr03hDL2ALkVgmF",
    "Subject":       "fldRjOw5g3AWRJ4Cq",
    "Status":        "fldUvaYeae9vLCdCp",
    "Assignee":      "fldvZuQaPw7qtWpxV",
    "Category":      "fldeUn3ynF0GfkDaD",
    "Created Date":  "fldfS6sxSPL44LiJ8",
    "Resolved Date": "fldpv9VFnGSUrerH9",
    "Zendesk URL":   "fldEXjMzIj33Szi5U",
}

ZENDESK_BASE_URL = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com"
ZENDESK_TICKET_URL = f"{ZENDESK_BASE_URL}/agent/tickets"

STATUS_MAP = {
    "new": "New",
    "open": "Open",
    "pending": "Pending",
    "hold": "Hold",
    "solved": "Solved",
    "closed": "Closed",
}

# ── Zendesk helpers ─────────────────────────────────────────────────────────

def zendesk_headers():
    return {"Content-Type": "application/json"}


def zendesk_auth():
    return (f"{ZENDESK_EMAIL}/token", ZENDESK_API_TOKEN)


def fetch_zendesk_tickets(group_name="IT-Operations-Projects"):
    """Fetch all recently updated tickets in the given group via Zendesk Search API."""
    url = f"{ZENDESK_BASE_URL}/api/v2/groups.json"
    resp = requests.get(url, auth=zendesk_auth(), headers=zendesk_headers())
    if not resp.ok:
        print(f"Zendesk groups API error {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
    groups = resp.json().get("groups", [])
    group_id = None
    for g in groups:
        if g["name"] == group_name:
            group_id = g["id"]
            break

    if not group_id:
        print(f"Group '{group_name}' not found. Available groups:")
        for g in groups:
            print(f"   - {g['name']} (id: {g['id']})")
        sys.exit(1)

    tickets = []
    url = (
        f"{ZENDESK_BASE_URL}/api/v2/search.json"
        f"?query=type:ticket group_id:{group_id}&sort_by=updated_at&sort_order=desc"
    )

    while url:
        resp = requests.get(url, auth=zendesk_auth(), headers=zendesk_headers())
        if not resp.ok:
            print(f"Zendesk search API error {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        data = resp.json()
        tickets.extend(data.get("results", []))
        url = data.get("next_page")
        if len(tickets) >= 500:
            break

    print(f"Fetched {len(tickets)} Zendesk tickets from '{group_name}'")
    return tickets


# ── Airtable helpers ────────────────────────────────────────────────────────

def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_PAT}",
        "Content-Type": "application/json",
    }


def fetch_airtable_records():
    """Fetch all records from the Ticket Log table, handling pagination.
    Uses returnFieldsByFieldId=true so field keys match our FIELDS dict."""
    records = []
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TICKET_LOG_TABLE_ID}"
    params = {"pageSize": 100, "returnFieldsByFieldId": "true"}

    while True:
        resp = requests.get(url, headers=airtable_headers(), params=params)
        if not resp.ok:
            print(f"Airtable API error {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset

    print(f"Found {len(records)} existing Airtable records")
    return records


def create_airtable_records(records_to_create):
    """Create new records in Airtable (max 10 per request)."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TICKET_LOG_TABLE_ID}"
    created = 0
    for i in range(0, len(records_to_create), 10):
        batch = records_to_create[i : i + 10]
        payload = {"records": batch, "typecast": True}
        resp = requests.post(url, headers=airtable_headers(), json=payload)
        if not resp.ok:
            print(f"Airtable create error {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        created += len(batch)
    return created


def update_airtable_records(records_to_update):
    """Update existing records in Airtable (max 10 per request)."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TICKET_LOG_TABLE_ID}"
    updated = 0
    for i in range(0, len(records_to_update), 10):
        batch = records_to_update[i : i + 10]
        payload = {"records": batch, "typecast": True}
        resp = requests.patch(url, headers=airtable_headers(), json=payload)
        if not resp.ok:
            print(f"Airtable update error {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        updated += len(batch)
    return updated


def delete_airtable_records(record_ids):
    """Delete records from Airtable (max 10 per request)."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TICKET_LOG_TABLE_ID}"
    deleted = 0
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i : i + 10]
        params = "&".join(f"records[]={r}" for r in batch)
        resp = requests.delete(f"{url}?{params}", headers=airtable_headers())
        if not resp.ok:
            print(f"Airtable delete error {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        deleted += len(batch)
        time.sleep(0.2)  # Rate limit courtesy
    return deleted


# ── Mapping ─────────────────────────────────────────────────────────────────

def zendesk_to_airtable_fields(ticket):
    """Convert a Zendesk ticket dict to Airtable field values."""
    status_raw = ticket.get("status", "")
    status = STATUS_MAP.get(status_raw, status_raw.capitalize())

    created = ticket.get("created_at", "")[:10]  # YYYY-MM-DD
    updated = ticket.get("updated_at", "")[:10]

    assignee_name = ""
    if ticket.get("assignee") and isinstance(ticket["assignee"], dict):
        assignee_name = ticket["assignee"].get("name", "")

    fields = {
        FIELDS["Ticket ID"]:    ticket["id"],
        FIELDS["Subject"]:      ticket.get("subject", "(no subject)"),
        FIELDS["Status"]:       status,
        FIELDS["Created Date"]: created,
        FIELDS["Zendesk URL"]:  f"{ZENDESK_TICKET_URL}/{ticket['id']}",
    }

    if assignee_name:
        fields[FIELDS["Assignee"]] = assignee_name

    if status in ("Solved", "Closed"):
        fields[FIELDS["Resolved Date"]] = updated

    tags = ticket.get("tags", [])
    if tags:
        fields[FIELDS["Category"]] = tags[0]

    return fields


# ── Cleanup mode ────────────────────────────────────────────────────────────

def cleanup_duplicates():
    """Find and delete duplicate Ticket ID records, keeping the oldest."""
    print("Cleanup mode: scanning for duplicate Ticket IDs...")
    print()

    airtable_records = fetch_airtable_records()

    # Group records by Ticket ID
    by_ticket_id = defaultdict(list)
    for rec in airtable_records:
        tid = rec["fields"].get(FIELDS["Ticket ID"])
        if tid:
            by_ticket_id[int(tid)].append({
                "record_id": rec["id"],
                "created": rec.get("createdTime", ""),
            })

    # Find duplicates — keep the oldest record, delete the rest
    to_delete = []
    for tid, records in by_ticket_id.items():
        if len(records) > 1:
            # Sort by createdTime ascending, keep the first (oldest)
            sorted_recs = sorted(records, key=lambda r: r["created"])
            for dup in sorted_recs[1:]:
                to_delete.append(dup["record_id"])

    if not to_delete:
        print("No duplicates found.")
        return

    print(f"Found {len(to_delete)} duplicate record(s) to delete")
    print("Deleting...")
    deleted = delete_airtable_records(to_delete)

    print()
    print("═" * 50)
    print("  Cleanup Complete")
    print("═" * 50)
    print(f"  Duplicates removed: {deleted}")
    print("═" * 50)


# ── Main sync ───────────────────────────────────────────────────────────────

def main():
    print(f"Zendesk > Airtable sync starting at {datetime.now(timezone.utc).isoformat()}")
    print()

    # Step 1: Fetch existing Airtable records and index by Ticket ID
    airtable_records = fetch_airtable_records()
    existing = {}  # ticket_id → { "record_id": ..., "status": ..., ... }
    for rec in airtable_records:
        tid = rec["fields"].get(FIELDS["Ticket ID"])
        if tid:
            existing[int(tid)] = {
                "record_id": rec["id"],
                "status": rec["fields"].get(FIELDS["Status"], ""),
                "assignee": rec["fields"].get(FIELDS["Assignee"], ""),
                "fields": rec["fields"],
            }

    print(f"   (Indexed {len(existing)} unique Ticket IDs)")

    # Step 2: Fetch Zendesk tickets
    zendesk_tickets = fetch_zendesk_tickets()

    # Step 3: Compare and build create/update lists
    to_create = []
    to_update = []

    for ticket in zendesk_tickets:
        tid = ticket["id"]
        new_fields = zendesk_to_airtable_fields(ticket)
        new_status = new_fields.get(FIELDS["Status"], "")

        if tid not in existing:
            to_create.append({"fields": new_fields})
        else:
            rec = existing[tid]
            changes = {}

            if new_status and new_status != rec["status"]:
                changes[FIELDS["Status"]] = new_status
                if new_status in ("Solved", "Closed"):
                    resolved = new_fields.get(FIELDS["Resolved Date"])
                    if resolved:
                        changes[FIELDS["Resolved Date"]] = resolved

            new_assignee = new_fields.get(FIELDS["Assignee"], "")
            if new_assignee and new_assignee != rec["assignee"]:
                changes[FIELDS["Assignee"]] = new_assignee

            if changes:
                to_update.append({
                    "id": rec["record_id"],
                    "fields": changes,
                })

    # Step 4: Apply changes
    print()
    created_count = 0
    updated_count = 0

    if to_create:
        print(f"Creating {len(to_create)} new ticket(s)...")
        created_count = create_airtable_records(to_create)

    if to_update:
        print(f"Updating {len(to_update)} ticket(s)...")
        updated_count = update_airtable_records(to_update)

    # Step 5: Summary
    print()
    print("═" * 50)
    print("  Sync Complete")
    print("═" * 50)
    print(f"  Existing records:  {len(existing)}")
    print(f"  Zendesk tickets:   {len(zendesk_tickets)}")
    print(f"  New records:       {created_count}")
    print(f"  Updated records:   {updated_count}")
    if not to_create and not to_update:
        print("  Ticket Log is already up to date.")
    print("═" * 50)


if __name__ == "__main__":
    if "--cleanup" in sys.argv:
        cleanup_duplicates()
    else:
        main()
