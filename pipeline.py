#!/usr/bin/env python3
import os
import sys
import json
import csv
import glob
import hashlib
import re
from datetime import datetime, timezone, timedelta
import argparse
import sqlite3

IST = timezone(timedelta(hours=5, minutes=30))
SALT = os.environ.get("PII_SALT", "agentic-universe-salt-v1")

def hash_phone(phone: str) -> str:
    return hashlib.sha256(f"{SALT}:{phone}".encode()).hexdigest()

def normalize_phone(raw: str):
    if not raw or not str(raw).strip():
        return None
    digits = re.sub(r'\D', '', str(raw))
    if len(digits) == 12 and digits.startswith('91'):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith('0'):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in '6789':
        return f"+91{digits}"
    return None

def normalize_header(h: str) -> str:
    return re.sub(r'[_,\s]+', ' ', h.lower()).strip()

def parse_date(val: str):
    if not val or not str(val).strip():
        return None
    val = val.strip()
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"]
    for fmt in formats:
        try:
            dt = datetime.strptime(val, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue
    return None

def parse_decimal(val: str):
    if val is None or str(val).strip() == "":
        return None
    cleaned = re.sub(r'(?i)^(₹|rs\.?|inr)\s*', '', str(val)).replace(',', '').strip()
    try:
        num = float(cleaned)
        if num < 0:
            return None
        return round(num, 2)
    except ValueError:
        return None

def init_db(db_path="pipeline.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()
    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS processed_batches (
        drop_id TEXT,
        batch_id TEXT,
        workflow_id TEXT,
        PRIMARY KEY (drop_id, batch_id)
    );
    CREATE TABLE IF NOT EXISTS leads (
        lead_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        phone_canonical TEXT NOT NULL,
        phone_hash TEXT NOT NULL UNIQUE,
        phone_masked TEXT NOT NULL,
        external_ref TEXT,
        batch_id TEXT NOT NULL,
        attributes TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS event_ledger (
        event_id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        call_id TEXT,
        occurred_at TEXT NOT NULL,
        occurred_at_ist TEXT NOT NULL,
        phone_canonical TEXT,
        phone_hash TEXT,
        payload TEXT NOT NULL,
        lead_id TEXT REFERENCES leads(lead_id),
        is_orphan INTEGER DEFAULT 0,
        raw_line_hash TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS rejections (
        workflow_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        row_number INTEGER NOT NULL,
        reason TEXT NOT NULL,
        PRIMARY KEY (workflow_id, batch_id, row_number)
    );
    CREATE TABLE IF NOT EXISTS metrics (
        key TEXT PRIMARY KEY,
        val INTEGER
    );
    """)
    conn.commit()
    return conn

def load_catalogue(schemas_dir):
    workflows = {}
    for sf in glob.glob(os.path.join(schemas_dir, "*.json")):
        w_id = os.path.basename(sf).replace(".json", "")
        with open(sf, 'r') as f:
            workflows[w_id] = json.load(f)

    dispositions = {}
    disp_path = os.path.join(schemas_dir, "dispositions.csv")
    if os.path.exists(disp_path):
        with open(disp_path, mode='r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                w_id = row['workflow_id']
                if w_id not in dispositions:
                    dispositions[w_id] = {}
                disp_key = row.get('disposition_code') or row.get('code') or ''
                dispositions[w_id][disp_key.lower()] = {
                    "description": row.get('description', ''),
                    "requires_followup": row.get('requires_followup', 'false').lower() == 'true'
                }
    return workflows, dispositions
def ingest_drop(drop_path, schemas_dir, db_path="pipeline.db"):
    conn = init_db(db_path)
    workflows, dispositions = load_catalogue(schemas_dir)
    drop_id = os.path.basename(os.path.normpath(drop_path))
    cursor = conn.cursor()
    
    metrics = {
        "lead_rows_total": 0, "lead_rows_rejected": 0, "duplicate_lead_rows_merged": 0,
        "field_warnings": 0, "events_total": 0, "malformed_events": 0,
        "duplicate_events_dropped": 0, "orphan_events": 0, "unknown_disposition_events": 0
    }
    
    # Ingest Leads
    leads_dir = os.path.join(drop_path, "leads")
    if os.path.exists(leads_dir):
        for w_id in os.listdir(leads_dir):
            w_dir = os.path.join(leads_dir, w_id)
            if not os.path.isdir(w_dir): continue
            schema = workflows.get(w_id, {}).get("fields", [])
            ref_field = next((f["name"] for f in schema if f.get("reference")), None)
            
            for bf in sorted(glob.glob(os.path.join(w_dir, "*.csv"))):
                batch_id = os.path.basename(bf).replace(".csv", "")
                cursor.execute("SELECT 1 FROM processed_batches WHERE drop_id=? AND batch_id=?", (drop_id, batch_id))
                if cursor.fetchone(): continue
                
                with open(bf, mode='r', encoding='utf-8-sig', errors='replace') as f:
                    reader = list(csv.reader(f))
                    if not reader: continue
                    header = reader[0]
                    norm_headers = [normalize_header(h) for h in header]
                    
                    field_indices = {}
                    for sf in schema:
                        name = sf["name"]
                        aliases = [normalize_header(a) for a in sf.get("aliases", [])]
                        aliases.append(normalize_header(name))
                        matched_idx = next((idx for idx, nh in enumerate(norm_headers) if nh in aliases), -1)
                        field_indices[name] = matched_idx
                    
                    row_num = 0
                    for row in reader[1:]:
                        if not any(cell.strip() for cell in row): continue
                        row_num += 1
                        metrics["lead_rows_total"] += 1
                        
                        rejected, reject_reason, row_data, warnings_in_row = False, None, {}, 0
                        for sf in schema:
                            fname, ftype, required = sf["name"], sf["type"], sf.get("required", False)
                            idx = field_indices.get(fname, -1)
                            val = row[idx].strip() if (idx != -1 and idx < len(row)) else ""
                            
                            if val == "":
                                if required:
                                    rejected, reject_reason = True, f"missing_required:{fname}"
                                    break
                                else:
                                    row_data[fname] = None
                                    continue
                            
                            if ftype == "phone":
                                c_phone = normalize_phone(val)
                                if not c_phone:
                                    rejected, reject_reason = True, "invalid_phone"
                                    break
                                row_data[fname] = c_phone
                            elif ftype == "date":
                                parsed_d = parse_date(val)
                                if not parsed_d: warnings_in_row += 1
                                row_data[fname] = parsed_d
                            elif ftype == "decimal":
                                parsed_dec = parse_decimal(val)
                                if parsed_dec is None: warnings_in_row += 1
                                row_data[fname] = parsed_dec
                            elif ftype == "enum":
                                allowed = [v.lower() for v in sf.get("values", [])]
                                if val.lower() not in allowed: warnings_in_row += 1
                                row_data[fname] = val.lower() if val.lower() in allowed else None
                            else:
                                row_data[fname] = val
                        
                        if rejected:
                            metrics["lead_rows_rejected"] += 1
                            cursor.execute("INSERT OR REPLACE INTO rejections (workflow_id, batch_id, row_number, reason) VALUES (?, ?, ?, ?)",
                                           (w_id, batch_id, row_num, reject_reason))
                        else:
                            metrics["field_warnings"] += warnings_in_row
                            for sf in schema:
                                if row_data.get(sf["name"]) is None and "default" in sf:
                                    row_data[sf["name"]] = sf["default"]
                            
                            phone_col = next(sf["name"] for sf in schema if sf["type"] == "phone")
                            canonical_phone = row_data[phone_col]
                            p_hash = hash_phone(canonical_phone)
                            masked = f"+91******{canonical_phone[-4:]}"
                            ext_ref = row_data.get(ref_field) if ref_field else None
                            
                            cursor.execute("SELECT lead_id, attributes FROM leads WHERE phone_hash=?", (p_hash,))
                            existing = cursor.fetchone()
                            if existing:
                                metrics["duplicate_lead_rows_merged"] += 1
                                ex_id, ex_attrs_json = existing
                                ex_attrs = json.loads(ex_attrs_json)
                                for k, v in row_data.items():
                                    if v is not None: ex_attrs[k] = v
                                cursor.execute("UPDATE leads SET attributes=?, external_ref=COALESCE(?, external_ref) WHERE lead_id=?",
                                               (json.dumps(ex_attrs), ext_ref, ex_id))
                            else:
                                lead_id = f"{w_id}_{p_hash[:12]}"
                                cursor.execute("INSERT INTO leads (lead_id, workflow_id, phone_canonical, phone_hash, phone_masked, external_ref, batch_id, attributes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                               (lead_id, w_id, canonical_phone, p_hash, masked, ext_ref, batch_id, json.dumps(row_data)))
                cursor.execute("INSERT OR IGNORE INTO processed_batches (drop_id, batch_id, workflow_id) VALUES (?, ?, ?)", (drop_id, batch_id, w_id))
        conn.commit()

    # Ingest Events
    events_file = os.path.join(drop_path, "events", f"events_{drop_id.replace('drop_', '')}.jsonl")
    if not os.path.exists(events_file):
        ev_files = glob.glob(os.path.join(drop_path, "events", "*.jsonl"))
        if ev_files: events_file = ev_files[0]
            
    if os.path.exists(events_file):
        with open(events_file, 'r', encoding='utf-8') as ef:
            for line in ef:
                if not line.strip(): continue
                metrics["events_total"] += 1
                line_hash = hashlib.md5(line.encode()).hexdigest()
                
                try: ev = json.loads(line)
                except json.JSONDecodeError:
                    metrics["malformed_events"] += 1
                    continue
                
                ev_id = ev.get("event_id")
                if not ev_id:
                    metrics["malformed_events"] += 1
                    continue
                
                cursor.execute("SELECT 1 FROM event_ledger WHERE event_id=? OR raw_line_hash=?", (ev_id, line_hash))
                if cursor.fetchone():
                    metrics["duplicate_events_dropped"] += 1
                    continue
                
                ev_type, w_id, call_id, raw_phone, payload, occ_at = ev.get("event_type"), ev.get("workflow_id"), ev.get("call_id"), ev.get("phone"), ev.get("payload", {}), ev.get("occurred_at")
                
                try:
                    if occ_at.endswith('Z'): dt_obj = datetime.strptime(occ_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    elif '+' in occ_at or '-' in occ_at[10:]: dt_obj = datetime.fromisoformat(occ_at)
                    else: dt_obj = datetime.strptime(occ_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                except Exception: dt_obj = datetime.now(IST)
                
                occ_utc, occ_ist = dt_obj.astimezone(timezone.utc).isoformat(), dt_obj.astimezone(IST).isoformat()
                
                if ev_type.startswith("disposition."):
                    code = payload.get("disposition_code", "").lower()
                    if w_id in dispositions and code not in dispositions[w_id]:
                        metrics["unknown_disposition_events"] += 1
                        continue
                
                lead_id, p_canonical, p_hash = None, None, None
                if raw_phone:
                    p_canonical = normalize_phone(raw_phone)
                    if p_canonical:
                        p_hash = hash_phone(p_canonical)
                        cursor.execute("SELECT lead_id FROM leads WHERE phone_hash=?", (p_hash,))
                        res = cursor.fetchone()
                        if res: lead_id = res[0]
                elif ev_type == "disposition.override" and call_id:
                    cursor.execute("SELECT lead_id FROM event_ledger WHERE workflow_id=? AND call_id=? AND lead_id IS NOT NULL LIMIT 1", (w_id, call_id))
                    res = cursor.fetchone()
                    if res: lead_id = res[0]
                
                is_orphan = 0
                if not lead_id:
                    is_orphan = 1
                    metrics["orphan_events"] += 1
                
                cursor.execute("INSERT OR IGNORE INTO event_ledger (event_id, event_type, workflow_id, call_id, occurred_at, occurred_at_ist, phone_canonical, phone_hash, payload, lead_id, is_orphan, raw_line_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (ev_id, ev_type, w_id, call_id, occ_utc, occ_ist, p_canonical, p_hash, json.dumps(payload), lead_id, is_orphan, line_hash))
        conn.commit()
    
    for k, v in metrics.items():
        cursor.execute("INSERT INTO metrics(key, val) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET val=val+?", (k, v, v))
    conn.commit()
    conn.close()

def generate_reports(schemas_dir, db_path="pipeline.db", out_dir="reports"):
    os.makedirs(out_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    _, dispositions = load_catalogue(schemas_dir)
    
    cursor.execute("UPDATE event_ledger SET lead_id = (SELECT l.lead_id FROM leads l WHERE l.phone_hash = event_ledger.phone_hash), is_orphan = 0 WHERE is_orphan = 1 AND phone_hash IN (SELECT phone_hash FROM leads);")
    conn.commit()

    leads_all = cursor.execute("SELECT lead_id, workflow_id, batch_id, phone_masked, external_ref, attributes FROM leads ORDER BY workflow_id, external_ref").fetchall()
    
    # 1. batch_summary.csv
    batch_stats = {}
    for l in leads_all:
        w_id, b_id, l_id = l["workflow_id"], l["batch_id"], l["lead_id"]
        key = (w_id, b_id)
        if key not in batch_stats:
            batch_stats[key] = {"total": 0, "yet_to_reach_out": 0, "telephony_failed": 0, "dialed": 0, "completed": 0}
        batch_stats[key]["total"] += 1
        
        evs = cursor.execute("SELECT call_id, event_type, payload FROM event_ledger WHERE lead_id=? AND is_orphan=0", (l_id,)).fetchall()
        calls = {}
        for ev in evs:
            cid = ev["call_id"]
            if not cid: continue
            if cid not in calls: calls[cid] = {"initiated": False, "telephony_failed": False, "connected": False, "dispositions": []}
            if ev["event_type"] == "call.initiated": calls[cid]["initiated"] = True
            elif ev["event_type"] == "call.telephony_failed": calls[cid]["telephony_failed"] = True
            elif ev["event_type"] == "call.connected": calls[cid]["connected"] = True
            elif ev["event_type"] in ("disposition.set", "disposition.override"):
                p = json.loads(ev["payload"])
                calls[cid]["dispositions"].append(p.get("disposition_code", "").lower())
        
        valid_calls = {cid: data for cid, data in calls.items() if data["initiated"]}
        if not valid_calls:
            batch_stats[key]["yet_to_reach_out"] += 1
            continue
        
        is_completed, all_failed = False, True
        for cid, cdata in valid_calls.items():
            if not cdata["telephony_failed"]: all_failed = False
            eff_code = cdata["dispositions"][-1] if cdata["dispositions"] else None
            if eff_code and w_id in dispositions and dispositions[w_id].get(eff_code, {}).get("is_terminal"):
                is_completed = True
                break
        
        if is_completed: batch_stats[key]["completed"] += 1
        elif all_failed: batch_stats[key]["telephony_failed"] += 1
        else: batch_stats[key]["dialed"] += 1

    with open(os.path.join(out_dir, "batch_summary.csv"), mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["workflow_id", "batch_id", "total_leads", "yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed"])
        for (w_id, b_id), stats in sorted(batch_stats.items()):
            writer.writerow([w_id, b_id, stats["total"], stats["yet_to_reach_out"], stats["telephony_failed"], stats["dialed"], stats["completed"]])

    # 2. lead_report.csv
    with open(os.path.join(out_dir, "lead_report.csv"), mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success"])
        for l in leads_all:
            l_id, w_id, b_id, masked, ext_ref = l["lead_id"], l["workflow_id"], l["batch_id"], l["phone_masked"], l["external_ref"] or ""
            evs = cursor.execute("SELECT call_id, event_type, occurred_at_ist, payload FROM event_ledger WHERE lead_id=? AND is_orphan=0 ORDER BY occurred_at ASC", (l_id,)).fetchall()
            
            calls = {}
            for ev in evs:
                cid = ev["call_id"]
                if not cid: continue
                if cid not in calls: calls[cid] = {"initiated": False, "connected": False, "initiated_at": ev["occurred_at_ist"], "dispositions": []}
                if ev["event_type"] == "call.initiated":
                    calls[cid]["initiated"] = True
                    calls[cid]["initiated_at"] = ev["occurred_at_ist"]
                elif ev["event_type"] == "call.connected": calls[cid]["connected"] = True
                elif ev["event_type"] in ("disposition.set", "disposition.override"):
                    p = json.loads(ev["payload"])
                    calls[cid]["dispositions"].append(p.get("disposition_code", "").lower())
            
            valid_calls = {cid: data for cid, data in calls.items() if data["initiated"]}
            total_calls, connected_calls = len(valid_calls), sum(1 for c in valid_calls.values() if c["connected"])
            last_call_at, current_disp, ever_success = "", "", False
            
            if valid_calls:
                sorted_calls = sorted(valid_calls.values(), key=lambda x: x["initiated_at"])
                last_call_at = sorted_calls[-1]["initiated_at"]
                for c in reversed(sorted_calls):
                    if c["dispositions"]:
                        current_disp = c["dispositions"][-1]
                        break
            
            for c in valid_calls.values():
                if c["dispositions"]:
                    code = c["dispositions"][-1]
                    if w_id in dispositions and dispositions[w_id].get(code, {}).get("is_success"):
                        ever_success = True
            
            if total_calls == 0: bucket = "yet_to_reach_out"
            else:
                terminal = any(c["dispositions"] and w_id in dispositions and dispositions[w_id].get(c["dispositions"][-1], {}).get("is_terminal") for c in valid_calls.values())
                if terminal: bucket = "completed"
                else:
                    all_failed = all(any(ev["event_type"] == "call.telephony_failed" for ev in evs if ev["call_id"] == cid) for cid in valid_calls)
                    bucket = "in_progress_telephony_failed" if all_failed else "in_progress_dialed"

            writer.writerow([w_id, ext_ref, b_id, masked, bucket, total_calls, connected_calls, last_call_at, current_disp, str(ever_success).lower()])

    # 3. daily_overview.csv
    daily_data = {}
    for l in leads_all:
        l_id, w_id = l["lead_id"], l["workflow_id"]
        evs = cursor.execute("SELECT call_id, event_type, occurred_at_ist, payload FROM event_ledger WHERE lead_id=? AND is_orphan=0", (l_id,)).fetchall()
        calls = {}
        for ev in evs:
            cid = ev["call_id"]
            if not cid: continue
            if cid not in calls: calls[cid] = {"initiated": False, "connected": False, "date": ev["occurred_at_ist"][:10], "dispositions": []}
            if ev["event_type"] == "call.initiated":
                calls[cid]["initiated"] = True
                calls[cid]["date"] = ev["occurred_at_ist"][:10]
            elif ev["event_type"] == "call.connected": calls[cid]["connected"] = True
            elif ev["event_type"] in ("disposition.set", "disposition.override"):
                p = json.loads(ev["payload"])
                calls[cid]["dispositions"].append(p.get("disposition_code", "").lower())
        
        for cid, cdata in calls.items():
            if not cdata["initiated"]: continue
            d = cdata["date"]
            key = (w_id, d)
            if key not in daily_data: daily_data[key] = {"total_calls": 0, "connected_calls": 0, "success_lead_ids": set()}
            daily_data[key]["total_calls"] += 1
            if cdata["connected"]: daily_data[key]["connected_calls"] += 1
            if cdata["dispositions"]:
                code = cdata["dispositions"][-1]
                if w_id in dispositions and dispositions[w_id].get(code, {}).get("is_success"):
                    daily_data[key]["success_lead_ids"].add(l_id)

    with open(os.path.join(out_dir, "daily_overview.csv"), mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"])
        for (w_id, d), dvals in sorted(daily_data.items()):
            tot, conn_c = dvals["total_calls"], dvals["connected_calls"]
            rate = f"{conn_c / tot:.4f}" if tot > 0 else "0.0000"
            writer.writerow([w_id, d, tot, conn_c, rate, len(dvals["success_lead_ids"])])

    # 4. rejections.csv
    rejs = cursor.execute("SELECT workflow_id, batch_id, row_number, reason FROM rejections ORDER BY workflow_id, batch_id, row_number").fetchall()
    with open(os.path.join(out_dir, "rejections.csv"), mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["workflow_id", "batch_id", "row_number", "reason"])
        for r in rejs: writer.writerow([r["workflow_id"], r["batch_id"], r["row_number"], r["reason"]])

    # 5. data_quality.json
    metrics_db = {row["key"]: row["val"] for row in cursor.execute("SELECT key, val FROM metrics").fetchall()}
    rejections_by_reason = {row["reason"]: row["count"] for row in cursor.execute("SELECT reason, COUNT(*) as count FROM rejections GROUP BY reason").fetchall()}
    
    dq = {
        "lead_rows_total": metrics_db.get("lead_rows_total", 0),
        "lead_rows_rejected": metrics_db.get("lead_rows_rejected", 0),
        "rejections_by_reason": rejections_by_reason,
        "duplicate_lead_rows_merged": metrics_db.get("duplicate_lead_rows_merged", 0),
        "field_warnings": metrics_db.get("field_warnings", 0),
        "events_total": metrics_db.get("events_total", 0),
        "malformed_events": metrics_db.get("malformed_events", 0),
        "duplicate_events_dropped": metrics_db.get("duplicate_events_dropped", 0),
        "orphan_events": metrics_db.get("orphan_events", 0),
        "unknown_disposition_events": metrics_db.get("unknown_disposition_events", 0)
    }
    with open(os.path.join(out_dir, "data_quality.json"), mode='w', encoding='utf-8') as f:
        json.dump(dq, f, indent=2)
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    
    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("drop_path")
    ingest_parser.add_argument("--schemas", default="assignment-a-data/schemas")
    ingest_parser.add_argument("--db", default="pipeline.db")
    
    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("--out", default="reports")
    report_parser.add_argument("--schemas", default="assignment-a-data/schemas")
    report_parser.add_argument("--db", default="pipeline.db")
    
    args = parser.parse_args()
    if args.command == "ingest": ingest_drop(args.drop_path, args.schemas, args.db)
    elif args.command == "report": generate_reports(args.schemas, args.db, args.out)
    else: parser.print_help()