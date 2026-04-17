#!/usr/bin/env python3
"""
Fetch all deals from RD Station CRM API and generate deals.json
Runs via GitHub Actions every 6 hours.
Token comes from GitHub Secrets (environment variable RD_TOKEN).
"""
import os
import json
import sys
import time
import urllib.request
import urllib.error
import unicodedata

TOKEN = os.environ.get("RD_TOKEN", "")
API_BASE = "https://crm.rdstation.com/api/v1"
LIMIT = 200  # max per page

MAX_PAGES = 50  # RD Station API limit: 50 pages * 200 = 10,000 deals max

def fetch_json(endpoint):
    """Fetch a single JSON endpoint (no pagination)."""
    url = f"{API_BASE}/{endpoint}?token={TOKEN}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            if attempt < 2:
                print(f"  Retry {attempt+1}: {e}")
                time.sleep(5)
                continue
            raise
    return None

def fetch_pipelines():
    """Fetch all pipelines and build stage_id -> pipeline_name mapping."""
    data = fetch_json("deal_pipelines")
    stage_map = {}
    pipelines = data.get("deal_pipelines", data) if isinstance(data, dict) else data
    if isinstance(pipelines, list):
        for p in pipelines:
            pname = p.get("name", "")
            for s in p.get("deal_stages", []):
                sid = s.get("id") or s.get("_id", "")
                if sid:
                    stage_map[sid] = pname
    print(f"   Loaded {len(stage_map)} stage-to-pipeline mappings across {len(pipelines)} pipelines")
    return stage_map

def fetch_page(endpoint, page=1, params=""):
    url = f"{API_BASE}/{endpoint}?token={TOKEN}&limit={LIMIT}&page={page}{params}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 10 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if e.code == 400 and page > MAX_PAGES:
                print(f"  API pagination limit reached at page {page}")
                return None
            raise
        except Exception as e:
            if attempt < 2:
                print(f"  Retry {attempt+1}: {e}")
                time.sleep(5)
                continue
            raise
    return None

def fetch_all_deals():
    """Fetch all deals with pagination."""
    all_deals = []
    page = 1
    total = None

    while True:
        print(f"  Fetching page {page}...", end=" ")
        data = fetch_page("deals", page=page)

        if not data or "deals" not in data:
            print("ERROR: unexpected response")
            break

        deals = data["deals"]
        if total is None:
            total = data.get("total", "?")
            print(f"Total: {total} deals")

        if not deals:
            print("done (empty page)")
            break

        all_deals.extend(deals)
        print(f"got {len(deals)}, total so far: {len(all_deals)}")

        if len(all_deals) >= data.get("total", float("inf")):
            break

        if page >= MAX_PAGES:
            print(f"\n   Reached API pagination limit ({MAX_PAGES} pages = {len(all_deals)} deals)")
            print(f"   Total in CRM: {data.get('total', '?')} \u2014 fetched most recent {len(all_deals)}")
            break

        page += 1
        time.sleep(0.5)

    return all_deals

def normalize_funnel(name):
    """Normalize funnel name for display."""
    if not name:
        return ""
    return name.strip()

def extract_campo(deal, campo_name):
    """Extract custom field value from deal."""
    campos = deal.get("deal_custom_fields", [])
    for c in campos:
        if c.get("custom_field", {}).get("label", "") == campo_name:
            return c.get("value", "")
    return ""

def categorize_fonte(fonte, campanha):
    """Categorize lead source."""
    f = (fonte or "").lower()
    c = (campanha or "").lower()
    if "google" in c or "google" in f or "busca paga" in f:
        return "Meta Ads" if "meta" in c or "facebook" in c or "instagram" in c else "Google Ads"
    if "meta" in c or "facebook" in c or "instagram" in c or "facebook" in f or "instagram" in f:
        return "Meta Ads"
    if "organi" in f or "busca org" in f:
        return "Organico"
    if "indica" in f or "indicac" in f:
        return "Indicacao"
    if "recorr" in f or "cliente rec" in f:
        return "Cliente Recorrente"
    if "ligac" in f or "cold" in f:
        return "SDR - Ryan"
    if "whatsapp" in f or "wpp" in f:
        return "WhatsApp"
    return "Outros"

def format_date(dt_str):
    """Extract date from ISO datetime string."""
    if not dt_str:
        return ""
    return dt_str[:10]

def process_deal(deal, stage_map=None):
    """Convert RD Station deal to our compact format."""
    deal_stage = deal.get("deal_stage", {}) or {}
    stage_id = deal_stage.get("id") or deal_stage.get("_id", "")
    funnel_name = ""
    if stage_map and stage_id:
        funnel_name = stage_map.get(stage_id, "")
    contacts = deal.get("contacts", [])
    contact = contacts[0] if contacts else {}
    user = deal.get("user", {}) or {}
    # RD Station API: win=true (won), win=false (lost), win=null (open)
    win = deal.get("win")
    if win is True or win == "won":
        estado = "Vendida"
    elif win is False or win == "lost":
        estado = "Perdida"
    else:
        estado = "Em Andamento"
    loss_reason = ""
    if estado == "Perdida":
        lr = deal.get("deal_lost_reason", {}) or {}
        loss_reason = lr.get("name", "")
    return {
        "f": normalize_funnel(funnel_name),
        "e": estado,
        "dc": format_date(deal.get("created_at", "")),
        "df": format_date(deal.get("closed_at", "")),
        "pf": format_date(deal.get("prediction_date", "")),
        "v": float(deal.get("amount_total", 0) or 0),
        "c": deal.get("campaign", {}).get("name", "") if deal.get("campaign") else "",
        "fo": deal.get("deal_source", {}).get("name", "") if deal.get("deal_source") else "",
        "r": user.get("name", ""),
        "n": deal.get("name", ""),
        "tel": contact.get("phone", "") or contact.get("mobile_phone", "") or "",
        "em": contact.get("email", ""),
        "emp": deal.get("organization", {}).get("name", "") if deal.get("organization") else "",
        "cid": extract_campo(deal, "Cidade") or contact.get("city", ""),
        "nec": extract_campo(deal, "Necessidade"),
        "eta": deal_stage.get("name", ""),
        "mp": loss_reason
    }

def main():
    if not TOKEN:
        print("ERROR: RD_TOKEN environment variable not set!")
        sys.exit(1)
    print("=" * 50)
    print("PAAS Dashboard - Fetching RD Station CRM Data")
    print("=" * 50)
    print("\n1. Fetching pipeline/funnel mappings...")
    stage_map = fetch_pipelines()
    print("\n2. Fetching all deals...")
    raw_deals = fetch_all_deals()
    print(f"\n   Total fetched: {len(raw_deals)} deals")
    print("\n3. Processing deals...")
    processed = []
    errors = 0
    for d in raw_deals:
        try:
            processed.append(process_deal(d, stage_map))
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"   Error processing deal {d.get('id','?')}: {e}")
    print(f"   Processed: {len(processed)} deals ({errors} errors)")
    unique_funnels = sorted(set(d["f"] for d in processed if d["f"]))
    print(f"   Unique funnels found: {unique_funnels}")
    active_funnels_raw = [
        "Po\u00e7o Artesiano", "Outorga", "Hidropaas", "Irriga\u00e7\u00e3o",
        "Manuten\u00e7\u00e3o", "Filtro", "Sondagem SPT", "An\u00e1lise de \u00e1gua", "Funil Padr\u00e3o"
    ]
    def normalize_str(s):
        return unicodedata.normalize("NFKD", s.lower().strip()).encode("ascii", "ignore").decode("ascii")
    active_normalized = [normalize_str(x) for x in active_funnels_raw]
    filtered = [d for d in processed if normalize_str(d["f"]) in active_normalized]
    print(f"   Active funnels matched: {len(filtered)} deals")
    if len(filtered) == 0 and len(processed) > 0:
        print("   WARNING: No funnel matches! Using ALL deals instead.")
        filtered = processed
    vendas = [d for d in filtered if d["e"] == "Vendida"]
    fat_total = sum(d["v"] for d in vendas)
    print(f"\n   Vendas: {len(vendas)} | Faturamento total: R$ {fat_total:,.2f}")
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "deals.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False)
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\n4. Saved to {output_path} ({size_mb:.1f} MB)")
    meta = {
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "total_deals": len(filtered),
        "total_vendas": len(vendas),
        "total_faturamento": round(fat_total, 2),
        "funnels": list(set(d["f"] for d in filtered))
    }
    meta_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"   Metadata saved to {meta_path}")
    print("\n\u2713 Done!")

if __name__ == "__main__":
    main()
