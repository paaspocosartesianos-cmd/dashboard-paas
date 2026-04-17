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

        # Check if we've fetched all
        if len(all_deals) >= data.get("total", float("inf")):
            break

        page += 1
        time.sleep(0.5)  # Be nice to the API

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
    if "indica" in f or "indicaç" in f:
        return "Indicacao"
    if "recorr" in f or "cliente rec" in f:
        return "Cliente Recorrente"
    if "ligaç" in f or "ligac" in f or "cold" in f:
        return "SDR - Ryan"
    if "whatsapp" in f or "wpp" in f:
        return "WhatsApp"
    return "Outros"

def format_date(dt_str):
    """Extract date from ISO datetime string."""
    if not dt_str:
        return ""
    return dt_str[:10]  # "2026-01-15T..." -> "2026-01-15"

def process_deal(deal):
    """Convert RD Station deal to our compact format."""
    # Get deal stage/pipeline info
    deal_stage = deal.get("deal_stage", {}) or {}
    pipeline = deal.get("deal_pipeline", {}) or {}

    # Get contact info
    contacts = deal.get("contacts", [])
    contact = contacts[0] if contacts else {}

    # Get user/responsible
    user = deal.get("user", {}) or {}

    # Determine status
    win = deal.get("win")
    if win == "won":
        estado = "Vendida"
    elif win == "lost":
        estado = "Perdida"
    else:
        estado = "Em Andamento"

    # Get loss reason
    loss_reason = ""
    if estado == "Perdida":
        lr = deal.get("deal_lost_reason", {}) or {}
        loss_reason = lr.get("name", "")

    return {
        "f": normalize_funnel(pipeline.get("name", "")),
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

    # Fetch all deals
    print("\n1. Fetching all deals...")
    raw_deals = fetch_all_deals()
    print(f"\n   Total fetched: {len(raw_deals)} deals")

    # Process deals
    print("\n2. Processing deals...")
    processed = []
    errors = 0
    for d in raw_deals:
        try:
            processed.append(process_deal(d))
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"   Error processing deal {d.get('id','?')}: {e}")

    print(f"   Processed: {len(processed)} deals ({errors} errors)")

    # Filter to active funnels
    active_funnels = [
        "Poço Artesiano", "Outorga", "Hidropaas", "Irrigação",
        "Manutenção", "Filtro", "Sondagem SPT", "Análise de água", "Funil Padrão"
    ]
    filtered = [d for d in processed if d["f"] in active_funnels]
    print(f"   Active funnels: {len(filtered)} deals")

    # Stats
    vendas = [d for d in filtered if d["e"] == "Vendida"]
    fat_total = sum(d["v"] for d in vendas)
    print(f"\n   Vendas: {len(vendas)} | Faturamento total: R$ {fat_total:,.2f}")

    # Save JSON
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "deals.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\n3. Saved to {output_path} ({size_mb:.1f} MB)")

    # Also save metadata
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
    print("\n✓ Done!")

if __name__ == "__main__":
    main()
