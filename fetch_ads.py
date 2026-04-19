#!/usr/bin/env python3
"""
fetch_ads.py - Busca dados HISTORICOS de campanhas Meta Ads para o dashboard PAAS
Usa a Marketing API do Facebook/Meta para obter insights de campanhas.

Busca desde 2025-01-01 ate hoje, com paginacao automatica.
"""
import os
import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta

META_TOKEN = os.environ.get("META_TOKEN", "")
META_APP_ID = os.environ.get("META_APP_ID", "")
META_APP_SECRET = os.environ.get("META_APP_SECRET", "")

AD_ACCOUNT_IDS = [
    "act_656534222273647",   # PAAS Pocos Artesianos (principal)
    "act_568178800458922",   # Chert Bobsin Paas Pocos Artesianos
    "act_545763446485919",   # Chert bobsin/PAAS Pocos Artesianos
]
API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# Data inicial do historico - busca desde jan 2025
HISTORY_START = "2025-01-01"

OUTPUT_FILE = "ads_data.json"
DATA_DIR_FILE = os.path.join("data", "ads_data.json")

def fetch_json(url):
    """Faz GET request e retorna JSON."""
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"HTTP {e.code}: {body[:500]}")
        if e.code == 400 and "expired" in body.lower():
            return {"error": "token_expired", "details": body[:200]}
        if e.code == 400 and ("OAuthException" in body or "access token" in body.lower()):
            return {"error": "token_invalid", "details": body[:200]}
        return None
    except Exception as e:
        print(f"Erro: {e}")
        return None

def fetch_all_pages(url, max_pages=50):
    """Busca todas as paginas de resultados da API (paginacao automatica)."""
    all_data = []
    page = 0
    while url and page < max_pages:
        result = fetch_json(url)
        if not result:
            break
        if isinstance(result, dict) and result.get("error"):
            return result  # Retorna o erro
        if result.get("data"):
            all_data.extend(result["data"])
            print(f"  Pagina {page+1}: +{len(result['data'])} registros (total: {len(all_data)})")
        # Proxima pagina
        paging = result.get("paging", {})
        url = paging.get("next")
        page += 1
    return {"data": all_data}

def try_extend_token(short_token):
    """Tenta trocar token de curta duracao por um de longa duracao (60 dias)."""
    if not META_APP_ID or not META_APP_SECRET:
        print("APP_ID/APP_SECRET nao configurados - nao e possivel estender token")
        return None
    url = (
        f"{BASE_URL}/oauth/access_token"
        f"?grant_type=fb_exchange_token"
        f"&client_id={META_APP_ID}"
        f"&client_secret={META_APP_SECRET}"
        f"&fb_exchange_token={short_token}"
    )
    result = fetch_json(url)
    if result and result.get("access_token"):
        new_token = result["access_token"]
        expires = result.get("expires_in", "desconhecido")
        print(f"Token estendido com sucesso! Expira em {expires} segundos")
        return new_token
    else:
        print("Falha ao estender token")
        return None

def validate_token(token):
    """Verifica se o token e valido."""
    url = f"{BASE_URL}/me?access_token={token}"
    result = fetch_json(url)
    if result and isinstance(result, dict):
        if result.get("error") in ("token_expired", "token_invalid"):
            return False, "Token expirado ou invalido"
        if result.get("name") or result.get("id"):
            print(f"Token valido - Usuario: {result.get('name', result.get('id', '?'))}")
            return True, "ok"
    return False, "Token possivelmente invalido"

def load_previous_data():
    """Carrega dados anteriores do arquivo existente."""
    for path in [DATA_DIR_FILE, OUTPUT_FILE]:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("campaigns") and len(data["campaigns"]) > 0:
                    print(f"Dados anteriores carregados de {path} ({len(data['campaigns'])} campanhas)")
                    return data
            except Exception as e:
                print(f"Erro ao carregar {path}: {e}")
    return None

def extract_action_value(actions, action_type):
    """Extrai valor de uma acao especifica da lista de actions."""
    if not actions:
        return 0
    for a in actions:
        if a.get("action_type") == action_type:
            return int(a.get("value", 0))
    return 0

def extract_cost_per_action(cost_actions, action_type):
    """Extrai custo por acao especifica."""
    if not cost_actions:
        return 0
    for a in cost_actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0

def fetch_daily_insights_chunked(account_id, since, until, token):
    """Busca insights diarios em blocos de 3 meses para evitar timeout/limites."""
    all_data = []
    chunk_start = datetime.strptime(since, "%Y-%m-%d")
    end_date = datetime.strptime(until, "%Y-%m-%d")

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=89), end_date)
        s = chunk_start.strftime("%Y-%m-%d")
        u = chunk_end.strftime("%Y-%m-%d")
        print(f"  Daily {s} a {u}...")

        fields = "spend,impressions,clicks,actions,reach"
        url = (
            f"{BASE_URL}/{account_id}/insights"
            f"?fields={fields}"
            f"&time_increment=1"
            f'&time_range={{"since":"{s}","until":"{u}"}}'
            f"&limit=100"
            f"&access_token={token}"
        )
        result = fetch_all_pages(url)
        if isinstance(result, dict) and result.get("error"):
            print(f"  ERRO: {result.get('error')}")
            break
        if result and result.get("data"):
            all_data.extend(result["data"])

        chunk_start = chunk_end + timedelta(days=1)

    return all_data

def fetch_campaign_daily_chunked(account_id, since, until, token):
    """Busca insights diarios POR CAMPANHA em blocos de 3 meses."""
    all_data = []
    chunk_start = datetime.strptime(since, "%Y-%m-%d")
    end_date = datetime.strptime(until, "%Y-%m-%d")

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=89), end_date)
        s = chunk_start.strftime("%Y-%m-%d")
        u = chunk_end.strftime("%Y-%m-%d")
        print(f"  Campaign daily {s} a {u}...")

        fields = "campaign_name,campaign_id,spend,impressions,clicks,actions,reach,cpc,cpm,ctr"
        url = (
            f"{BASE_URL}/{account_id}/insights"
            f"?fields={fields}"
            f"&level=campaign"
            f"&time_increment=1"
            f'&time_range={{"since":"{s}","until":"{u}"}}'
            f"&limit=500"
            f"&access_token={token}"
        )
        result = fetch_all_pages(url)
        if isinstance(result, dict) and result.get("error"):
            print(f"  ERRO: {result.get('error')}")
            break
        if result and result.get("data"):
            all_data.extend(result["data"])

        chunk_start = chunk_end + timedelta(days=1)

    return all_data

def main():
    global META_TOKEN

    if not META_TOKEN:
        print("ERRO: META_TOKEN nao configurado")
        prev = load_previous_data()
        if prev:
            prev["meta"]["token_status"] = "missing"
            prev["meta"]["last_attempt"] = datetime.now().isoformat()
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False, indent=2)
            print("Dados anteriores preservados")
        return

    today = datetime.now()

    # Validar token
    token_valid, msg = validate_token(META_TOKEN)
    if not token_valid:
        print(f"AVISO: {msg}")
        print("Tentando estender token...")
        new_token = try_extend_token(META_TOKEN)
        if new_token:
            META_TOKEN = new_token
            token_valid = True
        else:
            print("Token invalido e nao foi possivel estender.")
            prev = load_previous_data()
            if prev:
                prev["meta"]["token_status"] = "expired"
                prev["meta"]["last_attempt"] = today.isoformat()
                prev["meta"]["error"] = msg
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(prev, f, ensure_ascii=False, indent=2)
                print("Dados anteriores preservados com flag de token expirado")
            return

    # Periodos
    since_all = HISTORY_START
    until_all = today.strftime("%Y-%m-%d")

    # Mes atual para KPIs e campanhas resumo
    first_day = today.replace(day=1)
    since_current = first_day.strftime("%Y-%m-%d")
    until_current = until_all

    # Mes anterior para comparativo
    last_month_end = first_day - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    since_prev = last_month_start.strftime("%Y-%m-%d")
    until_prev = last_month_end.strftime("%Y-%m-%d")

    all_campaigns = []
    daily_data_raw = []
    campaign_daily_raw = []
    api_errors = []

    for account_id in AD_ACCOUNT_IDS:
        print(f"\n=== Conta: {account_id} ===")

        # 1. Campanhas resumo - mes atual (para KPIs do mes)
        print("Buscando campanhas mes atual...")
        fields = ",".join([
            "campaign_name", "campaign_id", "spend", "impressions",
            "clicks", "actions", "cost_per_action_type",
            "cpc", "cpm", "ctr", "reach", "frequency"
        ])
        url = (
            f"{BASE_URL}/{account_id}/insights"
            f"?fields={fields}"
            f"&level=campaign"
            f'&time_range={{"since":"{since_current}","until":"{until_current}"}}'
            f"&limit=100"
            f"&access_token={META_TOKEN}"
        )
        result = fetch_all_pages(url)

        if isinstance(result, dict) and result.get("error") in ("token_expired", "token_invalid"):
            api_errors.append(f"{account_id}: {result.get('error')}")
            continue

        if result and result.get("data"):
            for c in result["data"]:
                leads = extract_action_value(c.get("actions"), "lead")
                messages = extract_action_value(
                    c.get("actions"),
                    "onsite_conversion.messaging_conversation_started_7d"
                )
                landing_views = extract_action_value(c.get("actions"), "landing_page_view")
                link_clicks = extract_action_value(c.get("actions"), "link_click")
                cpl = extract_cost_per_action(c.get("cost_per_action_type"), "lead")
                spend = float(c.get("spend", 0))
                total_conversions = leads + messages

                campaign = {
                    "campaign_name": c.get("campaign_name", ""),
                    "campaign_id": c.get("campaign_id", ""),
                    "ad_account": account_id,
                    "spend": spend,
                    "impressions": int(c.get("impressions", 0)),
                    "clicks": int(c.get("clicks", 0)),
                    "reach": int(c.get("reach", 0)),
                    "cpc": float(c.get("cpc", 0)),
                    "cpm": float(c.get("cpm", 0)),
                    "ctr": float(c.get("ctr", 0)),
                    "frequency": float(c.get("frequency", 0)),
                    "leads": leads,
                    "messages": messages,
                    "landing_page_views": landing_views,
                    "link_clicks": link_clicks,
                    "cpl": cpl if cpl > 0 else (spend / leads if leads > 0 else 0),
                    "cost_per_msg": spend / messages if messages > 0 else 0,
                    "cost_per_conversion": spend / total_conversions if total_conversions > 0 else 0,
                }
                all_campaigns.append(campaign)

        # 2. Insights diarios HISTORICOS (desde 2025-01-01)
        print(f"Buscando dados diarios historicos desde {since_all}...")
        daily_raw = fetch_daily_insights_chunked(account_id, since_all, until_all, META_TOKEN)
        daily_data_raw.extend([{
            "date": d.get("date_start", ""),
            "ad_account": account_id,
            "spend": float(d.get("spend", 0)),
            "impressions": int(d.get("impressions", 0)),
            "clicks": int(d.get("clicks", 0)),
            "reach": int(d.get("reach", 0)),
            "leads": extract_action_value(d.get("actions"), "lead"),
            "messages": extract_action_value(
                d.get("actions"),
                "onsite_conversion.messaging_conversation_started_7d"
            ),
        } for d in daily_raw])

        # 3. Insights diarios POR CAMPANHA HISTORICOS
        print(f"Buscando dados diarios por campanha historicos desde {since_all}...")
        camp_daily_raw = fetch_campaign_daily_chunked(account_id, since_all, until_all, META_TOKEN)
        campaign_daily_raw.extend([{
            "date": d.get("date_start", ""),
            "campaign_name": d.get("campaign_name", ""),
            "campaign_id": d.get("campaign_id", ""),
            "spend": float(d.get("spend", 0)),
            "impressions": int(d.get("impressions", 0)),
            "clicks": int(d.get("clicks", 0)),
            "reach": int(d.get("reach", 0)),
            "leads": extract_action_value(d.get("actions"), "lead"),
            "messages": extract_action_value(
                d.get("actions"),
                "onsite_conversion.messaging_conversation_started_7d"
            ),
            "landing_page_views": extract_action_value(d.get("actions"), "landing_page_view"),
        } for d in camp_daily_raw])

    # Se todas as contas falharam
    if len(api_errors) == len(AD_ACCOUNT_IDS) and not all_campaigns:
        print(f"\nTODAS as contas falharam: {api_errors}")
        prev = load_previous_data()
        if prev:
            prev["meta"]["token_status"] = "expired"
            prev["meta"]["last_attempt"] = today.isoformat()
            prev["meta"]["errors"] = api_errors
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False, indent=2)
            print("Dados anteriores preservados")
        return

    # Agregar daily_data por data (somar contas)
    daily_agg = {}
    for d in daily_data_raw:
        dt = d["date"]
        if dt not in daily_agg:
            daily_agg[dt] = {"date": dt, "spend": 0, "impressions": 0, "clicks": 0, "reach": 0, "leads": 0, "messages": 0}
        daily_agg[dt]["spend"] += d["spend"]
        daily_agg[dt]["impressions"] += d["impressions"]
        daily_agg[dt]["clicks"] += d["clicks"]
        daily_agg[dt]["reach"] += d["reach"]
        daily_agg[dt]["leads"] += d["leads"]
        daily_agg[dt]["messages"] += d["messages"]
    daily_data = list(daily_agg.values())

    # Buscar dados do mes anterior para comparativo
    print(f"\nBuscando mes anterior para comparativo ({since_prev} a {until_prev})...")
    prev_campaigns = []
    for account_id in AD_ACCOUNT_IDS:
        fields = ",".join([
            "campaign_name", "spend", "impressions", "clicks", "actions"
        ])
        url = (
            f"{BASE_URL}/{account_id}/insights"
            f"?fields={fields}"
            f"&level=campaign"
            f'&time_range={{"since":"{since_prev}","until":"{until_prev}"}}'
            f"&limit=100"
            f"&access_token={META_TOKEN}"
        )
        result = fetch_all_pages(url)
        if result and result.get("data") and not (isinstance(result, dict) and result.get("error")):
            for c in result["data"]:
                leads = extract_action_value(c.get("actions"), "lead")
                messages = extract_action_value(
                    c.get("actions"),
                    "onsite_conversion.messaging_conversation_started_7d"
                )
                prev_campaigns.append({
                    "campaign_name": c.get("campaign_name", ""),
                    "spend": float(c.get("spend", 0)),
                    "impressions": int(c.get("impressions", 0)),
                    "clicks": int(c.get("clicks", 0)),
                    "leads": leads,
                    "messages": messages,
                })

    # Calcular totais mes atual
    total_spend = sum(c["spend"] for c in all_campaigns)
    total_impressions = sum(c["impressions"] for c in all_campaigns)
    total_clicks = sum(c["clicks"] for c in all_campaigns)
    total_leads = sum(c["leads"] for c in all_campaigns)
    total_messages = sum(c["messages"] for c in all_campaigns)
    total_reach = sum(c["reach"] for c in all_campaigns)
    total_conversions = total_leads + total_messages

    # Totais mes anterior
    prev_spend = sum(c["spend"] for c in prev_campaigns)
    prev_leads = sum(c["leads"] for c in prev_campaigns)
    prev_messages = sum(c["messages"] for c in prev_campaigns)
    prev_clicks = sum(c["clicks"] for c in prev_campaigns)

    # Montar output
    output = {
        "meta": {
            "fetched_at": today.isoformat(),
            "history_start": HISTORY_START,
            "period_current": f"{since_current} a {until_current}",
            "period_prev": f"{since_prev} a {until_prev}",
            "ad_accounts": AD_ACCOUNT_IDS,
            "token_status": "valid",
            "daily_records": len(daily_data),
            "campaign_daily_records": len(campaign_daily_raw),
        },
        "kpis": {
            "investimento": total_spend,
            "impressoes": total_impressions,
            "cliques": total_clicks,
            "alcance": total_reach,
            "leads": total_leads,
            "mensagens": total_messages,
            "conversoes": total_conversions,
            "cpl": total_spend / total_leads if total_leads > 0 else 0,
            "cpm_medio": (total_spend / total_impressions * 1000) if total_impressions > 0 else 0,
            "ctr_medio": (total_clicks / total_impressions * 100) if total_impressions > 0 else 0,
            "custo_por_conversao": total_spend / total_conversions if total_conversions > 0 else 0,
        },
        "kpis_prev": {
            "investimento": prev_spend,
            "leads": prev_leads,
            "mensagens": prev_messages,
            "cliques": prev_clicks,
            "conversoes": prev_leads + prev_messages,
        },
        "campaigns": sorted(all_campaigns, key=lambda x: x["spend"], reverse=True),
        "daily": sorted(daily_data, key=lambda x: x["date"]),
        "campaign_daily": sorted(campaign_daily_raw, key=lambda x: (x["date"], x["campaign_name"])),
    }

    if api_errors:
        output["meta"]["partial_errors"] = api_errors

    # Salvar JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== Meta Ads - Resumo ===")
    print(f"Historico desde: {HISTORY_START}")
    print(f"Dias de dados diarios: {len(daily_data)}")
    print(f"Registros campanha/dia: {len(campaign_daily_raw)}")
    print(f"Campanhas mes atual: {len(all_campaigns)}")
    print(f"Investimento mes atual: R$ {total_spend:,.2f}")
    print(f"Leads mes atual: {total_leads}")
    print(f"Mensagens mes atual: {total_messages}")
    if api_errors:
        print(f"\nAVISO - Erros parciais: {api_errors}")
    print(f"\nArquivo salvo: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
