#!/usr/bin/env python3
"""
fetch_ads.py - Busca dados de campanhas Meta Ads para o dashboard PAAS
Usa a Marketing API do Facebook/Meta para obter insights de campanhas.

Melhorias:
- Detecta token expirado e tenta estender automaticamente
- Preserva dados anteriores quando a API falha
- Suporte a token de longa duraÃ§Ã£o (60 dias)
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

# Caminho do arquivo de saida
OUTPUT_FILE = "ads_data.json"
DATA_DIR_FILE = os.path.join("data", "ads_data.json")

def fetch_json(url):
    """Faz GET request e retorna JSON."""
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"HTTP {e.code}: {body[:500]}")
        # Detectar token expirado
        if e.code == 400 and "expired" in body.lower():
            return {"error": "token_expired", "details": body[:200]}
        if e.code == 400 and ("OAuthException" in body or "access token" in body.lower()):
            return {"error": "token_invalid", "details": body[:200]}
        return None
    except Exception as e:
        print(f"Erro: {e}")
        return None

def try_extend_token(short_token):
    """Tenta trocar token de curta duracao por um de longa duracao (60 dias).
    Requer META_APP_ID e META_APP_SECRET configurados como secrets."""
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
    """Verifica se o token e valido fazendo uma chamada simples."""
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

def fetch_account_insights(account_id, since, until, token):
    """Busca insights por campanha de uma conta de anuncios."""
    fields = ",".join([
        "campaign_name", "campaign_id", "spend", "impressions",
        "clicks", "actions", "cost_per_action_type",
        "cpc", "cpm", "ctr", "reach", "frequency"
    ])
    url = (
        f"{BASE_URL}/{account_id}/insights"
        f"?fields={fields}"
        f"&level=campaign"
        f'&time_range={{"since":"{since}","until":"{until}"}}'
        f"&limit=100"
        f"&access_token={token}"
    )
    return fetch_json(url)

def fetch_daily_insights(account_id, since, until, token):
    """Busca insights diarios agregados da conta."""
    fields = "spend,impressions,clicks,actions,reach"
    url = (
        f"{BASE_URL}/{account_id}/insights"
        f"?fields={fields}"
        f"&time_increment=1"
        f'&time_range={{"since":"{since}","until":"{until}"}}'
        f"&limit=100"
        f"&access_token={token}"
    )
    return fetch_json(url)

def fetch_campaign_daily_insights(account_id, since, until, token):
    """Busca insights diarios POR CAMPANHA para permitir filtro de data na tabela."""
    fields = "campaign_name,campaign_id,spend,impressions,clicks,actions,reach,cpc,cpm,ctr"
    url = (
        f"{BASE_URL}/{account_id}/insights"
        f"?fields={fields}"
        f"&level=campaign"
        f"&time_increment=1"
        f'&time_range={{\"since\":\"{since}\",\"until\":\"{until}\"}}'
        f"&limit=500"
        f"&access_token={token}"
    )
    return fetch_json(url)

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

def main():
    global META_TOKEN

    if not META_TOKEN:
        print("ERRO: META_TOKEN nao configurado")
        print("Tentando preservar dados anteriores...")
        prev = load_previous_data()
        if prev:
            prev["meta"]["token_status"] = "missing"
            prev["meta"]["last_attempt"] = datetime.now().isoformat()
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False, indent=2)
            print("Dados anteriores preservados")
        return

    today = datetime.now()

    # Validar token antes de comecar
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
            print("Preservando dados anteriores...")
            prev = load_previous_data()
            if prev:
                prev["meta"]["token_status"] = "expired"
                prev["meta"]["last_attempt"] = today.isoformat()
                prev["meta"]["error"] = msg
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(prev, f, ensure_ascii=False, indent=2)
                print("Dados anteriores preservados com flag de token expirado")
            else:
                print("ERRO CRITICO: Sem dados anteriores e token invalido!")
            return

    # Periodo: mes atual
    first_day = today.replace(day=1)
    since_current = first_day.strftime("%Y-%m-%d")
    until_current = today.strftime("%Y-%m-%d")

    # Periodo: mes anterior (para comparativo)
    last_month_end = first_day - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    since_prev = last_month_start.strftime("%Y-%m-%d")
    until_prev = last_month_end.strftime("%Y-%m-%d")

    all_campaigns = []
    daily_data = []
    campaign_daily_data = []
    api_errors = []

    for account_id in AD_ACCOUNT_IDS:
        print(f"Buscando dados de {account_id}...")

        # Insights por campanha - mes atual
        result = fetch_account_insights(account_id, since_current, until_current, META_TOKEN)

        # Verificar se houve erro de token durante fetch
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

        # Insights diarios - mes atual
        daily_result = fetch_daily_insights(account_id, since_current, until_current, META_TOKEN)
        if daily_result and daily_result.get("data"):
            for d in daily_result["data"]:
                daily_data.append({
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
                })

        # Insights diarios POR CAMPANHA - para filtro de data na tabela
        camp_daily_result = fetch_campaign_daily_insights(account_id, since_current, until_current, META_TOKEN)
        if camp_daily_result and camp_daily_result.get("data"):
            for d in camp_daily_result["data"]:
                leads = extract_action_value(d.get("actions"), "lead")
                messages = extract_action_value(
                    d.get("actions"),
                    "onsite_conversion.messaging_conversation_started_7d"
                )
                landing_views = extract_action_value(d.get("actions"), "landing_page_view")
                campaign_daily_data.append({
                    "date": d.get("date_start", ""),
                    "campaign_name": d.get("campaign_name", ""),
                    "campaign_id": d.get("campaign_id", ""),
                    "spend": float(d.get("spend", 0)),
                    "impressions": int(d.get("impressions", 0)),
                    "clicks": int(d.get("clicks", 0)),
                    "reach": int(d.get("reach", 0)),
                    "leads": leads,
                    "messages": messages,
                    "landing_page_views": landing_views,
                })

    # Se todas as contas falharam com erro de token, preservar dados anteriores
    if len(api_errors) == len(AD_ACCOUNT_IDS) and not all_campaigns:
        print(f"\nTODAS as contas falharam: {api_errors}")
        print("Preservando dados anteriores...")
        prev = load_previous_data()
        if prev:
            prev["meta"]["token_status"] = "expired"
            prev["meta"]["last_attempt"] = today.isoformat()
            prev["meta"]["errors"] = api_errors
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False, indent=2)
            print("Dados anteriores preservados")
        else:
            print("ERRO: Sem dados anteriores para preservar!")
        return

    # Agregar daily_data por data (somar contas)
    daily_agg = {}
    for d in daily_data:
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
    prev_campaigns = []
    for account_id in AD_ACCOUNT_IDS:
        result = fetch_account_insights(account_id, since_prev, until_prev, META_TOKEN)
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

    # Calcular totais
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
            "period": f"{since_current} a {until_current}",
            "prev_period": f"{since_prev} a {until_prev}",
            "ad_accounts": AD_ACCOUNT_IDS,
            "token_status": "valid",
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
        "campaign_daily": sorted(campaign_daily_data, key=lambda x: (x["date"], x["campaign_name"])),
    }

    if api_errors:
        output["meta"]["partial_errors"] = api_errors

    # Salvar JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== Meta Ads - Resumo ===")
    print(f"Periodo: {since_current} a {until_current}")
    print(f"Campanhas: {len(all_campaigns)}")
    print(f"Investimento: R$ {total_spend:,.2f}")
    print(f"Impressoes: {total_impressions:,}")
    print(f"Cliques: {total_clicks:,}")
    print(f"Leads: {total_leads}")
    print(f"Mensagens WhatsApp: {total_messages}")
    print(f"CPL: R$ {(total_spend/total_leads if total_leads > 0 else 0):,.2f}")
    print(f"Custo/Conversao: R$ {(total_spend/total_conversions if total_conversions > 0 else 0):,.2f}")
    if api_errors:
        print(f"\nAVISO - Erros parciais: {api_errors}")
    print(f"\nArquivo salvo: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
