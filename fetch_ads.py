#!/usr/bin/env python3
"""
fetch_ads.py - Busca dados de campanhas Meta Ads para o dashboard PAAS
Usa a Marketing API do Facebook/Meta para obter insights de campanhas.
"""
import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta

META_TOKEN = os.environ.get("META_TOKEN", "")
AD_ACCOUNT_IDS = [
    "act_656534222273647",   # PAAS PoÃ§os Artesianos (principal)
    "act_568178800458922",   # Chert Bobsin Paas PoÃ§os Artesianos
    "act_545763446485919",   # Chert bobsin/PAAS PoÃ§os Artesianos
]
API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

def fetch_json(url):
    """Faz GET request e retorna JSON."""
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"HTTP {e.code}: {body[:500]}")
        return None
    except Exception as e:
        print(f"Erro: {e}")
        return None

def fetch_account_insights(account_id, since, until):
    """Busca insights por campanha de uma conta de anÃºncios."""
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
        f"&access_token={META_TOKEN}"
    )
    return fetch_json(url)

def fetch_daily_insights(account_id, since, until):
    """Busca insights diÃ¡rios agregados da conta."""
    fields = "spend,impressions,clicks,actions,reach"
    url = (
        f"{BASE_URL}/{account_id}/insights"
        f"?fields={fields}"
        f"&time_increment=1"
        f'&time_range={{"since":"{since}","until":"{until}"}}'
        f"&limit=100"
        f"&access_token={META_TOKEN}"
    )
    return fetch_json(url)

def extract_action_value(actions, action_type):
    """Extrai valor de uma aÃ§Ã£o especÃ­fica da lista de actions."""
    if not actions:
        return 0
    for a in actions:
        if a.get("action_type") == action_type:
            return int(a.get("value", 0))
    return 0

def extract_cost_per_action(cost_actions, action_type):
    """Extrai custo por aÃ§Ã£o especÃ­fica."""
    if not cost_actions:
        return 0
    for a in cost_actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0

def main():
    if not META_TOKEN:
        print("ERRO: META_TOKEN nÃ£o configurado")
        return

    today = datetime.now()

    # PerÃ­odo: mÃªs atual
    first_day = today.replace(day=1)
    since_current = first_day.strftime("%Y-%m-%d")
    until_current = today.strftime("%Y-%m-%d")

    # PerÃ­odo: mÃªs anterior (para comparativo)
    last_month_end = first_day - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    since_prev = last_month_start.strftime("%Y-%m-%d")
    until_prev = last_month_end.strftime("%Y-%m-%d")

    all_campaigns = []
    daily_data = []

    for account_id in AD_ACCOUNT_IDS:
        print(f"Buscando dados de {account_id}...")

        # Insights por campanha - mÃªs atual
        result = fetch_account_insights(account_id, since_current, until_current)
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

        # Insights diÃ¡rios - mÃªs atual
        daily_result = fetch_daily_insights(account_id, since_current, until_current)
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

    # Buscar dados do mÃªs anterior para comparativo
    prev_campaigns = []
    for account_id in AD_ACCOUNT_IDS:
        result = fetch_account_insights(account_id, since_prev, until_prev)
        if result and result.get("data"):
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

    # Totais mÃªs anterior
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
    }

    # Salvar JSON
    with open("ads_data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== Meta Ads - Resumo ===")
    print(f"PerÃ­odo: {since_current} a {until_current}")
    print(f"Campanhas: {len(all_campaigns)}")
    print(f"Investimento: R$ {total_spend:,.2f}")
    print(f"ImpressÃµes: {total_impressions:,}")
    print(f"Cliques: {total_clicks:,}")
    print(f"Leads: {total_leads}")
    print(f"Mensagens WhatsApp: {total_messages}")
    print(f"CPL: R$ {(total_spend/total_leads if total_leads > 0 else 0):,.2f}")
    print(f"Custo/ConversÃ£o: R$ {(total_spend/total_conversions if total_conversions > 0 else 0):,.2f}")
    print(f"\nArquivo salvo: ads_data.json")

if __name__ == "__main__":
    main()

