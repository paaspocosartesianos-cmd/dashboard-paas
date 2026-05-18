#!/usr/bin/env python3
"""
fetch_google_ads.py - Busca dados HISTORICOS de campanhas Google Ads para o dashboard PAAS
Usa a Google Ads REST API (v18) cm OAuth2 para obter metricas de campanhas.

Busca desde 2025-01-01 ate hoje, com dados diarios por campanha.
Sem dependencias externas (apenas stdlib).
"""
import os
import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta

# === CONFIGURACAO ===
GOOGLE_ADS_CLIENT_ID = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
GOOGLE_ADS_CLIENT_SECRET = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
GOOGLE_ADS_REFRESH_TOKEN = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GOOGLE_ADS_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "8136550806")
# Login customer ID - se for MCC (Manager account), coloque o ID do MCC aqui
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")

API_VERSION = "v23"
BASE_URL = f"https://googleads.googleapis.com/{API_VERSION}"
TOKEN_URL = "https://oauth2.googleapis.com/token"

HISTORY_START = "2025-01-01"
OUTPUT_FILE = "google_ads_data.json"
DATA_DIR_FILE = os.path.join("data", "google_ads_data.json")


def get_access_token():
    """Troca refresh_token por um access_token valido."""
    data = urllib.parse.urlencode({
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }).encode("utf-8")

    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            token = result.get("access_token")
            if token:
                print("Access token obtido com sucesso")
                return token
            else:
                print(f"Erro ao obter token: {result}")
                return None
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Erro HTTP {e.code} ao obter token: {body[:500]}")
        return None
    except Exception as e:
        print(f"Erro ao obter token: {e}")
        return None


def gaql_search(access_token, customer_id, query):
    """Executa uma query GAQL usando searchStream (retorna tudo em uma chamada)."""
    url = f"{BASE_URL}/customers/{customer_id}/googleAds:searchStream"

    payload = json.dumps({"query": query}).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("developer-token", GOOGLE_ADS_DEVELOPER_TOKEN)
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        req.add_header("login-customer-id", GOOGLE_ADS_LOGIN_CUSTOMER_ID)

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode()
            results = json.loads(raw)
            # searchStream retorna array de batches
            all_rows = []
            if isinstance(results, list):
                for batch in results:
                    rows = batch.get("results", [])
                    all_rows.extend(rows)
            elif isinstance(results, dict):
                all_rows = results.get("results", [])
            return all_rows
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Erro HTTP {e.code} na query GAQL: {body[:800]}")
        return None
    except Exception as e:
        print(f"Erro na query GAQL: {e}")
        return None


def load_previous_data():
    """Carrega dados anteriores do arquivo existente (fallback)."""
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


def fetch_campaign_daily(access_token, customer_id, since, until):
    """Busca metricas diarias por campanha."""
    query = f"""
        SELECT
            campaign.name,
            campaign.id,
            campaign.advertising_channel_type,
            campaign.status,
            segments.date,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.all_conversions,
            metrics.interactions
        FROM campaign
        WHERE segments.date BETWEEN '{since}' AND '{until}'
            AND metrics.cost_micros > 0
        ORDER BY segments.date ASC
    """

    print(f"Buscando dados diarios de {since} a {until}...")
    rows = gaql_search(access_token, customer_id, query)

    if rows is None:
        return None

    daily_data = []
    for row in rows:
        campaign = row.get("campaign", {})
        segments = row.get("segments", {})
        metrics = row.get("metrics", {})

        cost_micros = int(metrics.get("costMicros", 0))
        cost = cost_micros / 1_000_000  # Converter micros para reais

        # Mapear channel type para tipo legivel
        channel_type = campaign.get("advertisingChannelType", "UNSPECIFIED")
        type_map = {
            "SEARCH": "Pesquisa",
            "DISPLAY": "Display",
            "VIDEO": "Video",
            "SHOPPING": "Shopping",
            "SMART": "Smart",
            "PERFORMANCE_MAX": "Performance Max",
            "DEMAND_GEN": "Geracao de demanda",
            "LOCAL": "Local",
        }
        campaign_type = type_map.get(channel_type, channel_type)

        # Mapear status
        status_raw = campaign.get("status", "UNKNOWN")
        status_map = {
            "ENABLED": "Ativada",
            "PAUSED": "Pausada",
            "REMOVED": "Removida",
        }
        status = status_map.get(status_raw, status_raw)

        daily_data.append({
            "date": segments.get("date", ""),
            "campaign_name": campaign.get("name", ""),
            "campaign_id": str(campaign.get("id", "")),
            "campaign_type": campaign_type,
            "status": status,
            "cost": round(cost, 2),
            "impressions": int(metrics.get("impressions", 0)),
            "clicks": int(metrics.get("clicks", 0)),
            "conversions": round(float(metrics.get("conversions", 0)), 1),
        })

    print(f"  {len(daily_data)} registros campanha/dia obtidos")
    return daily_data


def fetch_campaign_totals(access_token, customer_id, since, until):
    """Busca totais por campanha para o periodo atual (resumo)."""
    query = f"""
        SELECT
            campaign.name,
            campaign.id,
            campaign.advertising_channel_type,
            campaign.status,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.all_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{since}' AND '{until}'
        ORDER BY metrics.cost_micros DESC
    """

    print(f"Buscando totais de campanha {since} a {until}...")
    rows = gaql_search(access_token, customer_id, query)

    if rows is None:
        return None

    campaigns = []
    for row in rows:
        campaign = row.get("campaign", {})
        metrics = row.get("metrics", {})

        cost_micros = int(metrics.get("costMicros", 0))
        cost = cost_micros / 1_000_000
        clicks = int(metrics.get("clicks", 0))
        impressions = int(metrics.get("impressions", 0))
        conversions = round(float(metrics.get("conversions", 0)), 1)

        channel_type = campaign.get("advertisingChannelType", "UNSPECIFIED")
        type_map = {
            "SEARCH": "Pesquisa",
            "DISPLAY": "Display",
            "VIDEO": "Video",
            "SHOPPING": "Shopping",
            "SMART": "Smart",
            "PERFORMANCE_MAX": "Performance Max",
            "DEMAND_GEN": "Geracao de demanda",
        }
        campaign_type = type_map.get(channel_type, channel_type)

        status_raw = campaign.get("status", "UNKNOWN")
        status_map = {"ENABLED": "Ativada", "PAUSED": "Pausada", "REMOVED": "Removida"}
        status = status_map.get(status_raw, status_raw)

        campaigns.append({
            "campaign_name": campaign.get("name", ""),
            "campaign_id": str(campaign.get("id", "")),
            "campaign_type": campaign_type,
            "status": status,
            "cost": round(cost, 2),
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "conv_rate": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
        })

    print(f"  {len(campaigns)} campanhas com dados")
    return campaigns


def main():
    import urllib.parse  # Needed for urlencode in get_access_token

    today = datetime.now()
    until_all = today.strftime("%Y-%m-%d")

    # Verificar credenciais
    missing = []
    if not GOOGLE_ADS_CLIENT_ID:
        missing.append("GOOGLE_ADS_CLIENT_ID")
    if not GOOGLE_ADS_CLIENT_SECRET:
        missing.append("GOOGLE_ADS_CLIENT_SECRET")
    if not GOOGLE_ADS_REFRESH_TOKEN:
        missing.append("GOOGLE_ADS_REFRESH_TOKEN")
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        missing.append("GOOGLE_ADS_DEVELOPER_TOKEN")

    if missing:
        print(f"AVISO: Credenciais Google Ads ausentes: {', '.join(missing)}")
        prev = load_previous_data()
        if prev:
            prev["meta"] = prev.get("meta", {})
            prev["meta"]["credentials_status"] = "missing"
            prev["meta"]["missing_credentials"] = missing
            prev["meta"]["last_attempt"] = today.isoformat()
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False, indent=2)
            print("Dados anteriores preservados")
        else:
            print("ERRO: Sem credenciais e sem dados anteriores")
        return

    # Obter access token
    access_token = get_access_token()
    if not access_token:
        print("ERRO: Nao foi possivel obter access token")
        prev = load_previous_data()
        if prev:
            prev["meta"] = prev.get("meta", {})
            prev["meta"]["credentials_status"] = "token_error"
            prev["meta"]["last_attempt"] = today.isoformat()
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False, indent=2)
            print("Dados anteriores preservados")
        return

    customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")

    # === 1. Dados diarios historicos por campanha (desde 2025-01-01) ===
    campaign_daily = fetch_campaign_daily(access_token, customer_id, HISTORY_START, until_all)
    if campaign_daily is None:
        print("ERRO: Falha ao buscar dados diarios")
        prev = load_previous_data()
        if prev:
            prev["meta"] = prev.get("meta", {})
            prev["meta"]["credentials_status"] = "api_error"
            prev["meta"]["last_attempt"] = today.isoformat()
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False, indent=2)
            print("Dados anteriores preservados")
        return

    # === 2. Totais all-time por campanha (para manter compatibilidade) ===
    all_time_campaigns = fetch_campaign_totals(access_token, customer_id, "2014-01-01", until_all)

    # === 3. Agregar daily por data (soma todas campanhas) ===
    daily_agg = {}
    for d in campaign_daily:
        dt = d["date"]
        if dt not in daily_agg:
            daily_agg[dt] = {"date": dt, "cost": 0, "impressions": 0, "clicks": 0, "conversions": 0}
        daily_agg[dt]["cost"] += d["cost"]
        daily_agg[dt]["impressions"] += d["impressions"]
        daily_agg[dt]["clicks"] += d["clicks"]
        daily_agg[dt]["conversions"] += d["conversions"]
    daily_data = sorted(daily_agg.values(), key=lambda x: x["date"])

    # === 4. Calcular totais ===
    total_cost = sum(d["cost"] for d in daily_data)
    total_impressions = sum(d["impressions"] for d in daily_data)
    total_clicks = sum(d["clicks"] for d in daily_data)
    total_conversions = sum(d["conversions"] for d in daily_data)

    # Totais all-time (para manter compatibilidade com formato antigo)
    if all_time_campaigns:
        alltime_cost = sum(c["cost"] for c in all_time_campaigns)
        alltime_clicks = sum(c["clicks"] for c in all_time_campaigns)
        alltime_impressions = sum(c["impressions"] for c in all_time_campaigns)
        alltime_conversions = sum(c["conversions"] for c in all_time_campaigns)
    else:
        alltime_cost = total_cost
        alltime_clicks = total_clicks
        alltime_impressions = total_impressions
        alltime_conversions = total_conversions

    # Encontrar range de datas
    dates = [d["date"] for d in campaign_daily if d["date"]]
    first_date = min(dates) if dates else HISTORY_START
    last_date = max(dates) if dates else until_all

    # === 5. Montar output ===
    output = {
        "platform": "google_ads",
        "account_id": GOOGLE_ADS_CUSTOMER_ID,
        "account_name": "PAAS Pocos Artesianos",
        "date_range": f"{first_date} a {last_date}",
        "last_updated": until_all,
        "meta": {
            "fetched_at": today.isoformat(),
            "history_start": HISTORY_START,
            "credentials_status": "valid",
            "daily_records": len(daily_data),
            "campaign_daily_records": len(campaign_daily),
            "api_version": API_VERSION,
        },
        # Campanhas all-time (formato antigo - para compatibilidade)
        "campaigns": all_time_campaigns or [],
        # NOVO: Dados diarios agregados
        "daily": daily_data,
        # NOVO: Dados diarios por campanha (para filtro exato por periodo)
        "campaign_daily": sorted(campaign_daily, key=lambda x: (x["date"], x["campaign_name"])),
        # Totais all-time (formato antigo - para compatibilidade)
        "totals": {
            "impressions": alltime_impressions,
            "clicks": alltime_clicks,
            "cost": round(alltime_cost, 2),
            "conversions": alltime_conversions,
            "cpc_avg": round(alltime_cost / alltime_clicks, 2) if alltime_clicks > 0 else 0,
        },
    }

    # Salvar JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== Google Ads - Resumo ===")
    print(f"Historico desde: {HISTORY_START}")
    print(f"Dias de dados diarios: {len(daily_data)}")
    print(f"Registros campanha/dia: {len(campaign_daily)}")
    print(f"Campanhas totais: {len(all_time_campaigns or [])}")
    print(f"Investimento total (periodo): R$ {total_cost:,.2f}")
    print(f"Cliques (periodo): {total_clicks:,}")
    print(f"Impressoes (periodo): {total_impressions:,}")
    print(f"Conversoes (periodo): {total_conversions:,.0f}")
    print(f"\nArquivo salvo: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
