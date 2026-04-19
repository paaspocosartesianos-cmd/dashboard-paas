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
FVbfWF6öFÇöç6vG5ö6Væ¶VB66÷VçEöBÂ6æ6RÂVçFÂÂFö¶Vâ ¢""$'W66ç6vG2F&÷2VÒ&Æö6÷2FR2ÖW6W2&WfF"FÖV÷WBöÆÖFW2â"" ¢ÆÅöFFÒµÐ¢6Væµ÷7F'BÒFFWFÖRç7G'FÖR6æ6RÂ"UÒVÒÒVB"¢VæEöFFRÒFFWFÖRç7G'FÖRVçFÂÂ"UÒVÒÒVB" ¢vÆR6Væµ÷7F'BÃÒVæEöFFS ¢6VæµöVæBÒÖâ6Væµ÷7F'B²FÖVFVÇFF3ÓÂVæEöFFR¢2Ò6Væµ÷7F'Bç7G&gFÖR"UÒVÒÒVB"¢RÒ6VæµöVæBç7G&gFÖR"UÒVÒÒVB"¢&çBb"FÇ·7Ò·WÒâââ" ¢fVÆG2Ò'7VæBÆ×&W76öç2Æ6Æ6·2Æ7Föç2Ç&V6 ¢W&ÂÒ¢b'´$4UõU$ÇÒ÷¶66÷VçEöGÒöç6vG2 ¢b#öfVÆG3×¶fVÆG7Ò ¢b"gFÖUöæ7&VÖVçCÓ ¢brgFÖU÷&ævS×·²'6æ6R#¢'·7Ò"Â'VçFÂ#¢'·WÒ'×Òp¢b"fÆÖCÓ ¢b"f66W75÷Fö¶Vã×·Fö¶VçÒ ¢¢&W7VÇBÒfWF6öÆÅ÷vW2W&Â¢b6ç7Fæ6R&W7VÇBÂF7BæB&W7VÇBævWB&W'&÷"" ¢&çBb"U%$ó¢·&W7VÇBævWBvW'&÷"rÒ"¢'&V°¢b&W7VÇBæB&W7VÇBævWB&FF" ¢ÆÅöFFæWFVæB&W7VÇE²&FF%Ò ¢6Væµ÷7F'BÒ6VæµöVæB²FÖVFVÇFF3Ó ¢&WGW&âÆÅöFF ¦FVbfWF6ö6×våöFÇö6Væ¶VB66÷VçEöBÂ6æ6RÂVçFÂÂFö¶Vâ ¢""$'W66ç6vG2F&÷2õ"4ÕäVÒ&Æö6÷2FR2ÖW6W2â"" ¢ÆÅöFFÒµÐ¢6Væµ÷7F'BÒFFWFÖRç7G'FÖR6æ6RÂ"UÒVÒÒVB"¢VæEöFFRÒFFWFÖRç7G'FÖRVçFÂÂ"UÒVÒÒVB" ¢vÆR6Væµ÷7F'BÃÒVæEöFFS ¢6VæµöVæBÒÖâ6Væµ÷7F'B²FÖVFVÇFF3ÓÂVæEöFFR¢2Ò6Væµ÷7F'Bç7G&gFÖR"UÒVÒÒVB"¢RÒ6VæµöVæBç7G&gFÖR"UÒVÒÒVB"¢&çBb"6×vâFÇ·7Ò·WÒâââ" ¢fVÆG2Ò&6×våöæÖRÆ6×våöBÇ7VæBÆ×&W76öç2Æ6Æ6·2Æ7Föç2Ç&V6Æ72Æ7ÒÆ7G" ¢W&ÂÒ¢b'´$4UõU$ÇÒ÷¶66÷VçEöGÒöç6vG2 ¢b#öfVÆG3×¶fVÆG7Ò ¢b"fÆWfVÃÖ6×vâ ¢b"gFÖUöæ7&VÖVçCÓ ¢brgFÖU÷&ævS×·²'6æ6R#¢'·7Ò"Â'VçFÂ#¢'·WÒ'×Òp¢b"fÆÖCÓS ¢b"f66W75÷Fö¶Vã×·Fö¶VçÒ ¢¢&W7VÇBÒfWF6öÆÅ÷vW2W&Â¢b6ç7Fæ6R&W7VÇBÂF7BæB&W7VÇBævWB&W'&÷"" ¢&çBb"U%$ó¢·&W7VÇBævWBvW'&÷"rÒ"¢'&V°¢b&W7VÇBæB&W7VÇBævWB&FF" ¢ÆÅöFFæWFVæB&W7VÇE²&FF%Ò ¢6Væµ÷7F'BÒ6VæµöVæB²FÖVFVÇFF3Ó ¢&WGW&âÆÅöFF ¦FVbÖâ ¢vÆö&ÂÔUDõDô´Tà ¢bæ÷BÔUDõDô´Tã ¢&çB$U%$ó¢ÔUDõDô´Tâæò6öæfwW&Fò"¢&WbÒÆöE÷&Wf÷W5öFF¢b&Wc ¢&We²&ÖWF%Õ²'Fö¶Vå÷7FGW2%ÒÒ&Ö76ær ¢&We²&ÖWF%Õ²&Æ7EöGFV×B%ÒÒFFWFÖRææ÷ræ6öf÷&ÖB¢vF÷VâõUEUEôdÄRÂ'r"ÂVæ6öFæsÒ'WFbÓ"2c ¢§6öâæGV×&WbÂbÂVç7W&Uö66ÔfÇ6RÂæFVçCÓ"¢&çB$FF÷2çFW&÷&W2&W6W'fF÷2"¢&WGW&à ¢FöFÒFFWFÖRææ÷r ¢2fÆF"Fö¶Và¢Fö¶Vå÷fÆBÂ×6rÒfÆFFU÷Fö¶VâÔUDõDô´Tâ¢bæ÷BFö¶Vå÷fÆC ¢&çBb$d4ó¢¶×6wÒ"¢&çB%FVçFæFòW7FVæFW"Fö¶Vââââ"¢æWu÷Fö¶VâÒG'öWFVæE÷Fö¶VâÔUDõDô´Tâ¢bæWu÷Fö¶Vã ¢ÔUDõDô´TâÒæWu÷Fö¶Và¢Fö¶Vå÷fÆBÒG'VP¢VÇ6S ¢&çB%Fö¶VâçfÆFòRæòfö÷76fVÂW7FVæFW"â"¢&WbÒÆöE÷&Wf÷W5öFF¢b&Wc      prev["meta"]["token_status"] = "expired"
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
ÈØ[Ý[\ÝZ\ÈY\È]X[Ý[ÜÜ[HÝ[JÖÈÜ[HÜÈ[[ØØ[\ZYÛÊBÝ[Ú[\\ÜÚ[ÛÈHÝ[JÖÈ[\\ÜÚ[ÛÈHÜÈ[[ØØ[\ZYÛÊBÝ[ØÛXÚÜÈHÝ[JÖÈÛXÚÜÈHÜÈ[[ØØ[\ZYÛÊBÝ[ÛXYÈHÝ[JÖÈXYÈHÜÈ[[ØØ[\ZYÛÊBÝ[ÛY\ÜØYÙ\ÈHÝ[JÖÈY\ÜØYÙ\ÈHÜÈ[[ØØ[\ZYÛÊBÝ[ÜXXÚHÝ[JÖÈXXÚHÜÈ[[ØØ[\ZYÛÊBÝ[ØÛÛ\Ú[ÛÈHÝ[ÛXYÈ
ÈÝ[ÛY\ÜØYÙ\ÂÈÝZ\ÈY\È[\[Ü]ÜÜ[HÝ[JÖÈÜ[HÜÈ[]ØØ[\ZYÛÊB]ÛXYÈHÝ[JÖÈXYÈHÜÈ[]ØØ[\ZYÛÊB]ÛY\ÜØYÙ\ÈHÝ[JÖÈY\ÜØYÙ\ÈHÜÈ[]ØØ[\ZYÛÊB]ØÛXÚÜÈHÝ[JÖÈÛXÚÜÈHÜÈ[]ØØ[\ZYÛÊBÈ[Û\Ý]]Ý]]HÂY]HÂ]ÚYØ]Ù^K\ÛÙÜX]

K\ÝÜWÜÝ\TÕÔWÔÕT\[ÙØÝ\[ÜÚ[ÙWØÝ\[HHÝ[[ØÝ\[H\[ÙÜ]ÜÚ[ÙWÜ]HHÝ[[Ü]HYØXØÛÝ[ÈQÐPÐÓÕSÒQËÚÙ[ÜÝ]\È[YZ[WÜXÛÜÈ[Z[WÙ]JKØ[\ZYÛÙZ[WÜXÛÜÈ[Ø[\ZYÛÙZ[WÜ]ÊKKÜ\ÈÂ[\Ý[Y[ÈÝ[ÜÜ[[\\ÜÛÙ\ÈÝ[Ú[\\ÜÚ[ÛËÛ\]Y\ÈÝ[ØÛXÚÜË[Ø[ÙHÝ[ÜXXÚXYÈÝ[ÛXYËY[ØYÙ[ÈÝ[ÛY\ÜØYÙ\ËÛÛ\ÛÙ\ÈÝ[ØÛÛ\Ú[ÛËÜÝ[ÜÜ[ÈÝ[ÛXYÈYÝ[ÛXYÈ[ÙHÜWÛYY[È
Ý[ÜÜ[ÈÝ[Ú[\\ÜÚ[ÛÈ
L
HYÝ[Ú[\\ÜÚ[ÛÈ[ÙHÝÛYY[È
Ý[ØÛXÚÜÈÈÝ[Ú[\\ÜÚ[ÛÈ
L
HYÝ[Ú[\\ÜÚ[ÛÈ[ÙHÝ\Ý×ÜÜØÛÛ\Ø[ÈÝ[ÜÜ[ÈÝ[ØÛÛ\Ú[ÛÈYÝ[ØÛÛ\Ú[ÛÈ[ÙHKÜ\×Ü]Â[\Ý[Y[È]ÜÜ[XYÈ]ÛXYËY[ØYÙ[È]ÛY\ÜØYÙ\ËÛ\]Y\È]ØÛXÚÜËÛÛ\ÛÙ\È]ÛXYÈ
È]ÛY\ÜØYÙ\ËKØ[\ZYÛÈÛÜY
[ØØ[\ZYÛËÙ^O[[XHÈÜ[K]\ÙOUYJKZ[HÛÜY
Z[WÙ]KÙ^O[[XHÈ]HJKØ[\ZYÛÙZ[HÛÜY
Ø[\ZYÛÙZ[WÜ]ËÙ^O[[XH
È]HKÈØ[\ZYÛÛ[YHJJKBY\WÙ\ÜÎÝ]]ÈY]HVÈ\X[Ù\ÜÈHH\WÙ\ÜÂÈØ[\ÓÓÚ]Ü[ÕUUÑSKÈ[ÛÙ[ÏH]NH\ÈÛÛ[\
Ý]][Ý\WØ\ØÚZOQ[ÙK[[LB[
OOHY]HYÈH\Ý[[ÈOOHB[
\ÝÜXÛÈ\ÙNÒTÕÔWÔÕTHB[
X\ÈHYÜÈX\[ÜÎÛ[Z[WÙ]J_HB[
YÚ\ÝÜÈØ[\[KÙXNÛ[Ø[\ZYÛÙZ[WÜ]Ê_HB[
Ø[\[\ÈY\È]X[Û[[ØØ[\ZYÛÊ_HB[
[\Ý[Y[ÈY\È]X[ÝÝ[ÜÜ[HB[
XYÈY\È]X[ÝÝ[ÛXYßHB[
Y[ØYÙ[ÈY\È]X[ÝÝ[ÛY\ÜØYÙ\ßHBY\WÙ\ÜÎ[
UTÓÈH\ÜÈ\ÚXZ\ÎØ\WÙ\ÜßHB[
\]Z]ÈØ[ÎÓÕUUÑS_HBY×Û[YW×ÈOH×ÛXZ[×ÈXZ[
B
