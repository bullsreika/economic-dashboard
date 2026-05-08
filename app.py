"""
실시간 경제 대시보드 백엔드 (v7 — 바이낸스 + 한투 해외선물 연동)
"""

import os, json, time, hmac, hashlib, logging, traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import feedparser, requests as req

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_ACCOUNT_NO = os.environ.get("KIS_ACCOUNT_NO", "")

NEWS_REFRESH_INTERVAL = 3600
MARKET_CACHE_TTL = 120
TRADES_CACHE_TTL = 300
KST = timezone(timedelta(hours=9))
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("dashboard")

TICKERS = {
    "USD/KRW":{"symbol":"KRW=X","category":"fx"},"USD/JPY":{"symbol":"JPY=X","category":"fx"},
    "EUR/USD":{"symbol":"EURUSD=X","category":"fx"},"DXY":{"symbol":"DX-Y.NYB","category":"fx"},
    "Bitcoin":{"symbol":"BTC-USD","category":"crypto"},"Ethereum":{"symbol":"ETH-USD","category":"crypto"},
    "S&P 500":{"symbol":"^GSPC","category":"index"},"Nasdaq":{"symbol":"^IXIC","category":"index"},
    "Dow Jones":{"symbol":"^DJI","category":"index"},"KOSPI":{"symbol":"^KS11","category":"index"},
    "Nasdaq Futures":{"symbol":"NQ=F","category":"index"},
    "Gold":{"symbol":"GC=F","category":"commodity"},"WTI Oil":{"symbol":"CL=F","category":"commodity"},
    "Silver":{"symbol":"SI=F","category":"commodity"},
    "VIX":{"symbol":"^VIX","category":"volatility"},"US 10Y Treasury":{"symbol":"^TNX","category":"volatility"},
    "US 2Y Treasury":{"symbol":"^IRX","category":"volatility"},
}

market_cache = {"data": None, "ts": 0}
news_cache = {"articles": [], "ts": 0}
trades_cache = {"data": None, "ts": 0}
kis_trades_cache = {"data": None, "ts": 0}
kis_token_cache = {"token": None, "expires": 0}

# ═══ Yahoo Finance ═══
YF_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"}

def fetch_via_chart_api(symbol):
    try:
        resp = req.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}", params={"range":"5d","interval":"1d"}, headers=YF_HEADERS, timeout=10)
        resp.raise_for_status()
        result = resp.json().get("chart",{}).get("result",[{}])[0]
        meta = result.get("meta",{}); closes = result.get("indicators",{}).get("quote",[{}])[0].get("close",[])
        price = meta.get("regularMarketPrice"); prev = meta.get("previousClose") or meta.get("chartPreviousClose")
        if not price and closes:
            cc = [c for c in closes if c is not None]
            if cc: price = cc[-1]
            if len(cc)>=2: prev = cc[-2]
        if price:
            cp = round(((price-prev)/prev)*100,2) if prev and prev!=0 else None
            return {"price":round(price,2),"prev_close":round(prev,2) if prev else None,"change_pct":cp}
    except: pass
    return None

def fetch_yahoo_quote(symbols):
    try:
        resp = req.get("https://query1.finance.yahoo.com/v7/finance/quote", params={"symbols":",".join(symbols),"fields":"regularMarketPrice,regularMarketChangePercent,regularMarketPreviousClose"}, headers=YF_HEADERS, timeout=15)
        resp.raise_for_status()
        return {q.get("symbol"):{"price":q.get("regularMarketPrice"),"change_pct":q.get("regularMarketChangePercent"),"prev_close":q.get("regularMarketPreviousClose")} for q in resp.json().get("quoteResponse",{}).get("result",[])}
    except: return {}

def fetch_market_data():
    now = time.time()
    if market_cache["data"] and (now-market_cache["ts"])<MARKET_CACHE_TTL: return market_cache["data"]
    symbols = [v["symbol"] for v in TICKERS.values()]
    log.info("시세 조회 시작: %d개", len(symbols))
    quotes = fetch_yahoo_quote(symbols); results = {}; missing = []
    for name, meta in TICKERS.items():
        sym = meta["symbol"]
        if sym in quotes and quotes[sym].get("price"):
            q = quotes[sym]; results[name] = {"name":name,"category":meta["category"],"price":round(q["price"],2),"prev_close":round(q["prev_close"],2) if q.get("prev_close") else None,"change_pct":round(q["change_pct"],2) if q.get("change_pct") is not None else None,"symbol":sym}
        else: missing.append((name,meta))
    for name, meta in missing:
        sym = meta["symbol"]; cd = fetch_via_chart_api(sym)
        if cd: results[name] = {"name":name,"category":meta["category"],"price":cd["price"],"prev_close":cd.get("prev_close"),"change_pct":cd.get("change_pct"),"symbol":sym}
        else: results[name] = {"name":name,"category":meta["category"],"price":None,"prev_close":None,"change_pct":None,"symbol":sym}
    success = sum(1 for v in results.values() if v.get("price"))
    log.info("시세 완료: %d/%d", success, len(TICKERS))
    if success>0: market_cache["data"]=results; market_cache["ts"]=time.time()
    return results

# ═══ 바이낸스 선물 ═══
def binance_signed_request(endpoint, params=None):
    if not BINANCE_API_KEY or not BINANCE_API_SECRET: return None
    if params is None: params = {}
    params["timestamp"] = int(time.time()*1000); params["recvWindow"] = 10000
    qs = urlencode(params); sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest(); qs += f"&signature={sig}"
    try:
        resp = req.get(f"https://fapi.binance.com{endpoint}?{qs}", headers={"X-MBX-APIKEY":BINANCE_API_KEY}, timeout=15); resp.raise_for_status(); return resp.json()
    except Exception as e: log.error("바이낸스 실패 [%s]: %s", endpoint, e); return None

def fetch_binance_income(start_ms):
    all_inc = []; cur = start_ms
    for _ in range(20):
        data = binance_signed_request("/fapi/v1/income", {"incomeType":"REALIZED_PNL","startTime":cur,"limit":1000})
        if not data or len(data)==0: break
        all_inc.extend(data)
        if len(data)<1000: break
        cur = data[-1]["time"]+1
    return all_inc

def fetch_trades_data():
    now = time.time()
    if trades_cache["data"] and (now-trades_cache["ts"])<TRADES_CACHE_TTL: return trades_cache["data"]
    if not BINANCE_API_KEY: return {"error":"Binance API key not configured"}
    log.info("바이낸스 매매 조회 시작")
    start_ms = int(datetime(2026,3,1,tzinfo=timezone.utc).timestamp()*1000)
    income = fetch_binance_income(start_ms)
    if income is None: return {"error":"Failed to fetch"}
    daily_pnl, by_sym, total_pnl, wins, losses, total = {}, {}, 0, 0, 0, 0
    for item in income:
        pnl = float(item.get("income",0)); ts = int(item.get("time",0)); sym = item.get("symbol","?")
        dt = datetime.fromtimestamp(ts/1000, tz=KST); ds = dt.strftime("%Y-%m-%d")
        daily_pnl[ds] = daily_pnl.get(ds,0)+pnl
        if sym not in by_sym: by_sym[sym]={"pnl":0,"count":0,"wins":0}
        by_sym[sym]["pnl"]+=pnl; by_sym[sym]["count"]+=1
        if pnl>0: by_sym[sym]["wins"]+=1
        total_pnl+=pnl; total+=1
        if pnl>0: wins+=1
        elif pnl<0: losses+=1
    cum=0; eq=[]
    for d in sorted(daily_pnl): cum+=daily_pnl[d]; eq.append({"date":d,"daily_pnl":round(daily_pnl[d],2),"cumulative":round(cum,2)})
    peak=0; mdd=0
    for p in eq:
        if p["cumulative"]>peak: peak=p["cumulative"]
        dd=peak-p["cumulative"]
        if dd>mdd: mdd=dd
    top = sorted(by_sym.items(), key=lambda x:x[1]["pnl"], reverse=True)
    top_sym = [{"symbol":s,"pnl":round(v["pnl"],2),"count":v["count"],"win_rate":round(v["wins"]/v["count"]*100,1) if v["count"]>0 else 0} for s,v in top[:10]]
    recent = income
    rc = [{"symbol":i.get("symbol",""),"pnl":round(float(i.get("income",0)),2),"time":datetime.fromtimestamp(int(i.get("time",0))/1000,tz=KST).strftime("%m/%d %H:%M"),"date":datetime.fromtimestamp(int(i.get("time",0))/1000,tz=KST).strftime("%Y-%m-%d")} for i in reversed(recent)]
    wr = round(wins/(wins+losses)*100,1) if (wins+losses)>0 else 0
    result = {"total_pnl":round(total_pnl,2),"total_trades":total,"win_rate":wr,"win_count":wins,"loss_count":losses,"max_drawdown":round(mdd,2),"equity_curve":eq,"top_symbols":top_sym,"recent_trades":rc,"period":f"2026.03.01 ~ {datetime.now(KST).strftime('%Y.%m.%d')}"}
    trades_cache["data"]=result; trades_cache["ts"]=time.time()
    return result

# ═══ 한국투자증권 해외선물 ═══
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
PRODUCT_NAMES = {"MES":"Micro S&P500", "MNQ":"Micro Nasdaq", "MGC":"Micro Gold", "NQ":"Nasdaq", "ES":"S&P500", "GC":"Gold"}

def kis_get_token():
    now = time.time()
    if kis_token_cache["token"] and now < kis_token_cache["expires"]: return kis_token_cache["token"]
    if not KIS_APP_KEY or not KIS_APP_SECRET: return None
    try:
        resp = req.post(f"{KIS_BASE_URL}/oauth2/tokenP", json={"grant_type":"client_credentials","appkey":KIS_APP_KEY,"appsecret":KIS_APP_SECRET}, timeout=10)
        resp.raise_for_status(); data = resp.json(); token = data.get("access_token")
        kis_token_cache["token"] = token; kis_token_cache["expires"] = now + int(data.get("expires_in", 86400)) - 60
        log.info("KIS 토큰 발급 성공"); return token
    except Exception as e: log.error("KIS 토큰 실패: %s", e); return None

def kis_request(method, endpoint, tr_id, params=None, body=None):
    token = kis_get_token()
    if not token: return None
    headers = {"Content-Type":"application/json; charset=utf-8","authorization":f"Bearer {token}","appkey":KIS_APP_KEY,"appsecret":KIS_APP_SECRET,"tr_id":tr_id,"custtype":"P"}
    try:
        if method == "GET": resp = req.get(f"{KIS_BASE_URL}{endpoint}", headers=headers, params=params, timeout=15)
        else: resp = req.post(f"{KIS_BASE_URL}{endpoint}", headers=headers, json=body, timeout=15)
        resp.raise_for_status(); return resp.json()
    except Exception as e:
        log.error("KIS API 실패 [%s %s]: %s", method, endpoint, e)
        try: log.error("응답: %s", resp.text[:500])
        except: pass
        return None

def fetch_kis_trade_history(cano, acnt_prdt_cd, start_date, end_date):
    """한투 해외선물 체결내역 매칭하여 손익 계산"""
    all_execs = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=30), end_date)
        params = {
            "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd,
            "STRT_DT": current.strftime("%Y%m%d"), "END_DT": chunk_end.strftime("%Y%m%d"),
            "FUOP_DVSN_CD": "01", "FM_PDGR_CD": "", "CRCY_CD": "USD",
            "FM_ITEM_FTNG_YN": "N", "SLL_BUY_DVSN_CD": "%%",
            "CTX_AREA_FK200": "", "CTX_AREA_NK200": "",
        }
        data = kis_request("GET", "/uapi/overseas-futureoption/v1/trading/inquire-daily-ccld", "OTFM3122R", params=params)
        if data and data.get("rt_cd") == "0":
            for t in data.get("output1", []):
                sym = t.get("ovrs_futr_fx_pdno", "")
                if not sym: continue
                base = sym[:-3] if len(sym) > 3 else sym
                display = PRODUCT_NAMES.get(base, base)
                side_cd = t.get("sll_buy_dvsn_cd", "")
                dt_raw = t.get("ccld_dtl_dtime", t.get("ord_dt", ""))
                date_str = f"{dt_raw[:4]}-{dt_raw[4:6]}-{dt_raw[6:8]}" if len(dt_raw) >= 8 else ""
                if len(dt_raw) >= 12:
                    time_str = f"{dt_raw[4:6]}/{dt_raw[6:8]} {dt_raw[8:10]}:{dt_raw[10:12]}"
                else:
                    time_str = f"{dt_raw[4:6]}/{dt_raw[6:8]}" if len(dt_raw) >= 8 else dt_raw
                qty = int(t.get("fm_ccld_qty", 0) or 0)
                amt = float(t.get("fm_futr_ccld_amt", 0) or 0)
                all_execs.append({"symbol": display, "side_cd": side_cd, "qty": qty, "amt": amt, "time": time_str, "date": date_str, "dt_raw": dt_raw})
        current = chunk_end + timedelta(days=1)
        time.sleep(0.3)
    # 종목별 매수/매도 매칭
    all_execs.sort(key=lambda x: x["dt_raw"])
    by_product = {}
    for ex in all_execs:
        by_product.setdefault(ex["symbol"], []).append(ex)
    matched = []
    for product, execs in by_product.items():
        pending = None
        for ex in execs:
            if pending is None:
                pending = ex
            elif pending["side_cd"] != ex["side_cd"]:
                if pending["side_cd"] == "02":  # 매수→매도 (롱)
                    pnl = ex["amt"] - pending["amt"]; direction = "롱"
                else:  # 매도→매수 (숏)
                    pnl = pending["amt"] - ex["amt"]; direction = "숏"
                matched.append({"symbol": product, "direction": direction, "qty": str(pending["qty"]),
                    "pnl": round(pnl, 2), "open_time": pending["time"], "close_time": ex["time"],
                    "time": ex["time"], "date": ex["date"]})
                pending = None
            else:
                pending = ex
    matched.sort(key=lambda x: x.get("date", ""), reverse=True)
    return matched[:100]

def fetch_kis_trades_data():
    """한투 해외선물 기간계좌손익 조회 (OTFM3118R)"""
    now = time.time()
    if kis_trades_cache["data"] and (now - kis_trades_cache["ts"]) < TRADES_CACHE_TTL: return kis_trades_cache["data"]
    if not KIS_APP_KEY or not KIS_ACCOUNT_NO: return {"error": "KIS API not configured"}
    log.info("한투 해외선물 기간손익 조회 시작")
    acct_parts = KIS_ACCOUNT_NO.split("-")
    if len(acct_parts) != 2: return {"error": f"Invalid account format: {KIS_ACCOUNT_NO}"}
    cano = acct_parts[0]; acnt_prdt_cd = acct_parts[1]
    start_date = datetime(2026, 3, 1, tzinfo=KST); end_date = datetime.now(KST)
    equity_curve = []; cumulative = 0; all_symbols = {}
    current = start_date
    while current < end_date:
        week_end = min(current + timedelta(days=30), end_date)
        start_str = current.strftime("%Y%m%d"); end_str = week_end.strftime("%Y%m%d")
        params = {"CANO":cano,"ACNT_PRDT_CD":acnt_prdt_cd,"INQR_TERM_FROM_DT":start_str,"INQR_TERM_TO_DT":end_str,"CRCY_CD":"USD","WHOL_TRSL_YN":"N","FUOP_DVSN":"00","CTX_AREA_FK200":"","CTX_AREA_NK200":""}
        data = kis_request("GET", "/uapi/overseas-futureoption/v1/trading/inquire-period-ccld", "OTFM3118R", params=params)
        if data and data.get("rt_cd") == "0":
            output1 = data.get("output1", []); week_pnl = 0
            for item in output1:
                net = float(item.get("fm_lqd_pfls_amt", 0) or 0); fee = float(item.get("fm_fee", 0) or 0)
                week_pnl += net - abs(fee)
            if week_pnl != 0:
                cumulative += week_pnl
                equity_curve.append({"date": f"{start_str[:4]}-{start_str[4:6]}-{start_str[6:8]}", "daily_pnl": round(week_pnl, 2), "cumulative": round(cumulative, 2)})
            output2 = data.get("output2", [])
            for item in output2:
                sym = item.get("ovrs_futr_fx_pdno", "UNKNOWN")
                if not sym or sym == "UNKNOWN": continue
                base = sym[:-3] if len(sym) > 3 else sym
                display_name = PRODUCT_NAMES.get(base, base)
                net = float(item.get("fm_lqd_pfls_amt", 0) or 0); fee = float(item.get("fm_fee", 0) or 0)
                buy_qty = int(item.get("fm_buy_qty", 0) or 0); sll_qty = int(item.get("fm_sll_qty", 0) or 0)
                if display_name not in all_symbols: all_symbols[display_name] = {"pnl": 0, "trades": 0}
                all_symbols[display_name]["pnl"] += net - abs(fee)
                all_symbols[display_name]["trades"] += buy_qty + sll_qty
        else:
            msg = data.get("msg1","") if data else "no response"
            log.warning("한투 기간손익 %s~%s: %s", start_str, end_str, msg)
        current = week_end + timedelta(days=1); time.sleep(0.3)
    if equity_curve: equity_curve.insert(0, {"date": "2026-03-01", "daily_pnl": 0, "cumulative": 0})
    total_pnl = cumulative
    peak=0; mdd=0
    for p in equity_curve:
        if p["cumulative"]>peak: peak=p["cumulative"]
        dd=peak-p["cumulative"]
        if dd>mdd: mdd=dd
    top = sorted(all_symbols.items(), key=lambda x:x[1]["pnl"], reverse=True)
    top_symbols = [{"symbol":s,"pnl":round(v["pnl"],2),"count":v["trades"],"win_rate":0} for s,v in top[:10]]
    wins = sum(1 for s,v in all_symbols.items() if v["pnl"]>0)
    losses = sum(1 for s,v in all_symbols.items() if v["pnl"]<0)
    total_trades = sum(v["trades"] for v in all_symbols.values())
    wr = round(wins/(wins+losses)*100,1) if (wins+losses)>0 else 0
    result = {"source":"kis","total_pnl":round(total_pnl,2),"total_trades":total_trades,"win_rate":wr,"win_count":wins,"loss_count":losses,"max_drawdown":round(mdd,2),"equity_curve":equity_curve,"top_symbols":top_symbols,
        "recent_trades": fetch_kis_trade_history(cano, acnt_prdt_cd, start_date, end_date),
        "period":f"2026.03.01 ~ {datetime.now(KST).strftime('%Y.%m.%d')}","currency":"USD"}
    kis_trades_cache["data"]=result; kis_trades_cache["ts"]=time.time()
    log.info("한투 기간손익 완료: PnL=%s USD", total_pnl); return result

# ═══ 뉴스 ═══
RSS_FEEDS = [
    {"name":"Reuters Business","url":"https://www.rss.app/feeds/v1.1/tsYtWXiMnQNTAM7E.json","category":"international"},
    {"name":"CNBC Economy","url":"https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258","category":"international"},
    {"name":"Reuters World","url":"https://www.rss.app/feeds/v1.1/toXBmjkzfJFiCMjr.json","category":"international"},
    {"name":"CoinDesk","url":"https://www.coindesk.com/arc/outboundfeeds/rss/","category":"international"},
    {"name":"한경 증권","url":"https://rss.hankyung.com/stock.xml","category":"domestic"},
    {"name":"한경 경제","url":"https://rss.hankyung.com/economy.xml","category":"domestic"},
    {"name":"매경 증권","url":"https://file.mk.co.kr/news/rss/rss_50200011.xml","category":"domestic"},
    {"name":"연합뉴스 경제","url":"https://www.yonhapnewstv.co.kr/category/news/economy/feed/","category":"domestic"},
]

def refresh_news():
    log.info("뉴스 갱신 시작")
    articles = []
    for f in RSS_FEEDS:
        try:
            feed = feedparser.parse(f["url"])
            for e in feed.entries[:8]: articles.append({"title":e.get("title",""),"summary":e.get("summary",e.get("description",""))[:200],"source":f["name"],"category":f["category"],"link":e.get("link",""),"published":e.get("published","")})
        except Exception as e: log.warning("RSS 실패 [%s]: %s",f["name"],e)
    intl, dom = [], []
    for a in articles: (intl if a["category"]=="international" else dom).append(a)
    def dedupe(arts,limit):
        seen,u=set(),[]
        for a in arts:
            k=a["title"].strip().lower()[:50]
            if k and k not in seen: seen.add(k); u.append(a)
        return u[:limit]
    news_cache["articles"]=dedupe(intl,5)+dedupe(dom,5); news_cache["ts"]=time.time()
    log.info("뉴스 완료: %d건",len(news_cache["articles"]))

# ═══ FastAPI ═══
scheduler = BackgroundScheduler()
scheduler.add_job(refresh_news, "interval", seconds=NEWS_REFRESH_INTERVAL, id="news_refresh")

@asynccontextmanager
async def lifespan(app: FastAPI):
    refresh_news()
    try: fetch_market_data()
    except: pass
    scheduler.start(); yield; scheduler.shutdown()

app = FastAPI(title="Economic Dashboard", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/market")
async def get_market(): return JSONResponse({"data":fetch_market_data(),"updated_at":datetime.now(KST).isoformat()})

@app.get("/api/news")
async def get_news(): return JSONResponse({"articles":news_cache["articles"],"updated_at":datetime.fromtimestamp(news_cache["ts"],tz=KST).isoformat() if news_cache["ts"] else None})

@app.get("/api/feargreed")
async def get_feargreed():
    try:
        resp = req.get("https://api.alternative.me/fng/?limit=1", timeout=10); fng = resp.json().get("data",[{}])[0]
        return {"value":int(fng.get("value",0)),"label":fng.get("value_classification","")}
    except Exception as e: return {"value":None,"label":"N/A","error":str(e)}

@app.get("/api/trades")
async def get_trades(): return JSONResponse(fetch_trades_data())

@app.get("/api/kis-trades")
async def get_kis_trades(): return JSONResponse(fetch_kis_trades_data())

@app.get("/api/status")
async def get_status(): return {"status":"running","binance":bool(BINANCE_API_KEY),"kis":bool(KIS_APP_KEY),"kis_account":KIS_ACCOUNT_NO,"market_cached":market_cache["ts"]>0,"news_count":len(news_cache["articles"])}

@app.get("/api/binance-test")
async def binance_test():
    if not BINANCE_API_KEY: return {"error": "no key"}
    start_ms = int(datetime(2026,3,1,tzinfo=timezone.utc).timestamp()*1000)
    result = binance_signed_request("/fapi/v1/income", {"incomeType":"REALIZED_PNL","startTime":start_ms,"limit":5})
    if result is None: return {"error": "API call returned None - check logs"}
    return {"count": len(result), "sample": result[:3] if result else "empty"}

@app.get("/api/myip")
async def get_my_ip():
    try: resp = req.get("https://api.ipify.org?format=json", timeout=5); return resp.json()
    except Exception as e: return {"error": str(e)}

@app.get("/api/debug")
async def debug():
    results = {}
    try: c = fetch_via_chart_api("BTC-USD"); results["chart_api"] = c if c else "no data"
    except Exception as e: results["chart_api"]=f"error: {e}"
    if BINANCE_API_KEY:
        try: test = binance_signed_request("/fapi/v2/balance"); results["binance"]="connected" if test else "failed"
        except: results["binance"]="error"
    return results

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(): return HTMLResponse((Path(__file__).parent / "index.html").read_text(encoding="utf-8"))

if __name__ == "__main__":
    import uvicorn; uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
