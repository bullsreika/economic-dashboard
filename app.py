"""
실시간 경제 대시보드 백엔드 (v5 — 바이낸스 선물 연동)
"""

import os
import json
import time
import hmac
import hashlib
import logging
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import feedparser
import requests as req

# ─── 설정 ───────────────────────────────────────────────────
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
NEWS_REFRESH_INTERVAL = 3600
MARKET_CACHE_TTL = 120
TRADES_CACHE_TTL = 300  # 매매기록 5분 캐시

KST = timezone(timedelta(hours=9))
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("dashboard")

# ─── 티커 정의 ──────────────────────────────────────────────
TICKERS = {
    "USD/KRW":        {"symbol": "KRW=X",    "category": "fx"},
    "USD/JPY":        {"symbol": "JPY=X",    "category": "fx"},
    "EUR/USD":        {"symbol": "EURUSD=X", "category": "fx"},
    "DXY":            {"symbol": "DX-Y.NYB", "category": "fx"},
    "Bitcoin":        {"symbol": "BTC-USD",  "category": "crypto"},
    "Ethereum":       {"symbol": "ETH-USD",  "category": "crypto"},
    "S&P 500":        {"symbol": "^GSPC",    "category": "index"},
    "Nasdaq":         {"symbol": "^IXIC",    "category": "index"},
    "Dow Jones":      {"symbol": "^DJI",     "category": "index"},
    "KOSPI":          {"symbol": "^KS11",    "category": "index"},
    "Nasdaq Futures": {"symbol": "NQ=F",     "category": "index"},
    "Gold":           {"symbol": "GC=F",     "category": "commodity"},
    "WTI Oil":        {"symbol": "CL=F",     "category": "commodity"},
    "Silver":         {"symbol": "SI=F",     "category": "commodity"},
    "VIX":            {"symbol": "^VIX",     "category": "volatility"},
    "US 10Y Treasury":{"symbol": "^TNX",     "category": "volatility"},
    "US 2Y Treasury": {"symbol": "^IRX",     "category": "volatility"},
}

market_cache = {"data": None, "ts": 0}
news_cache = {"articles": [], "ts": 0}
trades_cache = {"data": None, "ts": 0}

# ─── Yahoo Finance 직접 호출 ─────────────────────────────────
YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def fetch_via_chart_api(symbol: str) -> dict:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "5d", "interval": "1d"}
    try:
        resp = req.get(url, params=params, headers=YF_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("chart", {}).get("result", [{}])[0]
        meta = result.get("meta", {})
        closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        price = meta.get("regularMarketPrice")
        prev = meta.get("previousClose") or meta.get("chartPreviousClose")
        if not price and closes:
            closes_clean = [c for c in closes if c is not None]
            if closes_clean:
                price = closes_clean[-1]
            if len(closes_clean) >= 2:
                prev = closes_clean[-2]
        if price:
            change_pct = round(((price - prev) / prev) * 100, 2) if prev and prev != 0 else None
            return {"price": round(price, 2), "prev_close": round(prev, 2) if prev else None, "change_pct": change_pct}
    except Exception as e:
        log.warning("Chart API 실패 [%s]: %s", symbol, e)
    return None


def fetch_yahoo_quote(symbols: list) -> dict:
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols), "fields": "regularMarketPrice,regularMarketChange,regularMarketChangePercent,regularMarketPreviousClose"}
    try:
        resp = req.get(url, params=params, headers=YF_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = {}
        for quote in data.get("quoteResponse", {}).get("result", []):
            sym = quote.get("symbol", "")
            results[sym] = {"price": quote.get("regularMarketPrice"), "change_pct": quote.get("regularMarketChangePercent"), "prev_close": quote.get("regularMarketPreviousClose")}
        return results
    except:
        pass
    return {}


def fetch_market_data() -> dict:
    now = time.time()
    if market_cache["data"] and (now - market_cache["ts"]) < MARKET_CACHE_TTL:
        return market_cache["data"]

    symbols = [v["symbol"] for v in TICKERS.values()]
    log.info("시세 조회 시작: %d개 티커", len(symbols))
    quotes = fetch_yahoo_quote(symbols)
    results = {}
    missing = []

    for name, meta in TICKERS.items():
        sym = meta["symbol"]
        if sym in quotes and quotes[sym].get("price"):
            q = quotes[sym]
            results[name] = {"name": name, "category": meta["category"], "price": round(q["price"], 2), "prev_close": round(q["prev_close"], 2) if q.get("prev_close") else None, "change_pct": round(q["change_pct"], 2) if q.get("change_pct") is not None else None, "symbol": sym}
        else:
            missing.append((name, meta))

    for name, meta in missing:
        sym = meta["symbol"]
        chart_data = fetch_via_chart_api(sym)
        if chart_data:
            results[name] = {"name": name, "category": meta["category"], "price": chart_data["price"], "prev_close": chart_data.get("prev_close"), "change_pct": chart_data.get("change_pct"), "symbol": sym}
        else:
            results[name] = {"name": name, "category": meta["category"], "price": None, "prev_close": None, "change_pct": None, "symbol": sym}

    success = sum(1 for v in results.values() if v.get("price") is not None)
    log.info("시세 조회 완료: %d/%d 성공", success, len(TICKERS))
    if success > 0:
        market_cache["data"] = results
        market_cache["ts"] = time.time()
    return results


# ═══════════════════════════════════════════════════════════════
#  바이낸스 선물 매매 기록
# ═══════════════════════════════════════════════════════════════

def binance_signed_request(endpoint: str, params: dict = None) -> dict:
    """바이낸스 서명 요청"""
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return None

    base_url = "https://fapi.binance.com"
    if params is None:
        params = {}
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 10000

    query_string = urlencode(params)
    signature = hmac.new(BINANCE_API_SECRET.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    query_string += f"&signature={signature}"

    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    try:
        resp = req.get(f"{base_url}{endpoint}?{query_string}", headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.error("바이낸스 API 실패 [%s]: %s", endpoint, e)
        return None


def fetch_binance_income(start_time_ms: int) -> list:
    """실현 손익(REALIZED_PNL) 내역을 모두 가져오기 (1000개씩 페이징)"""
    all_income = []
    current_start = start_time_ms

    for _ in range(20):  # 최대 20회 페이징 (20000건)
        params = {
            "incomeType": "REALIZED_PNL",
            "startTime": current_start,
            "limit": 1000,
        }
        data = binance_signed_request("/fapi/v1/income", params)
        if not data or len(data) == 0:
            break

        all_income.extend(data)
        if len(data) < 1000:
            break
        current_start = data[-1]["time"] + 1

    return all_income


def fetch_trades_data() -> dict:
    """바이낸스 선물 매매 기록 조회 및 통계 계산"""
    now = time.time()
    if trades_cache["data"] and (now - trades_cache["ts"]) < TRADES_CACHE_TTL:
        return trades_cache["data"]

    if not BINANCE_API_KEY:
        return {"error": "API key not configured"}

    log.info("바이낸스 매매 기록 조회 시작")

    # 2026년 2월 1일부터
    start_date = datetime(2026, 2, 1, tzinfo=timezone.utc)
    start_ms = int(start_date.timestamp() * 1000)

    income_list = fetch_binance_income(start_ms)
    if income_list is None:
        return {"error": "Failed to fetch data"}

    log.info("바이낸스 매매 기록: %d건", len(income_list))

    # 일별 손익 집계
    daily_pnl = {}
    trades_by_symbol = {}
    total_pnl = 0
    win_count = 0
    loss_count = 0
    total_trades = 0

    for item in income_list:
        pnl = float(item.get("income", 0))
        ts = int(item.get("time", 0))
        symbol = item.get("symbol", "UNKNOWN")
        dt = datetime.fromtimestamp(ts / 1000, tz=KST)
        date_str = dt.strftime("%Y-%m-%d")

        # 일별 집계
        if date_str not in daily_pnl:
            daily_pnl[date_str] = 0
        daily_pnl[date_str] += pnl

        # 종목별 집계
        if symbol not in trades_by_symbol:
            trades_by_symbol[symbol] = {"pnl": 0, "count": 0, "wins": 0}
        trades_by_symbol[symbol]["pnl"] += pnl
        trades_by_symbol[symbol]["count"] += 1
        if pnl > 0:
            trades_by_symbol[symbol]["wins"] += 1

        total_pnl += pnl
        total_trades += 1
        if pnl > 0:
            win_count += 1
        elif pnl < 0:
            loss_count += 1

    # 누적 수익 곡선
    sorted_dates = sorted(daily_pnl.keys())
    cumulative = 0
    equity_curve = []
    for d in sorted_dates:
        cumulative += daily_pnl[d]
        equity_curve.append({"date": d, "daily_pnl": round(daily_pnl[d], 2), "cumulative": round(cumulative, 2)})

    # MDD 계산
    peak = 0
    max_dd = 0
    for point in equity_curve:
        if point["cumulative"] > peak:
            peak = point["cumulative"]
        dd = peak - point["cumulative"]
        if dd > max_dd:
            max_dd = dd

    # 종목별 순위 (손익 기준)
    symbol_ranking = sorted(trades_by_symbol.items(), key=lambda x: x[1]["pnl"], reverse=True)
    top_symbols = [{"symbol": s, "pnl": round(v["pnl"], 2), "count": v["count"], "win_rate": round(v["wins"] / v["count"] * 100, 1) if v["count"] > 0 else 0} for s, v in symbol_ranking[:10]]

    # 최근 거래 (마지막 20건)
    recent = income_list[-20:] if len(income_list) > 20 else income_list
    recent_trades = []
    for item in reversed(recent):
        pnl = float(item.get("income", 0))
        ts = int(item.get("time", 0))
        dt = datetime.fromtimestamp(ts / 1000, tz=KST)
        recent_trades.append({
            "symbol": item.get("symbol", ""),
            "pnl": round(pnl, 2),
            "time": dt.strftime("%m/%d %H:%M"),
        })

    win_rate = round(win_count / (win_count + loss_count) * 100, 1) if (win_count + loss_count) > 0 else 0

    result = {
        "total_pnl": round(total_pnl, 2),
        "total_trades": total_trades,
        "win_rate": win_rate,
        "win_count": win_count,
        "loss_count": loss_count,
        "max_drawdown": round(max_dd, 2),
        "equity_curve": equity_curve,
        "top_symbols": top_symbols,
        "recent_trades": recent_trades,
        "period": f"2026.02.01 ~ {datetime.now(KST).strftime('%Y.%m.%d')}",
    }

    trades_cache["data"] = result
    trades_cache["ts"] = time.time()
    log.info("바이낸스 매매 기록 처리 완료")
    return result


# ═══════════════════════════════════════════════════════════════
#  뉴스
# ═══════════════════════════════════════════════════════════════

RSS_FEEDS = [
    {"name": "Reuters Business", "url": "https://www.rss.app/feeds/v1.1/tsYtWXiMnQNTAM7E.json", "category": "international"},
    {"name": "CNBC Economy", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258", "category": "international"},
    {"name": "Reuters World", "url": "https://www.rss.app/feeds/v1.1/toXBmjkzfJFiCMjr.json", "category": "international"},
    {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "category": "international"},
    {"name": "한경 증권", "url": "https://rss.hankyung.com/stock.xml", "category": "domestic"},
    {"name": "한경 경제", "url": "https://rss.hankyung.com/economy.xml", "category": "domestic"},
    {"name": "매경 증권", "url": "https://file.mk.co.kr/news/rss/rss_50200011.xml", "category": "domestic"},
    {"name": "연합뉴스 경제", "url": "https://www.yonhapnewstv.co.kr/category/news/economy/feed/", "category": "domestic"},
]


def fetch_news_from_rss() -> list:
    articles = []
    for f in RSS_FEEDS:
        try:
            feed = feedparser.parse(f["url"])
            for entry in feed.entries[:8]:
                articles.append({
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", entry.get("description", ""))[:200],
                    "source": f["name"], "category": f["category"],
                    "link": entry.get("link", ""), "published": entry.get("published", ""),
                })
        except Exception as e:
            log.warning("RSS 실패 [%s]: %s", f["name"], e)
    return articles


def refresh_news():
    log.info("뉴스 갱신 시작")
    articles = fetch_news_from_rss()
    intl = []
    domestic = []
    for a in articles:
        if a["category"] == "international":
            intl.append(a)
        else:
            domestic.append(a)
    def dedupe(arts, limit):
        seen, unique = set(), []
        for a in arts:
            key = a["title"].strip().lower()[:50]
            if key and key not in seen:
                seen.add(key)
                unique.append(a)
        return unique[:limit]
    combined = dedupe(intl, 5) + dedupe(domestic, 5)
    news_cache["articles"] = combined
    news_cache["ts"] = time.time()
    log.info("뉴스 갱신 완료: %d건", len(combined))


# ═══════════════════════════════════════════════════════════════
#  FastAPI
# ═══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler()
scheduler.add_job(refresh_news, "interval", seconds=NEWS_REFRESH_INTERVAL, id="news_refresh")


@asynccontextmanager
async def lifespan(app: FastAPI):
    refresh_news()
    try:
        fetch_market_data()
    except Exception as e:
        log.error("시작 시 시세 실패: %s", e)
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Economic Dashboard", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/market")
async def get_market():
    return JSONResponse({"data": fetch_market_data(), "updated_at": datetime.now(KST).isoformat(), "cache_ttl": MARKET_CACHE_TTL})


@app.get("/api/news")
async def get_news():
    return JSONResponse({"articles": news_cache["articles"], "updated_at": datetime.fromtimestamp(news_cache["ts"], tz=KST).isoformat() if news_cache["ts"] else None})


@app.get("/api/feargreed")
async def get_feargreed():
    try:
        resp = req.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        data = resp.json()
        fng = data.get("data", [{}])[0]
        return {"value": int(fng.get("value", 0)), "label": fng.get("value_classification", "")}
    except Exception as e:
        return {"value": None, "label": "N/A", "error": str(e)}


@app.get("/api/trades")
async def get_trades():
    """바이낸스 선물 매매 기록"""
    data = fetch_trades_data()
    return JSONResponse(data)


@app.get("/api/status")
async def get_status():
    return {"status": "running", "binance_configured": bool(BINANCE_API_KEY), "market_cached": market_cache["ts"] > 0, "news_count": len(news_cache["articles"])}


@app.get("/api/debug")
async def debug():
    results = {}
    try:
        c = fetch_via_chart_api("BTC-USD")
        results["chart_api"] = c if c else "no data"
    except Exception as e:
        results["chart_api"] = f"error: {e}"
    if BINANCE_API_KEY:
        try:
            test = binance_signed_request("/fapi/v2/balance")
            results["binance"] = "connected" if test else "failed"
        except Exception as e:
            results["binance"] = f"error: {e}"
    else:
        results["binance"] = "not configured"
    return results


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return HTMLResponse((Path(__file__).parent / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
