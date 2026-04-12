"""
실시간 경제 대시보드 백엔드 (v4 — Yahoo Finance 직접 호출)
yfinance 라이브러리 대신 Yahoo Finance API를 직접 호출합니다.
클라우드 서버(Render 등)에서 yfinance가 차단되는 문제를 우회합니다.
"""

import os
import json
import time
import logging
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import feedparser
import requests as req

# ─── 설정 ───────────────────────────────────────────────────
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
NEWS_REFRESH_INTERVAL = 3600
MARKET_CACHE_TTL = 120

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

# ─── Yahoo Finance 직접 호출 헤더 ────────────────────────────
YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


# ═══════════════════════════════════════════════════════════════
#  시세 데이터 — Yahoo Finance API 직접 호출
# ═══════════════════════════════════════════════════════════════

def fetch_yahoo_quote(symbols: list) -> dict:
    """
    Yahoo Finance v8 quote API 직접 호출
    한 번의 요청으로 여러 티커를 동시 조회
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {
        "symbols": ",".join(symbols),
        "fields": "regularMarketPrice,regularMarketChange,regularMarketChangePercent,regularMarketPreviousClose,shortName",
    }

    try:
        resp = req.get(url, params=params, headers=YF_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = {}
        for quote in data.get("quoteResponse", {}).get("result", []):
            sym = quote.get("symbol", "")
            results[sym] = {
                "price": quote.get("regularMarketPrice"),
                "change": quote.get("regularMarketChange"),
                "change_pct": quote.get("regularMarketChangePercent"),
                "prev_close": quote.get("regularMarketPreviousClose"),
            }
        return results
    except Exception as e:
        log.warning("Yahoo v7 quote 실패: %s", e)

    # v7 실패 시 v6 시도
    try:
        url2 = "https://query2.finance.yahoo.com/v6/finance/quote"
        resp = req.get(url2, params=params, headers=YF_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = {}
        for quote in data.get("quoteResponse", {}).get("result", []):
            sym = quote.get("symbol", "")
            results[sym] = {
                "price": quote.get("regularMarketPrice"),
                "change": quote.get("regularMarketChange"),
                "change_pct": quote.get("regularMarketChangePercent"),
                "prev_close": quote.get("regularMarketPreviousClose"),
            }
        return results
    except Exception as e:
        log.warning("Yahoo v6 quote도 실패: %s", e)

    return {}


def fetch_yahoo_spark(symbols: list) -> dict:
    """
    Yahoo Finance spark API — quote가 차단될 때 백업
    """
    url = "https://query1.finance.yahoo.com/v8/finance/spark"
    params = {
        "symbols": ",".join(symbols),
        "range": "5d",
        "interval": "1d",
    }
    try:
        resp = req.get(url, params=params, headers=YF_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = {}
        for sym, spark_data in data.get("spark", {}).get("result", [{}]):
            closes = spark_data.get("response", [{}])[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
            if len(closes) >= 2:
                price = closes[-1]
                prev = closes[-2]
                if price and prev:
                    results[sym] = {
                        "price": round(price, 2),
                        "prev_close": round(prev, 2),
                        "change_pct": round(((price - prev) / prev) * 100, 2),
                    }
        return results
    except Exception as e:
        log.warning("Yahoo spark도 실패: %s", e)
        return {}


def fetch_via_chart_api(symbol: str) -> dict:
    """
    Yahoo Finance chart API — 개별 티커 조회 (최후의 수단)
    """
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
            return {
                "price": round(price, 2),
                "prev_close": round(prev, 2) if prev else None,
                "change_pct": change_pct,
            }
    except Exception as e:
        log.warning("Chart API 실패 [%s]: %s", symbol, e)
    return None


def fetch_market_data() -> dict:
    now = time.time()
    if market_cache["data"] and (now - market_cache["ts"]) < MARKET_CACHE_TTL:
        return market_cache["data"]

    symbols = [v["symbol"] for v in TICKERS.values()]
    log.info("시세 조회 시작: %d개 티커", len(symbols))

    # 1차: quote API (배치)
    quotes = fetch_yahoo_quote(symbols)

    results = {}
    missing_symbols = []

    for name, meta in TICKERS.items():
        sym = meta["symbol"]
        if sym in quotes and quotes[sym].get("price"):
            q = quotes[sym]
            results[name] = {
                "name": name, "category": meta["category"],
                "price": round(q["price"], 2) if q["price"] else None,
                "prev_close": round(q["prev_close"], 2) if q.get("prev_close") else None,
                "change_pct": round(q["change_pct"], 2) if q.get("change_pct") is not None else None,
                "symbol": sym,
            }
        else:
            missing_symbols.append((name, meta))

    # 2차: 실패한 티커만 chart API로 개별 조회
    for name, meta in missing_symbols:
        sym = meta["symbol"]
        log.info("Chart API 개별 조회: %s", sym)
        chart_data = fetch_via_chart_api(sym)
        if chart_data:
            results[name] = {
                "name": name, "category": meta["category"],
                "price": chart_data["price"],
                "prev_close": chart_data.get("prev_close"),
                "change_pct": chart_data.get("change_pct"),
                "symbol": sym,
            }
        else:
            results[name] = {
                "name": name, "category": meta["category"],
                "price": None, "prev_close": None, "change_pct": None, "symbol": sym,
            }

    success = sum(1 for v in results.values() if v.get("price") is not None)
    log.info("시세 조회 완료: %d/%d 성공", success, len(TICKERS))

    if success > 0:
        market_cache["data"] = results
        market_cache["ts"] = time.time()

    return results


# ═══════════════════════════════════════════════════════════════
#  뉴스
# ═══════════════════════════════════════════════════════════════

RSS_FEEDS = [
    # 국제 (영어) - 증권/선물/암호화폐/지정학
    {"name": "Reuters Business", "url": "https://www.rss.app/feeds/v1.1/tsYtWXiMnQNTAM7E.json", "category": "international"},
    {"name": "CNBC Economy", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258", "category": "international"},
    {"name": "Reuters World", "url": "https://www.rss.app/feeds/v1.1/toXBmjkzfJFiCMjr.json", "category": "international"},
    {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "category": "international"},
    # 국내 (한국어) - 증권/경제
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
            for entry in feed.entries[:5]:
                articles.append({
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", entry.get("description", ""))[:200],
                    "source": f["name"], "category": f["category"],
                    "link": entry.get("link", ""), "published": entry.get("published", ""),
                })
        except Exception as e:
            log.warning("RSS 실패 [%s]: %s", f["name"], e)
    return articles


def fetch_news_from_api() -> list:
    if not NEWS_API_KEY:
        return []
    articles = []
    for query, cat in [("Federal Reserve", "fed"), ("war geopolitics", "geopolitics"), ("stock market", "market"), ("oil gold", "trade")]:
        try:
            resp = req.get("https://newsapi.org/v2/everything",
                params={"q": query, "sortBy": "publishedAt", "pageSize": 5, "language": "en", "apiKey": NEWS_API_KEY}, timeout=10)
            for art in resp.json().get("articles", []):
                articles.append({"title": art.get("title",""), "summary": (art.get("description") or "")[:200],
                    "source": art.get("source",{}).get("name",""), "category": cat,
                    "link": art.get("url",""), "published": art.get("publishedAt","")})
        except Exception as e:
            log.warning("NewsAPI 실패: %s", e)
    return articles


def refresh_news():
    log.info("뉴스 갱신 시작")
    intl_articles = []
    domestic_articles = []
    for f in RSS_FEEDS:
        try:
            feed = feedparser.parse(f["url"])
            for entry in feed.entries[:8]:
                article = {
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", entry.get("description", ""))[:200],
                    "source": f["name"], "category": f["category"],
                    "link": entry.get("link", ""), "published": entry.get("published", ""),
                }
                if f["category"] == "international":
                    intl_articles.append(article)
                else:
                    domestic_articles.append(article)
        except Exception as e:
            log.warning("RSS 실패 [%s]: %s", f["name"], e)
    # 각각 중복 제거 후 5개씩
    def dedupe(articles, limit):
        seen, unique = set(), []
        for a in articles:
            key = a["title"].strip().lower()[:50]
            if key and key not in seen:
                seen.add(key)
                unique.append(a)
        return unique[:limit]
    combined = dedupe(intl_articles, 5) + dedupe(domestic_articles, 5)
    news_cache["articles"] = combined
    news_cache["ts"] = time.time()
    log.info("뉴스 갱신 완료: 국제 %d + 국내 %d", min(5, len(intl_articles)), min(5, len(domestic_articles)))


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

@app.get("/api/feargreed")
async def get_feargreed():
    """크립토 공포탐욕지수"""
    try:
        resp = req.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        data = resp.json()
        fng = data.get("data", [{}])[0]
        return {"value": int(fng.get("value", 0)), "label": fng.get("value_classification", ""), "timestamp": fng.get("timestamp", "")}
    except Exception as e:
        return {"value": None, "label": "N/A", "error": str(e)}

@app.get("/api/market")
async def get_market():
    return JSONResponse({
        "data": fetch_market_data(),
        "updated_at": datetime.now(KST).isoformat(),
        "cache_ttl": MARKET_CACHE_TTL,
    })


@app.get("/api/news")
async def get_news():
    return JSONResponse({
        "articles": news_cache["articles"],
        "updated_at": datetime.fromtimestamp(news_cache["ts"], tz=KST).isoformat() if news_cache["ts"] else None,
    })


@app.get("/api/status")
async def get_status():
    return {"status": "running", "market_cached": market_cache["ts"] > 0,
        "market_last": datetime.fromtimestamp(market_cache["ts"], tz=KST).isoformat() if market_cache["ts"] else None,
        "news_count": len(news_cache["articles"])}


@app.get("/api/debug")
async def debug():
    """디버깅: 각 방법으로 BTC 가격 조회 테스트"""
    results = {}

    # 방법1: quote API
    try:
        q = fetch_yahoo_quote(["BTC-USD"])
        results["quote_api"] = q.get("BTC-USD", "no data")
    except Exception as e:
        results["quote_api"] = f"error: {e}"

    # 방법2: chart API
    try:
        c = fetch_via_chart_api("BTC-USD")
        results["chart_api"] = c if c else "no data"
    except Exception as e:
        results["chart_api"] = f"error: {e}"

    return results


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return HTMLResponse((Path(__file__).parent / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
