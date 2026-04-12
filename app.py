"""
실시간 경제 대시보드 백엔드 (Render 최적화 버전)
- 경제지표: 개별 티커 조회로 안정성 향상
- 주요뉴스: 1시간 단위 캐시 갱신
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import feedparser
import requests

# ─── 설정 ───────────────────────────────────────────────────
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
NEWS_REFRESH_INTERVAL = 3600
MARKET_CACHE_TTL = 60  # 60초 캐시 (서버 부하 감소)

KST = timezone(timedelta(hours=9))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("dashboard")

# ─── 티커 정의 ──────────────────────────────────────────────
TICKERS = {
    "USD/KRW": {"symbol": "KRW=X",   "category": "fx"},
    "USD/JPY": {"symbol": "JPY=X",   "category": "fx"},
    "EUR/USD": {"symbol": "EURUSD=X","category": "fx"},
    "DXY":     {"symbol": "DX-Y.NYB","category": "fx"},
    "Bitcoin":  {"symbol": "BTC-USD", "category": "crypto"},
    "Ethereum": {"symbol": "ETH-USD", "category": "crypto"},
    "S&P 500":       {"symbol": "^GSPC",  "category": "index"},
    "Nasdaq":        {"symbol": "^IXIC",  "category": "index"},
    "Dow Jones":     {"symbol": "^DJI",   "category": "index"},
    "KOSPI":         {"symbol": "^KS11",  "category": "index"},
    "Nasdaq Futures":{"symbol": "NQ=F",   "category": "index"},
    "Gold":    {"symbol": "GC=F",  "category": "commodity"},
    "WTI Oil": {"symbol": "CL=F",  "category": "commodity"},
    "Silver":  {"symbol": "SI=F",  "category": "commodity"},
    "VIX":            {"symbol": "^VIX",   "category": "volatility"},
    "US 10Y Treasury":{"symbol": "^TNX",   "category": "volatility"},
    "US 2Y Treasury": {"symbol": "^IRX",   "category": "volatility"},
}

# ─── 캐시 ────────────────────────────────────────────────────
market_cache = {"data": None, "ts": 0}
news_cache = {"articles": [], "ts": 0}


# ═══════════════════════════════════════════════════════════════
#  시세 데이터 (yfinance) — 개별 조회 방식
# ═══════════════════════════════════════════════════════════════

def fetch_single_ticker(symbol: str) -> dict:
    """개별 티커 1개를 안전하게 조회"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d")

        if hist.empty or len(hist) < 1:
            return None

        price = float(hist["Close"].iloc[-1])
        prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else None

        change_pct = None
        if price and prev and prev != 0:
            change_pct = round(((price - prev) / prev) * 100, 2)

        return {
            "price": round(price, 2),
            "prev_close": round(prev, 2) if prev else None,
            "change_pct": change_pct,
        }
    except Exception as e:
        log.warning("티커 %s 조회 실패: %s", symbol, e)
        return None


def fetch_market_data() -> dict:
    """전체 시세 데이터 조회 (개별 방식)"""
    now = time.time()
    if market_cache["data"] and (now - market_cache["ts"]) < MARKET_CACHE_TTL:
        return market_cache["data"]

    log.info("시세 조회 시작: %d개 티커 (개별 조회)", len(TICKERS))
    results = {}
    success_count = 0

    for name, meta in TICKERS.items():
        data = fetch_single_ticker(meta["symbol"])
        if data:
            results[name] = {
                "name": name,
                "category": meta["category"],
                "price": data["price"],
                "prev_close": data["prev_close"],
                "change_pct": data["change_pct"],
                "symbol": meta["symbol"],
            }
            success_count += 1
        else:
            results[name] = {
                "name": name,
                "category": meta["category"],
                "price": None,
                "prev_close": None,
                "change_pct": None,
                "symbol": meta["symbol"],
            }

    if success_count > 0:
        market_cache["data"] = results
        market_cache["ts"] = time.time()

    log.info("시세 조회 완료: %d/%d 성공", success_count, len(TICKERS))
    return results


def prefetch_market_data():
    """서버 시작 시 백그라운드에서 시세 미리 조회"""
    try:
        fetch_market_data()
    except Exception as e:
        log.error("시세 사전 조회 실패: %s", e)


# ═══════════════════════════════════════════════════════════════
#  뉴스 데이터
# ═══════════════════════════════════════════════════════════════

RSS_FEEDS = [
    {"name": "Reuters Business", "url": "https://www.rss.app/feeds/v1.1/tsYtWXiMnQNTAM7E.json", "category": "market"},
    {"name": "CNBC Economy", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258", "category": "economy"},
    {"name": "Fed News", "url": "https://www.federalreserve.gov/feeds/press_all.xml", "category": "fed"},
    {"name": "Reuters World", "url": "https://www.rss.app/feeds/v1.1/toXBmjkzfJFiCMjr.json", "category": "geopolitics"},
]


def fetch_news_from_rss() -> list:
    articles = []
    for feed_info in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_info["url"])
            for entry in feed.entries[:5]:
                articles.append({
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", entry.get("description", ""))[:200],
                    "source": feed_info["name"],
                    "category": feed_info["category"],
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                })
        except Exception as e:
            log.warning("RSS 피드 실패 [%s]: %s", feed_info["name"], e)
    return articles


def fetch_news_from_api() -> list:
    if not NEWS_API_KEY:
        return []
    articles = []
    queries = [
        ("Federal Reserve interest rate", "fed"),
        ("war conflict geopolitics", "geopolitics"),
        ("stock market economy", "market"),
        ("oil gold commodity", "trade"),
    ]
    for query, category in queries:
        try:
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={"q": query, "sortBy": "publishedAt", "pageSize": 5, "language": "en", "apiKey": NEWS_API_KEY},
                timeout=10,
            )
            data = resp.json()
            for art in data.get("articles", []):
                articles.append({
                    "title": art.get("title", ""),
                    "summary": (art.get("description") or "")[:200],
                    "source": art.get("source", {}).get("name", ""),
                    "category": category,
                    "link": art.get("url", ""),
                    "published": art.get("publishedAt", ""),
                })
        except Exception as e:
            log.warning("NewsAPI 실패 [%s]: %s", query, e)
    return articles


def refresh_news():
    log.info("뉴스 갱신 시작")
    articles = []
    if NEWS_API_KEY:
        articles = fetch_news_from_api()
    rss_articles = fetch_news_from_rss()
    articles.extend(rss_articles)
    seen = set()
    unique = []
    for a in articles:
        key = a["title"].strip().lower()[:50]
        if key and key not in seen:
            seen.add(key)
            unique.append(a)
    news_cache["articles"] = unique[:20]
    news_cache["ts"] = time.time()
    log.info("뉴스 갱신 완료: %d건", len(unique))


# ═══════════════════════════════════════════════════════════════
#  FastAPI 앱
# ═══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler()
scheduler.add_job(refresh_news, "interval", seconds=NEWS_REFRESH_INTERVAL, id="news_refresh")
scheduler.add_job(prefetch_market_data, "interval", seconds=300, id="market_prefetch")  # 5분마다 시세 사전 조회


@asynccontextmanager
async def lifespan(app: FastAPI):
    refresh_news()
    prefetch_market_data()
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Economic Dashboard API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/market")
async def get_market():
    data = fetch_market_data()
    return JSONResponse({
        "data": data,
        "updated_at": datetime.now(KST).isoformat(),
        "cache_ttl": MARKET_CACHE_TTL,
    })


@app.get("/api/news")
async def get_news():
    return JSONResponse({
        "articles": news_cache["articles"],
        "updated_at": datetime.fromtimestamp(news_cache["ts"], tz=KST).isoformat() if news_cache["ts"] else None,
        "next_refresh": NEWS_REFRESH_INTERVAL,
    })


@app.get("/api/status")
async def get_status():
    return {
        "status": "running",
        "market_cached": market_cache["ts"] > 0,
        "market_last_updated": datetime.fromtimestamp(market_cache["ts"], tz=KST).isoformat() if market_cache["ts"] else None,
        "news_count": len(news_cache["articles"]),
        "news_last_updated": datetime.fromtimestamp(news_cache["ts"], tz=KST).isoformat() if news_cache["ts"] else None,
    }


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = Path(__file__).parent / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
