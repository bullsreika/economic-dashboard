"""
실시간 경제 대시보드 백엔드
- 경제지표: 요청 시마다 실시간 조회 (yfinance)
- 주요뉴스: 1시간 단위 캐시 갱신 (NewsAPI / RSS)
"""

import os
import asyncio
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
# NewsAPI 키 (https://newsapi.org 에서 무료 발급)
# 없어도 RSS 피드로 동작합니다.
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")

# 새로고침 간격 (초)
NEWS_REFRESH_INTERVAL = 3600  # 1시간
MARKET_CACHE_TTL = 30         # 시세 캐시 30초

KST = timezone(timedelta(hours=9))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("dashboard")

# ─── 티커 정의 ──────────────────────────────────────────────
TICKERS = {
    # 환율
    "USD/KRW": {"symbol": "KRW=X",   "category": "fx"},
    "USD/JPY": {"symbol": "JPY=X",   "category": "fx"},
    "EUR/USD": {"symbol": "EURUSD=X","category": "fx"},
    "DXY":     {"symbol": "DX-Y.NYB","category": "fx"},
    # 암호화폐
    "Bitcoin":  {"symbol": "BTC-USD", "category": "crypto"},
    "Ethereum": {"symbol": "ETH-USD", "category": "crypto"},
    # 지수
    "S&P 500":       {"symbol": "^GSPC",  "category": "index"},
    "Nasdaq":        {"symbol": "^IXIC",  "category": "index"},
    "Dow Jones":     {"symbol": "^DJI",   "category": "index"},
    "KOSPI":         {"symbol": "^KS11",  "category": "index"},
    "Nasdaq Futures":{"symbol": "NQ=F",   "category": "index"},
    # 원자재
    "Gold":    {"symbol": "GC=F",  "category": "commodity"},
    "WTI Oil": {"symbol": "CL=F",  "category": "commodity"},
    "Silver":  {"symbol": "SI=F",  "category": "commodity"},
    # 변동성·금리
    "VIX":            {"symbol": "^VIX",   "category": "volatility"},
    "US 10Y Treasury":{"symbol": "^TNX",   "category": "volatility"},
    "US 2Y Treasury": {"symbol": "^IRX",   "category": "volatility"},
}

# ─── 캐시 ────────────────────────────────────────────────────
market_cache = {"data": None, "ts": 0}
news_cache = {"articles": [], "ts": 0}


# ═══════════════════════════════════════════════════════════════
#  시세 데이터 (yfinance)
# ═══════════════════════════════════════════════════════════════

def fetch_market_data() -> dict:
    """yfinance로 전체 티커 시세를 한 번에 조회"""
    now = time.time()
    if market_cache["data"] and (now - market_cache["ts"]) < MARKET_CACHE_TTL:
        return market_cache["data"]

    symbols = [v["symbol"] for v in TICKERS.values()]
    log.info("시세 조회 시작: %d개 티커", len(symbols))

    try:
        tickers = yf.Tickers(" ".join(symbols))
        results = {}

        for name, meta in TICKERS.items():
            sym = meta["symbol"]
            try:
                t = tickers.tickers.get(sym)
                if not t:
                    results[name] = _empty_entry(name, meta["category"])
                    continue

                info = t.fast_info
                price = getattr(info, "last_price", None)
                prev = getattr(info, "previous_close", None)

                if price is None:
                    hist = t.history(period="2d")
                    if len(hist) >= 1:
                        price = hist["Close"].iloc[-1]
                    if len(hist) >= 2:
                        prev = hist["Close"].iloc[-2]

                change_pct = None
                if price and prev and prev != 0:
                    change_pct = ((price - prev) / prev) * 100

                results[name] = {
                    "name": name,
                    "category": meta["category"],
                    "price": round(price, 2) if price else None,
                    "prev_close": round(prev, 2) if prev else None,
                    "change_pct": round(change_pct, 2) if change_pct is not None else None,
                    "symbol": sym,
                }
            except Exception as e:
                log.warning("티커 %s 조회 실패: %s", sym, e)
                results[name] = _empty_entry(name, meta["category"])

        market_cache["data"] = results
        market_cache["ts"] = time.time()
        log.info("시세 조회 완료")
        return results

    except Exception as e:
        log.error("시세 조회 전체 실패: %s", e)
        return market_cache.get("data") or {}


def _empty_entry(name, category):
    return {
        "name": name, "category": category,
        "price": None, "prev_close": None,
        "change_pct": None, "symbol": "",
    }


# ═══════════════════════════════════════════════════════════════
#  뉴스 데이터
# ═══════════════════════════════════════════════════════════════

RSS_FEEDS = [
    {
        "name": "Reuters Business",
        "url": "https://www.rss.app/feeds/v1.1/tsYtWXiMnQNTAM7E.json",
        "category": "market",
    },
    {
        "name": "CNBC Economy",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258",
        "category": "economy",
    },
    {
        "name": "Fed News",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "category": "fed",
    },
    {
        "name": "Reuters World",
        "url": "https://www.rss.app/feeds/v1.1/toXBmjkzfJFiCMjr.json",
        "category": "geopolitics",
    },
]

NEWS_CATEGORIES = {
    "fed": "연준/중앙은행",
    "economy": "경제",
    "market": "시장",
    "geopolitics": "지정학/전쟁",
    "trade": "무역/통상",
    "general": "일반",
}


def fetch_news_from_rss() -> list:
    """RSS 피드에서 뉴스 수집"""
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
    """NewsAPI에서 뉴스 수집 (키가 있는 경우)"""
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
                params={
                    "q": query,
                    "sortBy": "publishedAt",
                    "pageSize": 5,
                    "language": "en",
                    "apiKey": NEWS_API_KEY,
                },
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
    """뉴스 캐시 갱신 (1시간마다 호출)"""
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

# 스케줄러 설정
scheduler = BackgroundScheduler()
scheduler.add_job(refresh_news, "interval", seconds=NEWS_REFRESH_INTERVAL, id="news_refresh")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """서버 시작/종료 시 실행"""
    refresh_news()
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Economic Dashboard API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/market")
async def get_market():
    """실시간 시세 데이터"""
    data = fetch_market_data()
    return JSONResponse({
        "data": data,
        "updated_at": datetime.now(KST).isoformat(),
        "cache_ttl": MARKET_CACHE_TTL,
    })


@app.get("/api/news")
async def get_news():
    """뉴스 데이터 (1시간 캐시)"""
    return JSONResponse({
        "articles": news_cache["articles"],
        "updated_at": datetime.fromtimestamp(
            news_cache["ts"], tz=KST
        ).isoformat() if news_cache["ts"] else None,
        "next_refresh": NEWS_REFRESH_INTERVAL,
    })


@app.get("/api/status")
async def get_status():
    """서버 상태"""
    return {
        "status": "running",
        "market_cached": market_cache["ts"] > 0,
        "news_count": len(news_cache["articles"]),
        "news_last_updated": datetime.fromtimestamp(
            news_cache["ts"], tz=KST
        ).isoformat() if news_cache["ts"] else None,
    }


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """메인 대시보드 페이지"""
    html_path = Path(__file__).parent / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
