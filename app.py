"""
실시간 경제 대시보드 백엔드 (Render 최적화 v3)
- yf.download() 배치 방식으로 안정성 대폭 향상
"""

import os
import json
import time
import logging
import traceback
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
MARKET_CACHE_TTL = 120  # 2분 캐시

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

# 심볼 → 이름 역매핑
SYMBOL_TO_NAME = {v["symbol"]: k for k, v in TICKERS.items()}

# ─── 캐시 ────────────────────────────────────────────────────
market_cache = {"data": None, "ts": 0}
news_cache = {"articles": [], "ts": 0}


# ═══════════════════════════════════════════════════════════════
#  시세 데이터 — yf.download() 배치 방식
# ═══════════════════════════════════════════════════════════════

def fetch_market_data() -> dict:
    now = time.time()
    if market_cache["data"] and (now - market_cache["ts"]) < MARKET_CACHE_TTL:
        return market_cache["data"]

    symbols = [v["symbol"] for v in TICKERS.values()]
    log.info("시세 조회 시작: %d개 티커 (배치 다운로드)", len(symbols))

    results = {}

    try:
        # 배치 다운로드 — 한 번의 요청으로 모든 티커 조회
        df = yf.download(
            tickers=symbols,
            period="5d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        log.info("다운로드 완료, 데이터 파싱 중...")

        for name, meta in TICKERS.items():
            sym = meta["symbol"]
            try:
                # 멀티 티커 결과에서 개별 티커 데이터 추출
                if len(symbols) == 1:
                    ticker_df = df
                else:
                    ticker_df = df[sym] if sym in df.columns.get_level_values(0) else None

                if ticker_df is None or ticker_df.empty:
                    results[name] = _empty(name, meta["category"], sym)
                    continue

                close = ticker_df["Close"].dropna()
                if len(close) < 1:
                    results[name] = _empty(name, meta["category"], sym)
                    continue

                price = float(close.iloc[-1])
                prev = float(close.iloc[-2]) if len(close) >= 2 else None
                change_pct = round(((price - prev) / prev) * 100, 2) if prev and prev != 0 else None

                results[name] = {
                    "name": name,
                    "category": meta["category"],
                    "price": round(price, 2),
                    "prev_close": round(prev, 2) if prev else None,
                    "change_pct": change_pct,
                    "symbol": sym,
                }
            except Exception as e:
                log.warning("티커 %s 파싱 실패: %s", sym, e)
                results[name] = _empty(name, meta["category"], sym)

    except Exception as e:
        log.error("배치 다운로드 실패: %s", e)
        log.error(traceback.format_exc())

        # 배치 실패 시 개별 조회 시도
        log.info("개별 조회로 재시도...")
        for name, meta in TICKERS.items():
            sym = meta["symbol"]
            try:
                t = yf.Ticker(sym)
                hist = t.history(period="5d")
                if not hist.empty and len(hist) >= 1:
                    price = float(hist["Close"].iloc[-1])
                    prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else None
                    change_pct = round(((price - prev) / prev) * 100, 2) if prev and prev != 0 else None
                    results[name] = {
                        "name": name, "category": meta["category"],
                        "price": round(price, 2),
                        "prev_close": round(prev, 2) if prev else None,
                        "change_pct": change_pct, "symbol": sym,
                    }
                else:
                    results[name] = _empty(name, meta["category"], sym)
            except Exception as e2:
                log.warning("개별 조회도 실패 %s: %s", sym, e2)
                results[name] = _empty(name, meta["category"], sym)

    success = sum(1 for v in results.values() if v.get("price") is not None)
    log.info("시세 조회 완료: %d/%d 성공", success, len(TICKERS))

    if success > 0:
        market_cache["data"] = results
        market_cache["ts"] = time.time()

    return results


def _empty(name, category, symbol=""):
    return {"name": name, "category": category, "price": None, "prev_close": None, "change_pct": None, "symbol": symbol}


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
    articles.extend(fetch_news_from_rss())
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    refresh_news()
    # 서버 시작 시 시세 미리 조회
    try:
        fetch_market_data()
    except Exception as e:
        log.error("시작 시 시세 조회 실패: %s", e)
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
    }


@app.get("/api/debug")
async def debug():
    """디버깅용 — 시세 조회 테스트"""
    try:
        df = yf.download("BTC-USD", period="2d", progress=False)
        if df.empty:
            return {"error": "yfinance returned empty data", "tip": "Yahoo Finance might be blocking this server"}
        price = float(df["Close"].iloc[-1])
        return {"status": "ok", "btc_price": price, "rows": len(df)}
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = Path(__file__).parent / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
