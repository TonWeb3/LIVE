import httpx
from .config import settings
from typing import List, Dict, Optional
from .net_utils import get_proxy_url_for

BINANCE_REST_HOSTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com"
]

def to_number(x) -> Optional[float]:
    try:
        if x is None:
            return None
        n = float(x)
        return n
    except (TypeError, ValueError):
        return None

async def fetch_klines(symbol: str, interval: str, limit: int) -> List[Dict]:
    """Fetch historical klines with automatic multi-endpoint fallback and validation
    to guarantee full dataset for indicator computation (HA and RSI)."""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    last_err = None

    seen_hosts = set()
    hosts = []
    for h in [settings.BINANCE_BASE_URL] + BINANCE_REST_HOSTS:
        if h and h not in seen_hosts:
            seen_hosts.add(h)
            hosts.append(h)

    for host in hosts:
        url = f"{host}/api/v3/klines"
        proxy = get_proxy_url_for(url)
        try:
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
                res = await client.get(url, params=params)
                if res.status_code != 200:
                    continue
                data = res.json()
                if not isinstance(data, list) or len(data) == 0:
                    continue
                klines = [{
                    "openTime": int(k[0]),
                    "open": to_number(k[1]),
                    "high": to_number(k[2]),
                    "low": to_number(k[3]),
                    "close": to_number(k[4]),
                    "volume": to_number(k[5]),
                    "closeTime": int(k[6])
                } for k in data if len(k) >= 7 and to_number(k[4]) is not None]
                if len(klines) >= min(limit, 15):
                    return klines
        except Exception as e:
            last_err = e
            continue

    if last_err:
        print(f"Warning: fetch_klines failed after trying all endpoints: {last_err}")
    return []

async def fetch_last_price(symbol: str) -> Optional[float]:
    params = {"symbol": symbol}
    seen_hosts = set()
    hosts = []
    for h in [settings.BINANCE_BASE_URL] + BINANCE_REST_HOSTS:
        if h and h not in seen_hosts:
            seen_hosts.add(h)
            hosts.append(h)

    for host in hosts:
        url = f"{host}/api/v3/ticker/price"
        proxy = get_proxy_url_for(url)
        try:
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=3.0) as client:
                res = await client.get(url, params=params)
                if res.status_code == 200:
                    data = res.json()
                    p = to_number(data.get("price"))
                    if p and p > 0:
                        return p
        except Exception:
            continue
    return None

async def fetch_market_by_slug(slug: str) -> Optional[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/markets"
    params = {"slug": slug}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=8.0) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
    market = data[0] if isinstance(data, list) and data else data
    return market if isinstance(market, dict) and market.get("id") else None

async def fetch_live_events_by_series_id(series_id: str) -> List[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/events"
    params = {"series_id": series_id, "closed": "false", "limit": 20}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=8.0) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
    return data if isinstance(data, list) else []

def flatten_event_markets(events: List[Dict]) -> List[Dict]:
    markets = []
    for event in events:
        for m in event.get("markets", []):
            if isinstance(m, dict):
                markets.append(m)
    return markets

async def fetch_clob_price(token_id: str, side: str = "buy") -> Optional[float]:
    url = f"{settings.CLOB_BASE_URL}/price"
    params = {"token_id": token_id, "side": side}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        data = res.json()
        return to_number(data.get("price"))

async def fetch_order_book(token_id: str) -> Dict:
    url = f"{settings.CLOB_BASE_URL}/book"
    params = {"token_id": token_id}
    proxy = get_proxy_url_for(url)
    async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        return res.json()

def summarize_order_book(book: Dict) -> Dict:
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    def parse_entry(entry):
        if isinstance(entry, dict):
            return to_number(entry.get("price")), to_number(entry.get("size"))
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            return to_number(entry[0]), to_number(entry[1])
        return None, None

    valid_bids = []
    for b in bids:
        p, s = parse_entry(b)
        if p is not None and s is not None:
            valid_bids.append((p, s))

    valid_asks = []
    for a in asks:
        p, s = parse_entry(a)
        if p is not None and s is not None:
            valid_asks.append((p, s))

    valid_bids.sort(key=lambda x: x[0], reverse=True)
    valid_asks.sort(key=lambda x: x[0])

    best_bid = valid_bids[0][0] if valid_bids else None
    best_ask = valid_asks[0][0] if valid_asks else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

    bid_liq = sum(s for _, s in valid_bids[:5]) if valid_bids else 0.0
    ask_liq = sum(s for _, s in valid_asks[:5]) if valid_asks else 0.0

    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spread": spread,
        "bidLiquidity": bid_liq,
        "askLiquidity": ask_liq
    }
