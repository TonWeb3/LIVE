import asyncio
import time
from typing import Optional, Callable, Dict, List
import aiohttp
from .config import settings
from .net_utils import get_proxy_url_for
from .indicators import ChainlinkTwapTracker

# ── Low-Level Performance: Fast JSON Acceleration (orjson / std json fallback) ──
try:
    import orjson
    def fast_loads(s):
        if isinstance(s, (bytes, bytearray)):
            return orjson.loads(s)
        return orjson.loads(s.encode("utf-8"))
    def fast_dumps(obj):
        return orjson.dumps(obj).decode("utf-8")
except ImportError:
    import json
    def fast_loads(s):
        return json.loads(s)
    def fast_dumps(obj):
        return json.dumps(obj)


class BinanceTradeStream:
    """Ultra-fast spot + futures leading price feed from Binance.
    Listens to Binance Futures (@bookTicker and @trade) which leads spot price discovery
    by 30ms-80ms, with seamless fallback/redundancy to Binance Spot (@trade)."""
    def __init__(self, symbol: str, on_update: Optional[Callable] = None):
        self.symbol = symbol.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_ts = None
        self.last_source = None
        self.closed = False

    async def _futures_worker(self):
        """Binance USDT-M Futures feed (@bookTicker) — the leading edge signal.
        The mid-price of best bid and ask updates before spot trades print."""
        endpoints = [
            f"wss://fstream.binance.com/ws/{self.symbol}@bookTicker",
            f"wss://fstream.binance.com/ws/{self.symbol}@trade"
        ]
        url = endpoints[0]
        while not self.closed:
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, compress=0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Futures Lead WS: {self.symbol}@bookTicker")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                # @bookTicker payload: {"b": best_bid, "a": best_ask}
                                if "b" in data_msg and "a" in data_msg:
                                    try:
                                        bid = float(data_msg["b"])
                                        ask = float(data_msg["a"])
                                        if bid > 0 and ask > 0:
                                            mid = (bid + ask) / 2.0
                                            self._process_trade(mid, source="binance_futures_book")
                                    except (TypeError, ValueError):
                                        pass
                                elif "p" in data_msg:
                                    try:
                                        p = float(data_msg["p"])
                                        if p > 0:
                                            self._process_trade(p, source="binance_futures_trade")
                                    except (TypeError, ValueError):
                                        pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                if not self.closed:
                    await asyncio.sleep(2)

    async def _spot_worker(self):
        """Binance Spot feed (@trade) — robust fallback and spot anchor."""
        endpoints = [
            f"wss://data-stream.binance.vision/ws/{self.symbol}@trade",
            f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"
        ]
        endpoint_idx = 0
        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, compress=0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Spot WS ({url}): {self.symbol}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                if "p" in data_msg:
                                    try:
                                        p = float(data_msg["p"])
                                        if p > 0:
                                            self._process_trade(p, source="binance_spot_trade")
                                    except (TypeError, ValueError):
                                        pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                if not self.closed:
                    await asyncio.sleep(2)

    async def start(self):
        # Run both Futures (leading edge) and Spot (redundancy) concurrently
        await asyncio.gather(
            self._futures_worker(),
            self._spot_worker(),
            return_exceptions=True
        )

    def _process_trade(self, p: float, source: str = "binance"):
        self.last_price = p
        self.last_ts = time.time()
        self.last_source = source

        if self.on_update:
            try:
                res = self.on_update({"price": self.last_price, "ts": self.last_ts, "source": source})
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception:
                pass

    def get_last(self):
        return {"price": self.last_price, "ts": self.last_ts, "source": self.last_source}

    def close(self):
        self.closed = True


class BinanceKlineStream:
    def __init__(self, symbol: str, interval: str, limit: int = 240):
        self.symbol = symbol.lower()
        self.interval = interval
        self.limit = limit
        self.candles = []
        self.closed = False

    async def start(self):
        endpoints = [
            f"wss://data-stream.binance.vision/ws/{self.symbol}@kline_{self.interval}",
            f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"
        ]
        endpoint_idx = 0

        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, compress=0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Kline WS ({url}): {self.symbol} {self.interval}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                k = data_msg.get("k", {})
                                if k.get("t") is not None:
                                    candle = {
                                        "openTime": int(k.get("t")),
                                        "open": float(k.get("o")),
                                        "high": float(k.get("h")),
                                        "low": float(k.get("l")),
                                        "close": float(k.get("c")),
                                        "volume": float(k.get("v")),
                                        "closeTime": int(k.get("T")),
                                        "isClosed": k.get("x")
                                    }
                                    self._update_candle(candle)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Binance Kline WS failed ({url}): {e}")
                if not self.closed:
                    await asyncio.sleep(5)

    def _update_candle(self, candle: Dict):
        if not self.candles:
            self.candles.append(candle)
        else:
            if candle["openTime"] == self.candles[-1]["openTime"]:
                self.candles[-1] = candle
            else:
                self.candles.append(candle)
        if len(self.candles) > self.limit:
            self.candles.pop(0)

    def set_candles(self, candles: List[Dict]):
        self.candles = candles[-self.limit:]

    def get_candles(self):
        return self.candles

    def close(self):
        self.closed = True


class PolymarketChainlinkStream:
    def __init__(self, ws_url: str, symbol_includes: str = "btc", on_update: Optional[Callable] = None):
        self.ws_url = ws_url
        self.symbol_includes = symbol_includes.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.twap_tracker = ChainlinkTwapTracker(60.0)
        self.closed = False

    async def start(self):
        if not self.ws_url:
            return

        async def ping_loop(ws):
            while not self.closed:
                try:
                    await ws.send_str("PING")
                    await asyncio.sleep(5)
                except:
                    break

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                async with aiohttp.ClientSession(headers=headers) as session:
                    print(f"Connecting to Polymarket WS: {self.ws_url}")
                    async with session.ws_connect(self.ws_url, compress=0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Polymarket WS. Subscribing to topics...")

                        topics = ["crypto_prices_chainlink", "price_chainlink", "settlement_prices"]
                        for t in topics:
                            await ws.send_json({"action": "subscribe", "topic": t})

                        asyncio.create_task(ping_loop(ws))

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_text = msg.data
                                if data_text in ("PONG", "OK", "PONG\n"):
                                    continue

                                try:
                                    data_msg = fast_loads(data_text)
                                except Exception:
                                    continue

                                topic = data_msg.get("topic")
                                if topic not in ("crypto_prices_chainlink", "price_chainlink", "settlement_prices"):
                                    continue

                                payload = data_msg.get("payload", {})
                                if isinstance(payload, str):
                                    try:
                                        payload = fast_loads(payload)
                                    except:
                                        continue

                                updates = payload if isinstance(payload, list) else [payload]

                                for update in updates:
                                    if not isinstance(update, dict): continue

                                    sym = str(update.get("symbol") or update.get("pair") or update.get("ticker") or update.get("asset") or "").lower()
                                    if self.symbol_includes:
                                        target = self.symbol_includes.lower()
                                        if target not in sym and not (target == "btc" and "bitcoin" in sym):
                                            continue

                                    try:
                                        price_val = update.get("price") or update.get("value") or update.get("current")
                                        if price_val is not None:
                                            self.last_price = float(price_val)
                                            ts_val = update.get("timestamp") or update.get("updated_at") or time.time()
                                            updated_at = float(ts_val)
                                            if updated_at < 10000000000: updated_at *= 1000
                                            self.last_updated_at = updated_at
                                            self.twap_tracker.add_tick(self.last_price, self.last_updated_at)

                                            if self.on_update:
                                                res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "polymarket_ws"})
                                                if asyncio.iscoroutine(res):
                                                    asyncio.create_task(res)
                                    except:
                                        continue

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS Error (Polymarket): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_twap(self) -> Optional[float]:
        return self.twap_tracker.get_twap()

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "twap": self.twap_tracker.get_twap(), "source": "polymarket_ws"}

    def close(self):
        self.closed = True


class PolymarketClobMarketStream:
    """Real-time orderbook and price feed for active Polymarket tokens over CLOB Market WS."""
    def __init__(self, ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market", on_update: Optional[Callable] = None):
        self.ws_url = ws_url
        self.on_update = on_update
        self.asset_ids: List[str] = []
        self.books: Dict[str, Dict] = {} # asset_id -> {"bids": [...], "asks": [...], "best_bid": float, "best_ask": float}
        self.last_ts: float = 0
        self.closed = False
        self._ws = None

    def update_assets(self, asset_ids: List[str]):
        new_ids = sorted(list(set(asset_ids)))
        if new_ids != self.asset_ids:
            self.asset_ids = new_ids
            if self._ws and not self._ws.closed and self.asset_ids:
                asyncio.create_task(self._subscribe(self._ws))

    async def _subscribe(self, ws):
        if not self.asset_ids:
            return
        sub_msg = {
            "assets_ids": self.asset_ids,
            "type": "market",
            "custom_feature_enabled": True
        }
        try:
            await ws.send_json(sub_msg)
            print(f"Subscribed to Polymarket CLOB Market WS for tokens: {self.asset_ids}")
        except Exception as e:
            print(f"Failed to subscribe to CLOB market WS: {e}")

    async def start(self):
        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, compress=0, proxy=proxy if proxy else None) as ws:
                        self._ws = ws
                        print(f"Connected to Polymarket CLOB Market WS: {self.ws_url}")
                        await self._subscribe(ws)

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = fast_loads(msg.data)
                                self._process_msg(data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Polymarket CLOB Market WS error: {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def _process_msg(self, data):
        self.last_ts = time.time()

        # 1. Native best_bid_ask event (emitted when custom_feature_enabled: true)
        if isinstance(data, dict) and data.get("event_type") == "best_bid_ask":
            aid = str(data.get("asset_id") or "")
            if aid:
                book = self.books.setdefault(aid, {"bids": [], "asks": [], "best_bid": None, "best_ask": None, "updated_at": self.last_ts})
                book["updated_at"] = self.last_ts
                if data.get("best_bid") is not None:
                    try:
                        val = float(data["best_bid"])
                        if 0.0 < val < 1.0:
                            book["best_bid"] = val
                    except (TypeError, ValueError):
                        pass
                if data.get("best_ask") is not None:
                    try:
                        val = float(data["best_ask"])
                        if 0.0 < val < 1.0:
                            book["best_ask"] = val
                    except (TypeError, ValueError):
                        pass
                if self.on_update:
                    try:
                        res = self.on_update({"source": "clob_book_ws", "asset_id": aid, "ts": self.last_ts})
                        if asyncio.iscoroutine(res):
                            asyncio.create_task(res)
                    except Exception:
                        pass
            return

        # 2. Single book dict snapshot
        if isinstance(data, dict) and (data.get("event_type") in ("book", "market") or "bids" in data or "asks" in data):
            aid = str(data.get("asset_id") or data.get("market") or "")
            if aid:
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                valid_bids = [float(b["price"]) for b in bids if isinstance(b, dict) and b.get("price") is not None and float(b.get("size", 0) or 0) > 0]
                valid_asks = [float(a["price"]) for a in asks if isinstance(a, dict) and a.get("price") is not None and float(a.get("size", 0) or 0) > 0]
                best_bid = max(valid_bids) if valid_bids else None
                best_ask = min(valid_asks) if valid_asks else None
                if best_bid is None and data.get("best_bid") is not None:
                    try:
                        v = float(data["best_bid"])
                        if 0.0 < v < 1.0: best_bid = v
                    except (TypeError, ValueError): pass
                if best_ask is None and data.get("best_ask") is not None:
                    try:
                        v = float(data["best_ask"])
                        if 0.0 < v < 1.0: best_ask = v
                    except (TypeError, ValueError): pass

                self.books[aid] = {
                    "bids": bids,
                    "asks": asks,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "updated_at": self.last_ts
                }

        # 3. Full book snapshot (list of dicts)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "asset_id" in item:
                    aid = str(item["asset_id"])
                    bids = item.get("bids", [])
                    asks = item.get("asks", [])
                    valid_bids = [float(b["price"]) for b in bids if isinstance(b, dict) and b.get("price") is not None and float(b.get("size", 0) or 0) > 0]
                    valid_asks = [float(a["price"]) for a in asks if isinstance(a, dict) and a.get("price") is not None and float(a.get("size", 0) or 0) > 0]
                    best_bid = max(valid_bids) if valid_bids else None
                    best_ask = min(valid_asks) if valid_asks else None
                    if best_bid is None and item.get("best_bid") is not None:
                        try:
                            v = float(item["best_bid"])
                            if 0.0 < v < 1.0: best_bid = v
                        except (TypeError, ValueError): pass
                    if best_ask is None and item.get("best_ask") is not None:
                        try:
                            v = float(item["best_ask"])
                            if 0.0 < v < 1.0: best_ask = v
                        except (TypeError, ValueError): pass

                    self.books[aid] = {
                        "bids": bids,
                        "asks": asks,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "updated_at": self.last_ts
                    }

        # 4. Price / book delta updates
        elif isinstance(data, dict):
            price_changes = data.get("price_changes") or data.get("changes") or []
            if isinstance(price_changes, list):
                for pc in price_changes:
                    if not isinstance(pc, dict): continue
                    aid = str(pc.get("asset_id") or data.get("asset_id") or "")
                    if not aid: continue
                    book = self.books.setdefault(aid, {"bids": [], "asks": [], "best_bid": None, "best_ask": None, "updated_at": self.last_ts})
                    book["updated_at"] = self.last_ts
                    # Use best_bid and best_ask fields directly provided in the delta payload.
                    if pc.get("best_bid") is not None:
                        try:
                            val = float(pc["best_bid"])
                            if 0.0 < val < 1.0:
                                book["best_bid"] = val
                        except (TypeError, ValueError): pass
                    if pc.get("best_ask") is not None:
                        try:
                            val = float(pc["best_ask"])
                            if 0.0 < val < 1.0:
                                book["best_ask"] = val
                        except (TypeError, ValueError): pass

            aid = str(data.get("asset_id") or "")
            if aid:
                book = self.books.setdefault(aid, {"bids": [], "asks": [], "best_bid": None, "best_ask": None, "updated_at": self.last_ts})
                book["updated_at"] = self.last_ts
                if data.get("best_bid") is not None:
                    try:
                        val = float(data["best_bid"])
                        if 0.0 < val < 1.0: book["best_bid"] = val
                    except (TypeError, ValueError): pass
                if data.get("best_ask") is not None:
                    try:
                        val = float(data["best_ask"])
                        if 0.0 < val < 1.0: book["best_ask"] = val
                    except (TypeError, ValueError): pass

        if self.on_update:
            try:
                res = self.on_update({"source": "clob_book_ws", "ts": self.last_ts})
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception:
                pass

    def get_token_market(self, asset_id: str) -> Dict:
        return self.books.get(str(asset_id), {})

    def get_summary(self, asset_id: str, max_age_s: float = 5.0) -> Optional[Dict]:
        """Return best bid/ask, spread, liquidity and bids list for `asset_id` if fresh."""
        aid = str(asset_id)
        book = self.books.get(aid)
        if not book:
            return None
        updated_at = book.get("updated_at", 0)
        if max_age_s > 0 and (time.time() - updated_at) > max_age_s:
            return None
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = book.get("best_bid")
        best_ask = book.get("best_ask")

        if best_bid is None and bids:
            try:
                best_bid = float(bids[0]["price"])
            except:
                pass
        if best_ask is None and asks:
            try:
                best_ask = float(asks[0]["price"])
            except:
                pass

        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        bid_liq = 0.0
        for b in bids[:5]:
            try:
                bid_liq += float(b.get("size", 0))
            except:
                pass

        ask_liq = 0.0
        for a in asks[:5]:
            try:
                ask_liq += float(a.get("size", 0))
            except:
                pass

        return {
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "spread": spread,
            "bidLiquidity": bid_liq,
            "askLiquidity": ask_liq,
            "bids": bids,
            "asks": asks,
            "updated_at": updated_at
        }

    def close(self):
        self.closed = True


class ChainlinkPriceStream:
    def __init__(self, aggregator: str, decimals: int = 8, on_update: Optional[Callable] = None):
        self.aggregator = aggregator
        self.decimals = decimals
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.twap_tracker = ChainlinkTwapTracker(60.0)
        self.closed = False
        self.wss_urls = settings.POLYGON_WSS_URLS + ([settings.POLYGON_WSS_URL] if settings.POLYGON_WSS_URL else [])

    async def start(self):
        if not self.wss_urls or not self.aggregator:
            return

        url_idx = 0
        while not self.closed:
            url = self.wss_urls[url_idx % len(self.wss_urls)]
            url_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, compress=0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Chainlink RPC WS: {url}")
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": [
                                "logs",
                                {
                                    "address": self.aggregator,
                                    "topics": ["0x05598845ccd9c46647361c770d3023029a3514781ca1029c91d84f2913e79435"] # AnswerUpdated topic
                                }
                            ]
                        }
                        await ws.send_json(sub_msg)

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_res = fast_loads(msg.data)
                                if data_res.get("method") == "eth_subscription":
                                    log = data_res.get("params", {}).get("result", {})
                                    topics = log.get("topics", [])
                                    if len(topics) >= 2:
                                        answer = int(topics[1], 16)
                                        if answer >= 2**255:
                                            answer -= 2**256

                                        self.last_price = answer / (10 ** self.decimals)
                                        data_hex = log.get("data", "0x")
                                        if len(data_hex) >= 66:
                                            self.last_updated_at = int(data_hex[2:66], 16) * 1000
                                        else:
                                            self.last_updated_at = int(time.time() * 1000)
                                        self.twap_tracker.add_tick(self.last_price, self.last_updated_at)

                                        if self.on_update:
                                            res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                                            if asyncio.iscoroutine(res):
                                                asyncio.create_task(res)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS Error (Chainlink RPC): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_twap(self) -> Optional[float]:
        return self.twap_tracker.get_twap()

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "twap": self.twap_tracker.get_twap(), "source": "chainlink_ws"}

    def close(self):
        self.closed = True
