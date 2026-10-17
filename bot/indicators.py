import pandas as pd
import numpy as np
import math
import time
from ta.momentum import RSIIndicator
from typing import List, Optional, Dict


def compute_rsi(closes: List[float], period: int) -> Optional[float]:
    """RSI(period). Used as a veto: don't buy UP into >70 / DOWN into <30."""
    if not closes or len(closes) < period:
        return None
    series = pd.Series(closes)
    rsi = RSIIndicator(close=series, window=period).rsi()
    if rsi.empty:
        return None
    val = rsi.iloc[-1]
    return float(val) if not pd.isna(val) else None


def compute_heiken_ashi(candles: List[Dict]) -> List[Dict]:
    """Heiken-Ashi candles. Used (via count_consecutive) for the exhaustion veto."""
    if not candles:
        return []

    ha = []
    for i in range(len(candles)):
        c = candles[i]
        ha_close = (c["open"] + c["high"] + c["low"] + c["close"]) / 4

        if i > 0:
            prev = ha[i - 1]
            ha_open = (prev["open"] + prev["close"]) / 2
        else:
            ha_open = (c["open"] + c["close"]) / 2

        ha_high = max(c["high"], ha_open, ha_close)
        ha_low = min(c["low"], ha_open, ha_close)

        ha.append({
            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close,
            "isGreen": ha_close >= ha_open,
            "body": abs(ha_close - ha_open)
        })
    return ha


def count_consecutive(ha_candles: List[Dict]) -> Dict:
    """Length of the current same-colour Heiken-Ashi streak (>=6 = exhausted)."""
    if not ha_candles or len(ha_candles) < 2:
        return {"color": None, "count": None}

    last = ha_candles[-1]
    target = "green" if last["isGreen"] else "red"

    count = 0
    for i in range(len(ha_candles) - 1, -1, -1):
        c = ha_candles[i]
        color = "green" if c["isGreen"] else "red"
        if color != target:
            break
        count += 1

    return {"color": target, "count": count}


def realized_drift_vol(candles: List[Dict], lookback: int = 300):
    """(mean, std) of per-candle log returns — the per-step drift & sigma for the
    fair-prob model. Returns (None, None) if there isn't enough data."""
    closes = [c["close"] for c in candles[-lookback:] if c.get("close")]
    if len(closes) < 20:
        return None, None
    arr = np.asarray(closes, dtype=float)
    rets = np.diff(np.log(arr))
    rets = rets[np.isfinite(rets)]
    if len(rets) < 10:
        return None, None
    return float(np.mean(rets)), float(np.std(rets))


def fair_prob_up(current_price: float, strike: float, steps: int,
                 sigma_per_step: Optional[float], drift_per_step: float = 0.0) -> float:
    """Closed-form GBM probability that price closes ABOVE `strike` after `steps`
    5-minute intervals — the core direction/edge model. The model is just
    persistence: "is spot above the open, given the volatility still to come?"
    Returns 0..1.
    """
    if not current_price or not strike or current_price <= 0 or strike <= 0:
        return 0.5
    n = max(1, int(steps))
    if sigma_per_step is None or sigma_per_step <= 0:
        return 1.0 if current_price > strike else 0.0
    mu = (drift_per_step - 0.5 * sigma_per_step ** 2) * n
    sd = sigma_per_step * math.sqrt(n)
    # P(S * exp(X) > K) for X ~ N(mu, sd^2)  ->  1 - Phi(z)  ->  0.5 * erfc(z/sqrt2)
    z = (math.log(strike / current_price) - mu) / sd
    prob = 0.5 * math.erfc(z / math.sqrt(2))
    return float(min(1.0, max(0.0, prob)))


class ChainlinkTwapTracker:
    """O(1) incremental 60-second Chainlink TWAP (time-weighted average price) tracker.
    Used for tracking Polymarket 15m official settlement prices."""
    def __init__(self, window_seconds: float = 60.0):
        self.window_ms = window_seconds * 1000.0
        from collections import deque
        self.ticks = deque()
        self.latest_price = None

    def add_tick(self, price: float, ts_ms: Optional[float] = None) -> Optional[float]:
        if price is None or price <= 0:
            return self.get_twap(ts_ms)
        now_ms = ts_ms if ts_ms is not None else (time.time() * 1000.0)
        self.latest_price = float(price)
        self.ticks.append((now_ms, self.latest_price))
        self._prune(now_ms)
        return self.get_twap(now_ms)

    def _prune(self, now_ms: float):
        cutoff = now_ms - self.window_ms
        while len(self.ticks) > 1 and self.ticks[1][0] < cutoff:
            self.ticks.popleft()

    def get_twap(self, now_ms: Optional[float] = None) -> Optional[float]:
        if not self.ticks:
            return self.latest_price
        cur_ms = now_ms if now_ms is not None else (time.time() * 1000.0)
        self._prune(cur_ms)
        if len(self.ticks) == 1:
            return self.ticks[0][1]

        total_weight = 0.0
        weighted_sum = 0.0
        cutoff = cur_ms - self.window_ms

        for i in range(len(self.ticks) - 1):
            t_start, p = self.ticks[i]
            t_end, _ = self.ticks[i + 1]
            effective_start = max(t_start, cutoff)
            dt = t_end - effective_start
            if dt > 0:
                weighted_sum += p * dt
                total_weight += dt

        last_t, last_p = self.ticks[-1]
        dt_last = max(0.0, cur_ms - max(last_t, cutoff))
        if dt_last > 0:
            weighted_sum += last_p * dt_last
            total_weight += dt_last

        if total_weight > 0:
            return weighted_sum / total_weight
        return self.latest_price or self.ticks[-1][1]
