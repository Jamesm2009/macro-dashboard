"""
Macro Market Dashboard v3 — yFinance + Upstash Redis
4×3 grid: 12 key markets with Bollinger Bands, 63 EMA, Volume, Matrix Series.
Ratio tab: 6 key ratio charts with trend lines.
Finviz breadth from shared Redis cache. Template-based commentary (~1000 chars).
Daily cron refresh via /refresh after market close.
"""

from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import numpy as np
import requests as _req
import threading
import time
import json
import os
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)
CT = ZoneInfo("America/Chicago")

# ── Config ────────────────────────────────────────────────────────────────────

REDIS_URL    = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN  = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
REDIS_KEY    = "macro_dashboard_v3"
REDIS_KEY_FV = "finviz_breadth_v1"

DISPLAY_DAYS = 170
WARMUP_DAYS  = 100

TICKERS = [
    {"symbol": "SPY",  "name": "S&P 500",          "yf": "SPY",      "prefix": "$", "row": 1, "group": "us"},
    {"symbol": "IWM",  "name": "Russell 2000",      "yf": "IWM",      "prefix": "$", "row": 1, "group": "us"},
    {"symbol": "QQQ",  "name": "NASDAQ-100",         "yf": "QQQ",      "prefix": "$", "row": 1, "group": "us"},
    {"symbol": "VVIX", "name": "VIX of VIX",         "yf": "^VVIX",    "prefix": "",  "row": 1, "group": "vol"},
    {"symbol": "VGK",  "name": "Europe (FTSE)",      "yf": "VGK",      "prefix": "$", "row": 2, "group": "intl"},
    {"symbol": "EEM",  "name": "Emerging Markets",   "yf": "EEM",      "prefix": "$", "row": 2, "group": "intl"},
    {"symbol": "EWJ",  "name": "Japan (MSCI)",       "yf": "EWJ",      "prefix": "$", "row": 2, "group": "intl"},
    {"symbol": "DXY",  "name": "US Dollar Index",    "yf": "DX-Y.NYB", "prefix": "",  "row": 2, "group": "fx",
     "fallback": "UUP", "fb_name": "USD (via UUP)"},
    {"symbol": "USO",  "name": "Crude Oil (WTI)",    "yf": "USO",      "prefix": "$", "row": 3, "group": "com"},
    {"symbol": "GLD",  "name": "Gold",               "yf": "GLD",      "prefix": "$", "row": 3, "group": "com"},
    {"symbol": "IEF",  "name": "7-10Y Treasury",     "yf": "IEF",      "prefix": "$", "row": 3, "group": "bond"},
    {"symbol": "TNX",  "name": "10Y Yield",           "yf": "^TNX",     "prefix": "",  "row": 3, "group": "rate"},
]

# ── Ratio pairs ──────────────────────────────────────────────────────────────

RATIO_PAIRS = [
    {"id": "SPY_TLT",   "name": "SPY / TLT",   "desc": "Stocks vs Bonds",           "num": "SPY",  "den": "TLT",  "num_yf": "SPY",  "den_yf": "TLT"},
    {"id": "SPHB_SPLV", "name": "SPHB / SPLV",  "desc": "High Beta vs Low Vol",      "num": "SPHB", "den": "SPLV", "num_yf": "SPHB", "den_yf": "SPLV"},
    {"id": "IWD_IWF",   "name": "IWD / IWF",    "desc": "Value vs Growth",            "num": "IWD",  "den": "IWF",  "num_yf": "IWD",  "den_yf": "IWF"},
    {"id": "IWM_MGK",   "name": "IWM / MGK",    "desc": "Small Cap vs Mega Cap",      "num": "IWM",  "den": "MGK",  "num_yf": "IWM",  "den_yf": "MGK"},
    {"id": "HYG_TNX",   "name": "HYG / TNX",    "desc": "High Yield vs 10Y Yield",    "num": "HYG",  "den": "TNX",  "num_yf": "HYG",  "den_yf": "^TNX"},
    {"id": "XLY_XLU",   "name": "XLY / XLU",    "desc": "Discretionary vs Utilities",  "num": "XLY",  "den": "XLU",  "num_yf": "XLY",  "den_yf": "XLU"},
]

# All unique yfinance symbols needed for ratios
RATIO_YF_SYMBOLS = list({p["num_yf"] for p in RATIO_PAIRS} | {p["den_yf"] for p in RATIO_PAIRS})

# ── FOMC dates ───────────────────────────────────────────────────────────────

FOMC_DATES = [
    date(2025, 1, 29), date(2025, 3, 19), date(2025, 5, 7),
    date(2025, 6, 18), date(2025, 7, 30), date(2025, 9, 17),
    date(2025, 10, 29), date(2025, 12, 10),
    date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29),
    date(2026, 6, 17), date(2026, 7, 29), date(2026, 9, 16),
    date(2026, 10, 28), date(2026, 12, 9),
]

# ── In-memory cache ──────────────────────────────────────────────────────────

cache = {
    "tickers":      {},
    "ratios":       {},
    "commentary":   "",
    "last_updated": "—",
    "phase":        0,
    "progress":     "Starting...",
    "error":        None,
}
_lock    = threading.Lock()
_started = False


# ── Redis helpers ────────────────────────────────────────────────────────────

def _rget(key):
    if not REDIS_URL or not REDIS_TOKEN:
        return None
    try:
        r = _req.get(f"{REDIS_URL}/get/{key}",
                     headers={"Authorization": f"Bearer {REDIS_TOKEN}"}, timeout=10)
        if r.status_code != 200:
            return None
        result = r.json().get("result")
        return json.loads(result) if result else None
    except Exception as e:
        print(f"  Redis GET error: {e}")
        return None


def _rset(key, value, ex=90000):
    if not REDIS_URL or not REDIS_TOKEN:
        return False
    try:
        r = _req.post(
            f"{REDIS_URL}/pipeline",
            headers={"Authorization": f"Bearer {REDIS_TOKEN}",
                     "Content-Type": "application/json"},
            data=json.dumps([["SET", key, json.dumps(value), "EX", ex]]),
            timeout=15,
        )
        return r.status_code == 200
    except Exception as e:
        print(f"  Redis SET error: {e}")
        return False


# ── Commentary history ───────────────────────────────────────────────────────

REDIS_KEY_HIST = "macro_commentary_history_v1"

def save_commentary_history(commentary, ticker_data):
    history = _rget(REDIS_KEY_HIST) or []
    trend_changes = []
    for sym, td in ticker_data.items():
        fl = td.get("flags", {})
        if fl.get("flipped_above_63ema"):
            trend_changes.append(f"{sym} crossed above 63 EMA")
        if fl.get("flipped_below_63ema"):
            trend_changes.append(f"{sym} crossed below 63 EMA")
        if fl.get("flipped_above_21dma"):
            trend_changes.append(f"{sym} crossed above 21 DMA")
        if fl.get("flipped_below_21dma"):
            trend_changes.append(f"{sym} crossed below 21 DMA")
    today_str = str(date.today())
    entry = {"date": today_str, "commentary": commentary, "trend_changes": trend_changes}
    history = [h for h in history if h["date"] != today_str]
    history.append(entry)
    history = history[-90:]
    _rset(REDIS_KEY_HIST, history, ex=60 * 60 * 24 * 95)
    print(f"  Commentary history saved ({len(history)} days)")


# ── Calendar helpers ─────────────────────────────────────────────────────────

def next_opex():
    today = date.today()
    for m_offset in range(0, 3):
        m = today.month + m_offset
        y = today.year
        if m > 12:
            m -= 12; y += 1
        first = date(y, m, 1)
        first_fri = first + timedelta(days=(4 - first.weekday()) % 7)
        third_fri = first_fri + timedelta(weeks=2)
        if third_fri >= today:
            return third_fri
    return None


def next_fomc():
    today = date.today()
    for d in FOMC_DATES:
        if d >= today:
            return d
    return None


# ── Indicator calculations ───────────────────────────────────────────────────

def calc_bollinger(close, window=21, num_std=2):
    mid = close.rolling(window).mean()
    std = close.rolling(window).std()
    return mid, mid + num_std * std, mid - num_std * std

def calc_ema(close, span=63):
    return close.ewm(span=span, adjust=False).mean()

def calc_vol_ma(volume, window=20):
    return volume.rolling(window).mean()

def calc_matrix_series(high, low, close, n=5):
    ys1 = (high + low + close * 2) / 4
    rk3 = ys1.ewm(span=n, adjust=False).mean()
    rk4 = ys1.rolling(n).std()
    rk5 = ((ys1 - rk3) * 200 / (rk4 + 1e-10)).fillna(0)
    rk6 = rk5.ewm(span=n, adjust=False).mean()
    up   = rk6.ewm(span=n, adjust=False).mean()
    down = up.ewm(span=n, adjust=False).mean()
    return up, down


# ── Fetch helpers ────────────────────────────────────────────────────────────

def _yf_download(yf_sym):
    """Download daily OHLCV from yFinance with enough warmup."""
    total_cal = int((DISPLAY_DAYS + WARMUP_DAYS) * 1.6)
    start = (date.today() - timedelta(days=total_cal)).strftime("%Y-%m-%d")
    end   = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    df = yf.download(yf_sym, start=start, end=end,
                     interval="1d", auto_adjust=True, progress=False)
    if df is not None and not df.empty and isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def fetch_closes(yf_sym):
    """Fetch just close prices for ratio calculations."""
    try:
        df = _yf_download(yf_sym)
        if df is None or df.empty or len(df) < 50:
            return None
        return df["Close"].squeeze().dropna()
    except Exception as e:
        print(f"    ratio fetch ERR {yf_sym}: {e}")
        return None


# ── Process one main ticker ──────────────────────────────────────────────────

def process_ticker(tcfg):
    symbol   = tcfg["symbol"]
    yf_sym   = tcfg["yf"]
    fallback = tcfg.get("fallback")
    name     = tcfg["name"]

    try:
        df = _yf_download(yf_sym)

        if (df is None or df.empty or len(df) < 50) and fallback:
            print(f"    {yf_sym} failed — trying {fallback}")
            df = _yf_download(fallback)
            if df is not None and not df.empty:
                name = tcfg.get("fb_name", name)

        if df is None or df.empty or len(df) < 50:
            return None

        close  = df["Close"].squeeze()
        high   = df["High"].squeeze()
        low    = df["Low"].squeeze()
        volume = df["Volume"].squeeze() if "Volume" in df.columns else pd.Series(0, index=df.index)

        bb_mid, bb_upper, bb_lower = calc_bollinger(close)
        ema63  = calc_ema(close, 63)
        vol_ma = calc_vol_ma(volume, 20)
        ms_up, ms_down = calc_matrix_series(high, low, close)

        n = min(DISPLAY_DAYS, len(close))
        dates_list = [str(d.date()) for d in close.index[-n:]]

        def to_list(s):
            return [round(float(v), 4) if pd.notna(v) else None for v in s.iloc[-n:]]

        has_vol = float(volume.iloc[-n:].sum()) > 0

        lc   = float(close.iloc[-1])
        lbm  = float(bb_mid.iloc[-1])  if pd.notna(bb_mid.iloc[-1])  else None
        le63 = float(ema63.iloc[-1])   if pd.notna(ema63.iloc[-1])   else None
        lvol = float(volume.iloc[-1])  if pd.notna(volume.iloc[-1])  else 0
        lvm  = float(vol_ma.iloc[-1])  if pd.notna(vol_ma.iloc[-1])  else 0
        lmu  = float(ms_up.iloc[-1])   if pd.notna(ms_up.iloc[-1])   else 0
        lmd  = float(ms_down.iloc[-1]) if pd.notna(ms_down.iloc[-1]) else 0

        pc   = float(close.iloc[-2])   if len(close) >= 2 else None
        pbm  = float(bb_mid.iloc[-2])  if len(bb_mid) >= 2  and pd.notna(bb_mid.iloc[-2])  else None
        pe63 = float(ema63.iloc[-2])   if len(ema63) >= 2   and pd.notna(ema63.iloc[-2])   else None

        flags = {
            "above_21dma": lbm is not None and lc > lbm,
            "above_63ema": le63 is not None and lc > le63,
            "vol_up":      has_vol and lvol > lvm,
            "ms_bull":     lmu > lmd,
        }

        if pc is not None and pe63 is not None and le63 is not None:
            flags["flipped_above_63ema"] = (pc <= pe63) and (lc > le63)
            flags["flipped_below_63ema"] = (pc >= pe63) and (lc < le63)
        else:
            flags["flipped_above_63ema"] = False
            flags["flipped_below_63ema"] = False

        if pc is not None and pbm is not None and lbm is not None:
            flags["flipped_above_21dma"] = (pc <= pbm) and (lc > lbm)
            flags["flipped_below_21dma"] = (pc >= pbm) and (lc < lbm)
        else:
            flags["flipped_above_21dma"] = False
            flags["flipped_below_21dma"] = False

        change_1d = round((lc - pc) / pc * 100, 2) if pc and pc != 0 else 0

        core_green = sum([flags["above_21dma"], flags["above_63ema"], flags["ms_bull"]])
        if core_green == 3:
            bg_signal = "strong-bull" if flags.get("vol_up") else "bull"
        elif core_green == 0:
            bg_signal = "strong-bear" if not flags.get("vol_up") else "bear"
        else:
            bg_signal = "neutral"

        ref_lines = []
        if symbol == "VVIX":
            ref_lines.append({"value": 110, "color": "#dc2626", "label": "110 Vomit"})
        elif symbol == "TNX":
            ref_lines.append({"value": 4.5, "color": "#dc2626", "label": "4.5%"})

        vol_spike_ratio = round(lvol / lvm, 1) if (has_vol and lvm > 0) else 0

        return {
            "symbol": symbol, "name": name, "prefix": tcfg["prefix"],
            "row": tcfg["row"], "group": tcfg.get("group", ""),
            "dates": dates_list, "close": to_list(close),
            "bb_upper": to_list(bb_upper), "bb_mid": to_list(bb_mid),
            "bb_lower": to_list(bb_lower), "ema63": to_list(ema63),
            "volume": to_list(volume) if has_vol else [],
            "vol_ma20": to_list(vol_ma) if has_vol else [],
            "has_volume": has_vol, "flags": flags,
            "last_close": round(lc, 2), "change_1d": change_1d,
            "bg_signal": bg_signal, "ref_lines": ref_lines,
            "vol_spike_ratio": vol_spike_ratio,
        }

    except Exception as e:
        print(f"    ERR {symbol}: {e}")
        import traceback; traceback.print_exc()
        return None


# ── Ratio calculations ───────────────────────────────────────────────────────

def calc_rolling_slope(series, window=42):
    """Rolling linear regression slope — positive = uptrend, negative = downtrend."""
    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    x_var = ((x - x_mean) ** 2).sum()
    def _slope(y):
        return ((x - x_mean) * (y - y.mean())).sum() / x_var
    return series.rolling(window, min_periods=window).apply(_slope, raw=True)


def calc_ratio_pair(close_num, close_den, display_days=170):
    """Calculate ratio, 63 EMA, and linear trend for two close series."""
    combined = pd.DataFrame({"num": close_num, "den": close_den}).dropna()
    if len(combined) < 50:
        return None

    ratio = (combined["num"] / combined["den"]).replace([np.inf, -np.inf], np.nan).dropna()
    if len(ratio) < 50:
        return None

    n = min(display_days, len(ratio))
    r_disp = ratio.iloc[-n:]
    dates  = [str(d.date()) for d in r_disp.index]
    values = [round(float(v), 4) for v in r_disp.values]

    # Rolling slope (42-day window) — drives line color
    slopes = calc_rolling_slope(ratio, 42).iloc[-n:]
    slope_positive = [bool(v > 0) if pd.notna(v) else True for v in slopes.values]

    # Current slope direction
    latest_slope = slopes.dropna()
    if len(latest_slope) >= 1:
        current_slope_dir = "up" if latest_slope.iloc[-1] > 0 else "down"
    else:
        current_slope_dir = "flat"

    # Full-period linear regression trend line (reference)
    x = np.arange(n, dtype=float)
    y = np.array(values, dtype=float)
    mask = ~np.isnan(y)
    if mask.sum() >= 2:
        coeffs = np.polyfit(x[mask], y[mask], 1)
        trend = np.polyval(coeffs, x)
        trend_list = [round(float(v), 4) for v in trend]
        trend_dir = "up" if coeffs[0] > 0 else "down"
    else:
        trend_list = [None] * n
        trend_dir = "flat"

    return {
        "dates": dates, "ratio": values,
        "slope_positive": slope_positive,
        "trend": trend_list,
        "current_slope_dir": current_slope_dir,
        "trend_direction": trend_dir,
        "current": values[-1] if values else None,
    }


# ── Commentary generator (~1000 chars, HTML spans) ──────────────────────────

def generate_commentary(td, finviz=None, ratios=None):
    def fg(sym, key):
        return td.get(sym, {}).get("flags", {}).get(key, False)
    def green_count(sym):
        fl = td.get(sym, {}).get("flags", {})
        return sum(1 for k in ("above_21dma", "above_63ema", "vol_up", "ms_bull") if fl.get(k))

    parts = []

    # Calendar alerts
    opex = next_opex()
    if opex:
        days_to = (opex - date.today()).days
        if 0 <= days_to <= 2:
            lbl = "TODAY" if days_to == 0 else f"in {days_to}d"
            parts.append(f'<span class="cmt-alert">OPEX {lbl} ({opex.strftime("%-m/%-d")})</span>')

    fomc = next_fomc()
    if fomc:
        days_to = (fomc - date.today()).days
        if 0 <= days_to <= 2:
            lbl = "TODAY" if days_to == 0 else f"in {days_to}d"
            parts.append(f'<span class="cmt-alert">FOMC {lbl} ({fomc.strftime("%-m/%-d")})</span>')

    # VVIX
    vvix = td.get("VVIX", {})
    vvix_close = vvix.get("last_close", 0)
    if vvix_close >= 110:
        parts.append(f'<span class="cmt-red">VVIX {vvix_close} — ABOVE 110 VOMIT LEVEL</span>')
    elif vvix_close >= 100:
        parts.append(f'<span class="cmt-alert">VVIX {vvix_close} — nearing 110 vomit level</span>')
    if not fg("VVIX", "above_63ema") and vvix_close < 90:
        parts.append("VVIX below 63 EMA — vol crush")

    # SPY volume spike
    spy_ratio = td.get("SPY", {}).get("vol_spike_ratio", 0)
    if spy_ratio >= 2.0:
        parts.append(f'<span class="cmt-alert">SPY volume surge {spy_ratio}x avg</span>')
    elif spy_ratio >= 1.5:
        parts.append(f"SPY volume elevated {spy_ratio}x avg")

    # US Equities
    us = [s for s in ("SPY", "IWM", "QQQ") if s in td]
    us_up = [s for s in us if fg(s, "above_63ema")]
    if len(us_up) == len(us) and us:
        parts.append("US: All above 63 EMA — broad strength")
    elif len(us_up) == 0 and us:
        parts.append("US: All below 63 EMA — broad weakness")
    elif us:
        parts.append(f"US: {'/'.join(us_up)} above, {'/'.join(s for s in us if s not in us_up)} below 63 EMA")

    # Regional vs Dollar
    dxy_up = fg("DXY", "above_63ema")
    vgk_up = fg("VGK", "above_63ema")
    eem_up = fg("EEM", "above_63ema")
    if dxy_up and not vgk_up and not eem_up:
        parts.append("Strong $ pressuring Europe & EM")
    elif not dxy_up and (vgk_up or eem_up):
        rising = "/".join(s for s in ("VGK", "EEM", "EWJ") if fg(s, "above_63ema"))
        if rising:
            parts.append(f"Weak $ lifting {rising}")

    # Dollar / Commodities
    gld_up = fg("GLD", "above_63ema")
    uso_up = fg("USO", "above_63ema")
    if dxy_up and not gld_up and not uso_up:
        parts.append("$ up / commodities down")
    elif not dxy_up and gld_up and uso_up:
        parts.append("$ down / commodities up — inflation signal")
    elif not dxy_up and gld_up and not uso_up:
        parts.append("$ down / gold up — safety bid")

    # Rates / Bonds
    tnx_up = fg("TNX", "above_63ema")
    ief_up = fg("IEF", "above_63ema")
    tnx_close = td.get("TNX", {}).get("last_close", 0)
    if tnx_up and not ief_up:
        parts.append("Yields rising, bonds pressured")
    elif not tnx_up and ief_up:
        parts.append("Yields falling, bonds bid")
    if tnx_close >= 4.5:
        parts.append(f"TNX at {tnx_close}% — above 4.5 threshold")

    # Breadth confirmation
    if finviz:
        sma200_pct = finviz.get("sma200_above_pct")
        if sma200_pct is not None:
            if sma200_pct >= 60:
                parts.append(f"Breadth confirms: {sma200_pct}% above SMA200")
            elif sma200_pct <= 40:
                parts.append(f"Breadth weak: only {sma200_pct}% above SMA200")

    # All 4 flags aligned (colored)
    all_green = [s for s in td if green_count(s) == 4]
    all_red   = [s for s in td if green_count(s) == 0]
    if all_green:
        parts.append(f'<span class="cmt-green">ALL GREEN: {", ".join(all_green[:5])}</span>')
    if all_red:
        parts.append(f'<span class="cmt-red">ALL RED: {", ".join(all_red[:5])}</span>')

    # Flips
    for key, label in [
        ("flipped_above_63ema", "Crossed above 63 EMA"),
        ("flipped_below_63ema", "Crossed below 63 EMA"),
        ("flipped_above_21dma", "Crossed above 21 DMA"),
        ("flipped_below_21dma", "Crossed below 21 DMA"),
    ]:
        flipped = [s for s in td if fg(s, key)]
        if flipped:
            arrow = "\u2191" if "above" in key else "\u2193"
            parts.append(f"{arrow} {label}: {', '.join(flipped)}")

    # Ratio slope reads
    ratio_reads = []
    ratio_data = ratios or {}
    RATIO_LABELS = {
        "SPY_TLT":   ("risk-on", "risk-off / bonds bid"),
        "SPHB_SPLV": ("high-beta leading", "low-vol leading"),
        "IWD_IWF":   ("value over growth", "growth over value"),
        "IWM_MGK":   ("small-cap leading", "mega-cap leading"),
        "HYG_TNX":   ("credit healthy", "credit stress"),
        "XLY_XLU":   ("cyclical strength", "defensive rotation"),
    }
    for rid, (up_lbl, dn_lbl) in RATIO_LABELS.items():
        rd = ratio_data.get(rid)
        if rd:
            d = rd.get("current_slope_dir", "flat")
            if d == "up":
                ratio_reads.append(f"{rd['name']} \u25b2 {up_lbl}")
            elif d == "down":
                ratio_reads.append(f"{rd['name']} \u25bc {dn_lbl}")
    if ratio_reads:
        parts.append('<span class="cmt-ratio">RATIOS: ' + " \u00b7 ".join(ratio_reads) + '</span>')

    return " \u00b7 ".join(parts)[:1500]


# ── Main update routine ─────────────────────────────────────────────────────
def fetch_finviz_headline():
    """Scrape the one-line market summary from Finviz homepage."""
    import re
    try:
        r = _req.get("https://finviz.com",
                     headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                     timeout=10)
        if r.status_code != 200:
            return None
        match = re.search(r'why-stock-moving-init-data[^>]*>(.*?)</script>', r.text, re.DOTALL)
        if match:
            data = json.loads(match.group(1))
            headline = data.get("whyMoving", {}).get("headline")
            return headline
        return None
    except Exception as e:
        print(f"  Finviz headline error: {e}")
        return None
        
def run_update():
    with _lock:
        cache["phase"] = 1
        cache["error"] = None

    try:
        total_main  = len(TICKERS)
        ticker_data = {}

        # Phase 1: Main grid tickers
        for i, tcfg in enumerate(TICKERS):
            sym = tcfg["symbol"]
            with _lock:
                cache["progress"] = f"Loading {i+1}/{total_main}: {sym}"
            print(f"  [{i+1}/{total_main}] {sym}")
            result = process_ticker(tcfg)
            if result:
                ticker_data[sym] = result
                print(f"    OK — {len(result['dates'])} days")
            else:
                print(f"    SKIP")
            time.sleep(0.5)

        # Phase 2: Ratio tickers
        with _lock:
            cache["progress"] = "Loading ratio data..."
        print("  Loading ratio tickers...")

        ratio_closes = {}
        for yf_sym in RATIO_YF_SYMBOLS:
            # Map yf symbol back to display symbol
            display_sym = yf_sym.replace("^", "")
            if display_sym not in ratio_closes:
                print(f"    ratio: {yf_sym}")
                closes = fetch_closes(yf_sym)
                if closes is not None:
                    ratio_closes[yf_sym] = closes
                time.sleep(0.3)

        # Calculate ratio pairs
        ratios = {}
        for pair in RATIO_PAIRS:
            num_c = ratio_closes.get(pair["num_yf"])
            den_c = ratio_closes.get(pair["den_yf"])
            if num_c is not None and den_c is not None:
                rd = calc_ratio_pair(num_c, den_c)
                if rd:
                    ratios[pair["id"]] = {
                        "id": pair["id"], "name": pair["name"],
                        "desc": pair["desc"], **rd,
                    }
                    print(f"    ratio {pair['name']}: OK ({rd['current_slope_dir']})")

        # Finviz headline
        with _lock:
            cache["progress"] = "Fetching Finviz headline..."
        finviz_headline = fetch_finviz_headline()
        print(f"  Finviz headline: {'OK' if finviz_headline else 'SKIP'}")

        # Phase 3: Commentary (with Finviz breadth)
        finviz = _rget(REDIS_KEY_FV)
        commentary = generate_commentary(ticker_data, finviz, ratios)
        save_commentary_history(commentary, ticker_data)

        with _lock:
            cache["tickers"]      = ticker_data
            cache["ratios"]       = ratios
            cache["finviz_headline"] = finviz_headline
            cache["commentary"]   = commentary
            cache["last_updated"] = datetime.now(CT).strftime("%-m/%-d/%y %H:%M CT")
            cache["phase"]        = 4
            cache["progress"]     = "Complete"

        payload = {
            "tickers":      ticker_data,
            "ratios":       ratios,
            "commentary":   commentary,
            "last_updated": cache["last_updated"],
            "finviz_headline": finviz_headline,
        }
        ok = _rset(REDIS_KEY, payload, ex=90000)
        print(f"  Redis save: {'OK' if ok else 'FAILED'} ({len(ticker_data)} tickers, {len(ratios)} ratios)")

    except Exception as e:
        import traceback; traceback.print_exc()
        with _lock:
            cache["error"] = str(e)
            cache["phase"] = 4


def _ensure_started():
    global _started
    if not _started:
        _started = True
        print("  Checking Redis cache...")
        payload = _rget(REDIS_KEY)
        if payload and payload.get("tickers"):
            cache["tickers"]      = payload["tickers"]
            cache["ratios"]       = payload.get("ratios", {})
            cache["commentary"]   = payload.get("commentary", "")
            cache["last_updated"] = payload.get("last_updated", "—")
            cache["phase"]        = 4
            cache["progress"]     = "Loaded from cache"
            print(f"  Redis restored {len(cache['tickers'])} tickers, {len(cache['ratios'])} ratios.")
        else:
            print("  No cache — starting fresh load.")
            threading.Thread(target=run_update, daemon=True).start()


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    _ensure_started()
    with _lock:
        snap = dict(cache)

    finviz     = _rget(REDIS_KEY_FV)
    is_loading = snap["phase"] < 4 or len(snap["tickers"]) == 0

    ordered = []
    for tcfg in TICKERS:
        sym = tcfg["symbol"]
        if sym in snap["tickers"]:
            ordered.append(snap["tickers"][sym])

    ratio_list = list(snap["ratios"].values())

    return render_template("index.html",
        tickers=ordered,
        ticker_json=json.dumps(ordered),
        ratios=ratio_list,
        ratio_json=json.dumps(ratio_list),
        finviz=finviz,
        finviz_headline=snap.get("finviz_headline", ""),
        commentary=snap["commentary"],
        last_updated=snap["last_updated"],
        is_loading=is_loading,
        phase=snap["phase"],
        progress=snap["progress"],
        error=snap["error"],
    )


@app.route("/refresh")
def refresh():
    _ensure_started()
    threading.Thread(target=run_update, daemon=True).start()
    return jsonify({"status": "refresh started — check /status"})


@app.route("/status")
def status():
    _ensure_started()
    with _lock:
        return jsonify({
            "phase": cache["phase"], "tickers": len(cache["tickers"]),
            "ratios": len(cache["ratios"]), "progress": cache["progress"],
            "last_updated": cache["last_updated"], "error": cache["error"],
        })


@app.route("/api/data")
def api_data():
    with _lock:
        return jsonify({"tickers": cache["tickers"], "ratios": cache["ratios"],
                        "commentary": cache["commentary"]})


@app.route("/api/history")
def api_history():
    history = _rget(REDIS_KEY_HIST) or []
    history.reverse()
    return jsonify(history)


@app.route("/redis-test")
def redis_test():
    test_ok = _rset("macro_test", {"ping": "pong"}, ex=60)
    read_back = _rget("macro_test") if test_ok else None
    return jsonify({"write": "OK" if test_ok else "FAILED", "read": read_back,
                    "redis_url_set": bool(REDIS_URL), "redis_token_set": bool(REDIS_TOKEN)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
