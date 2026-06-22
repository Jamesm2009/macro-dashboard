"""
Macro Market Dashboard — yFinance + Upstash Redis
4×3 grid: 12 key markets with Bollinger Bands, 63 EMA, Volume, Matrix Series.
Finviz breadth pulled from shared Redis cache (breadth dashboard).
Template-based commentary (~1000 chars).
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

REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
REDIS_KEY   = "macro_dashboard_v2"
REDIS_KEY_FV = "finviz_breadth_v1"          # shared with breadth dashboard

DISPLAY_DAYS = 170      # ~8 months of trading days
WARMUP_DAYS  = 100      # extra history for EMA / BB warm-up

TICKERS = [
    # Row 1 — US Equity + Volatility
    {"symbol": "SPY",  "name": "S&P 500",          "yf": "SPY",      "prefix": "$", "row": 1, "group": "us"},
    {"symbol": "IWM",  "name": "Russell 2000",      "yf": "IWM",      "prefix": "$", "row": 1, "group": "us"},
    {"symbol": "QQQ",  "name": "NASDAQ-100",         "yf": "QQQ",      "prefix": "$", "row": 1, "group": "us"},
    {"symbol": "VVIX", "name": "VIX of VIX",         "yf": "^VVIX",    "prefix": "",  "row": 1, "group": "vol"},
    # Row 2 — International + Dollar
    {"symbol": "VGK",  "name": "Europe (FTSE)",      "yf": "VGK",      "prefix": "$", "row": 2, "group": "intl"},
    {"symbol": "EEM",  "name": "Emerging Markets",   "yf": "EEM",      "prefix": "$", "row": 2, "group": "intl"},
    {"symbol": "EWJ",  "name": "Japan (MSCI)",       "yf": "EWJ",      "prefix": "$", "row": 2, "group": "intl"},
    {"symbol": "DXY",  "name": "US Dollar Index",    "yf": "DX-Y.NYB", "prefix": "",  "row": 2, "group": "fx",
     "fallback": "UUP", "fb_name": "USD (via UUP)"},
    # Row 3 — Commodities + Rates
    {"symbol": "USO",  "name": "Crude Oil (WTI)",    "yf": "USO",      "prefix": "$", "row": 3, "group": "com"},
    {"symbol": "GLD",  "name": "Gold",               "yf": "GLD",      "prefix": "$", "row": 3, "group": "com"},
    {"symbol": "IEF",  "name": "7-10Y Treasury",     "yf": "IEF",      "prefix": "$", "row": 3, "group": "bond"},
    {"symbol": "TNX",  "name": "10Y Yield",           "yf": "^TNX",     "prefix": "",  "row": 3, "group": "rate"},
]

# ── FOMC meeting decision dates (update annually) ───────────────────────────

FOMC_DATES = [
    # 2025
    date(2025, 1, 29), date(2025, 3, 19), date(2025, 5, 7),
    date(2025, 6, 18), date(2025, 7, 30), date(2025, 9, 17),
    date(2025, 10, 29), date(2025, 12, 10),
    # 2026
    date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29),
    date(2026, 6, 17), date(2026, 7, 29), date(2026, 9, 16),
    date(2026, 10, 28), date(2026, 12, 9),
]

# ── In-memory cache ──────────────────────────────────────────────────────────

cache = {
    "tickers":      {},
    "commentary":   "",
    "last_updated": "—",
    "phase":        0,       # 0=idle, 1=loading, 4=ready
    "progress":     "Starting...",
    "error":        None,
}
_lock    = threading.Lock()
_started = False


# ── Redis helpers (pipeline pattern — avoids double-encoding) ────────────────

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
    """Append today's commentary + trend changes to history (max 90 days)."""
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
    entry = {
        "date": today_str,
        "commentary": commentary,
        "trend_changes": trend_changes,
    }

    history = [h for h in history if h["date"] != today_str]
    history.append(entry)
    history = history[-90:]

    _rset(REDIS_KEY_HIST, history, ex=60 * 60 * 24 * 95)
    print(f"  Commentary history saved ({len(history)} days)")


# ── Calendar helpers ─────────────────────────────────────────────────────────

def next_opex():
    """Return next monthly options expiration (3rd Friday of the month)."""
    today = date.today()
    for m_offset in range(0, 3):
        m = today.month + m_offset
        y = today.year
        if m > 12:
            m -= 12
            y += 1
        first = date(y, m, 1)
        first_fri = first + timedelta(days=(4 - first.weekday()) % 7)
        third_fri = first_fri + timedelta(weeks=2)
        if third_fri >= today:
            return third_fri
    return None


def next_fomc():
    """Return next FOMC decision date."""
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
    """Port of Pine Script Matrix Series — returns (up, down) Series."""
    ys1 = (high + low + close * 2) / 4
    rk3 = ys1.ewm(span=n, adjust=False).mean()
    rk4 = ys1.rolling(n).std()
    rk5 = ((ys1 - rk3) * 200 / (rk4 + 1e-10)).fillna(0)
    rk6 = rk5.ewm(span=n, adjust=False).mean()
    up   = rk6.ewm(span=n, adjust=False).mean()
    down = up.ewm(span=n, adjust=False).mean()
    return up, down


# ── Process one ticker ───────────────────────────────────────────────────────

def process_ticker(tcfg):
    symbol   = tcfg["symbol"]
    yf_sym   = tcfg["yf"]
    fallback = tcfg.get("fallback")
    name     = tcfg["name"]

    total_cal = int((DISPLAY_DAYS + WARMUP_DAYS) * 1.6)
    start = (date.today() - timedelta(days=total_cal)).strftime("%Y-%m-%d")
    end   = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        df = yf.download(yf_sym, start=start, end=end,
                         interval="1d", auto_adjust=True, progress=False)

        if (df is None or df.empty or len(df) < 50) and fallback:
            print(f"    {yf_sym} failed — trying {fallback}")
            df = yf.download(fallback, start=start, end=end,
                             interval="1d", auto_adjust=True, progress=False)
            if df is not None and not df.empty:
                name = tcfg.get("fb_name", name)

        if df is None or df.empty or len(df) < 50:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

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

        # Reference lines for specific tickers
        ref_lines = []
        if symbol == "VVIX":
            ref_lines.append({"value": 110, "color": "#dc2626", "label": "110 Vomit"})
        elif symbol == "TNX":
            ref_lines.append({"value": 4.5, "color": "#dc2626", "label": "4.5%"})

        # Volume spike ratio (for SPY commentary)
        vol_spike_ratio = round(lvol / lvm, 1) if (has_vol and lvm > 0) else 0

        return {
            "symbol":    symbol,
            "name":      name,
            "prefix":    tcfg["prefix"],
            "row":       tcfg["row"],
            "group":     tcfg.get("group", ""),
            "dates":     dates_list,
            "close":     to_list(close),
            "bb_upper":  to_list(bb_upper),
            "bb_mid":    to_list(bb_mid),
            "bb_lower":  to_list(bb_lower),
            "ema63":     to_list(ema63),
            "volume":    to_list(volume)  if has_vol else [],
            "vol_ma20":  to_list(vol_ma)  if has_vol else [],
            "has_volume": has_vol,
            "flags":      flags,
            "last_close": round(lc, 2),
            "change_1d":  change_1d,
            "bg_signal":  bg_signal,
            "ref_lines":  ref_lines,
            "vol_spike_ratio": vol_spike_ratio,
        }

    except Exception as e:
        print(f"    ERR {symbol}: {e}")
        import traceback; traceback.print_exc()
        return None


# ── Commentary generator (~1000 chars, HTML-aware) ───────────────────────────

def generate_commentary(td):
    """Build template-based macro commentary from flag data.
    Uses HTML <span> tags for colored text — template must use |safe."""

    def fg(sym, key):
        return td.get(sym, {}).get("flags", {}).get(key, False)

    def green_count(sym):
        fl = td.get(sym, {}).get("flags", {})
        return sum(1 for k in ("above_21dma", "above_63ema", "vol_up", "ms_bull") if fl.get(k))

    parts = []

    # ── Calendar alerts ──
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

    # ── VVIX special ──
    vvix = td.get("VVIX", {})
    vvix_close = vvix.get("last_close", 0)
    if vvix_close >= 110:
        parts.append(f'<span class="cmt-red">VVIX {vvix_close} — ABOVE 110 VOMIT LEVEL</span>')
    elif vvix_close >= 100:
        parts.append(f'<span class="cmt-alert">VVIX {vvix_close} — approaching 110 vomit level</span>')
    if vvix.get("flags", {}).get("above_63ema") is False and vvix_close < 90:
        parts.append("VVIX below 63 EMA — vol crush")

    # ── SPY volume spike ──
    spy = td.get("SPY", {})
    spy_ratio = spy.get("vol_spike_ratio", 0)
    if spy_ratio >= 2.0:
        parts.append(f'<span class="cmt-alert">SPY volume surge {spy_ratio}x avg</span>')
    elif spy_ratio >= 1.5:
        parts.append(f"SPY volume elevated {spy_ratio}x avg")

    # ── US Equities ──
    us = [s for s in ("SPY", "IWM", "QQQ") if s in td]
    us_up = [s for s in us if fg(s, "above_63ema")]
    if len(us_up) == len(us) and us:
        parts.append("US: All above 63 EMA — broad strength")
    elif len(us_up) == 0 and us:
        parts.append("US: All below 63 EMA — broad weakness")
    elif us:
        parts.append(f"US: {'/'.join(us_up)} above, {'/'.join(s for s in us if s not in us_up)} below 63 EMA")

    # ── Regional vs Dollar ──
    dxy_up = fg("DXY", "above_63ema")
    vgk_up = fg("VGK", "above_63ema")
    eem_up = fg("EEM", "above_63ema")
    if dxy_up and not vgk_up and not eem_up:
        parts.append("Strong $ pressuring Europe & EM")
    elif not dxy_up and (vgk_up or eem_up):
        rising = "/".join(s for s in ("VGK", "EEM", "EWJ") if fg(s, "above_63ema"))
        if rising:
            parts.append(f"Weak $ lifting {rising}")

    # ── Dollar / Commodities ──
    gld_up = fg("GLD", "above_63ema")
    uso_up = fg("USO", "above_63ema")
    if dxy_up and not gld_up and not uso_up:
        parts.append("$ up / commodities down")
    elif not dxy_up and gld_up and uso_up:
        parts.append("$ down / commodities up — inflation signal")
    elif not dxy_up and gld_up and not uso_up:
        parts.append("$ down / gold up — safety bid")

    # ── Rates / Bonds ──
    tnx_up = fg("TNX", "above_63ema")
    ief_up = fg("IEF", "above_63ema")
    tnx_close = td.get("TNX", {}).get("last_close", 0)
    if tnx_up and not ief_up:
        parts.append("Yields rising, bonds pressured")
    elif not tnx_up and ief_up:
        parts.append("Yields falling, bonds bid")
    if tnx_close >= 4.5:
        parts.append(f"TNX at {tnx_close}% — above 4.5 threshold")

    # ── All 4 flags aligned (colored) ──
    all_green = [s for s in td if green_count(s) == 4]
    all_red   = [s for s in td if green_count(s) == 0]
    if all_green:
        names = ", ".join(all_green[:5])
        parts.append(f'<span class="cmt-green">ALL GREEN: {names}</span>')
    if all_red:
        names = ", ".join(all_red[:5])
        parts.append(f'<span class="cmt-red">ALL RED: {names}</span>')

    # ── Flips ──
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

    return " \u00b7 ".join(parts)[:1000]


# ── Main update routine ─────────────────────────────────────────────────────

def run_update():
    with _lock:
        cache["phase"] = 1
        cache["error"] = None

    try:
        total = len(TICKERS)
        ticker_data = {}

        for i, tcfg in enumerate(TICKERS):
            sym = tcfg["symbol"]
            with _lock:
                cache["progress"] = f"Loading {i+1}/{total}: {sym}"
            print(f"  [{i+1}/{total}] {sym}")

            result = process_ticker(tcfg)
            if result:
                ticker_data[sym] = result
                print(f"    OK — {len(result['dates'])} days")
            else:
                print(f"    SKIP")

            time.sleep(0.5)

        commentary = generate_commentary(ticker_data)
        save_commentary_history(commentary, ticker_data)

        with _lock:
            cache["tickers"]      = ticker_data
            cache["commentary"]   = commentary
            cache["last_updated"] = datetime.now(CT).strftime("%-m/%-d/%y %H:%M CT")
            cache["phase"]        = 4
            cache["progress"]     = "Complete"

        payload = {
            "tickers":      ticker_data,
            "commentary":   commentary,
            "last_updated": cache["last_updated"],
        }
        ok = _rset(REDIS_KEY, payload, ex=90000)
        print(f"  Redis save: {'OK' if ok else 'FAILED'} ({len(ticker_data)} tickers)")
        print(f"  Done — {len(ticker_data)} tickers loaded.")

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
            cache["commentary"]   = payload.get("commentary", "")
            cache["last_updated"] = payload.get("last_updated", "—")
            cache["phase"]        = 4
            cache["progress"]     = "Loaded from cache"
            print(f"  Redis restored {len(cache['tickers'])} tickers.")
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

    return render_template("index.html",
        tickers=ordered,
        ticker_json=json.dumps(ordered),
        finviz=finviz,
        commentary=snap["commentary"],
        last_updated=snap["last_updated"],
        is_loading=is_loading,
        phase=snap["phase"],
        progress=snap["progress"],
        error=snap["error"],
    )


@app.route("/refresh")
def refresh():
    """Daily cron endpoint — call after market close (e.g. 3:30 PM CT)."""
    _ensure_started()
    threading.Thread(target=run_update, daemon=True).start()
    return jsonify({"status": "refresh started — check /status"})


@app.route("/status")
def status():
    _ensure_started()
    with _lock:
        return jsonify({
            "phase":        cache["phase"],
            "tickers":      len(cache["tickers"]),
            "progress":     cache["progress"],
            "last_updated": cache["last_updated"],
            "error":        cache["error"],
        })


@app.route("/api/data")
def api_data():
    with _lock:
        return jsonify({
            "tickers":    cache["tickers"],
            "commentary": cache["commentary"],
        })


@app.route("/api/history")
def api_history():
    """Return commentary history (most recent first)."""
    history = _rget(REDIS_KEY_HIST) or []
    history.reverse()
    return jsonify(history)


@app.route("/redis-test")
def redis_test():
    test_ok = _rset("macro_test", {"ping": "pong"}, ex=60)
    read_back = _rget("macro_test") if test_ok else None
    return jsonify({
        "write":          "OK" if test_ok else "FAILED",
        "read":           read_back,
        "redis_url_set":  bool(REDIS_URL),
        "redis_token_set": bool(REDIS_TOKEN),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
