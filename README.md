# Macro Market Dashboard

A 4×3 grid of 12 key markets with technical overlays, market breadth bars, automated commentary, and ratio analysis. Part of the [market-dashboards.com](https://market-dashboards.com) suite.

**Live:** [macro.market-dashboards.com](https://macro.market-dashboards.com)

---

## What It Shows

### Markets Tab (4×3 Grid)

| Row | Markets | Purpose |
|-----|---------|---------|
| **US Equity & Volatility** | SPY, IWM, QQQ, VVIX | US large/small/tech + fear gauge |
| **International & FX** | VGK, EEM, EWJ, DXY | Europe, EM, Japan, US Dollar |
| **Commodities & Rates** | USO, GLD, IEF, TNX | Oil, Gold, Treasuries, 10Y Yield |

Each chart displays 8 months of daily data with:
- **Close price** (dark grey line)
- **Bollinger Bands** (21-day, 2σ) — blue fill with midline
- **63-day EMA** (orange dashed)
- **Volume bars** (green/red by up/down day) with 20-day MA
- **4 flag dots**: 21 DMA, 63 EMA, Volume, Matrix Series
- **Reference lines**: VVIX at 110 ("vomit level"), TNX at 4.5%

**Card borders** reflect flag alignment:
- Dark green = all 4 flags bullish (3 core + volume confirms)
- Light green = 3 core flags bullish
- Dark red = all 4 flags bearish
- Light red = 3 core flags bearish
- Grey = mixed / neutral

### Ratios Tab (3×2 Grid)

| Ratio | What It Measures |
|-------|------------------|
| SPY / TLT | Stocks vs Bonds (risk appetite) |
| SPHB / SPLV | High Beta vs Low Vol (risk-on/off) |
| IWD / IWF | Value vs Growth (style rotation) |
| IWM / MGK | Small Cap vs Mega Cap (size rotation) |
| HYG / TNX | High Yield vs 10Y (credit conditions) |
| XLY / XLU | Discretionary vs Utilities (cyclical vs defensive) |

Each ratio chart shows:
- **Ratio line** (blue) — rising = numerator outperforming
- **Trend line** (grey dashed) — full-period linear regression
- **TREND ▲/▼** label based on regression slope direction

### Market Pulse Commentary

Auto-generated observations (~1500 chars max) including:
- **Calendar alerts**: FOMC and monthly OpEx warnings (2 days ahead)
- **VVIX monitor**: Approaching/above 110 vomit level, vol crush detection
- **SPY volume spikes**: Flags when volume exceeds 1.5x the 20-day average
- **US equity posture**: All above/below 63 EMA assessment
- **International vs Dollar**: DXY strength/weakness impact on Europe & EM
- **Commodity/Dollar correlation**: Gold/Oil vs Dollar interplay
- **Rate/Bond signals**: TNX vs IEF, 4.5% yield threshold
- **Breadth confirmation**: Finviz SMA200 % reading (from shared Redis cache)
- **All green/red flags**: Charts with fully aligned signals (colored text)
- **63 EMA & 21 DMA crossovers**: Detected and highlighted
- **Ratio reads**: Current slope direction for all 6 ratio pairs

### Finviz Breadth Bars

Pulled from the breadth dashboard's shared Redis cache (`finviz_breadth_v1`):
- Advancing / Declining
- New Highs / New Lows
- Above / Below SMA50
- Above / Below SMA200

---

## Technical Stack

- **Backend**: Flask (Python), Gunicorn
- **Data**: Yahoo Finance (yfinance), Finviz breadth (via shared Redis)
- **Cache**: Upstash Redis (shared instance with breadth dashboard)
- **Charts**: Chart.js 4.4.1 + chartjs-plugin-annotation
- **Hosting**: Dokku on DigitalOcean
- **SSL**: Let's Encrypt via Dokku

## Redis Keys

| Key | Contents | TTL |
|-----|----------|-----|
| `macro_dashboard_v3` | All ticker data, ratios, commentary | 25 hours |
| `macro_commentary_history_v1` | Daily commentary + trend changes (90 days) | 95 days |
| `finviz_breadth_v1` | Finviz breadth bars (read-only, written by breadth dashboard) | 25 hours |

## Indicators

- **Bollinger Bands**: 21-day SMA ± 2 standard deviations
- **63 EMA**: Exponential moving average (trend direction)
- **Volume 20 DMA**: 20-day simple moving average of volume
- **Matrix Series**: Ported from Pine Script (wisestocktrader.com) — triple-smoothed EMA momentum oscillator. Bull when `up > down`, Bear when `up < down`.
- **RS Formula context**: The broader suite uses RS Score = (1D×0.10) + (1W×0.20) + (1M×0.30) + (3M×0.40)

## FOMC Dates

Hardcoded in `app.py` — update annually:
- **2025**: Jan 29, Mar 19, May 7, Jun 18, Jul 30, Sep 17, Oct 29, Dec 10
- **2026**: Jan 28, Mar 18, Apr 29, Jun 17, Jul 29, Sep 16, Oct 28, Dec 9

---

## Routes

| Route | Purpose |
|-------|---------|
| `/` | Main dashboard |
| `/refresh` | Trigger full data reload (daily cron target) |
| `/status` | JSON status (phase, ticker count, progress) |
| `/api/data` | Full JSON data (tickers, ratios, commentary) |
| `/api/history` | Commentary history (last 90 days, most recent first) |
| `/redis-test` | Redis connectivity diagnostic |

---

## Deployment

### Prerequisites
- Dokku on DigitalOcean droplet
- Upstash Redis (same instance as breadth dashboard)
- DNS A record for `macro.market-dashboards.com`

### Initial Setup

```bash
dokku apps:create macro-dashboard
dokku domains:add macro-dashboard macro.market-dashboards.com
dokku config:set macro-dashboard \
  UPSTASH_REDIS_REST_URL="your-url" \
  UPSTASH_REDIS_REST_TOKEN="your-token"
dokku git:sync --build macro-dashboard https://TOKEN@github.com/Jamesm2009/macro-dashboard.git main
dokku letsencrypt:enable macro-dashboard
```

### Rebuild After Code Changes

```bash
dokku git:sync --build macro-dashboard https://TOKEN@github.com/Jamesm2009/macro-dashboard.git main
```

### Daily Cron (on the Droplet)

```
CRON_TZ=America/Chicago
30 15 * * 1-5 curl -s https://breadth.market-dashboards.com/refresh
32 15 * * 1-5 curl -s https://breadth.market-dashboards.com/refresh-finviz
35 15 * * 1-5 curl -s https://macro.market-dashboards.com/refresh > /dev/null
```

Breadth refreshes first so Finviz data is fresh when the macro dashboard pulls it.

---

## File Structure

```
macro-dashboard/
├── app.py              # Flask app, data fetching, indicators, commentary
├── templates/
│   └── index.html      # Dashboard UI, Chart.js, tabs
├── requirements.txt    # Python dependencies
├── Procfile            # Gunicorn start command
└── README.md           # This file
```

## GitHub Upload

**Important**: Use drag-and-drop of file *contents* into GitHub's upload area. Do not paste code into the web editor — it corrupts quotes and indentation. Keep `index.html` inside the `templates/` folder.

---

## Related Dashboards

| Dashboard | URL |
|-----------|-----|
| Market Breadth | [breadth.market-dashboards.com](https://breadth.market-dashboards.com) |
| ETF Performance | [etf.market-dashboards.com](https://etf.market-dashboards.com) |
| ETF Volume Flow | [etf-volume.market-dashboards.com](https://etf-volume.market-dashboards.com) |
| Charts | [charts.market-dashboards.com](https://charts.market-dashboards.com) |
