"""
fetch_yields.py
Scrapes current yields for TEY calculator funds and writes data.json.
Run by GitHub Actions on weekdays at market close.
"""

import json, re, time, datetime, requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
}

# ---------------------------------------------------------------------------
# Fund definitions — taxType drives TEY calculation in the browser
# ---------------------------------------------------------------------------
FUND_META = {
    "SWKXX": {"name": "State Fund",            "taxType": "state_muni",    "series": "schwab_investor"},
    "SWTXX": {"name": "National Fund",         "taxType": "national_muni", "series": "schwab_investor"},
    "SNSXX": {"name": "Treasury Fund",         "taxType": "treasury",      "series": "schwab_investor"},
    "SWVXX": {"name": "Taxable (Prime)",        "taxType": "taxable",       "series": "schwab_investor"},
    "SCAXX": {"name": "State Fund (Ultra)",     "taxType": "state_muni",    "series": "schwab_ultra"},
    "SWOXX": {"name": "National Fund (Ultra)",  "taxType": "national_muni", "series": "schwab_ultra"},
    "SUTXX": {"name": "Treasury Fund (Ultra)",  "taxType": "treasury",      "series": "schwab_ultra"},
    "SNAXX": {"name": "Taxable (Ultra)",        "taxType": "taxable",       "series": "schwab_ultra"},
    "CALI":  {"name": "CA Muni ETF",            "taxType": "state_muni",    "series": "ishares"},
    "SUB":   {"name": "National Muni ETF",      "taxType": "national_muni", "series": "ishares"},
    "VGSH":  {"name": "Treasury ETF",           "taxType": "treasury",      "series": "vanguard"},
    "VCSH":  {"name": "Corp Bond ETF",          "taxType": "taxable",       "series": "vanguard"},
}

FALLBACKS = {
    "SWKXX": 0.0177,  "SWTXX": 0.02128, "SNSXX": 0.033856, "SWVXX": 0.034873,
    "SCAXX": 0.019201,"SWOXX": 0.02278, "SUTXX": 0.035356, "SNAXX": 0.036373,
    "CALI":  0.0256,  "SUB":   0.02455, "VGSH":  0.035791, "VCSH":  0.043699,
}

results = {}

# ---------------------------------------------------------------------------
# 1. Schwab MMFs — Yahoo Finance quoteSummary (most reliable public endpoint)
# ---------------------------------------------------------------------------
SCHWAB_TICKERS = ["SWKXX", "SWTXX", "SNSXX", "SWVXX", "SCAXX", "SWOXX", "SUTXX", "SNAXX"]

def fetch_yahoo_yield(ticker):
    """Try Yahoo Finance quoteSummary for 7-day yield (annualized)."""
    url = (
        f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        f"?modules=summaryDetail,defaultKeyStatistics"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        d = r.json()
        result = d.get("quoteSummary", {}).get("result", [{}])[0]
        sd = result.get("summaryDetail", {})
        ks = result.get("defaultKeyStatistics", {})
        # yield field = annualized yield for MMFs
        y = (sd.get("yield") or {}).get("raw") or (ks.get("yield") or {}).get("raw")
        return float(y) if y else None
    except Exception as e:
        print(f"  Yahoo error for {ticker}: {e}")
        return None

print("Fetching Schwab MMF yields via Yahoo Finance...")
for ticker in SCHWAB_TICKERS:
    y = fetch_yahoo_yield(ticker)
    meta = FUND_META[ticker]
    if y and 0.001 < y < 0.15:
        results[ticker] = {**meta, "yield": round(y, 6), "yieldType": "7day", "source": "Yahoo Finance", "live": True}
        print(f"  {ticker}: {y:.4%}  ✓")
    else:
        fb = FALLBACKS[ticker]
        results[ticker] = {**meta, "yield": fb, "yieldType": "7day", "source": "fallback", "live": False}
        print(f"  {ticker}: fallback {fb:.4%}")
    time.sleep(0.3)

# ---------------------------------------------------------------------------
# 2. iShares ETFs — iShares product API (JSON, public)
#    CALI = iShares California Muni Bond ETF
#    SUB  = iShares Short-Term National Muni Bond ETF
# ---------------------------------------------------------------------------
ISHARES_MAP = {
    "CALI": "239453",   # iShares fund ID
    "SUB":  "239837",
}

def fetch_ishares_ytm(ticker, fund_id):
    """Fetch YTM from iShares product data API."""
    url = (
        f"https://www.ishares.com/us/products/{fund_id}/"
        f"ishares-california-muni-bond-etf/1467271812596.ajax"
        f"?tab=portfolio&fileType=json"
    )
    # Generic endpoint pattern that works across iShares products
    generic_url = f"https://www.ishares.com/us/products/{fund_id}/_/1467271812596.ajax?tab=portfolio&fileType=json"
    try:
        r = requests.get(generic_url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        d = r.json()
        # YTM is in portfolioCharacteristics
        chars = d.get("portfolioCharacteristics", {}).get("stats", [])
        for item in chars:
            label = (item.get("label") or "").lower()
            if "yield to maturity" in label or "ytm" in label:
                val = item.get("value")
                if val:
                    # value comes as "4.36%" or 4.36
                    v = str(val).replace("%","").strip()
                    return float(v) / 100
        return None
    except Exception as e:
        print(f"  iShares API error for {ticker}: {e}")
        # Fallback: scrape the product page
        return fetch_ishares_scrape(ticker)

def fetch_ishares_scrape(ticker):
    """Scrape iShares ETF page for YTM as fallback."""
    urls = {
        "CALI": "https://www.ishares.com/us/products/239453/ishares-california-muni-bond-etf",
        "SUB":  "https://www.ishares.com/us/products/239837/ishares-short-term-national-muni-bond-etf",
    }
    try:
        r = requests.get(urls[ticker], headers=HEADERS, timeout=12)
        soup = BeautifulSoup(r.text, "lxml")
        # Look for YTM in fund characteristics table
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                if "yield to maturity" in label:
                    val = cells[1].get_text(strip=True).replace("%","")
                    try:
                        return float(val) / 100
                    except:
                        pass
        # Try JSON embedded in page
        scripts = soup.find_all("script")
        for s in scripts:
            txt = s.get_text()
            m = re.search(r'"yieldToMaturity"\s*:\s*"?([\d.]+)%?"', txt)
            if m:
                return float(m.group(1)) / 100
    except Exception as e:
        print(f"  iShares scrape error for {ticker}: {e}")
    return None

print("\nFetching iShares ETF yields...")
for ticker, fund_id in ISHARES_MAP.items():
    y = fetch_ishares_ytm(ticker, fund_id)
    meta = FUND_META[ticker]
    if y and 0.001 < y < 0.15:
        results[ticker] = {**meta, "yield": round(y, 6), "yieldType": "ytm", "source": "iShares", "live": True}
        print(f"  {ticker}: {y:.4%}  ✓")
    else:
        fb = FALLBACKS[ticker]
        results[ticker] = {**meta, "yield": fb, "yieldType": "ytm", "source": "fallback", "live": False}
        print(f"  {ticker}: fallback {fb:.4%}")
    time.sleep(0.5)

# ---------------------------------------------------------------------------
# 3. Vanguard ETFs — try Yahoo first, then Vanguard's internal API
#    VCSH fund ID = 3145, VGSH fund ID = 3142
# ---------------------------------------------------------------------------
VANGUARD_MAP = {
    "VCSH": {"fund_id": "3145", "ticker": "VCSH"},
    "VGSH": {"fund_id": "3142", "ticker": "VGSH"},
}

def fetch_vanguard_ytm(ticker, fund_id):
    """Try Vanguard's internal fund data API for YTM."""
    # Vanguard exposes portfolio characteristics at this endpoint
    url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/portfolio-data"
    try:
        r = requests.get(url, headers={**HEADERS, "Referer": "https://investor.vanguard.com/"}, timeout=12)
        r.raise_for_status()
        d = r.json()
        # Navigate to yield-to-maturity
        fund_data = d.get("fundData", {}) or d.get("data", {})
        chars = (
            fund_data.get("portfolioCharacteristics")
            or fund_data.get("fixedIncomeCharacteristics")
            or {}
        )
        for key in ["yieldToMaturity", "yield_to_maturity", "ytm"]:
            val = chars.get(key)
            if val:
                v = str(val).replace("%","").strip()
                return float(v) / 100 if float(v) > 1 else float(v)
    except Exception as e:
        print(f"  Vanguard API error for {ticker}: {e}")

    # Fallback: Yahoo Finance
    return fetch_yahoo_yield(ticker)

print("\nFetching Vanguard ETF yields...")
for ticker, info in VANGUARD_MAP.items():
    y = fetch_vanguard_ytm(ticker, info["fund_id"])
    meta = FUND_META[ticker]
    if y and 0.001 < y < 0.15:
        src = "Vanguard" if y != fetch_yahoo_yield(ticker) else "Yahoo Finance"
        results[ticker] = {**meta, "yield": round(y, 6), "yieldType": "ytm", "source": src, "live": True}
        print(f"  {ticker}: {y:.4%}  ✓")
    else:
        fb = FALLBACKS[ticker]
        results[ticker] = {**meta, "yield": fb, "yieldType": "ytm", "source": "fallback", "live": False}
        print(f"  {ticker}: fallback {fb:.4%}")
    time.sleep(0.5)

# ---------------------------------------------------------------------------
# Write data.json
# ---------------------------------------------------------------------------
live_count = sum(1 for v in results.values() if v.get("live"))
output = {
    "meta": {
        "last_updated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "GitHub Actions — fetch_yields.py",
        "live_count": live_count,
        "total_count": len(results),
        "stale": live_count == 0,
    },
    "funds": results,
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\ndata.json written — {live_count}/{len(results)} live yields fetched.")
