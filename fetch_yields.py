"""
fetch_yields.py — TEY Calculator yield fetcher
Rebuilt April 2026 with confirmed working sources:
  - Schwab MMFs:  schwabassetmanagement.com/products/money-fund-yields (plain HTML table)
  - Bond ETFs:    Yahoo Finance v10 quoteSummary (30-day SEC yield / yield field)
  - Fallback:     hardcoded values from 04/09/2026
"""

import json, re, time, datetime, requests
from bs4 import BeautifulSoup

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})

# ── Hardcoded fallbacks — Schwab rates page 04/09/2026 ──────────────────────
FALLBACKS = {
    "SWKXX": 0.0182, "SWYXX": 0.0212, "SWTXX": 0.0213, "SWWXX": 0.0211,
    "SNOXX": 0.0338, "SNSXX": 0.0338, "SNVXX": 0.0337, "SWVXX": 0.0347,
    "SCAXX": 0.0197, "SNYXX": 0.0227, "SWOXX": 0.0228, "SCTXX": 0.0226,
    "SCOXX": 0.0353, "SUTXX": 0.0353, "SGUXX": 0.0352, "SNAXX": 0.0362,
    "CALI":  0.0256, "SUB":   0.02455, "CMF":  0.0310, "MUB":   0.0330,
    "NYF":   0.0305, "IEI":   0.0400,  "VCSH": 0.043699, "VGSH": 0.035791,
    "VCIT":  0.0480, "FORCX": 0.0320,
}

FUND_META = {
    # Schwab Investor Shares
    "SWKXX": {"name": "CA Municipal Money Fund",          "taxType": "state_muni",    "series": "schwab"},
    "SWYXX": {"name": "NY Municipal Money Fund",          "taxType": "state_muni",    "series": "schwab"},
    "SWTXX": {"name": "Municipal Money Fund",             "taxType": "national_muni", "series": "schwab"},
    "SWWXX": {"name": "AMT Tax-Free Money Fund",          "taxType": "national_muni", "series": "schwab"},
    "SNOXX": {"name": "Treasury Obligations Money Fund",  "taxType": "treasury",      "series": "schwab"},
    "SNSXX": {"name": "U.S. Treasury Money Fund",         "taxType": "treasury",      "series": "schwab"},
    "SNVXX": {"name": "Government Money Fund",            "taxType": "treasury",      "series": "schwab"},
    "SWVXX": {"name": "Prime Advantage Money Fund",       "taxType": "taxable",       "series": "schwab"},
    # Schwab Ultra Shares
    "SCAXX": {"name": "CA Municipal Money Fund (Ultra)",  "taxType": "state_muni",    "series": "schwab"},
    "SNYXX": {"name": "NY Municipal Money Fund (Ultra)",  "taxType": "state_muni",    "series": "schwab"},
    "SWOXX": {"name": "Municipal Money Fund (Ultra)",     "taxType": "national_muni", "series": "schwab"},
    "SCTXX": {"name": "AMT Tax-Free Money Fund (Ultra)",  "taxType": "national_muni", "series": "schwab"},
    "SCOXX": {"name": "Treasury Obligations (Ultra)",     "taxType": "treasury",      "series": "schwab"},
    "SUTXX": {"name": "U.S. Treasury Money Fund (Ultra)", "taxType": "treasury",      "series": "schwab"},
    "SGUXX": {"name": "Government Money Fund (Ultra)",    "taxType": "treasury",      "series": "schwab"},
    "SNAXX": {"name": "Prime Advantage Money Fund (Ultra)","taxType": "taxable",      "series": "schwab"},
    # Bond ETFs
    "CALI":  {"name": "CA Muni Bond ETF",                 "taxType": "state_muni",    "series": "etf"},
    "SUB":   {"name": "Short-Term National Muni ETF",     "taxType": "national_muni", "series": "etf"},
    "CMF":   {"name": "CA Intermediate Muni ETF",         "taxType": "state_muni",    "series": "etf"},
    "MUB":   {"name": "National Muni ETF",                "taxType": "national_muni", "series": "etf"},
    "NYF":   {"name": "NY Muni Bond ETF",                 "taxType": "state_muni",    "series": "etf"},
    "IEI":   {"name": "Intermediate Treasury ETF",        "taxType": "treasury",      "series": "etf"},
    "VCSH":  {"name": "Short-Term Corp Bond ETF",         "taxType": "taxable",       "series": "etf"},
    "VGSH":  {"name": "Short-Term Treasury ETF",          "taxType": "treasury",      "series": "etf"},
    "VCIT":  {"name": "Intermediate Corp Bond ETF",       "taxType": "taxable",       "series": "etf"},
    "FORCX": {"name": "OR Intermediate Muni Fund",        "taxType": "state_muni",    "series": "etf"},
}

results = {}

def safe_float(val):
    """Convert a yield value to a decimal (0.035 form). Handles % strings and raw decimals."""
    try:
        v = float(str(val).replace("%", "").replace(",", "").strip())
        if 0.0001 <= v <= 0.15:   return round(v, 6)   # already decimal (0.035)
        if 0.01   <= v <= 15.0:   return round(v / 100, 6)  # percentage (3.5)
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 1: Schwab Asset Management money-fund-yields page
#   URL: schwabassetmanagement.com/products/money-fund-yields
#   Format: plain HTML table — no JS rendering required (confirmed accessible)
#   Returns: dict { "SWVXX": 0.0347, "SNAXX": 0.0362, ... }
# ═══════════════════════════════════════════════════════════════════════════
def fetch_schwab_all():
    url = "https://www.schwabassetmanagement.com/products/money-fund-yields"
    print(f"\n[Schwab] Fetching {url}")
    out = {}
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # The page has tables. Each row has the ticker as text somewhere near the yield.
        # Pattern: rows containing a ticker like SWVXX followed by a yield like 3.47%
        text = r.text

        # Extract all ticker→yield pairs from the page text
        # Schwab tickers are 5 chars starting with S
        ticker_pattern = re.compile(
            r'\b(S[WNCTGA][A-Z]{3})\b'   # Schwab MMF tickers
            r'(?:(?!</).){1,400}?'         # up to 400 chars ahead (non-greedy)
            r'([\d]+\.[\d]+)\s*%',         # first percentage after ticker
            re.DOTALL
        )

        # Better approach: parse table rows
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                texts = [c.get_text(strip=True) for c in cells]
                # Find the ticker cell (5-char, starts with S, all caps)
                ticker = None
                for t in texts:
                    if re.match(r'^S[WNCTGBOA][A-Z]{3}$', t):
                        ticker = t
                        break
                if not ticker:
                    continue
                # Find the first yield cell (looks like "3.47%")
                for t in texts:
                    m = re.match(r'^([\d]+\.[\d]+)%$', t)
                    if m:
                        v = safe_float(m.group(1))
                        if v and ticker in FUND_META:
                            out[ticker] = v
                            print(f"  {ticker}: {v:.4%}")
                        break

        print(f"[Schwab] {len(out)} yields parsed")
    except Exception as e:
        print(f"[Schwab] ERROR: {e}")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 2: Yahoo Finance v10 quoteSummary
#   Endpoint: query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}
#   Modules: summaryDetail, defaultKeyStatistics
#   Field: yield (raw decimal for ETFs; may be annual dividend yield for MMFs)
#   Best for: ETFs (CALI, SUB, CMF, MUB, NYF, IEI, VCSH, VGSH, VCIT)
# ═══════════════════════════════════════════════════════════════════════════
def fetch_yahoo_yield(ticker):
    url = (
        f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        f"?modules=summaryDetail,defaultKeyStatistics"
    )
    try:
        r = SESSION.get(url, timeout=12)
        r.raise_for_status()
        d = r.json()
        result = d.get("quoteSummary", {}).get("result", [{}])[0]
        for section in ["summaryDetail", "defaultKeyStatistics"]:
            sec = result.get(section, {})
            # Try multiple field names
            for field in ["yield", "trailingAnnualDividendYield", "dividendYield"]:
                raw = (sec.get(field) or {})
                val = raw.get("raw") if isinstance(raw, dict) else raw
                v = safe_float(val)
                if v:
                    return v, field
    except Exception as e:
        print(f"    [Yahoo] {ticker} error: {e}")
    return None, None


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 3: Yahoo Finance v7 quote (faster, returns more fields at once)
# ═══════════════════════════════════════════════════════════════════════════
def fetch_yahoo_v7_batch(tickers):
    """Fetch multiple ETF tickers in one call."""
    symbols = ",".join(tickers)
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"
    out = {}
    try:
        r = SESSION.get(url, timeout=15)
        r.raise_for_status()
        items = r.json().get("quoteResponse", {}).get("result", [])
        for item in items:
            t = item.get("symbol", "")
            for field in ["yield", "trailingAnnualDividendYield", "dividendYield"]:
                val = item.get(field)
                v = safe_float(val)
                if v:
                    out[t] = (v, field)
                    break
    except Exception as e:
        print(f"  [Yahoo v7 batch] error: {e}")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("TEY Calculator — fetch_yields.py")
print(f"Run time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 60)

# Step 1: Schwab MMF page — all 16 Schwab funds in one request
schwab_yields = fetch_schwab_all()

# Step 2: ETF yields via Yahoo Finance — batch v7 first, then v10 fallback per ticker
ETF_TICKERS = ["CALI", "SUB", "CMF", "MUB", "NYF", "IEI", "VCSH", "VGSH", "VCIT", "FORCX"]
print(f"\n[Yahoo v7 batch] Fetching {len(ETF_TICKERS)} ETF tickers")
etf_batch = fetch_yahoo_v7_batch(ETF_TICKERS)

# Step 3: For any ETF not in batch result, try v10 individually
etf_yields = {}
for t in ETF_TICKERS:
    if t in etf_batch:
        v, field = etf_batch[t]
        print(f"  {t}: {v:.4%} (v7/{field})")
        etf_yields[t] = (v, f"yahoo_v7/{field}")
    else:
        print(f"  {t}: not in batch, trying v10...")
        time.sleep(0.3)
        v, field = fetch_yahoo_yield(t)
        if v:
            print(f"  {t}: {v:.4%} (v10/{field})")
            etf_yields[t] = (v, f"yahoo_v10/{field}")
        else:
            print(f"  {t}: all Yahoo attempts failed")

# Step 4: Build results dict
print(f"\n[Results]")
for ticker, meta in FUND_META.items():
    is_schwab = meta["series"] == "schwab"
    is_etf    = meta["series"] == "etf"

    if is_schwab and ticker in schwab_yields:
        y = schwab_yields[ticker]
        src = "schwabassetmanagement.com"
        live = True
    elif is_etf and ticker in etf_yields:
        y, src = etf_yields[ticker]
        live = True
    else:
        y = FALLBACKS.get(ticker, 0)
        src = "fallback_04092026"
        live = False

    results[ticker] = {
        **meta,
        "yield":     round(y, 6),
        "yieldType": "7day" if is_schwab else "sec_yield",
        "source":    src,
        "live":      live,
    }
    status = "LIVE" if live else "FALLBACK"
    print(f"  {status:8s} {ticker:6s} {y:.4%}  ({src})")

# Step 5: Write data.json
live_count = sum(1 for v in results.values() if v["live"])
output = {
    "meta": {
        "last_updated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":       "GitHub Actions — fetch_yields.py",
        "live_count":   live_count,
        "total_count":  len(results),
        "stale":        live_count == 0,
    },
    "funds": results,
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n{'='*60}")
print(f"data.json written: {live_count}/{len(results)} live yields")
print("=" * 60)
