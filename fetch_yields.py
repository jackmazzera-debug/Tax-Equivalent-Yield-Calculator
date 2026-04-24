"""
fetch_yields.py — TEY Calculator yield fetcher (GitHub Actions)
Sources:
  - Schwab MMFs:  schwab.com/money-market-funds          (7-day yield)
  - iShares ETFs: ishares.com product pages              (yield to maturity)
  - Vanguard ETFs: investor.vanguard.com via Playwright  (yield to maturity)
  - FORCX:        hardcoded — update FORCX_YTM monthly from nuveen.com
"""

import json, re, datetime, requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
})

# ---------------------------------------------------------------------------
# !! MANUAL UPDATE — check nuveen.com monthly !!
# ---------------------------------------------------------------------------
FORCX_YTM    = 0.032
FORCX_AS_OF  = "2026-04-09"
# ---------------------------------------------------------------------------

FALLBACKS_DATE = "2026-04-24"

FALLBACKS = {
    "SWKXX": 0.0259, "SWYXX": 0.0317, "SWTXX": 0.0297, "SWWXX": 0.0295,
    "SNOXX": 0.0339, "SNSXX": 0.0338, "SNVXX": 0.0338, "SWVXX": 0.0349,
    "SCAXX": 0.0273, "SNYXX": 0.0332, "SWOXX": 0.0312, "SCTXX": 0.031,
    "SCOXX": 0.0354, "SUTXX": 0.0353, "SGUXX": 0.0353, "SNAXX": 0.0364,
    "CALI":  0.0301, "SUB":   0.026,  "CMF":   0.0319, "MUB":   0.0344,
    "NYF":   0.0345, "IEI":   0.0397, "VCSH":  0.046,  "VGSH":  0.038,
    "VCIT":  0.051,  "FORCX": 0.032,
}

FUND_META = {
    "SWKXX": {"name": "CA Municipal Money Fund",           "taxType": "state_muni",    "series": "schwab"},
    "SWYXX": {"name": "NY Municipal Money Fund",           "taxType": "state_muni",    "series": "schwab"},
    "SWTXX": {"name": "Municipal Money Fund",              "taxType": "national_muni", "series": "schwab"},
    "SWWXX": {"name": "AMT Tax-Free Money Fund",           "taxType": "national_muni", "series": "schwab"},
    "SNOXX": {"name": "Treasury Obligations Money Fund",   "taxType": "treasury",      "series": "schwab"},
    "SNSXX": {"name": "U.S. Treasury Money Fund",          "taxType": "treasury",      "series": "schwab"},
    "SNVXX": {"name": "Government Money Fund",             "taxType": "treasury",      "series": "schwab"},
    "SWVXX": {"name": "Prime Advantage Money Fund",        "taxType": "taxable",       "series": "schwab"},
    "SCAXX": {"name": "CA Municipal Money Fund (Ultra)",   "taxType": "state_muni",    "series": "schwab"},
    "SNYXX": {"name": "NY Municipal Money Fund (Ultra)",   "taxType": "state_muni",    "series": "schwab"},
    "SWOXX": {"name": "Municipal Money Fund (Ultra)",      "taxType": "national_muni", "series": "schwab"},
    "SCTXX": {"name": "AMT Tax-Free Money Fund (Ultra)",   "taxType": "national_muni", "series": "schwab"},
    "SCOXX": {"name": "Treasury Obligations (Ultra)",      "taxType": "treasury",      "series": "schwab"},
    "SUTXX": {"name": "U.S. Treasury Money Fund (Ultra)",  "taxType": "treasury",      "series": "schwab"},
    "SGUXX": {"name": "Government Money Fund (Ultra)",     "taxType": "treasury",      "series": "schwab"},
    "SNAXX": {"name": "Prime Advantage Money Fund (Ultra)","taxType": "taxable",       "series": "schwab"},
    "CALI":  {"name": "CA Muni Bond ETF",                  "taxType": "state_muni",    "series": "etf"},
    "SUB":   {"name": "Short-Term National Muni ETF",      "taxType": "national_muni", "series": "etf"},
    "CMF":   {"name": "CA Intermediate Muni ETF",          "taxType": "state_muni",    "series": "etf"},
    "MUB":   {"name": "National Muni ETF",                 "taxType": "national_muni", "series": "etf"},
    "NYF":   {"name": "NY Muni Bond ETF",                  "taxType": "state_muni",    "series": "etf"},
    "IEI":   {"name": "Intermediate Treasury ETF",         "taxType": "treasury",      "series": "etf"},
    "VCSH":  {"name": "Short-Term Corp Bond ETF",          "taxType": "taxable",       "series": "etf"},
    "VGSH":  {"name": "Short-Term Treasury ETF",           "taxType": "treasury",      "series": "etf"},
    "VCIT":  {"name": "Intermediate Corp Bond ETF",        "taxType": "taxable",       "series": "etf"},
    "FORCX": {"name": "OR Intermediate Muni Fund",         "taxType": "state_muni",    "series": "etf"},
}

SCHWAB_TICKERS = [
    "SWKXX","SWYXX","SWTXX","SWWXX","SNOXX","SNSXX","SNVXX","SWVXX",
    "SCAXX","SNYXX","SWOXX","SCTXX","SCOXX","SUTXX","SGUXX","SNAXX",
]

ISHARES_URLS = {
    "CALI": "https://www.ishares.com/us/products/332497/ishares-short-term-california-muni-active-etf",
    "SUB":  "https://www.ishares.com/us/products/239772/ishares-shortterm-national-amtfree-muni-bond-etf",
    "CMF":  "https://www.ishares.com/us/products/239731/ishares-california-amtfree-muni-bond-etf",
    "MUB":  "https://www.ishares.com/us/products/239766/MUB",
    "NYF":  "https://www.ishares.com/us/products/239767/ishares-new-york-amtfree-muni-bond-etf",
    "IEI":  "https://www.ishares.com/us/products/239455/ishares-37-year-treasury-bond-etf",
}

VANGUARD_TICKERS = ["VCSH", "VGSH", "VCIT"]


def _fallback(ticker):
    return FALLBACKS[ticker], f"fallback_{FALLBACKS_DATE}", False


def fetch_schwab():
    print("[Schwab] Fetching schwab.com/money-market-funds")
    out = {}
    try:
        resp = SESSION.get("https://www.schwab.com/money-market-funds", timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for ticker in SCHWAB_TICKERS:
            anchor = (
                soup.find("a", title=lambda t: t and t.upper() == ticker)
                or soup.find("a", href=lambda h: h and f"/{ticker.lower()}" in h.lower())
            )
            if not anchor:
                print(f"  {ticker}: anchor not found — fallback")
                continue
            parent_td = anchor.find_parent("td")
            next_td = parent_td.find_next_sibling("td") if parent_td else None
            if not next_td:
                continue
            m = re.search(r"[\d.]+", next_td.get_text(strip=True))
            if m:
                out[ticker] = round(float(m.group()) / 100, 8)
                print(f"  {ticker}: {out[ticker]:.4%} [live]")
    except Exception as e:
        print(f"  ERROR: {e}")
    return out


def fetch_ishares():
    print("[iShares] Fetching ETF YTMs")
    out = {}
    for ticker, url in ISHARES_URLS.items():
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            idx = resp.text.find("Yield to Maturity")
            if idx == -1:
                raise ValueError("YTM label not found")
            chunk = resp.text[idx:idx + 800]
            m = re.search(r'class="data">\s*([\d.]+)%', chunk)
            if not m:
                raise ValueError("YTM value not found")
            out[ticker] = round(float(m.group(1)) / 100, 8)
            print(f"  {ticker}: {out[ticker]:.4%} [live]")
        except Exception as e:
            print(f"  {ticker}: {e} — fallback")
    return out


def fetch_vanguard():
    print("[Vanguard] Fetching ETF YTMs via Playwright")
    out = {}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            for ticker in VANGUARD_TICKERS:
                try:
                    page = browser.new_page()
                    url = f"https://investor.vanguard.com/investment-products/etfs/profile/{ticker.lower()}"
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_selector("text=Yield to maturity", timeout=30000)
                    content = page.content()
                    page.close()
                    m = re.search(r'symbolYieldToMaturity[^>]*>\s*([\d.]+)%', content)
                    if not m:
                        raise ValueError("YTM value not found")
                    out[ticker] = round(float(m.group(1)) / 100, 8)
                    print(f"  {ticker}: {out[ticker]:.4%} [live]")
                except Exception as e:
                    print(f"  {ticker}: {e} — fallback")
                    try: page.close()
                    except Exception: pass
            browser.close()
    except Exception as e:
        print(f"  ERROR launching Playwright: {e}")
    return out


print("=" * 60)
print("TEY Calculator — fetch_yields.py")
print(f"Run time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 60)

schwab_live  = fetch_schwab()
ishares_live = fetch_ishares()
vanguard_live = fetch_vanguard()

results = {}
for ticker, meta in FUND_META.items():
    if ticker == "FORCX":
        y, src, live = FORCX_YTM, f"Nuveen (manual {FORCX_AS_OF})", False
    elif ticker in schwab_live:
        y, src, live = schwab_live[ticker], "Schwab", True
    elif ticker in ishares_live:
        y, src, live = ishares_live[ticker], "iShares", True
    elif ticker in vanguard_live:
        y, src, live = vanguard_live[ticker], "Vanguard", True
    else:
        y, src, live = _fallback(ticker)

    results[ticker] = {
        **meta,
        "yield":     round(y, 6),
        "yieldType": "7day" if meta["series"] == "schwab" else "sec_yield",
        "source":    src,
        "live":      live,
    }
    print(f"  {'LIVE' if live else 'FALLBACK':8s} {ticker:6s} {y:.4%}  ({src})")

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
