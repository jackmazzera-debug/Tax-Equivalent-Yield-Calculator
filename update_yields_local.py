"""
update_yields_local.py

Sources:
  - Schwab money funds (16):  schwab.com/money-market-funds  (7-day yield)
  - iShares ETFs (6):         ishares.com product pages       (yield to maturity)
  - Vanguard ETFs (3):        investor.vanguard.com           (yield to maturity, via Playwright)
  - FORCX:                    MANUAL – update FORCX_YTM below monthly from nuveen.com

Run from the root of your local clone of jackmazzera-debug/Tax-Equivalent-Yield-Calculator
so that data.json is written to the right place and git commands work.
"""

import json
import re
import subprocess
import sys
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# !! MANUAL UPDATE – check nuveen.com monthly !!
# ---------------------------------------------------------------------------
FORCX_YTM = 0.032          # last updated 2026-04-09
FORCX_AS_OF = "2026-04-09"
# ---------------------------------------------------------------------------

SCHWAB_URL = "https://www.schwab.com/money-market-funds"

SCHWAB_TICKERS = [
    "SWKXX", "SWYXX", "SWTXX", "SWWXX",
    "SNOXX", "SNSXX", "SNVXX", "SWVXX",
    "SCAXX", "SNYXX", "SWOXX", "SCTXX",
    "SCOXX", "SUTXX", "SGUXX", "SNAXX",
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

FALLBACKS_DATE = "2026-04-09"

FALLBACKS = {
    "SWKXX": 0.0182,   "SWYXX": 0.0212,   "SWTXX": 0.0213,   "SWWXX": 0.0211,
    "SNOXX": 0.0338,   "SNSXX": 0.0338,   "SNVXX": 0.0337,   "SWVXX": 0.0347,
    "SCAXX": 0.0197,   "SNYXX": 0.0227,   "SWOXX": 0.0228,   "SCTXX": 0.0226,
    "SCOXX": 0.0353,   "SUTXX": 0.0353,   "SGUXX": 0.0352,   "SNAXX": 0.0362,
    "CALI":  0.0256,   "SUB":   0.02455,  "CMF":   0.031,    "MUB":   0.033,
    "NYF":   0.0305,   "IEI":   0.04,     "VCSH":  0.043699, "VGSH":  0.035791,
    "VCIT":  0.048,    "FORCX": 0.032,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}

DATA_JSON_PATH = "data.json"


# ---------------------------------------------------------------------------
# Schwab money funds — schwab.com/money-market-funds (7-day yield)
# ---------------------------------------------------------------------------

def fetch_schwab_yields() -> dict[str, dict]:
    print(f"Fetching Schwab money fund yields …")
    results: dict[str, dict] = {}

    try:
        resp = requests.get(SCHWAB_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for ticker in SCHWAB_TICKERS:
            anchor = (
                soup.find("a", title=lambda t: t and t.upper() == ticker)
                or soup.find("a", href=lambda h: h and f"/{ticker.lower()}" in h.lower())
            )
            if not anchor:
                print(f"  WARNING: {ticker} anchor not found — fallback.")
                results[ticker] = _fallback(ticker, "Schwab")
                continue

            parent_td = anchor.find_parent("td")
            next_td = parent_td.find_next_sibling("td") if parent_td else None
            if not next_td:
                print(f"  WARNING: {ticker} no yield cell — fallback.")
                results[ticker] = _fallback(ticker, "Schwab")
                continue

            m = re.search(r"[\d.]+", next_td.get_text(strip=True))
            if not m:
                print(f"  WARNING: {ticker} couldn't parse yield — fallback.")
                results[ticker] = _fallback(ticker, "Schwab")
                continue

            yld = round(float(m.group()) / 100, 8)
            results[ticker] = {"yield": yld, "source": "Schwab", "live": True}

    except Exception as exc:
        print(f"  ERROR fetching Schwab page: {exc}")
        for ticker in SCHWAB_TICKERS:
            results.setdefault(ticker, _fallback(ticker, "Schwab"))

    for t, v in results.items():
        status = "live" if v["live"] else "FALLBACK"
        print(f"  {t}: {v['yield']:.4%}  [{status}]")

    return results


# ---------------------------------------------------------------------------
# iShares ETFs — ishares.com product pages (yield to maturity)
# ---------------------------------------------------------------------------

def fetch_ishares_yields() -> dict[str, dict]:
    print("Fetching iShares ETF yields (YTM) …")
    results: dict[str, dict] = {}

    for ticker, url in ISHARES_URLS.items():
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()

            idx = resp.text.find("Yield to Maturity")
            if idx == -1:
                raise ValueError("YTM label not found")

            chunk = resp.text[idx:idx + 800]
            m = re.search(r'class="data">\s*([\d.]+)%', chunk)
            if not m:
                raise ValueError("YTM value not found after label")

            yld = round(float(m.group(1)) / 100, 8)
            results[ticker] = {"yield": yld, "source": "iShares", "live": True}

        except Exception as exc:
            print(f"  WARNING: {ticker} failed ({exc}) — fallback.")
            results[ticker] = _fallback(ticker, "iShares")

        status = "live" if results[ticker]["live"] else "FALLBACK"
        print(f"  {ticker}: {results[ticker]['yield']:.4%}  [{status}]")

    return results


# ---------------------------------------------------------------------------
# Vanguard ETFs — investor.vanguard.com (yield to maturity, via Playwright)
# ---------------------------------------------------------------------------

def fetch_vanguard_yields() -> dict[str, dict]:
    print("Fetching Vanguard ETF yields (YTM) via Playwright …")
    results: dict[str, dict] = {}

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
                        raise ValueError("YTM value not found in rendered page")

                    yld = round(float(m.group(1)) / 100, 8)
                    results[ticker] = {"yield": yld, "source": "Vanguard", "live": True}

                except Exception as exc:
                    print(f"  WARNING: {ticker} failed ({exc}) — fallback.")
                    results[ticker] = _fallback(ticker, "Vanguard")
                    try:
                        page.close()
                    except Exception:
                        pass

                status = "live" if results[ticker]["live"] else "FALLBACK"
                print(f"  {ticker}: {results[ticker]['yield']:.4%}  [{status}]")

            browser.close()

    except Exception as exc:
        print(f"  ERROR launching Playwright: {exc}")
        for ticker in VANGUARD_TICKERS:
            results.setdefault(ticker, _fallback(ticker, "Vanguard"))

    return results


# ---------------------------------------------------------------------------
# FORCX — manual entry (update FORCX_YTM at top of file monthly)
# ---------------------------------------------------------------------------

def get_forcx() -> dict[str, dict]:
    print(f"FORCX: manual entry {FORCX_YTM:.4%} (as of {FORCX_AS_OF}) — update monthly from nuveen.com")
    return {
        "FORCX": {
            "yield": FORCX_YTM,
            "source": f"Nuveen (manual {FORCX_AS_OF})",
            "live": False,
        }
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fallback(ticker: str, source: str) -> dict:
    return {
        "yield": FALLBACKS[ticker],
        "source": f"{source} (fallback {FALLBACKS_DATE})",
        "live": False,
    }


# ---------------------------------------------------------------------------
# Self-update fallback reference data
# ---------------------------------------------------------------------------

def update_fallbacks_in_script(funds: dict) -> None:
    """Rewrite FALLBACKS and FALLBACKS_DATE in this script with today's live values."""
    live_funds = {t: v for t, v in funds.items() if v.get("live")}
    if not live_funds:
        print("No live data fetched — skipping fallback update.")
        return

    script_path = __file__
    with open(script_path, "r") as f:
        content = f.read()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for ticker, data in live_funds.items():
        yld = data["yield"]
        content = re.sub(
            rf'("{re.escape(ticker)}":\s*)[\d.]+',
            lambda m, y=yld: f'{m.group(1)}{y}',
            content,
        )

    content = re.sub(
        r'(FALLBACKS_DATE\s*=\s*")[^"]+(")',
        rf'\g<1>{today}\g<2>',
        content,
    )

    with open(script_path, "w") as f:
        f.write(content)

    print(f"Updated fallback reference data for {len(live_funds)} tickers (date: {today}).")


# ---------------------------------------------------------------------------
# Build & write data.json
# ---------------------------------------------------------------------------

def build_and_write(funds: dict) -> dict:
    live_count = sum(1 for v in funds.values() if v.get("live"))
    total_count = len(funds)

    payload = {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "live_count": live_count,
            "total_count": total_count,
            "stale": live_count < total_count,
        },
        "funds": funds,
    }

    with open(DATA_JSON_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"\nWrote {DATA_JSON_PATH}  (live={live_count}/{total_count}, stale={payload['meta']['stale']})")
    return payload


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------

def git_push() -> None:
    print("\nCommitting and pushing data.json …")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cmds = [
        ["git", "add", DATA_JSON_PATH],
        ["git", "commit", "-m", f"chore: update fund yields {ts}"],
        ["git", "push", "origin", "main"],
    ]
    for cmd in cmds:
        print(f"  $ {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.stdout:
            print(f"    {result.stdout.strip()}")
        if result.stderr:
            print(f"    {result.stderr.strip()}")
        if result.returncode != 0:
            print(f"  ERROR: command failed (exit {result.returncode})")
            sys.exit(1)
    print("  Done.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    schwab = fetch_schwab_yields()
    ishares = fetch_ishares_yields()
    vanguard = fetch_vanguard_yields()
    forcx = get_forcx()

    funds = {**schwab, **ishares, **vanguard, **forcx}
    payload = build_and_write(funds)
    update_fallbacks_in_script(funds)

    print("\ndata.json meta:")
    print(json.dumps(payload["meta"], indent=2))

    push = input("\nPush to GitHub? [y/N] ").strip().lower()
    if push == "y":
        git_push()
    else:
        print("Skipped git push.")


if __name__ == "__main__":
    main()
