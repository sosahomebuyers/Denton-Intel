#!/usr/bin/env python3
"""
Denton County, Texas - Motivated Seller Lead Scraper (v6)
==========================================================

v6 changes vs v5
----------------
* DCAD search button targeting fixed: v5's `button:has(svg)` selector was
  greedy and clicked the page's scroll-to-top arrow instead of the
  magnifying-glass search button, so the search never fired. v6 targets
  the search button by its position relative to the search input and
  prefers Enter-key submission as the primary path.
* DCAD XHR response interception added: captures the JSON the True Prodigy
  grid loads from and parses addresses out of that directly. Falls back
  to DOM scraping only if the response capture misses.
* Wider net of search query formats tried per owner (last-name + initial,
  last-name only, full name) until one returns rows.
* N/A filter rewritten: now drops rows where the FINAL owner field would
  end up empty or "N/A", regardless of which raw column had the value.
* Probe screenshot saved to .cache/dcad_probe.png so we can SEE what DCAD
  rendered during automation.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import re
import sys
import time
import zipfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlencode, quote

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
except ImportError:
    DBF = None  # type: ignore

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    async_playwright = None  # type: ignore
    PWTimeout = Exception  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

COUNTY = "Denton"
STATE = "TX"
CLERK_URL = "https://denton.tx.publicsearch.us"
PAD_URLS = [
    "https://www.dentoncad.com/data-and-fees",
    "https://www.dentoncad.com/downloads",
    "https://www.dentoncad.com/data",
    "https://www.dentoncad.com/public-data",
]
PAD_URL = PAD_URLS[0]  # primary
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
MAX_RETRIES = 3
RETRY_BACKOFF = 2.5
HTTP_TIMEOUT = 60
PW_TIMEOUT = 60_000  # ms

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR = ROOT / "data"
CACHE_DIR = ROOT / ".cache"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Doc type taxonomy + classification
# ---------------------------------------------------------------------------
# Each entry: (display label, motivated-seller flag, list of (search_term, doc_type_regex))
# The doc_type_regex narrows which result rows we keep, so e.g. searching
# "LIEN" doesn't pull every release/extension.

DOC_TYPES: Dict[str, Tuple[str, str, List[Tuple[str, str]]]] = {
    "LP":       ("Lis Pendens",            "Lis pendens",
                 [("LIS PENDENS",                  r"\bLIS\s*PENDENS\b(?!.*RELEASE)")]),
    "RELLP":    ("Release Lis Pendens",    "Lis pendens",
                 [("RELEASE LIS PENDENS",          r"RELEASE.*LIS\s*PENDENS")]),
    "NOFC":     ("Notice of Foreclosure",  "Pre-foreclosure",
                 [("SUBSTITUTE TRUSTEES SALE",     r"(SUBSTITUTE.*TRUSTEE|TRUSTEE.*SALE|FORECLOSURE)"),
                  ("NOTICE OF FORECLOSURE",        r"NOTICE.*FORECLOSURE"),
                  ("APPOINTMENT OF SUBSTITUTE TRUSTEE",
                                                    r"(APPOINTMENT.*TRUSTEE|SUBSTITUTE.*TRUSTEE)")]),
    "TAXDEED":  ("Tax Deed",               "Tax lien",
                 [("TAX DEED",                     r"\bTAX\s*DEED\b"),
                  ("SHERIFF DEED",                 r"SHERIFF.*DEED")]),
    "JUD":      ("Judgment",               "Judgment lien",
                 [("ABSTRACT OF JUDGMENT",         r"ABSTRACT.*JUDGMENT"),
                  ("JUDGMENT",                     r"\bJUDGMENT\b")]),
    "DRJUD":    ("Domestic Judgment",      "Judgment lien",
                 [("DOMESTIC RELATIONS",           r"DOMESTIC.*RELATIONS")]),
    "LNCORPTX": ("State Tax Lien",         "Tax lien",
                 [("STATE TAX LIEN",               r"STATE.*TAX.*LIEN")]),
    "LNIRS":    ("Federal/IRS Tax Lien",   "Tax lien",
                 [("FEDERAL TAX LIEN",             r"(FEDERAL|IRS).*(TAX\s*)?LIEN")]),
    "LNMECH":   ("Mechanic's Lien",        "Mechanic lien",
                 [("MECHANIC",                     r"(MECHANIC|MATERIALMAN|M\s*&\s*M).*LIEN"),
                  ("AFFIDAVIT OF LIEN",            r"AFFIDAVIT.*LIEN")]),
    "LNHOA":    ("HOA Lien",               "Mechanic lien",
                 [("ASSESSMENT LIEN",              r"(ASSESSMENT|HOA|HOMEOWNERS).*LIEN"),
                  ("HOA LIEN",                     r"(HOA|HOMEOWNERS).*LIEN")]),
    "MEDLN":    ("Medicaid Lien",          "Tax lien",
                 [("MEDICAID LIEN",                r"MEDICAID.*LIEN")]),
    "PRO":      ("Probate",                "Probate / estate",
                 [("AFFIDAVIT OF HEIRSHIP",        r"AFFIDAVIT.*HEIRSHIP"),
                  ("PROBATE",                      r"PROBATE"),
                  ("LETTERS TESTAMENTARY",         r"LETTERS.*TESTAMENTARY"),
                  ("EXECUTOR DEED",                r"EXECUTOR.*DEED")]),
    "NOC":      ("Notice of Commencement", "Mechanic lien",
                 [("NOTICE OF COMMENCEMENT",       r"NOTICE.*COMMENCEMENT")]),
}

# For these doc types, the GRANTEE is the actual property owner / lead.
# (Banks file judgments against debtors; HOAs file liens against homeowners; etc.)
# For everything else (probate, lis pendens, deed-style transfers) the
# grantor is correct.
GRANTEE_AS_OWNER = {"JUD", "DRJUD", "LNCORPTX", "LNIRS", "LNMECH", "LNHOA",
                    "MEDLN", "TAXDEED", "NOC", "NOFC"}


# ---------------------------------------------------------------------------
# Lead model
# ---------------------------------------------------------------------------

@dataclass
class Lead:
    doc_num: str = ""
    doc_type: str = ""
    cat: str = ""
    cat_label: str = ""
    filed: str = ""
    owner: str = ""
    grantee: str = ""
    amount: float = 0.0
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "TX"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    clerk_url: str = ""
    flags: List[str] = field(default_factory=list)
    score: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def safe(fn, *args, default=None, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log(f"safe() suppressed {fn.__name__}: {e}")
        return default


def retry(times: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF):
    def deco(fn):
        def wrap(*args, **kwargs):
            last = None
            for i in range(times):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last = e
                    sleep = backoff ** i
                    log(f"{fn.__name__} attempt {i+1}/{times} failed: {e} — retrying in {sleep:.1f}s")
                    time.sleep(sleep)
            raise last  # type: ignore[misc]
        return wrap
    return deco


async def aretry(coro_factory, times: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF, label: str = ""):
    last = None
    for i in range(times):
        try:
            return await coro_factory()
        except Exception as e:
            last = e
            sleep = backoff ** i
            log(f"[async] {label} attempt {i+1}/{times} failed: {e} — retrying in {sleep:.1f}s")
            await asyncio.sleep(sleep)
    raise last  # type: ignore[misc]


_money_re = re.compile(r"[-+]?\$?\s*([0-9][0-9,]*(?:\.\d+)?)")

def parse_amount(text: str) -> float:
    if not text:
        return 0.0
    m = _money_re.search(text)
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def parse_date(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    fmts = ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y")
    for f in fmts:
        try:
            return datetime.strptime(text, f).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.split("T")[0]).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return ""


def normalize_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", " ", name).strip().upper()


def name_variants(name: str) -> List[str]:
    n = normalize_name(name)
    if not n:
        return []
    out = {n}
    if "," in n:
        last, _, first = n.partition(",")
        last = last.strip()
        first = first.strip()
        if last and first:
            out.add(f"{first} {last}")
            out.add(f"{last} {first}")
            out.add(f"{last}, {first}")
    else:
        parts = n.split()
        if len(parts) >= 2:
            first = parts[0]
            last = parts[-1]
            mids = " ".join(parts[1:-1])
            full_first = f"{first} {mids}".strip()
            out.add(f"{full_first} {last}".strip())
            out.add(f"{last} {full_first}".strip())
            out.add(f"{last}, {full_first}".strip())
    return [v for v in out if v]


def is_entity(name: str) -> bool:
    if not name:
        return False
    n = " " + name.upper() + " "
    markers = (" LLC", " L.L.C", " INC", " INC.", " CORP", " CO.", " COMPANY",
               " LP ", " L.P", " LLP", " TRUST", " ESTATE", " BANK",
               " ASSOCIATION", " HOA", " PARTNERS", " PARTNERSHIP",
               " HOLDINGS", " GROUP", " ENTERPRISES", " VENTURES",
               " STATE OF", " COUNTY OF", " CITY OF")
    return any(m in n for m in markers)


# ---------------------------------------------------------------------------
# Property Appraiser (DCAD) — Playwright-based bulk download
# ---------------------------------------------------------------------------

class ParcelLookup:
    def __init__(self) -> None:
        self.by_name: Dict[str, Dict[str, str]] = {}
        self.loaded = False

    def add(self, rec: Dict[str, str]) -> None:
        owner = rec.get("owner", "")
        if not owner:
            return
        for v in name_variants(owner):
            self.by_name.setdefault(v, rec)

    def lookup(self, owner: str) -> Optional[Dict[str, str]]:
        if not self.loaded:
            return None
        for v in name_variants(owner):
            hit = self.by_name.get(v)
            if hit:
                return hit
        n = normalize_name(owner)
        if n:
            tokens = [t for t in re.split(r"[ ,]+", n) if t]
            if tokens:
                last = tokens[-1] if "," not in n else tokens[0].rstrip(",")
                for k, v in self.by_name.items():
                    if k.startswith(last + " ") or k.startswith(last + ","):
                        return v
        return None


def _pick(rec: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k in rec and rec[k] not in (None, ""):
            return str(rec[k]).strip()
    upper = {str(k).upper(): v for k, v in rec.items()}
    for k in keys:
        v = upper.get(k.upper())
        if v not in (None, ""):
            return str(v).strip()
    return ""


DCAD_SEARCH_URL = "https://www.dentoncad.com/property-search"
DCAD_CACHE_PATH = CACHE_DIR / "dcad_owner_cache.json"


def _load_dcad_cache() -> Dict[str, Dict[str, str]]:
    if DCAD_CACHE_PATH.exists():
        try:
            return json.loads(DCAD_CACHE_PATH.read_text())
        except Exception:
            return {}
    return {}


def _save_dcad_cache(cache: Dict[str, Dict[str, str]]) -> None:
    try:
        DCAD_CACHE_PATH.write_text(json.dumps(cache, indent=2))
    except Exception as e:
        log(f"DCAD cache save failed: {e}")


def _dcad_search_queries(owner: str) -> List[str]:
    """Generate a fallback ladder of DCAD search queries for one owner.

    DCAD's True Prodigy SPA has a 'too many results' guard that suppresses
    overly-broad searches.  We avoid last-name-only queries entirely and
    always start narrow.
    """
    n = normalize_name(owner)
    if not n:
        return []
    if "," in n:
        last = n.split(",", 1)[0].strip()
        rest = n.split(",", 1)[1].strip()
        first = (rest.split() or [""])[0]
    else:
        parts = [p for p in n.split() if p]
        last = parts[-1] if parts else n
        first = parts[0] if len(parts) >= 2 else ""

    queries: List[str] = []
    if last and first:
        queries.append(f"{last}, {first}")        # "TAYLOR, JULIE"  (DCAD's native order)
        queries.append(f"{last} {first}")         # "TAYLOR JULIE"
        queries.append(f"{first} {last}")         # "JULIE TAYLOR"
        if len(first) >= 2:
            queries.append(f"{last} {first[:2]}") # "TAYLOR JU"
    # Intentionally NOT appending bare last name — triggers DCAD's
    # "too many results to display" guard.
    seen = set()
    out = []
    for q in queries:
        if q and q not in seen:
            seen.add(q)
            out.append(q)
    return out


async def _wait_for_dcad_input(page, timeout_ms: int = 15_000):
    """Wait for the True Prodigy search input to render."""
    selectors = [
        'input[placeholder*="Account Number" i]',
        'input[placeholder*="Owner Name" i]',
        'input[placeholder*="Address or Owner" i]',
        'input[placeholder*="Search by" i]',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            await loc.wait_for(state="visible", timeout=timeout_ms)
            return loc
        except PWTimeout:
            continue
        except Exception:
            continue
    return None


def _parse_us_address(addr: str) -> Tuple[str, str, str]:
    """Split 'STREET, CITY, TX 75XXX' into (street, city, zip). Tolerant."""
    if not addr:
        return "", "", ""
    site_addr = addr.strip()
    site_city = ""
    site_zip = ""
    m = re.match(r"^(.*?),\s*([A-Z][A-Z .'\-]+?),?\s*TX\s*(\d{5})",
                 site_addr.upper())
    if m:
        site_addr = addr[: m.start(2)].rstrip(", ").strip()
        site_city = m.group(2).title().strip()
        site_zip  = m.group(3)
    else:
        m2 = re.search(r"(\d{5})(?:-\d{4})?\s*$", site_addr)
        if m2:
            site_zip = m2.group(1)
    return site_addr, site_city, site_zip


def _extract_address_from_json(data: Any, owner_tokens: set) -> Tuple[int, str, str]:
    """Walk a True Prodigy/ag-Grid JSON response, return (overlap, owner, addr)."""
    best = (0, "", "")
    def visit(node):
        nonlocal best
        if isinstance(node, dict):
            owner = ""
            addr = ""
            for k, v in node.items():
                ku = str(k).upper()
                if isinstance(v, str):
                    if "OWNER" in ku and "NAME" in ku:
                        owner = v
                    elif ku in ("OWNERNAME", "OWNER"):
                        owner = v
                    elif "PROPERTY" in ku and "ADDRESS" in ku:
                        addr = v
                    elif ku in ("SITUSADDRESS", "SITUS_ADDRESS", "PROPADDR",
                                "PROPERTYADDRESS", "ADDRESS"):
                        addr = v
            if owner or addr:
                on_tokens = set(re.findall(r"[A-Z]{2,}", owner.upper()))
                overlap = len(owner_tokens & on_tokens) if owner_tokens else 0
                if addr and overlap >= best[0]:
                    best = (overlap, owner, addr)
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for v in node:
                visit(v)
    visit(data)
    return best


async def _click_dcad_search(page, inp) -> bool:
    """Submit the DCAD search. Prefer Enter; fall back to clicking the
    magnifying-glass button next to the input."""
    # Strategy 1: just press Enter on the input
    try:
        await inp.press("Enter")
        return True
    except Exception:
        pass
    # Strategy 2: find a button immediately following the input in the DOM
    for xp in ("xpath=following::button[1]",
               "xpath=parent::*/following-sibling::*//button[1]",
               "xpath=ancestor::form[1]//button[last()]"):
        try:
            btn = inp.locator(xp).first
            if await btn.count():
                await btn.click(timeout=2500)
                return True
        except Exception:
            continue
    # Strategy 3: button with explicit Search aria-label
    try:
        btn = page.locator('button[aria-label*="Search" i]').first
        if await btn.count():
            await btn.click(timeout=2500)
            return True
    except Exception:
        pass
    return False


async def dcad_lookup_address(page, owner: str, *, save_screenshot: bool = False) -> Dict[str, str]:
    """Search DCAD by owner name. Returns address dict + diagnostic _status."""
    blank = {"site_addr": "", "site_city": "", "site_zip": "",
             "mail_addr": "", "mail_city": "", "mail_state": "", "mail_zip": "",
             "_status": "blank"}
    if not owner:
        return {**blank, "_status": "no-owner"}
    if is_entity(owner) and "DECEASED" not in owner.upper() and "ESTATE" not in owner.upper():
        return {**blank, "_status": "skipped-entity"}

    queries = _dcad_search_queries(owner)
    if not queries:
        return {**blank, "_status": "no-query"}

    owner_tokens = set(re.findall(r"[A-Z]{2,}", owner.upper()))

    # Set up XHR capture BEFORE navigating
    captured: List[Any] = []
    def _on_response(resp):
        try:
            url = resp.url.lower()
            ct = (resp.headers.get("content-type", "") or "").lower()
            if "json" in ct and any(k in url for k in
                ("search", "property", "compound", "parcel", "graphql",
                 "/api/", "results", "lookup")):
                # Schedule async JSON read on next tick
                asyncio.create_task(_capture_json(resp, captured))
        except Exception:
            pass
    async def _capture_json(resp, sink):
        try:
            data = await resp.json()
            sink.append({"url": resp.url, "data": data})
        except Exception:
            pass

    page.on("response", _on_response)

    try:
        try:
            await page.goto(DCAD_SEARCH_URL, wait_until="domcontentloaded",
                            timeout=PW_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=10_000)
        except PWTimeout:
            pass
        except Exception as e:
            return {**blank, "_status": f"goto-failed: {e}"}

        inp = await _wait_for_dcad_input(page)
        if not inp:
            return {**blank, "_status": "input-not-found"}

        last_status = "no-results"
        for q_index, query in enumerate(queries):
            try:
                await inp.click()
                await inp.fill("")
                await inp.type(query, delay=15)
            except Exception as e:
                last_status = f"type-failed: {e}"
                continue

            captured.clear()
            ok = await _click_dcad_search(page, inp)
            if not ok:
                last_status = "submit-failed"
                continue

            # Wait for either captured XHR or DOM rows to appear
            try:
                await page.wait_for_function(
                    """() => {
                        const empty = document.body.innerText.includes('No Rows To Show');
                        const rows  = document.querySelectorAll('[role="row"]').length;
                        return !empty || rows > 1;
                    }""",
                    timeout=10_000,
                )
            except PWTimeout:
                pass
            await asyncio.sleep(1.2)  # let final XHRs land

            # DCAD shows "Too many results" / "Please narrow your search" /
            # similar guard banners on broad searches. The user can re-submit
            # to bypass; we replicate that here.
            try:
                body_text = await page.evaluate("document.body.innerText")
            except Exception:
                body_text = ""
            too_many = any(p in body_text.lower() for p in
                           ("too many results", "narrow your search",
                            "refine your search", "exceeds", "too broad"))
            if too_many:
                log(f"  DCAD guard hit on q={query!r}; re-submitting once")
                # Try to dismiss any dialog first
                for sel in ('button:has-text("OK")',
                            'button:has-text("Continue")',
                            'button:has-text("Close")',
                            'button[aria-label*="close" i]'):
                    try:
                        b = page.locator(sel).first
                        if await b.count():
                            await b.click(timeout=1500)
                            break
                    except Exception:
                        continue
                # Re-submit
                captured.clear()
                try:
                    await inp.click()
                    await inp.press("Enter")
                except Exception:
                    pass
                try:
                    await page.wait_for_function(
                        """() => {
                            const rows = document.querySelectorAll('[role="row"]').length;
                            return rows > 1;
                        }""",
                        timeout=10_000,
                    )
                except PWTimeout:
                    pass
                await asyncio.sleep(1.0)

            if save_screenshot and q_index == 0:
                try:
                    shot = CACHE_DIR / "dcad_probe.png"
                    await page.screenshot(path=str(shot), full_page=False)
                    log(f"DCAD probe screenshot saved: {shot}")
                except Exception as e:
                    log(f"DCAD screenshot failed: {e}")

            # First try: parse from captured XHR JSON
            for cap in captured:
                ov, on, ad = _extract_address_from_json(cap["data"], owner_tokens)
                if ad and ov >= 1:
                    site_addr, site_city, site_zip = _parse_us_address(ad)
                    return {
                        "site_addr": site_addr, "site_city": site_city, "site_zip": site_zip,
                        "mail_addr": site_addr, "mail_city": site_city,
                        "mail_state": "TX", "mail_zip": site_zip,
                        "_status": f"matched-json (q={query!r}, overlap={ov}, owner={on!r})",
                    }

            # Fall back to DOM scraping
            try:
                grid = await page.evaluate("""() => {
                    const out = [];
                    const ag = document.querySelectorAll('[role="row"]');
                    ag.forEach(r => {
                        const cells = r.querySelectorAll('[role="gridcell"], [role="cell"]');
                        if (cells.length) {
                            out.push(Array.from(cells).map(c => (c.innerText||'').trim()));
                        }
                    });
                    if (!out.length) {
                        document.querySelectorAll('table tbody tr').forEach(r => {
                            const arr = Array.from(r.querySelectorAll('td')).map(c => (c.innerText||'').trim());
                            if (arr.length) out.push(arr);
                        });
                    }
                    const headers = [];
                    document.querySelectorAll('[role="columnheader"], table thead th').forEach(h => {
                        headers.push((h.innerText||'').trim());
                    });
                    return {headers, rows: out};
                }""")
            except Exception as e:
                last_status = f"grid-read-failed: {e}"
                continue

            headers = [h.upper() for h in (grid or {}).get("headers", [])]
            rows    = (grid or {}).get("rows", [])
            if not rows:
                last_status = f"no-results (q={query!r})"
                continue

            def col_index(*names):
                for n in names:
                    for i, h in enumerate(headers):
                        if n in h:
                            return i
                return -1
            i_owner = col_index("OWNER NAME", "OWNER")
            i_addr  = col_index("PROPERTY ADDRESS", "SITUS", "ADDRESS")

            best = (0, "", "")
            for row in rows:
                if not row or all(not c for c in row):
                    continue
                on = row[i_owner] if 0 <= i_owner < len(row) else ""
                ad = row[i_addr]  if 0 <= i_addr  < len(row) else ""
                if not ad:
                    continue
                on_tokens = set(re.findall(r"[A-Z]{2,}", on.upper()))
                overlap = len(owner_tokens & on_tokens)
                if overlap > best[0]:
                    best = (overlap, on, ad)
            if best[2] and best[0] >= 1:
                site_addr, site_city, site_zip = _parse_us_address(best[2])
                return {
                    "site_addr": site_addr, "site_city": site_city, "site_zip": site_zip,
                    "mail_addr": site_addr, "mail_city": site_city,
                    "mail_state": "TX", "mail_zip": site_zip,
                    "_status": f"matched-dom (q={query!r}, overlap={best[0]}, owner={best[1]!r})",
                }
            last_status = f"weak-match (q={query!r}, best-overlap={best[0]})"

        return {**blank, "_status": last_status}
    finally:
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            pass


async def enrich_with_dcad(leads: List["Lead"]) -> int:
    """Mutate `leads` in place, filling in addresses via DCAD owner search."""
    if async_playwright is None:
        log("Playwright unavailable; skipping DCAD address enrichment")
        return 0

    cache: Dict[str, Dict[str, str]] = _load_dcad_cache()
    enriched = 0
    skipped = 0
    candidates = [l for l in leads if l.owner and not l.prop_address]
    log(f"DCAD enrichment: {len(candidates)} candidates "
        f"({len(cache)} previously cached)")

    if not candidates:
        return 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=USER_AGENT,
                                        viewport={"width": 1480, "height": 900})
        page = await ctx.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        # Diagnostic: probe DCAD once with a known query, save screenshot
        try:
            probe = await dcad_lookup_address(page, "JOHN SMITH",
                                              save_screenshot=True)
            log(f"DCAD diagnostic probe (JOHN SMITH): status={probe.get('_status')!r} "
                f"addr={probe.get('site_addr')!r}")
        except Exception as e:
            log(f"DCAD diagnostic probe error: {e}")

        for i, lead in enumerate(candidates, 1):
            key = normalize_name(lead.owner)
            if key in cache:
                hit = cache[key]
                skipped += 1
            else:
                try:
                    hit = await dcad_lookup_address(page, lead.owner)
                except Exception as e:
                    log(f"  DCAD lookup error for {lead.owner!r}: {e}")
                    hit = {"_status": f"exception: {e}"}
                cache[key] = hit
                await asyncio.sleep(0.4)  # be polite between live searches

            if hit.get("site_addr"):
                lead.prop_address = hit["site_addr"]
                lead.prop_city    = hit.get("site_city", "")
                lead.prop_zip     = hit.get("site_zip", "")
                lead.mail_address = hit.get("mail_addr", lead.prop_address)
                lead.mail_city    = hit.get("mail_city", "")
                lead.mail_state   = hit.get("mail_state", "TX")
                lead.mail_zip     = hit.get("mail_zip", "")
                enriched += 1
                lead.score = min(100, lead.score + 5)

            if i % 20 == 0 or i == len(candidates):
                log(f"  DCAD progress: {i}/{len(candidates)} "
                    f"({enriched} enriched, {skipped} from cache)")

        await ctx.close()
        await browser.close()

    _save_dcad_cache(cache)
    # Surface a tally of failure modes so we can diagnose
    statuses: Dict[str, int] = {}
    for h in cache.values():
        s = h.get("_status", "unknown")
        # Normalize "matched (...)" into single bucket
        bucket = s.split("(")[0].strip() if "(" in s else s
        statuses[bucket] = statuses.get(bucket, 0) + 1
    log("DCAD status tally: " + ", ".join(f"{k}={v}" for k, v in
        sorted(statuses.items(), key=lambda x: -x[1])))
    log(f"DCAD enrichment done: {enriched}/{len(candidates)} leads got an address")
    return enriched


async def download_parcel_dbf_playwright() -> Optional[Path]:
    """Download the DCAD bulk parcel ZIP via a real browser session.

    The DCAD endpoint returns 403 to the python-requests UA, so we drive
    Chromium and accept the file via Playwright's download handling.
    """
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    extract_dir = CACHE_DIR / f"dcad_parcels_{today}"
    if extract_dir.exists():
        for p in extract_dir.rglob("*.dbf"):
            log(f"DCAD DBF cache hit: {p}")
            return p

    # 1) If the user/CI checked in a local DBF, prefer that.
    for p in DATA_DIR.rglob("*.dbf"):
        log(f"DCAD DBF (local fallback): {p}")
        return p

    if async_playwright is None:
        log("Playwright unavailable; skipping DCAD download")
        return None

    cache_zip = CACHE_DIR / f"dcad_parcels_{today}.zip"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=USER_AGENT, accept_downloads=True)
        page = await ctx.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        download_path: Optional[Path] = None

        for url in PAD_URLS:
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=PW_TIMEOUT)
                if not resp or resp.status >= 400:
                    log(f"DCAD {url} -> HTTP {resp.status if resp else 'no response'}; trying next")
                    continue
            except Exception as e:
                log(f"DCAD goto {url} failed: {e}")
                continue

            for sel in ('button:has-text("Accept")',
                        'button:has-text("I Agree")',
                        'button:has-text("Continue")'):
                try:
                    btn = page.locator(sel).first
                    if await btn.count():
                        await btn.click(timeout=2500)
                except Exception:
                    pass

            # Discover any downloadable links — DBF, ZIP, or specifically "appraisal roll"
            try:
                hrefs = await page.eval_on_selector_all(
                    "a[href]",
                    "els => els.map(e => ({href: e.href, text: (e.innerText||'').trim()}))",
                )
            except Exception:
                hrefs = []
            log(f"DCAD {url}: discovered {len(hrefs)} links")

            # Score candidates
            def score(h):
                u = (h.get("href") or "").lower()
                t = (h.get("text") or "").lower()
                s = 0
                if u.endswith(".zip"):     s += 5
                if u.endswith(".dbf"):     s += 6
                if "appraisal" in t:       s += 3
                if "roll" in t:            s += 3
                if "public" in t:          s += 2
                if "bulk" in t:            s += 2
                if "parcel" in t:          s += 2
                if "data" in t:            s += 1
                if "download" in t:        s += 1
                if "fee" in t:             s -= 2
                if "contact" in t:         s -= 5
                return s
            hrefs.sort(key=score, reverse=True)

            for h in hrefs[:8]:
                if score(h) <= 0:
                    continue
                try:
                    async with page.expect_download(timeout=PW_TIMEOUT) as dl:
                        await page.evaluate(f"window.location.href = {json.dumps(h['href'])}")
                    d = await dl.value
                    await d.save_as(cache_zip)
                    if cache_zip.exists() and cache_zip.stat().st_size >= 1024:
                        download_path = cache_zip
                        log(f"DCAD downloaded via {h.get('text') or h.get('href')}: "
                            f"{cache_zip.stat().st_size:,} bytes")
                        break
                except Exception as e:
                    log(f"DCAD download attempt for {h.get('href')} failed: {e}")
                    continue

            if download_path:
                break

        await browser.close()

    if not download_path or not download_path.exists() or download_path.stat().st_size < 1024:
        log("DCAD download did not produce a valid file; continuing without parcel enrichment")
        return None

    try:
        with zipfile.ZipFile(download_path) as zf:
            zf.extractall(extract_dir)
    except zipfile.BadZipFile:
        # Maybe it's an unwrapped DBF
        if download_path.suffix.lower() == ".dbf":
            return download_path
        log("DCAD download not a zip; skipping enrichment")
        return None

    for p in extract_dir.rglob("*.dbf"):
        log(f"DCAD DBF extracted: {p}")
        return p
    return None


def build_parcel_lookup_from_dbf(dbf_path: Path) -> ParcelLookup:
    lookup = ParcelLookup()
    if DBF is None:
        log("dbfread not installed; skipping parcel enrichment")
        return lookup
    try:
        table = DBF(str(dbf_path), load=False, ignore_missing_memofile=True, encoding="latin-1")
    except Exception as e:
        log(f"Failed to open DBF {dbf_path}: {e}")
        return lookup

    count = 0
    for row in table:
        try:
            rec = {
                "owner":      _pick(row, "OWNER", "OWN1", "OWNER1", "OWNERNAME"),
                "site_addr":  _pick(row, "SITE_ADDR", "SITEADDR", "SITUS_ADDR", "SITUSADDR"),
                "site_city":  _pick(row, "SITE_CITY", "SITUS_CITY", "SITECITY"),
                "site_zip":   _pick(row, "SITE_ZIP",  "SITUS_ZIP",  "SITEZIP"),
                "mail_addr":  _pick(row, "ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILADDR", "MAILING_AD"),
                "mail_city":  _pick(row, "CITY", "MAILCITY", "MAIL_CITY"),
                "mail_state": _pick(row, "STATE", "MAILSTATE", "MAIL_STATE"),
                "mail_zip":   _pick(row, "ZIP", "MAILZIP", "MAIL_ZIP", "ZIPCODE"),
            }
            if rec["owner"]:
                lookup.add(rec)
                count += 1
        except Exception as e:
            log(f"DBF row skipped: {e}")
            continue

    log(f"Parcel lookup built: {count:,} owners, {len(lookup.by_name):,} name variants")
    lookup.loaded = count > 0
    return lookup


# ---------------------------------------------------------------------------
# Clerk portal scraping (PublicSearch / neumo)
# ---------------------------------------------------------------------------

def _date_range(days: int = LOOKBACK_DAYS) -> Tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")


def _build_results_url(query: str, start: str, end: str, page_size: int = 50) -> str:
    """Best-effort direct URL into the results page (skips form interaction).

    PublicSearch surfaces query state in the URL; multiple parameter shapes have
    been observed across counties so we encode the most common one.
    """
    params = {
        "department": "RP",
        "searchType": "quickSearch",
        "searchValue": query,
        "recordedDateRange": f"{start},{end}",
        "searchOcrText": "false",
        "sort": "recordedDate",
        "sortDirection": "desc",
        "pageSize": str(page_size),
    }
    return f"{CLERK_URL}/results?{urlencode(params, quote_via=quote)}"


async def _accept_disclaimer(page) -> None:
    for sel in ('button:has-text("Accept")',
                'button:has-text("I Agree")',
                'button:has-text("Agree")',
                'button:has-text("Continue")',
                'button:has-text("OK")'):
        try:
            btn = page.locator(sel).first
            if await btn.count():
                await btn.click(timeout=2000)
                return
        except Exception:
            continue


async def _fill_form_search(page, query: str, start: str, end: str) -> bool:
    """Navigate to home, fill the search form, click Search. Returns success."""
    try:
        await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=PW_TIMEOUT)
    except PWTimeout:
        return False
    await _accept_disclaimer(page)

    # Search Term input — multiple selector strategies
    term_input = None
    for sel in ('input[placeholder*="Search Term" i]',
                'input[aria-label*="Search Term" i]',
                'input[name*="searchTerm" i]',
                'input[name*="searchValue" i]',
                'input[type="search"]',
                'input[placeholder*="Search" i]'):
        loc = page.locator(sel).first
        try:
            if await loc.count():
                term_input = loc
                break
        except Exception:
            continue
    if not term_input:
        return False

    try:
        await term_input.fill("")
        await term_input.type(query, delay=10)
    except Exception:
        return False

    # Date range — find the two date inputs near a "Date Range" label
    date_inputs = page.locator(
        'input[placeholder*="MM/DD" i], input[type="date"], '
        'input[aria-label*="date" i], input[name*="date" i]'
    )
    try:
        n = await date_inputs.count()
    except Exception:
        n = 0
    if n >= 2:
        try:
            await date_inputs.nth(0).fill("")
            await date_inputs.nth(0).type(start, delay=10)
            await date_inputs.nth(1).fill("")
            await date_inputs.nth(1).type(end, delay=10)
            await date_inputs.nth(1).press("Tab")
        except Exception:
            pass

    # Click search
    for sel in ('button:has-text("Search"):not(:has-text("Reset"))',
                'button[type="submit"]',
                'button:has-text("Search")'):
        try:
            btn = page.locator(sel).first
            if await btn.count():
                await btn.click(timeout=3000)
                break
        except Exception:
            continue

    try:
        await page.wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
    except PWTimeout:
        pass
    return True


async def _scrape_query(page, cat: str, label: str, query: str,
                        type_regex: str, start: str, end: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    type_re = re.compile(type_regex, re.I)
    start_d = datetime.strptime(start, "%m/%d/%Y").date()
    end_d = datetime.strptime(end, "%m/%d/%Y").date()

    # Strategy 1: direct URL
    url = _build_results_url(query, start, end)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PW_TIMEOUT)
        await _accept_disclaimer(page)
        await page.wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
    except PWTimeout:
        pass
    except Exception as e:
        log(f"  direct URL failed for {query!r}: {e}")

    # Detect "no rows visible" -> fall back to form interaction
    table_present = False
    try:
        await page.wait_for_selector("table tbody tr, [role='row']", timeout=10_000)
        table_present = True
    except PWTimeout:
        table_present = False

    if not table_present:
        ok = await _fill_form_search(page, query, start, end)
        if ok:
            try:
                await page.wait_for_selector("table tbody tr, [role='row']", timeout=15_000)
                table_present = True
            except PWTimeout:
                pass

    if not table_present:
        return rows

    # Try to crank page size to 100 if a Results-Per-Page selector exists
    for sel in ('select[aria-label*="results per page" i]',
                'select[name*="pageSize" i]',
                'select:has(option:has-text("100"))'):
        try:
            sel_loc = page.locator(sel).first
            if await sel_loc.count():
                await sel_loc.select_option(value="100")
                await page.wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
                break
        except Exception:
            continue

    # Paginate
    seen = set()
    for _ in range(15):  # safety cap
        try:
            html = await page.content()
        except Exception as e:
            log(f"  page.content() failed: {e}")
            break

        soup = BeautifulSoup(html, "lxml")
        new_rows = _parse_results_table(soup, page.url)

        # Pull clerk doc links via JS, since PublicSearch uses client-side
        # routing and the rendered HTML does NOT contain <a href="/doc/...">.
        try:
            row_links = await page.evaluate("""() => {
                const rows = Array.from(document.querySelectorAll('table tbody tr'));
                return rows.map(r => {
                    let a = r.querySelector('a[href*="/doc/"]');
                    if (a && a.href) return a.href;
                    const onclick = r.getAttribute('onclick') || '';
                    let m = onclick.match(/\\/doc\\/(\\d+)/);
                    if (m) return location.origin + '/doc/' + m[1];
                    const ds = r.dataset || {};
                    if (ds.docId)  return location.origin + '/doc/' + ds.docId;
                    if (ds.id)     return location.origin + '/doc/' + ds.id;
                    return '';
                });
            }""")
        except Exception as e:
            log(f"  link extraction failed: {e}")
            row_links = []

        for i, r in enumerate(new_rows):
            if i < len(row_links) and row_links[i]:
                r["clerk_url"] = row_links[i]
        added = 0
        for r in new_rows:
            # Filter by doc type pattern
            doctype_text = (r.get("doc_type") or "").upper()
            if doctype_text and not type_re.search(doctype_text):
                continue
            # Filter by date (client side safety net)
            f_iso = parse_date(r.get("filed", ""))
            if f_iso:
                try:
                    fd = datetime.strptime(f_iso, "%Y-%m-%d").date()
                    if fd < start_d or fd > end_d:
                        continue
                except ValueError:
                    pass
            key = (r.get("doc_num") or "", doctype_text)
            if key in seen:
                continue
            seen.add(key)
            rows.append(r)
            added += 1

        # Try Next
        moved = False
        for sel in ('button[aria-label="Next page" i]',
                    'a[aria-label="Next page" i]',
                    'button:has-text("Next"):not([disabled])',
                    'a:has-text("Next")'):
            try:
                nxt = page.locator(sel).first
                if not await nxt.count():
                    continue
                disabled = await nxt.get_attribute("disabled")
                aria_disabled = await nxt.get_attribute("aria-disabled")
                if disabled is not None or aria_disabled == "true":
                    continue
                await nxt.click(timeout=2500)
                await page.wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
                moved = True
                break
            except Exception:
                continue
        if not moved:
            break

    return rows


def _parse_results_table(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for tbl in soup.find_all("table"):
        ths = tbl.find_all("th")
        if not ths:
            continue
        head = [th.get_text(" ", strip=True).upper() for th in ths]
        if not any("DOC" in h or "GRANTOR" in h or "RECORDED" in h for h in head):
            continue

        # Build column index map
        idx = {h: i for i, h in enumerate(head)}
        def col(name_options: List[str]) -> int:
            for n in name_options:
                for h, i in idx.items():
                    if n in h:
                        return i
            return -1

        c_grantor  = col(["GRANTOR", "PARTY 1", "FROM"])
        c_grantee  = col(["GRANTEE", "PARTY 2", "TO"])
        c_doctype  = col(["DOC TYPE", "DOCUMENT TYPE", "INSTRUMENT TYPE", "TYPE"])
        c_recorded = col(["RECORDED DATE", "FILED", "FILE DATE", "RECORDED"])
        c_docnum   = col(["DOC NUMBER", "DOCUMENT", "INSTRUMENT", "DOC #", "DOC NO"])
        c_legal    = col(["LEGAL DESCRIPTION", "LEGAL"])
        c_amount   = col(["AMOUNT", "CONSIDERATION"])

        for tr in tbl.select("tbody tr"):
            cells = tr.find_all(["td"])
            if not cells:
                continue
            text_cells = [c.get_text(" ", strip=True) for c in cells]
            row: Dict[str, str] = {}
            def get(i: int) -> str:
                return text_cells[i] if 0 <= i < len(text_cells) else ""
            row["grantor"]   = get(c_grantor)
            row["grantee"]   = get(c_grantee)
            row["doc_type"]  = get(c_doctype)
            row["filed"]     = get(c_recorded)
            row["doc_num"]   = get(c_docnum)
            row["legal"]     = get(c_legal)
            row["amount"]    = get(c_amount)

            # link to detail
            link = ""
            a = tr.find("a", href=True)
            if a:
                link = urljoin(base_url, a["href"])
            row["clerk_url"] = link

            if row["doc_num"] or row["doc_type"]:
                rows.append(row)
        if rows:
            break
    return rows


async def scrape_clerk(start: str, end: str) -> List[Dict[str, str]]:
    if async_playwright is None:
        log("Playwright not available; skipping clerk scrape")
        return []

    all_rows: List[Dict[str, str]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=USER_AGENT,
                                        viewport={"width": 1480, "height": 920})
        page = await ctx.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        for cat, (label, _flag, queries) in DOC_TYPES.items():
            for q, type_regex in queries:
                async def factory(_q=q, _re=type_regex):
                    return await _scrape_query(page, cat, label, _q, _re, start, end)
                try:
                    rows = await aretry(factory, label=f"clerk {cat}/{q}")
                except Exception as e:
                    log(f"clerk query {cat}/{q!r} failed permanently: {e}")
                    rows = []
                for r in rows:
                    r["_cat"] = cat
                    r["_cat_label"] = label
                all_rows.extend(rows)
                log(f"  {cat:<9} {q:<40} -> {len(rows)} rows (kept after type+date filter)")

        await ctx.close()
        await browser.close()

    seen = set()
    deduped: List[Dict[str, str]] = []
    for r in all_rows:
        k = ((r.get("doc_num") or "").strip(), (r.get("_cat") or "").strip())
        if not k[0]:
            deduped.append(r)
            continue
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)

    log(f"Clerk scrape complete: {len(deduped)} unique rows (from {len(all_rows)} raw)")
    return deduped


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_lead(lead: Lead, *, new_this_week: bool = True) -> Tuple[int, List[str]]:
    score = 30
    flags: List[str] = []
    cat_flag = DOC_TYPES.get(lead.cat, (None, None, None))[1]
    if cat_flag and cat_flag not in flags:
        flags.append(cat_flag)
        score += 10
    has_lp = lead.cat in ("LP", "RELLP")
    has_fc = lead.cat == "NOFC"
    if has_lp and has_fc:
        score += 20
    if lead.amount > 100_000:
        score += 15
    elif lead.amount > 50_000:
        score += 10
    if new_this_week:
        flags.append("New this week")
        score += 5
    if lead.prop_address or lead.mail_address:
        score += 5
    if is_entity(lead.owner) and "LLC / corp owner" not in flags:
        flags.append("LLC / corp owner")
    return max(0, min(100, score)), flags


# ---------------------------------------------------------------------------
# Build leads
# ---------------------------------------------------------------------------

def split_owner_name(owner: str) -> Tuple[str, str]:
    n = (owner or "").strip()
    if not n:
        return "", ""
    if is_entity(n):
        return n, ""
    if "," in n:
        last, _, first = n.partition(",")
        return first.strip().title(), last.strip().title()
    parts = n.split()
    if len(parts) == 1:
        return "", parts[0].title()
    return " ".join(parts[:-1]).title(), parts[-1].title()


def build_leads(raw_rows: List[Dict[str, str]], parcels: ParcelLookup,
                start: str, end: str) -> List[Lead]:
    start_d = datetime.strptime(start, "%m/%d/%Y").date()
    end_d = datetime.strptime(end, "%m/%d/%Y").date()
    leads: List[Lead] = []
    for r in raw_rows:
        try:
            cat = r.get("_cat", "")
            cat_label = r.get("_cat_label") or DOC_TYPES.get(cat, ("",))[0]
            filed = parse_date(r.get("filed", ""))
            grantor_raw = (r.get("grantor") or "").strip()
            grantee_raw = (r.get("grantee") or "").strip()
            # Normalize "N/A" / "NONE" placeholders to empty so downstream
            # logic can pick the other party transparently.
            BAD_VALS = {"N/A", "NA", "NONE", "UNKNOWN", "-", "--"}
            if grantor_raw.upper() in BAD_VALS:
                grantor_raw = ""
            if grantee_raw.upper() in BAD_VALS:
                grantee_raw = ""
            # Drop rows with no usable party data at all
            if not grantor_raw and not grantee_raw:
                continue
            # For liens / judgments, the GRANTEE is the actual property owner.
            if cat in GRANTEE_AS_OWNER and grantee_raw:
                owner = grantee_raw
                grantee = grantor_raw  # show the creditor in the grantee column
            elif cat in GRANTEE_AS_OWNER and not grantee_raw and grantor_raw:
                # Fall back to grantor when grantee was a placeholder
                owner = grantor_raw
                grantee = ""
            else:
                owner = grantor_raw
                grantee = grantee_raw
            # Final guard: skip rows where the lead "owner" still ends up empty
            if not owner.strip():
                continue
            amount = parse_amount(r.get("amount", ""))

            new_this_week = True
            if filed:
                try:
                    fd = datetime.strptime(filed, "%Y-%m-%d").date()
                    new_this_week = start_d <= fd <= end_d
                except ValueError:
                    pass

            lead = Lead(
                doc_num=(r.get("doc_num") or "").strip(),
                doc_type=(r.get("doc_type") or cat_label).strip(),
                cat=cat,
                cat_label=cat_label,
                filed=filed,
                owner=owner,
                grantee=grantee,
                amount=amount,
                legal=(r.get("legal") or "").strip(),
                clerk_url=(r.get("clerk_url") or "").strip(),
            )

            if owner and parcels.loaded:
                hit = parcels.lookup(owner)
                if hit:
                    lead.prop_address = hit.get("site_addr", "")
                    lead.prop_city    = hit.get("site_city", "")
                    lead.prop_zip     = hit.get("site_zip", "")
                    lead.mail_address = hit.get("mail_addr", "")
                    lead.mail_city    = hit.get("mail_city", "")
                    lead.mail_state   = hit.get("mail_state", "") or "TX"
                    lead.mail_zip     = hit.get("mail_zip", "")

            score, flags = score_lead(lead, new_this_week=new_this_week)
            lead.score = score
            lead.flags = flags
            leads.append(lead)
        except Exception as e:
            log(f"build_leads skipped row: {e}")
            continue

    leads.sort(key=lambda x: (-x.score, x.filed or "", x.doc_num))
    return leads


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_records(leads: List[Lead], start: str, end: str) -> Dict[str, Any]:
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": f"{COUNTY} County, {STATE} — Clerk + DCAD",
        "date_range": {"start": parse_date(start), "end": parse_date(end)},
        "total": len(leads),
        "with_address": sum(1 for l in leads if l.prop_address or l.mail_address),
        "records": [l.to_dict() for l in leads],
    }
    for path in (DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log(f"Wrote {path} ({len(leads)} records)")
    return payload


GHL_HEADERS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def export_ghl_csv(leads: List[Lead], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(GHL_HEADERS)
        for l in leads:
            first, last = split_owner_name(l.owner)
            w.writerow([
                first, last,
                l.mail_address, l.mail_city, l.mail_state, l.mail_zip,
                l.prop_address, l.prop_city, l.prop_state, l.prop_zip,
                l.cat_label, l.doc_type, l.filed, l.doc_num,
                f"{l.amount:.2f}" if l.amount else "",
                l.score, "; ".join(l.flags),
                f"{COUNTY} County, {STATE}",
                l.clerk_url,
            ])
    log(f"GHL CSV exported: {out_path} ({len(leads)} rows)")
    return out_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def amain(args: argparse.Namespace) -> int:
    start, end = _date_range(args.lookback)
    log(f"Date range: {start} -> {end}")

    # Try bulk DBF first (fast, complete) — if that fails (paywalled etc),
    # fall through to per-property DCAD owner search after we have leads.
    dbf_path = await download_parcel_dbf_playwright()
    parcels = build_parcel_lookup_from_dbf(dbf_path) if dbf_path else ParcelLookup()
    if not parcels.loaded:
        log("Bulk parcel data unavailable; will use per-property DCAD search instead")

    raw = await scrape_clerk(start, end)
    leads = build_leads(raw, parcels, start, end)

    if not parcels.loaded and leads:
        await enrich_with_dcad(leads)
        # re-sort because scores changed
        leads.sort(key=lambda x: (-x.score, x.filed or "", x.doc_num))

    payload = write_records(leads, start, end)

    if args.ghl:
        export_ghl_csv(leads, ROOT / "data" / "ghl_export.csv")

    log(f"Done. {payload['total']} leads, {payload['with_address']} with address.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Denton County motivated seller scraper")
    parser.add_argument("--lookback", type=int, default=LOOKBACK_DAYS,
                        help="Days back from today (default: %(default)s)")
    parser.add_argument("--ghl", action="store_true",
                        help="Also write data/ghl_export.csv")
    args = parser.parse_args()
    try:
        return asyncio.run(amain(args))
    except KeyboardInterrupt:
        log("Interrupted")
        return 130
    except Exception as e:
        log(f"FATAL: {e}")
        empty = {
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source": f"{COUNTY} County, {STATE} — Clerk + DCAD",
            "date_range": {"start": "", "end": ""},
            "total": 0, "with_address": 0, "records": [], "error": str(e),
        }
        for path in (DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(empty, indent=2), encoding="utf-8")
        return 1


if __name__ == "__main__":
    sys.exit(main())
