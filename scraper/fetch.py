#!/usr/bin/env python3
"""
Denton County, Texas - Motivated Seller Lead Scraper
=====================================================
Scrapes the Denton County Clerk public records portal for motivated-seller
indicator filings (lis pendens, pre-foreclosure, tax / mechanic liens,
probate, judgments, etc.), enriches each lead with parcel/owner mailing
address data from the Denton Central Appraisal District (DCAD) bulk DBF
download, scores each record 0-100, and writes:
    dashboard/records.json
    data/records.json

Also exposes a CLI flag (--ghl) to emit a GoHighLevel-ready CSV.

Design notes
------------
* Clerk portal is JavaScript-driven (PublicSearch / Tyler) -> Playwright async.
* DCAD property search posts back via __doPostBack to download a ZIP that
  contains a DBF file -> requests + BeautifulSoup, parsed with dbfread.
* All network/parse code is wrapped in retry-with-backoff and per-record
  try/except so a single bad row never crashes the run.
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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
except ImportError:  # pragma: no cover - dbfread is in requirements.txt
    DBF = None  # type: ignore

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:  # pragma: no cover
    async_playwright = None  # type: ignore
    PWTimeout = Exception  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

COUNTY = "Denton"
STATE = "TX"
CLERK_URL = "https://denton.tx.publicsearch.us"
PAD_URL = "https://www.dentoncad.com/property-search"
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
MAX_RETRIES = 3
RETRY_BACKOFF = 2.5  # seconds, exponential
HTTP_TIMEOUT = 60
PW_TIMEOUT = 45_000  # ms

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR = ROOT / "data"
CACHE_DIR = ROOT / ".cache"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Document type taxonomy
# ---------------------------------------------------------------------------
# Each entry: (full label, motivated-seller flag label, search query terms)
DOC_TYPES: Dict[str, Tuple[str, str, List[str]]] = {
    "LP":       ("Lis Pendens",            "Lis pendens",      ["LIS PENDENS", "NOTICE LIS PENDENS"]),
    "NOFC":     ("Notice of Foreclosure",  "Pre-foreclosure",  ["NOTICE OF FORECLOSURE", "NOTICE OF SUBSTITUTE TRUSTEE", "NOTICE OF TRUSTEE SALE"]),
    "TAXDEED":  ("Tax Deed",               "Tax lien",         ["TAX DEED", "TAX SALE DEED", "SHERIFF TAX DEED"]),
    "JUD":      ("Judgment",               "Judgment lien",    ["JUDGMENT", "ABSTRACT OF JUDGMENT"]),
    "CCJ":      ("Certified Judgment",     "Judgment lien",    ["CERTIFIED JUDGMENT", "CERTIFIED COPY OF JUDGMENT"]),
    "DRJUD":    ("Domestic Judgment",      "Judgment lien",    ["DOMESTIC JUDGMENT", "DOMESTIC RELATIONS JUDGMENT"]),
    "LNCORPTX": ("Corporate Tax Lien",     "Tax lien",         ["STATE TAX LIEN", "TEXAS TAX LIEN", "CORPORATE TAX LIEN"]),
    "LNIRS":    ("IRS Tax Lien",           "Tax lien",         ["IRS LIEN", "FEDERAL TAX LIEN", "NOTICE OF FEDERAL TAX LIEN"]),
    "LNFED":    ("Federal Lien",           "Tax lien",         ["FEDERAL LIEN"]),
    "LN":       ("Lien",                   "Mechanic lien",    ["LIEN"]),
    "LNMECH":   ("Mechanic's Lien",        "Mechanic lien",    ["MECHANIC LIEN", "MECHANICS LIEN", "MATERIALMAN LIEN", "M&M LIEN"]),
    "LNHOA":    ("HOA Lien",               "Mechanic lien",    ["HOA LIEN", "ASSESSMENT LIEN", "HOMEOWNERS ASSOCIATION LIEN"]),
    "MEDLN":    ("Medicaid Lien",          "Tax lien",         ["MEDICAID LIEN", "MERP LIEN"]),
    "PRO":      ("Probate",                "Probate / estate", ["PROBATE", "AFFIDAVIT OF HEIRSHIP", "LETTERS TESTAMENTARY", "SMALL ESTATE AFFIDAVIT"]),
    "NOC":      ("Notice of Commencement", "Mechanic lien",    ["NOTICE OF COMMENCEMENT"]),
    "RELLP":    ("Release of Lis Pendens", "Lis pendens",      ["RELEASE LIS PENDENS", "RELEASE OF LIS PENDENS"]),
}


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
    except Exception as e:  # noqa: BLE001
        log(f"safe() suppressed {fn.__name__}: {e}")
        return default


def retry(times: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF):
    """Sync retry decorator."""
    def deco(fn):
        def wrap(*args, **kwargs):
            last = None
            for i in range(times):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:  # noqa: BLE001
                    last = e
                    sleep = backoff ** i
                    log(f"{fn.__name__} attempt {i+1}/{times} failed: {e} — retrying in {sleep:.1f}s")
                    time.sleep(sleep)
            raise last  # type: ignore[misc]
        return wrap
    return deco


async def aretry(coro_factory, times: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF, label: str = ""):
    """Async retry helper. Pass a zero-arg lambda that returns a coroutine."""
    last = None
    for i in range(times):
        try:
            return await coro_factory()
        except Exception as e:  # noqa: BLE001
            last = e
            sleep = backoff ** i
            log(f"[async] {label} attempt {i+1}/{times} failed: {e} — retrying in {sleep:.1f}s")
            await asyncio.sleep(sleep)
    raise last  # type: ignore[misc]


_money_re = re.compile(r"[-+]?\$?\s*([0-9][0-9,]*(?:\.\d+)?)")

def parse_amount(text: str) -> float:
    if not text:
        return 0.0
    m = _money_re.search(text.replace(",", ","))
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def parse_date(text: str) -> str:
    """Return ISO yyyy-mm-dd or ''. Accepts a wide range of US date formats."""
    if not text:
        return ""
    text = text.strip()
    fmts = ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y")
    for f in fmts:
        try:
            return datetime.strptime(text, f).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # ISO-ish with time
    try:
        return datetime.fromisoformat(text.split("T")[0]).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return ""


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"\s+", " ", name).strip().upper()
    # Strip common entity / title noise for matching only (preserve original elsewhere)
    return s


def name_variants(name: str) -> List[str]:
    """Generate "FIRST LAST", "LAST FIRST", "LAST, FIRST" variants."""
    n = normalize_name(name)
    if not n:
        return []
    out = {n}
    if "," in n:
        # already LAST, FIRST
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
    n = name.upper()
    markers = (" LLC", " L.L.C", " INC", " INC.", " CORP", " CO.", " COMPANY",
               " LP", " L.P", " LLP", " TRUST", " ESTATE", " BANK", " ASSOCIATION",
               " HOA", " PARTNERS", " PARTNERSHIP", " HOLDINGS", " GROUP",
               " ENTERPRISES", " VENTURES")
    return any(m in f" {n} " for m in markers)


# ---------------------------------------------------------------------------
# Property Appraiser (DCAD) — bulk DBF download via __doPostBack
# ---------------------------------------------------------------------------

class ParcelLookup:
    """In-memory owner-name -> mailing/site address lookup built from DCAD DBF."""

    def __init__(self) -> None:
        self.by_name: Dict[str, Dict[str, str]] = {}
        self.loaded = False

    def add(self, rec: Dict[str, str]) -> None:
        owner = rec.get("owner", "")
        if not owner:
            return
        for v in name_variants(owner):
            # First write wins; later collisions don't overwrite (keeps most-recent first parcel)
            self.by_name.setdefault(v, rec)

    def lookup(self, owner: str) -> Optional[Dict[str, str]]:
        for v in name_variants(owner):
            hit = self.by_name.get(v)
            if hit:
                return hit
        # Fuzzy fallback: last-name token match if first/last fully reversed unknown
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
    # Case-insensitive fallback
    upper = {str(k).upper(): v for k, v in rec.items()}
    for k in keys:
        v = upper.get(k.upper())
        if v not in (None, ""):
            return str(v).strip()
    return ""


@retry()
def _get(session: requests.Session, url: str, **kw) -> requests.Response:
    kw.setdefault("timeout", HTTP_TIMEOUT)
    kw.setdefault("headers", {"User-Agent": USER_AGENT})
    r = session.get(url, **kw)
    r.raise_for_status()
    return r


@retry()
def _post(session: requests.Session, url: str, data: Dict[str, str], **kw) -> requests.Response:
    kw.setdefault("timeout", HTTP_TIMEOUT)
    kw.setdefault("headers", {"User-Agent": USER_AGENT})
    r = session.post(url, data=data, **kw)
    r.raise_for_status()
    return r


def _aspx_state(html: str) -> Dict[str, str]:
    """Extract __VIEWSTATE / __EVENTVALIDATION / __VIEWSTATEGENERATOR fields."""
    soup = BeautifulSoup(html, "lxml")
    fields: Dict[str, str] = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION", "__VIEWSTATEENCRYPTED"):
        el = soup.find("input", {"name": name})
        if el and el.get("value") is not None:
            fields[name] = el.get("value", "")
    return fields


def download_parcel_dbf() -> Optional[Path]:
    """Download DCAD bulk DBF parcel file. Cached daily.

    DCAD's property-search page exposes a "Download Public Data" link that fires
    a __doPostBack to stream a ZIP containing parcel DBF files.  The exact event
    target name has shifted over time; we discover it by scraping the page first.
    """
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    cache_zip = CACHE_DIR / f"dcad_parcels_{today}.zip"
    extract_dir = CACHE_DIR / f"dcad_parcels_{today}"

    if extract_dir.exists():
        for p in extract_dir.rglob("*.dbf"):
            log(f"DCAD DBF cache hit: {p}")
            return p

    sess = requests.Session()
    sess.headers["User-Agent"] = USER_AGENT
    try:
        r = _get(sess, PAD_URL)
    except Exception as e:  # noqa: BLE001
        log(f"DCAD page fetch failed: {e}")
        return None

    state = _aspx_state(r.text)
    soup = BeautifulSoup(r.text, "lxml")

    # Find the __doPostBack target whose surrounding text mentions "public data" / "DBF" / "download"
    target = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text(" ", strip=True) or "").lower()
        if "dopostback" not in href.lower():
            continue
        if any(kw in text for kw in ("public data", "bulk", "dbf", "download data", "appraisal data")):
            m = re.search(r"__doPostBack\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]*)['\"]", href)
            if m:
                target = (m.group(1), m.group(2))
                break

    # Fallback to a known control name if discovery fails
    if not target:
        target = ("ctl00$ContentPlaceHolder1$lnkDownloadData", "")

    payload = {
        "__EVENTTARGET": target[0],
        "__EVENTARGUMENT": target[1],
        **state,
    }

    try:
        resp = sess.post(PAD_URL, data=payload, timeout=HTTP_TIMEOUT, stream=True,
                         headers={"User-Agent": USER_AGENT, "Referer": PAD_URL})
        resp.raise_for_status()
    except Exception as e:  # noqa: BLE001
        log(f"DCAD postback download failed: {e}")
        return None

    ctype = resp.headers.get("Content-Type", "").lower()
    if "zip" not in ctype and "octet-stream" not in ctype and "application" not in ctype:
        log(f"DCAD response not a zip (Content-Type={ctype}); skipping parcel enrichment")
        return None

    cache_zip.write_bytes(resp.content)
    try:
        with zipfile.ZipFile(cache_zip) as zf:
            zf.extractall(extract_dir)
    except zipfile.BadZipFile:
        log("DCAD response was not a valid zip — skipping enrichment")
        return None

    for p in extract_dir.rglob("*.dbf"):
        log(f"DCAD DBF extracted: {p}")
        return p

    log("DCAD zip contained no DBF — skipping enrichment")
    return None


def build_parcel_lookup() -> ParcelLookup:
    lookup = ParcelLookup()
    if DBF is None:
        log("dbfread not installed — skipping parcel enrichment")
        return lookup

    dbf_path = safe(download_parcel_dbf)
    if not dbf_path:
        return lookup

    try:
        table = DBF(str(dbf_path), load=False, ignore_missing_memofile=True, encoding="latin-1")
    except Exception as e:  # noqa: BLE001
        log(f"Failed to open DBF {dbf_path}: {e}")
        return lookup

    count = 0
    for row in table:
        try:
            rec = {
                "owner": _pick(row, "OWNER", "OWN1", "OWNER1", "OWNERNAME"),
                "site_addr": _pick(row, "SITE_ADDR", "SITEADDR", "SITUS_ADDR", "SITUSADDR"),
                "site_city": _pick(row, "SITE_CITY", "SITUS_CITY", "SITECITY"),
                "site_zip":  _pick(row, "SITE_ZIP",  "SITUS_ZIP",  "SITEZIP"),
                "mail_addr": _pick(row, "ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILADDR", "MAILING_AD"),
                "mail_city": _pick(row, "CITY", "MAILCITY", "MAIL_CITY"),
                "mail_state":_pick(row, "STATE", "MAILSTATE", "MAIL_STATE"),
                "mail_zip":  _pick(row, "ZIP", "MAILZIP", "MAIL_ZIP", "ZIPCODE"),
            }
            if rec["owner"]:
                lookup.add(rec)
                count += 1
        except Exception as e:  # noqa: BLE001
            log(f"DBF row skipped: {e}")
            continue

    log(f"Built parcel lookup with {count:,} owner records ({len(lookup.by_name):,} name variants)")
    lookup.loaded = count > 0
    return lookup


# ---------------------------------------------------------------------------
# Clerk Portal scraping (Playwright async)
# ---------------------------------------------------------------------------

def _date_range(days: int = LOOKBACK_DAYS) -> Tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")


async def _scrape_query(page, query: str, start: str, end: str) -> List[Dict[str, str]]:
    """Run a single keyword query against the PublicSearch portal and return raw rows."""
    rows: List[Dict[str, str]] = []
    try:
        await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=PW_TIMEOUT)
    except PWTimeout:
        log(f"clerk goto timeout for query={query!r}")
        return rows

    # Dismiss disclaimer / cookie banner if present
    for sel in ('button:has-text("Accept")',
                'button:has-text("I Agree")',
                'button:has-text("Agree")',
                'button:has-text("Continue")',
                'button:has-text("OK")'):
        try:
            btn = page.locator(sel).first
            if await btn.count():
                await btn.click(timeout=2500)
                break
        except Exception:  # noqa: BLE001
            pass

    # Find the global search input
    search_box = None
    for sel in ('input[type="search"]',
                'input[placeholder*="Search" i]',
                'input[name*="search" i]',
                'input[aria-label*="search" i]'):
        loc = page.locator(sel).first
        try:
            if await loc.count():
                search_box = loc
                break
        except Exception:  # noqa: BLE001
            continue

    if not search_box:
        log("Could not locate clerk search input")
        return rows

    try:
        await search_box.fill("")
        await search_box.type(query, delay=15)
        await search_box.press("Enter")
    except Exception as e:  # noqa: BLE001
        log(f"search fill/submit failed for {query!r}: {e}")
        return rows

    # Apply date range filter if visible
    try:
        await page.wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
    except PWTimeout:
        pass

    # Try to set "Filed Date From / To" filters
    for label, value in (("from", start), ("to", end)):
        for sel in (f'input[name*="{label}" i]',
                    f'input[placeholder*="{label}" i]',
                    f'input[aria-label*="{label}" i]'):
            try:
                loc = page.locator(sel).first
                if await loc.count():
                    await loc.fill("")
                    await loc.type(value, delay=10)
                    await loc.press("Tab")
                    break
            except Exception:  # noqa: BLE001
                continue

    # Click apply / search again if a button exists
    for sel in ('button:has-text("Apply")',
                'button:has-text("Search")',
                'button:has-text("Filter")'):
        try:
            btn = page.locator(sel).first
            if await btn.count():
                await btn.click(timeout=2000)
                break
        except Exception:  # noqa: BLE001
            pass

    try:
        await page.wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
    except PWTimeout:
        pass

    # Paginate through up to N pages
    seen = set()
    for page_idx in range(1, 11):
        # Extract rows from the results table.  PublicSearch UIs all expose results
        # as a <table> with a <tbody>, or as a list of <a> rows linking to /doc/<id>.
        try:
            html = await page.content()
        except Exception as e:  # noqa: BLE001
            log(f"page.content() failed: {e}")
            break

        soup = BeautifulSoup(html, "lxml")
        new_rows = _parse_clerk_results(soup, page.url)
        added = 0
        for r in new_rows:
            key = (r.get("doc_num") or "", r.get("filed") or "", r.get("doc_type") or "")
            if key in seen:
                continue
            seen.add(key)
            rows.append(r)
            added += 1

        # Try Next button
        moved = False
        for sel in ('button[aria-label="Next page"]',
                    'button:has-text("Next")',
                    'a[aria-label="Next"]',
                    'a:has-text("Next")'):
            try:
                nxt = page.locator(sel).first
                if await nxt.count() and await nxt.is_enabled():
                    await nxt.click(timeout=2500)
                    await page.wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
                    moved = True
                    break
            except Exception:  # noqa: BLE001
                continue
        if not moved or added == 0:
            break

    return rows


def _parse_clerk_results(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    # Style 1: HTML table
    for tbl in soup.find_all("table"):
        head_cells = [c.get_text(" ", strip=True).lower() for c in tbl.find_all("th")]
        if not head_cells:
            continue
        if not any("doc" in h or "instrument" in h or "filed" in h for h in head_cells):
            continue
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["td"])
            if not cells:
                continue
            text_cells = [c.get_text(" ", strip=True) for c in cells]
            link = ""
            a = tr.find("a", href=True)
            if a:
                link = urljoin(base_url, a["href"])
            row = _row_from_cells(head_cells, text_cells, link)
            if row.get("doc_num") or row.get("doc_type"):
                rows.append(row)

    # Style 2: card/list result items linking to /doc/<id>
    if not rows:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/doc/" in href or "/document/" in href:
                container = a.find_parent(["li", "div", "article"]) or a
                text = container.get_text(" | ", strip=True)
                row = _row_from_text_blob(text, urljoin(base_url, href))
                if row.get("doc_num") or row.get("doc_type"):
                    rows.append(row)

    return rows


def _row_from_cells(head: List[str], cells: List[str], link: str) -> Dict[str, str]:
    out: Dict[str, str] = {"clerk_url": link}
    for h, v in zip(head, cells):
        h2 = h.strip().lower()
        if "doc" in h2 and ("num" in h2 or "#" in h2 or "instrument" in h2):
            out["doc_num"] = v
        elif "type" in h2 or "kind" in h2:
            out["doc_type"] = v
        elif "filed" in h2 or "record" in h2 or "date" in h2:
            out.setdefault("filed", v)
        elif "grantor" in h2 or "from" in h2 or "party 1" in h2:
            out["grantor"] = v
        elif "grantee" in h2 or "to" in h2 or "party 2" in h2:
            out["grantee"] = v
        elif "legal" in h2 or "description" in h2:
            out["legal"] = v
        elif "amount" in h2 or "consideration" in h2:
            out["amount"] = v
    return out


def _row_from_text_blob(text: str, link: str) -> Dict[str, str]:
    out: Dict[str, str] = {"clerk_url": link}
    # Doc number: strings like 2024-12345 / 2024R-12345 / 12345678
    m = re.search(r"\b(20\d{2}[-R]?\d{4,8}|\d{7,12})\b", text)
    if m:
        out["doc_num"] = m.group(1)
    # Date
    m = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", text)
    if m:
        out["filed"] = m.group(0)
    # Doc type guess: take ALLCAPS chunk
    m = re.search(r"\b([A-Z][A-Z &/'\-]{4,})\b", text)
    if m:
        out["doc_type"] = m.group(1).strip()
    out["grantor"] = ""
    out["grantee"] = ""
    return out


async def scrape_clerk(start: str, end: str) -> List[Dict[str, str]]:
    if async_playwright is None:
        log("Playwright not available — skipping clerk scrape")
        return []

    all_rows: List[Dict[str, str]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1400, "height": 900},
        )
        page = await context.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        for cat, (label, _flag, queries) in DOC_TYPES.items():
            for q in queries:
                async def factory(_q=q):
                    return await _scrape_query(page, _q, start, end)
                try:
                    rows = await aretry(factory, label=f"clerk {cat}/{q}")
                except Exception as e:  # noqa: BLE001
                    log(f"clerk query {cat}/{q!r} failed permanently: {e}")
                    rows = []
                for r in rows:
                    r["_cat"] = cat
                    r["_cat_label"] = label
                all_rows.extend(rows)
                log(f"  {cat:<9} {q:<40} -> {len(rows)} rows")

        await context.close()
        await browser.close()

    # De-dupe by (doc_num, doc_type)
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

    # Category-derived flag
    cat_flag = DOC_TYPES.get(lead.cat, (None, None, None))[1]
    if cat_flag and cat_flag not in flags:
        flags.append(cat_flag)
        score += 10

    # LP + FC combo
    has_lp = "Lis pendens" in flags or lead.cat in ("LP", "RELLP")
    has_fc = lead.cat == "NOFC" or "Pre-foreclosure" in flags
    if has_lp and has_fc:
        score += 20

    # Amount tiers
    if lead.amount > 100_000:
        score += 15
    elif lead.amount > 50_000:
        score += 10

    if new_this_week:
        if "New this week" not in flags:
            flags.append("New this week")
        score += 5

    if lead.prop_address or lead.mail_address:
        score += 5

    if is_entity(lead.owner) and "LLC / corp owner" not in flags:
        flags.append("LLC / corp owner")

    score = max(0, min(100, score))
    return score, flags


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
            cat_label = r.get("_cat_label", DOC_TYPES.get(cat, ("",))[0])
            filed = parse_date(r.get("filed", ""))
            owner = (r.get("grantor") or "").strip()
            grantee = (r.get("grantee") or "").strip()
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

            # Parcel enrichment
            if owner and parcels.loaded:
                hit = parcels.lookup(owner)
                if hit:
                    lead.prop_address = hit.get("site_addr", "")
                    lead.prop_city = hit.get("site_city", "")
                    lead.prop_zip = hit.get("site_zip", "")
                    lead.mail_address = hit.get("mail_addr", "")
                    lead.mail_city = hit.get("mail_city", "")
                    lead.mail_state = hit.get("mail_state", "") or "TX"
                    lead.mail_zip = hit.get("mail_zip", "")

            score, flags = score_lead(lead, new_this_week=new_this_week)
            lead.score = score
            lead.flags = flags
            leads.append(lead)
        except Exception as e:  # noqa: BLE001
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


# ---------------------------------------------------------------------------
# GoHighLevel CSV export
# ---------------------------------------------------------------------------

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

    parcels = build_parcel_lookup()

    raw = await scrape_clerk(start, end)

    leads = build_leads(raw, parcels, start, end)
    payload = write_records(leads, start, end)

    if args.ghl:
        export_ghl_csv(leads, ROOT / "data" / "ghl_export.csv")

    log(f"Done. {payload['total']} leads, {payload['with_address']} with address.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Denton County motivated seller scraper")
    parser.add_argument("--lookback", type=int, default=LOOKBACK_DAYS,
                        help="Days to look back from today (default: %(default)s)")
    parser.add_argument("--ghl", action="store_true",
                        help="Also write data/ghl_export.csv")
    args = parser.parse_args()

    try:
        return asyncio.run(amain(args))
    except KeyboardInterrupt:
        log("Interrupted")
        return 130
    except Exception as e:  # noqa: BLE001
        log(f"FATAL: {e}")
        # Always emit empty records so the dashboard never breaks
        empty = {
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source": f"{COUNTY} County, {STATE} — Clerk + DCAD",
            "date_range": {"start": "", "end": ""},
            "total": 0,
            "with_address": 0,
            "records": [],
            "error": str(e),
        }
        for path in (DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(empty, indent=2), encoding="utf-8")
        return 1


if __name__ == "__main__":
    sys.exit(main())
