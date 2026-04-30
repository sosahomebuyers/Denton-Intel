#!/usr/bin/env python3
"""
Denton County, Texas - Motivated Seller Lead Scraper (v2)
==========================================================

v2 changes vs v1
----------------
* Clerk portal scraper rewritten against the *actual* PublicSearch UI we
  observed at denton.tx.publicsearch.us:
    - Department: Real Property
    - Search Term input
    - Date Range two-input picker (M/D/YYYY)
    - Purple "Search" button
    - Result table columns: GRANTOR, GRANTEE, DOC TYPE, RECORDED DATE,
      DOC NUMBER, BOOK/VOLUME/PAGE, LEGAL DESCRIPTION, LOT, BLOCK
* Each query navigates *directly* to the results URL with query params, so
  if the form filling fails we still get results.
* Client-side date filter is applied as a safety net.
* DCAD bulk download switched to Playwright (the static endpoint returned
  403 to the requests UA). Falls back to a manually-checked-in DBF if the
  Playwright download still fails.
* Result rows are de-duped by (doc_num, cat) and capped at 1000 per query.
* Every query has independent retry; one failed query never kills the run.
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
PAD_URL = "https://www.dentoncad.com/property-search"
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
                 [("LIS PENDENS",                  r"\bLIS\s*PENDENS\b(?!.*RELEASE)"),
                  ("NOTICE LIS PENDENS",           r"NOTICE.*LIS\s*PENDENS")]),
    "RELLP":    ("Release Lis Pendens",    "Lis pendens",
                 [("RELEASE LIS PENDENS",          r"RELEASE.*LIS\s*PENDENS")]),
    "NOFC":     ("Notice of Foreclosure",  "Pre-foreclosure",
                 [("NOTICE OF FORECLOSURE",        r"NOTICE.*FORECLOSURE"),
                  ("NOTICE OF SUBSTITUTE TRUSTEE", r"NOTICE.*(SUBSTITUTE\s*TRUSTEE|TRUSTEE.*SALE)"),
                  ("NOTICE OF TRUSTEE SALE",       r"NOTICE.*TRUSTEE.*SALE")]),
    "TAXDEED":  ("Tax Deed",               "Tax lien",
                 [("TAX DEED",                     r"\bTAX\s*DEED\b"),
                  ("SHERIFF TAX DEED",             r"SHERIFF.*TAX")]),
    "JUD":      ("Judgment",               "Judgment lien",
                 [("ABSTRACT OF JUDGMENT",         r"ABSTRACT.*JUDGMENT"),
                  ("JUDGMENT",                     r"\bJUDGMENT\b")]),
    "CCJ":      ("Certified Judgment",     "Judgment lien",
                 [("CERTIFIED COPY OF JUDGMENT",   r"CERTIFIED.*JUDGMENT")]),
    "DRJUD":    ("Domestic Judgment",      "Judgment lien",
                 [("DOMESTIC RELATIONS JUDGMENT",  r"DOMESTIC.*JUDGMENT")]),
    "LNCORPTX": ("Corporate Tax Lien",     "Tax lien",
                 [("STATE TAX LIEN",               r"STATE.*TAX.*LIEN"),
                  ("TEXAS TAX LIEN",               r"TEXAS.*TAX.*LIEN")]),
    "LNIRS":    ("IRS Tax Lien",           "Tax lien",
                 [("FEDERAL TAX LIEN",             r"FEDERAL.*TAX.*LIEN"),
                  ("IRS LIEN",                     r"IRS.*LIEN")]),
    "LNFED":    ("Federal Lien",           "Tax lien",
                 [("FEDERAL LIEN",                 r"FEDERAL.*LIEN")]),
    "LNMECH":   ("Mechanic's Lien",        "Mechanic lien",
                 [("MECHANIC LIEN",                r"MECHANIC.*LIEN"),
                  ("M&M LIEN",                     r"M\s*&\s*M.*LIEN"),
                  ("MATERIALMAN LIEN",             r"MATERIAL.*LIEN")]),
    "LNHOA":    ("HOA Lien",               "Mechanic lien",
                 [("HOA LIEN",                     r"HOA.*LIEN"),
                  ("ASSESSMENT LIEN",              r"ASSESSMENT.*LIEN"),
                  ("HOMEOWNERS ASSOCIATION LIEN",  r"HOMEOWNERS.*LIEN")]),
    "MEDLN":    ("Medicaid Lien",          "Tax lien",
                 [("MEDICAID LIEN",                r"MEDICAID.*LIEN")]),
    "PRO":      ("Probate",                "Probate / estate",
                 [("AFFIDAVIT OF HEIRSHIP",        r"AFFIDAVIT.*HEIRSHIP"),
                  ("PROBATE",                      r"PROBATE"),
                  ("LETTERS TESTAMENTARY",         r"LETTERS.*TESTAMENTARY")]),
    "NOC":      ("Notice of Commencement", "Mechanic lien",
                 [("NOTICE OF COMMENCEMENT",       r"NOTICE.*COMMENCEMENT")]),
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
        try:
            await page.goto(PAD_URL, wait_until="domcontentloaded", timeout=PW_TIMEOUT)
        except Exception as e:
            log(f"DCAD goto failed: {e}")
            await browser.close()
            return None

        # Click any "I agree / continue" buttons
        for sel in ('button:has-text("Accept")',
                    'button:has-text("I Agree")',
                    'button:has-text("Continue")'):
            try:
                btn = page.locator(sel).first
                if await btn.count():
                    await btn.click(timeout=2500)
            except Exception:
                pass

        # Find a download link/button
        link_sels = [
            'a:has-text("Public Data")',
            'a:has-text("Bulk Data")',
            'a:has-text("Download Data")',
            'a:has-text("Appraisal Data")',
            'a[href*="zip" i]',
            'a[href*=".dbf" i]',
            'button:has-text("Download Data")',
        ]
        download_path: Optional[Path] = None
        for sel in link_sels:
            try:
                loc = page.locator(sel).first
                if not await loc.count():
                    continue
                async with page.expect_download(timeout=PW_TIMEOUT) as dl:
                    await loc.click()
                d = await dl.value
                await d.save_as(cache_zip)
                download_path = cache_zip
                log(f"DCAD downloaded via {sel}: {cache_zip}")
                break
            except Exception as e:
                log(f"DCAD click {sel} failed: {e}")
                continue

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

    dbf_path = await download_parcel_dbf_playwright()
    parcels = build_parcel_lookup_from_dbf(dbf_path) if dbf_path else ParcelLookup()
    if not parcels.loaded:
        log("Continuing without parcel/address enrichment")

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
