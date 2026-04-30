# Denton County, TX — Motivated Seller Lead Intel

Automated scraper that pulls motivated-seller indicator filings from the
Denton County Clerk public records portal, enriches each record with
mailing/site address data from the Denton Central Appraisal District (DCAD)
bulk parcel DBF, scores every lead 0–100, and publishes a live dashboard.

## Folder structure

```
denton-intel/
├── scraper/
│   ├── fetch.py
│   └── requirements.txt
├── dashboard/
│   ├── index.html
│   └── records.json
├── data/
│   └── records.json
├── .github/
│   └── workflows/
│       └── scrape.yml
└── README.md
```

## What gets collected

| Code      | Description                                          |
|-----------|------------------------------------------------------|
| LP        | Lis Pendens                                          |
| NOFC      | Notice of Foreclosure                                |
| TAXDEED   | Tax Deed                                             |
| JUD/CCJ/DRJUD | Judgment / Certified / Domestic Relations        |
| LNCORPTX  | Texas Corporate Tax Lien                             |
| LNIRS     | IRS / Federal Tax Lien                               |
| LNFED     | Federal Lien                                         |
| LN/LNMECH/LNHOA | Lien / Mechanic's / HOA                        |
| MEDLN     | Medicaid Lien                                        |
| PRO       | Probate, Affidavit of Heirship, Letters Testamentary |
| NOC       | Notice of Commencement                               |
| RELLP     | Release of Lis Pendens                               |

## Local quickstart

```bash
cd denton-intel
python3 -m venv .venv && source .venv/bin/activate
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
python scraper/fetch.py --ghl
```

Outputs:

- `dashboard/records.json` — drives the live dashboard
- `data/records.json` — same payload, archived alongside source data
- `data/ghl_export.csv` — GoHighLevel-ready import (only when `--ghl`)

Open `dashboard/index.html` directly in your browser to view the dashboard
locally.

## CLI flags

```
python scraper/fetch.py --help
  --lookback N    Days back from today (default: 7)
  --ghl           Also write data/ghl_export.csv
```

The `LOOKBACK_DAYS` environment variable overrides the default.

## GitHub Actions deploy

1. Push this folder to a new GitHub repo (no `.git` is included — `git init`
   and push to a fresh remote).
2. Repo **Settings → Pages → Source → GitHub Actions**.
3. The workflow at `.github/workflows/scrape.yml` runs daily at `07:00 UTC`
   (or on demand from the Actions tab), commits refreshed JSON/CSV back to
   the repo, and redeploys the dashboard to GitHub Pages.

## Scoring

Base 30, plus:

- +10 per motivated-seller flag
- +20 if Lis Pendens **and** Notice of Foreclosure both present
- +15 if amount > $100,000
- +10 if amount > $50,000
- +5 if filed inside the lookback window ("New this week")
- +5 if a property or mailing address was matched

Flags: *Lis pendens, Pre-foreclosure, Judgment lien, Tax lien, Mechanic
lien, Probate / estate, LLC / corp owner, New this week*.

## Resilience

- 3-attempt retry with exponential backoff on every HTTP/Playwright call
- Per-record `try/except` so a single malformed row never aborts the run
- DCAD column resolution is tolerant of `OWNER`/`OWN1`, `SITE_ADDR`/`SITEADDR`,
  `ADDR_1`/`MAILADR1`, etc.
- On fatal failure the scraper still writes empty `records.json` files so
  the dashboard never breaks
