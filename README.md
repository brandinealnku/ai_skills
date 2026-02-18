# AI Skills Demand Dashboard (Sheets + Apps Script + Worker + GitHub Pages)

This repo implements the exact architecture:

**Apps Script (scheduled)** → writes aggregated tables to **Google Sheets** → **Cloudflare Worker** reads `kpis` and serves CORS JSON → **GitHub Pages frontend** renders Chart.js dashboard and auto-refreshes every 10 minutes.

---

## 1) Google Sheets schema (tabs + columns)

Create one Google Sheet and add these tabs with headers in row 1.

### `raw_jobs` (append-only; keep last 90 days)
| Column |
|---|
| ingested_at |
| posted_date |
| source |
| source_id |
| url |
| title |
| company |
| location |
| region |
| description |
| job_family |
| skills_json |

### `skills_daily`
| Column |
|---|
| date |
| skill |
| count |
| source |
| region |

### `titles_daily`
| Column |
|---|
| date |
| job_family |
| count |
| region |

### `framework_skills`
| Column |
|---|
| framework |
| skill |
| category |
| last_reviewed |

### `curriculum_map`
| Column |
|---|
| skill |
| course |
| module |
| depth_0_3 |
| notes |

### `kpis`
| Column |
|---|
| window_days |
| region |
| as_of |
| total_postings |
| pct_change_prev_window |
| top_skills_json |
| fastest_growing_json |
| job_families_json |
| region_split_json |
| gap_chart_json |

### `blend_debug`
| Column |
|---|
| fetched_at |
| source |
| ok |
| keyword |
| page |
| location |
| http_status |
| content_type |
| job_count |
| raw_count |
| filtered_count |
| json_error |
| error |
| url |
| snippet |

---

## 2) Apps Script ETL

Copy full script from `apps-script/Code.gs`.

### Script Properties
Set these in **Apps Script > Project Settings > Script Properties**:

- `SHEET_ID`
- `USAJOBS_EMAIL`
- `USAJOBS_KEY` (recommended)
- `ADZUNA_APP_ID` (optional)
- `ADZUNA_APP_KEY` (optional)

### Scheduled jobs
- Run `setupDailyTrigger()` once to create daily 6am ET trigger.
- Optional: run `setupHourlyTrigger()` for hourly updates.
- Main entry points:
  - `runDailyBlend()`
  - `runHourlyBlend()`

What it does:
- Fetches USAJOBS + optional Adzuna.
- Normalizes, dedupes by `source_id || url`.
- Extracts skills with synonyms dictionary.
- Classifies `job_family` with rule-based title matching.
- Appends `raw_jobs`, prunes older than 90 days.
- Rebuilds today’s `skills_daily` and `titles_daily`.
- Recomputes `kpis` for windows `30/90` and regions `all/KY/OH/IN` with JSON blobs for Worker.
- Writes request diagnostics/errors to `blend_debug`.

---

## 3) Cloudflare Worker

Code: `worker/index.js`.

### Worker environment variables
Set in Cloudflare:
- `SHEET_ID`
- `SHEETS_API_KEY`

Notes:
- For simplest setup, publish the Google Sheet as readable (or permit API key read on the sheet).
- Worker endpoint: `GET /dashboard-summary?region=all|KY|OH|IN&window=30|90`
- Adds CORS headers for GitHub Pages and handles `OPTIONS`.
- Cache headers set to 5 minutes (`Cache-Control: public, max-age=300`).

### JSON response shape
```json
{
  "as_of": "ISO",
  "window_days": 30,
  "region": "KY",
  "kpis": {
    "total_postings": 1234,
    "pct_change_prev_window": 12.3,
    "top_skills": [{"skill":"Python","count":321}],
    "fastest_growing": [{"skill":"RAG","delta_pct":55.2,"count":44}],
    "top_job_families": [{"family":"Data/Analytics","count":210}],
    "region_split": [{"region":"KY","count":200}],
    "gap_chart": [{"skill":"MLOps","demand":120,"coverage":1,"gap":119}]
  },
  "meta": {"sources":["USAJOBS","Adzuna"],"note":"Aggregated from Sheets"}
}
```

---

## 4) GitHub Pages frontend

Code: `index.html`.

Features:
- Executive-friendly white/dark/gold NKU styling.
- Region toggle (`All/KY/OH/IN`) and window toggle (`30/90`).
- One API call to Worker endpoint only.
- KPI cards + 4 charts (skills bar, trendline proxy, job families, curriculum gap).
- Standards/framework reference table.
- Manual refresh + auto-refresh every 10 minutes.
- Empty and error states.

Before deploy, update:

```js
const WORKER_BASE = 'https://<worker-domain>';
```

---

## 5) Setup checklist

1. **Create Google Sheet** and tabs/headers listed above.
2. **Create Apps Script project** bound to or connected with the Sheet.
3. Paste `apps-script/Code.gs`.
4. Add Script Properties (`SHEET_ID`, USAJOBS credentials, optional Adzuna keys).
5. Run `runDailyBlend()` once manually to seed tables.
6. Run `setupDailyTrigger()` (and optional `setupHourlyTrigger()`).
7. **Create Cloudflare Worker**, paste `worker/index.js`, set env vars (`SHEET_ID`, `SHEETS_API_KEY`), deploy.
8. Verify endpoint: `/dashboard-summary?region=all&window=30`.
9. Update `WORKER_BASE` in `index.html`.
10. Push repo to GitHub and enable **GitHub Pages** for the branch/folder containing `index.html`.
11. Open Pages URL and validate filters, KPI cards, and chart rendering.

