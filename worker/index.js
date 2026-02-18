export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders() });
    }

    if (url.pathname !== '/dashboard-summary') {
      return jsonResponse({ error: 'Not found' }, 404);
    }

    try {
      const region = normalizeRegion(url.searchParams.get('region') || 'all');
      const windowDays = normalizeWindow(url.searchParams.get('window') || '30');
      const cacheKey = `${url.origin}/dashboard-summary?region=${region}&window=${windowDays}`;

      const cache = caches.default;
      const cached = await cache.match(cacheKey);
      if (cached) {
        return withCors(cached);
      }

      const rows = await fetchKpiRows(env);
      const row = rows.find((r) => r.window_days === String(windowDays) && r.region === region);

      if (!row) {
        return jsonResponse({ error: 'No KPI row found', region, window_days: windowDays }, 404);
      }

      const payload = {
        as_of: toIso(row.as_of),
        window_days: windowDays,
        region,
        kpis: {
          total_postings: num(row.total_postings),
          pct_change_prev_window: num(row.pct_change_prev_window),
          top_skills: parseJson(row.top_skills_json),
          fastest_growing: parseJson(row.fastest_growing_json),
          top_job_families: parseJson(row.job_families_json),
          region_split: parseJson(row.region_split_json),
          gap_chart: parseJson(row.gap_chart_json)
        },
        meta: {
          sources: ['USAJOBS', 'Adzuna'],
          note: 'Aggregated from Sheets'
        }
      };

      const response = jsonResponse(payload, 200);
      response.headers.set('Cache-Control', 'public, max-age=300, s-maxage=300');
      await cache.put(cacheKey, response.clone());
      return response;
    } catch (err) {
      return jsonResponse({ error: 'Worker failure', detail: err.message }, 500);
    }
  }
};

async function fetchKpiRows(env) {
  if (!env.SHEETS_API_KEY || !env.SHEET_ID) {
    throw new Error('Missing SHEETS_API_KEY or SHEET_ID env variable');
  }

  const range = encodeURIComponent('kpis!A:J');
  const endpoint = `https://sheets.googleapis.com/v4/spreadsheets/${env.SHEET_ID}/values/${range}?key=${env.SHEETS_API_KEY}`;
  const res = await fetch(endpoint, {
    method: 'GET',
    headers: { Accept: 'application/json' }
  });

  if (!res.ok) {
    throw new Error(`Sheets API failed (${res.status})`);
  }

  const data = await res.json();
  const values = data.values || [];
  if (values.length < 2) return [];

  const headers = values[0];
  return values.slice(1).map((row) => {
    const out = {};
    headers.forEach((h, idx) => {
      out[h] = row[idx] || '';
    });
    return out;
  });
}

function parseJson(str) {
  if (!str) return [];
  try {
    return JSON.parse(str);
  } catch {
    return [];
  }
}

function normalizeRegion(value) {
  const allowed = ['all', 'KY', 'OH', 'IN'];
  return allowed.includes(value) ? value : 'all';
}

function normalizeWindow(value) {
  const parsed = Number(value);
  return parsed === 90 ? 90 : 30;
}

function toIso(value) {
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? new Date().toISOString() : dt.toISOString();
}

function num(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    Vary: 'Origin'
  };
}

function withCors(response) {
  const out = new Response(response.body, response);
  Object.entries(corsHeaders()).forEach(([k, v]) => out.headers.set(k, v));
  return out;
}

function jsonResponse(obj, status) {
  const res = new Response(JSON.stringify(obj), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8'
    }
  });
  return withCors(res);
}
