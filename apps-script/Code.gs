/**
 * AI Skills Demand ETL (Apps Script)
 *
 * Required Script Properties:
 * - SHEET_ID
 * - USAJOBS_EMAIL
 * - USAJOBS_KEY (optional but recommended)
 * - ADZUNA_APP_ID (optional)
 * - ADZUNA_APP_KEY (optional)
 */

const CONFIG = {
  regions: {
    KY: ['Kentucky', 'KY'],
    OH: ['Ohio', 'OH'],
    IN: ['Indiana', 'IN']
  },
  keepDays: 90,
  usajobs: {
    host: 'https://data.usajobs.gov/api/search',
    pages: 4,
    pageSize: 200,
    keyword: 'artificial intelligence OR machine learning OR data science'
  },
  adzuna: {
    host: 'https://api.adzuna.com/v1/api/jobs/us/search',
    pages: 3,
    pageSize: 50,
    keyword: 'artificial intelligence OR machine learning OR data science'
  }
};

const SKILL_DICTIONARY = {
  'GenAI': ['generative ai', 'genai', 'gen ai', 'foundation model'],
  'LLM': ['llm', 'large language model', 'large-language model'],
  'GPT': ['gpt', 'chatgpt', 'copilot'],
  'RAG': ['rag', 'retrieval augmented generation', 'retrieval-augmented generation'],
  'Embeddings': ['embedding', 'embeddings', 'vector database', 'vector search'],
  'Python': ['python', 'pandas', 'numpy', 'scikit'],
  'SQL': ['sql', 'postgres', 'mysql', 'snowflake', 'bigquery'],
  'Excel': ['excel', 'spreadsheet'],
  'Tableau': ['tableau'],
  'PowerBI': ['power bi', 'powerbi'],
  'ML': ['machine learning', 'ml model', 'predictive model'],
  'NLP': ['nlp', 'natural language processing', 'text mining'],
  'CV': ['computer vision', 'opencv', 'image model'],
  'MLOps': ['mlops', 'model ops', 'model deployment', 'model registry'],
  'Docker': ['docker', 'containerization', 'container'],
  'Git': ['git', 'github', 'gitlab', 'version control'],
  'AWS': ['aws', 'amazon web services', 'sagemaker'],
  'Azure': ['azure', 'microsoft azure'],
  'GCP': ['gcp', 'google cloud', 'vertex ai'],
  'Governance': ['governance', 'ai governance', 'model governance'],
  'Privacy': ['privacy', 'pii', 'data privacy'],
  'Risk': ['risk management', 'model risk', 'risk'],
  'Bias': ['bias', 'fairness', 'responsible ai'],
  'Compliance': ['compliance', 'regulatory', 'audit', 'policy']
};

const JOB_FAMILY_RULES = [
  { family: 'Data/Analytics', terms: ['data analyst', 'business intelligence', 'analytics', 'bi '] },
  { family: 'Data Science/ML', terms: ['data scientist', 'machine learning', 'ml engineer', 'ai engineer'] },
  { family: 'Software/Platform', terms: ['software engineer', 'developer', 'platform engineer', 'backend'] },
  { family: 'Cloud/DevOps', terms: ['devops', 'site reliability', 'sre', 'cloud engineer'] },
  { family: 'Cyber/GRC', terms: ['security', 'cyber', 'governance', 'risk', 'compliance'] },
  { family: 'Product/Management', terms: ['product manager', 'program manager', 'project manager'] },
  { family: 'Education/Training', terms: ['instructional', 'trainer', 'faculty', 'curriculum'] }
];

const RAW_HEADERS = [
  'ingested_at', 'posted_date', 'source', 'source_id', 'url', 'title', 'company',
  'location', 'region', 'description', 'job_family', 'skills_json'
];
const SKILLS_HEADERS = ['date', 'skill', 'count', 'source', 'region'];
const TITLES_HEADERS = ['date', 'job_family', 'count', 'region'];
const FRAMEWORK_HEADERS = ['framework', 'skill', 'category', 'last_reviewed'];
const CURRICULUM_HEADERS = ['skill', 'course', 'module', 'depth_0_3', 'notes'];
const KPI_HEADERS = [
  'window_days', 'region', 'as_of', 'total_postings', 'pct_change_prev_window',
  'top_skills_json', 'fastest_growing_json', 'job_families_json', 'region_split_json', 'gap_chart_json'
];
const DEBUG_HEADERS = [
  'fetched_at', 'source', 'ok', 'keyword', 'page', 'location', 'http_status',
  'content_type', 'job_count', 'raw_count', 'filtered_count', 'json_error', 'error', 'url', 'snippet'
];

function runDailyBlend() {
  runPipeline_({ updateAllRegions: true });
}

function runHourlyBlend() {
  runPipeline_({ updateAllRegions: false });
}

function setupDailyTrigger() {
  ScriptApp.newTrigger('runDailyBlend')
    .timeBased()
    .everyDays(1)
    .atHour(6)
    .inTimezone('America/New_York')
    .create();
}

function setupHourlyTrigger() {
  ScriptApp.newTrigger('runHourlyBlend')
    .timeBased()
    .everyHours(1)
    .create();
}

function runPipeline_(opts) {
  const sheetId = getProp_('SHEET_ID', true);
  const ss = SpreadsheetApp.openById(sheetId);
  ensureTabs_(ss);

  const fetched = [];
  fetched.push.apply(fetched, fetchUsaJobs_());
  fetched.push.apply(fetched, fetchAdzuna_());

  const deduped = dedupeJobs_(fetched);
  const todayRows = normalizeRows_(deduped);

  appendRawJobs_(ss, todayRows);
  pruneRawJobs_(ss);
  updateSkillsDaily_(ss, todayRows);
  updateTitlesDaily_(ss, todayRows);
  updateKpis_(ss, opts && opts.updateAllRegions);
}

function ensureTabs_(ss) {
  ensureSheet_(ss, 'raw_jobs', RAW_HEADERS);
  ensureSheet_(ss, 'skills_daily', SKILLS_HEADERS);
  ensureSheet_(ss, 'titles_daily', TITLES_HEADERS);
  ensureSheet_(ss, 'framework_skills', FRAMEWORK_HEADERS);
  ensureSheet_(ss, 'curriculum_map', CURRICULUM_HEADERS);
  ensureSheet_(ss, 'kpis', KPI_HEADERS);
  ensureSheet_(ss, 'blend_debug', DEBUG_HEADERS);
}

function ensureSheet_(ss, name, headers) {
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
  }
  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  } else {
    const current = sh.getRange(1, 1, 1, headers.length).getValues()[0];
    if (current.join('|') !== headers.join('|')) {
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
  }
}

function fetchUsaJobs_() {
  const email = getProp_('USAJOBS_EMAIL', true);
  const key = getProp_('USAJOBS_KEY', false);
  const jobs = [];

  Object.keys(CONFIG.regions).forEach(function(regionCode) {
    const regionTerms = CONFIG.regions[regionCode];
    for (let page = 1; page <= CONFIG.usajobs.pages; page++) {
      const query = {
        Keyword: CONFIG.usajobs.keyword,
        ResultsPerPage: CONFIG.usajobs.pageSize,
        Page: page,
        LocationName: regionTerms[0]
      };
      const url = CONFIG.usajobs.host + '?' + toQuery_(query);
      const options = {
        method: 'get',
        muteHttpExceptions: true,
        headers: {
          Host: 'data.usajobs.gov',
          'User-Agent': email,
          'Authorization-Key': key || ''
        }
      };
      const response = UrlFetchApp.fetch(url, options);
      const status = response.getResponseCode();
      const body = response.getContentText();
      const contentType = response.getHeaders()['Content-Type'] || '';
      let parsed;
      let jsonError = '';

      try {
        parsed = JSON.parse(body);
      } catch (err) {
        jsonError = err.message;
      }

      let found = [];
      if (parsed && parsed.SearchResult && parsed.SearchResult.SearchResultItems) {
        found = parsed.SearchResult.SearchResultItems.map(function(item) {
          const d = item.MatchedObjectDescriptor || {};
          return {
            source: 'USAJOBS',
            source_id: d.PositionID || d.PositionURI || '',
            url: d.PositionURI || '',
            title: d.PositionTitle || '',
            company: d.OrganizationName || 'US Government',
            location: ((d.PositionLocationDisplay || '') + ' ' + (d.PositionLocation || '')).trim(),
            posted_date: d.PublicationStartDate || d.PositionStartDate || new Date().toISOString(),
            description: ((d.UserArea && d.UserArea.Details && d.UserArea.Details.JobSummary) || ''),
            region: regionCode
          };
        });
      }

      logDebug_({
        source: 'USAJOBS', ok: status >= 200 && status < 300, keyword: CONFIG.usajobs.keyword,
        page: page, location: regionTerms[0], http_status: status, content_type: contentType,
        job_count: found.length, raw_count: found.length, filtered_count: found.length,
        json_error: jsonError, error: '', url: url, snippet: body.slice(0, 300)
      });

      jobs.push.apply(jobs, found);
      if (found.length === 0) break;
      Utilities.sleep(300);
    }
  });

  return jobs;
}

function fetchAdzuna_() {
  const appId = getProp_('ADZUNA_APP_ID', false);
  const appKey = getProp_('ADZUNA_APP_KEY', false);
  if (!appId || !appKey) {
    logDebug_({
      source: 'Adzuna', ok: false, keyword: CONFIG.adzuna.keyword, page: 0, location: 'US',
      http_status: 0, content_type: '', job_count: 0, raw_count: 0, filtered_count: 0,
      json_error: '', error: 'Adzuna credentials missing; skipped', url: '', snippet: ''
    });
    return [];
  }

  const jobs = [];
  for (let page = 1; page <= CONFIG.adzuna.pages; page++) {
    const query = {
      app_id: appId,
      app_key: appKey,
      what: CONFIG.adzuna.keyword,
      results_per_page: CONFIG.adzuna.pageSize,
      content_type: 'application/json'
    };
    const url = CONFIG.adzuna.host + '/' + page + '?' + toQuery_(query);
    const response = UrlFetchApp.fetch(url, { method: 'get', muteHttpExceptions: true });
    const status = response.getResponseCode();
    const body = response.getContentText();
    const contentType = response.getHeaders()['Content-Type'] || '';

    let parsed;
    let jsonError = '';
    try {
      parsed = JSON.parse(body);
    } catch (err) {
      jsonError = err.message;
    }

    const found = (parsed && parsed.results ? parsed.results : []).map(function(item) {
      const locationText = item.location && item.location.display_name ? item.location.display_name : 'United States';
      return {
        source: 'Adzuna',
        source_id: item.id || item.redirect_url || '',
        url: item.redirect_url || '',
        title: item.title || '',
        company: item.company && item.company.display_name ? item.company.display_name : '',
        location: locationText,
        posted_date: item.created || new Date().toISOString(),
        description: item.description || '',
        region: inferRegion_(locationText)
      };
    });

    logDebug_({
      source: 'Adzuna', ok: status >= 200 && status < 300, keyword: CONFIG.adzuna.keyword,
      page: page, location: 'US', http_status: status, content_type: contentType,
      job_count: found.length, raw_count: found.length, filtered_count: found.length,
      json_error: jsonError, error: '', url: url, snippet: body.slice(0, 300)
    });

    jobs.push.apply(jobs, found);
    if (found.length === 0) break;
    Utilities.sleep(300);
  }

  return jobs;
}

function dedupeJobs_(jobs) {
  const byKey = {};
  jobs.forEach(function(job) {
    const key = job.source + '|' + (job.source_id || job.url || '');
    if (!key || key.endsWith('|')) return;
    if (!byKey[key]) byKey[key] = job;
  });
  return Object.keys(byKey).map(function(k) { return byKey[k]; });
}

function normalizeRows_(jobs) {
  const nowIso = new Date().toISOString();
  return jobs.map(function(job) {
    const description = (job.description || '').replace(/\s+/g, ' ').trim();
    const title = (job.title || '').trim();
    const joinedText = (title + ' ' + description).toLowerCase();
    const skills = extractSkills_(joinedText);
    const family = classifyJobFamily_(title + ' ' + description);
    return {
      ingested_at: nowIso,
      posted_date: normalizeDate_(job.posted_date),
      source: job.source || 'Unknown',
      source_id: job.source_id || '',
      url: job.url || '',
      title: title,
      company: job.company || '',
      location: job.location || '',
      region: job.region || inferRegion_(job.location || ''),
      description: description,
      job_family: family,
      skills: skills
    };
  });
}

function appendRawJobs_(ss, rows) {
  if (!rows.length) return;
  const sh = ss.getSheetByName('raw_jobs');
  const values = rows.map(function(r) {
    return [
      r.ingested_at, r.posted_date, r.source, r.source_id, r.url, r.title,
      r.company, r.location, r.region, r.description, r.job_family,
      JSON.stringify(r.skills)
    ];
  });
  sh.getRange(sh.getLastRow() + 1, 1, values.length, RAW_HEADERS.length).setValues(values);
}

function pruneRawJobs_(ss) {
  const sh = ss.getSheetByName('raw_jobs');
  const lastRow = sh.getLastRow();
  if (lastRow <= 1) return;

  const values = sh.getRange(2, 1, lastRow - 1, RAW_HEADERS.length).getValues();
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - CONFIG.keepDays);

  const kept = values.filter(function(row) {
    const dt = new Date(row[1]);
    return dt >= cutoff;
  });

  sh.getRange(2, 1, Math.max(lastRow - 1, 1), RAW_HEADERS.length).clearContent();
  if (kept.length) {
    sh.getRange(2, 1, kept.length, RAW_HEADERS.length).setValues(kept);
  }
}

function updateSkillsDaily_(ss, rows) {
  const sh = ss.getSheetByName('skills_daily');
  const today = toDateOnly_(new Date());
  replaceDateRows_(sh, today, 1, SKILLS_HEADERS.length);

  const agg = {};
  rows.forEach(function(r) {
    r.skills.forEach(function(skill) {
      const key = [today, skill, r.source, r.region].join('|');
      agg[key] = (agg[key] || 0) + 1;
    });
  });

  const out = Object.keys(agg).map(function(k) {
    const parts = k.split('|');
    return [parts[0], parts[1], agg[k], parts[2], parts[3]];
  });

  if (out.length) {
    sh.getRange(sh.getLastRow() + 1, 1, out.length, SKILLS_HEADERS.length).setValues(out);
  }
}

function updateTitlesDaily_(ss, rows) {
  const sh = ss.getSheetByName('titles_daily');
  const today = toDateOnly_(new Date());
  replaceDateRows_(sh, today, 1, TITLES_HEADERS.length);

  const agg = {};
  rows.forEach(function(r) {
    const key = [today, r.job_family, r.region].join('|');
    agg[key] = (agg[key] || 0) + 1;
  });

  const out = Object.keys(agg).map(function(k) {
    const parts = k.split('|');
    return [parts[0], parts[1], agg[k], parts[2]];
  });

  if (out.length) {
    sh.getRange(sh.getLastRow() + 1, 1, out.length, TITLES_HEADERS.length).setValues(out);
  }
}

function updateKpis_(ss, updateAllRegions) {
  const kpiSheet = ss.getSheetByName('kpis');
  const raw = readSheetAsObjects_(ss.getSheetByName('raw_jobs'), RAW_HEADERS);
  const skillsDaily = readSheetAsObjects_(ss.getSheetByName('skills_daily'), SKILLS_HEADERS);
  const titlesDaily = readSheetAsObjects_(ss.getSheetByName('titles_daily'), TITLES_HEADERS);
  const curriculum = readSheetAsObjects_(ss.getSheetByName('curriculum_map'), CURRICULUM_HEADERS);

  const regions = updateAllRegions ? ['all', 'KY', 'OH', 'IN'] : ['all'];
  const windows = [30, 90];
  const rows = [];
  const asOf = new Date().toISOString();

  windows.forEach(function(windowDays) {
    regions.forEach(function(region) {
      const calc = computeKpiBundle_(windowDays, region, raw, skillsDaily, titlesDaily, curriculum);
      rows.push([
        windowDays,
        region,
        asOf,
        calc.total_postings,
        calc.pct_change_prev_window,
        JSON.stringify(calc.top_skills),
        JSON.stringify(calc.fastest_growing),
        JSON.stringify(calc.top_job_families),
        JSON.stringify(calc.region_split),
        JSON.stringify(calc.gap_chart)
      ]);
    });
  });

  if (kpiSheet.getLastRow() > 1) {
    kpiSheet.getRange(2, 1, kpiSheet.getLastRow() - 1, KPI_HEADERS.length).clearContent();
  }
  if (rows.length) {
    kpiSheet.getRange(2, 1, rows.length, KPI_HEADERS.length).setValues(rows);
  }
}

function computeKpiBundle_(windowDays, region, rawRows, skillsDaily, titlesDaily, curriculum) {
  const now = new Date();
  const start = new Date(now);
  start.setDate(now.getDate() - windowDays);
  const prevStart = new Date(start);
  prevStart.setDate(start.getDate() - windowDays);

  const currentRaw = rawRows.filter(function(r) {
    const d = new Date(r.posted_date);
    const regionOk = region === 'all' || r.region === region;
    return regionOk && d >= start && d <= now;
  });

  const prevRaw = rawRows.filter(function(r) {
    const d = new Date(r.posted_date);
    const regionOk = region === 'all' || r.region === region;
    return regionOk && d >= prevStart && d < start;
  });

  const total = currentRaw.length;
  const pct = prevRaw.length ? ((total - prevRaw.length) / prevRaw.length) * 100 : 0;

  const skillCounts = {};
  currentRaw.forEach(function(r) {
    let list = [];
    try {
      list = JSON.parse(r.skills_json || '[]');
    } catch (err) {
      list = [];
    }
    list.forEach(function(skill) {
      skillCounts[skill] = (skillCounts[skill] || 0) + 1;
    });
  });

  const topSkills = sortObjectEntries_(skillCounts, 'skill', 'count', 12);

  const growth = computeSkillGrowth_(skillsDaily, windowDays, region);
  const families = computeJobFamilies_(titlesDaily, windowDays, region);
  const regionSplit = computeRegionSplit_(currentRaw);
  const gaps = computeCurriculumGap_(topSkills, curriculum);

  return {
    total_postings: total,
    pct_change_prev_window: round_(pct, 1),
    top_skills: topSkills,
    fastest_growing: growth,
    top_job_families: families,
    region_split: regionSplit,
    gap_chart: gaps
  };
}

function computeSkillGrowth_(skillsDaily, windowDays, region) {
  const now = new Date();
  const curStart = new Date(now);
  curStart.setDate(now.getDate() - windowDays);
  const prevStart = new Date(curStart);
  prevStart.setDate(curStart.getDate() - windowDays);

  const cur = {};
  const prev = {};

  skillsDaily.forEach(function(r) {
    const d = new Date(r.date);
    const regionOk = region === 'all' || r.region === region;
    if (!regionOk) return;
    const count = Number(r.count) || 0;
    if (d >= curStart && d <= now) {
      cur[r.skill] = (cur[r.skill] || 0) + count;
    } else if (d >= prevStart && d < curStart) {
      prev[r.skill] = (prev[r.skill] || 0) + count;
    }
  });

  const rows = Object.keys(cur).map(function(skill) {
    const curr = cur[skill] || 0;
    const prior = prev[skill] || 0;
    const deltaPct = prior ? ((curr - prior) / prior) * 100 : (curr > 0 ? 100 : 0);
    return { skill: skill, delta_pct: round_(deltaPct, 1), count: curr };
  });

  return rows.sort(function(a, b) { return b.delta_pct - a.delta_pct; }).slice(0, 8);
}

function computeJobFamilies_(titlesDaily, windowDays, region) {
  const now = new Date();
  const start = new Date(now);
  start.setDate(now.getDate() - windowDays);
  const agg = {};

  titlesDaily.forEach(function(r) {
    const d = new Date(r.date);
    const regionOk = region === 'all' || r.region === region;
    if (!regionOk || d < start || d > now) return;
    agg[r.job_family] = (agg[r.job_family] || 0) + (Number(r.count) || 0);
  });

  return sortObjectEntries_(agg, 'family', 'count', 8);
}

function computeRegionSplit_(rawRows) {
  const agg = { KY: 0, OH: 0, IN: 0, Other: 0 };
  rawRows.forEach(function(r) {
    const region = r.region || 'Other';
    if (!agg.hasOwnProperty(region)) agg.Other += 1;
    else agg[region] += 1;
  });

  return Object.keys(agg)
    .filter(function(region) { return agg[region] > 0; })
    .map(function(region) { return { region: region, count: agg[region] }; })
    .sort(function(a, b) { return b.count - a.count; });
}

function computeCurriculumGap_(topSkills, curriculumRows) {
  const coverage = {};
  curriculumRows.forEach(function(r) {
    const skill = (r.skill || '').trim();
    if (!skill) return;
    coverage[skill] = Math.max(coverage[skill] || 0, Number(r.depth_0_3) || 0);
  });

  return topSkills.slice(0, 10).map(function(item) {
    const cov = coverage[item.skill] || 0;
    return {
      skill: item.skill,
      demand: item.count,
      coverage: cov,
      gap: Math.max(item.count - cov, 0)
    };
  });
}

function replaceDateRows_(sheet, dateValue, dateCol, totalCols) {
  const last = sheet.getLastRow();
  if (last <= 1) return;
  const values = sheet.getRange(2, 1, last - 1, totalCols).getValues();
  const kept = values.filter(function(row) { return row[dateCol - 1] !== dateValue; });
  sheet.getRange(2, 1, Math.max(last - 1, 1), totalCols).clearContent();
  if (kept.length) sheet.getRange(2, 1, kept.length, totalCols).setValues(kept);
}

function readSheetAsObjects_(sheet, headers) {
  const last = sheet.getLastRow();
  if (last <= 1) return [];
  const values = sheet.getRange(2, 1, last - 1, headers.length).getValues();
  return values.map(function(row) {
    const obj = {};
    headers.forEach(function(h, idx) { obj[h] = row[idx]; });
    return obj;
  });
}

function extractSkills_(textLower) {
  const matches = [];
  Object.keys(SKILL_DICTIONARY).forEach(function(skill) {
    const found = SKILL_DICTIONARY[skill].some(function(term) {
      return textLower.indexOf(term.toLowerCase()) > -1;
    });
    if (found) matches.push(skill);
  });
  return matches;
}

function classifyJobFamily_(text) {
  const lower = (text || '').toLowerCase();
  for (let i = 0; i < JOB_FAMILY_RULES.length; i++) {
    if (JOB_FAMILY_RULES[i].terms.some(function(term) { return lower.indexOf(term) > -1; })) {
      return JOB_FAMILY_RULES[i].family;
    }
  }
  return 'Other';
}

function inferRegion_(location) {
  const lower = (location || '').toLowerCase();
  if (lower.indexOf('kentucky') > -1 || /\bky\b/.test(lower)) return 'KY';
  if (lower.indexOf('ohio') > -1 || /\boh\b/.test(lower)) return 'OH';
  if (lower.indexOf('indiana') > -1 || /\bin\b/.test(lower)) return 'IN';
  return 'Other';
}

function logDebug_(row) {
  const sheetId = getProp_('SHEET_ID', true);
  const ss = SpreadsheetApp.openById(sheetId);
  const sh = ss.getSheetByName('blend_debug') || ss.insertSheet('blend_debug');
  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, DEBUG_HEADERS.length).setValues([DEBUG_HEADERS]);
  }

  sh.appendRow([
    new Date().toISOString(),
    row.source || '',
    String(row.ok),
    row.keyword || '',
    row.page || '',
    row.location || '',
    row.http_status || '',
    row.content_type || '',
    row.job_count || 0,
    row.raw_count || 0,
    row.filtered_count || 0,
    row.json_error || '',
    row.error || '',
    row.url || '',
    row.snippet || ''
  ]);
}

function getProp_(key, required) {
  const value = PropertiesService.getScriptProperties().getProperty(key);
  if (required && !value) throw new Error('Missing Script Property: ' + key);
  return value;
}

function normalizeDate_(value) {
  const d = new Date(value);
  if (isNaN(d.getTime())) return toDateOnly_(new Date());
  return toDateOnly_(d);
}

function toDateOnly_(date) {
  return Utilities.formatDate(date, 'America/New_York', 'yyyy-MM-dd');
}

function toQuery_(obj) {
  return Object.keys(obj)
    .map(function(k) { return encodeURIComponent(k) + '=' + encodeURIComponent(obj[k]); })
    .join('&');
}

function sortObjectEntries_(obj, keyName, valueName, limit) {
  return Object.keys(obj)
    .map(function(key) {
      const out = {};
      out[keyName] = key;
      out[valueName] = Number(obj[key]) || 0;
      return out;
    })
    .sort(function(a, b) { return b[valueName] - a[valueName]; })
    .slice(0, limit || 10);
}

function round_(n, digits) {
  const pow = Math.pow(10, digits || 0);
  return Math.round(n * pow) / pow;
}
