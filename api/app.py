"""
Flask app for Lead Dataset Builder (Vercel serverless entry in api/).
Set GOOGLE_PLACES_API_KEY in env on Vercel.
"""
import sys
from pathlib import Path

# Ensure project root is on path so we can import scrape_businesses
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import io
import csv as csv_module
import logging
import os
import uuid
from pathlib import Path
from threading import Thread

from flask import Flask, Response, jsonify, render_template_string, request, send_file

# Static script: no template vars, so URL/query string can never be injected into JS.
LEADS_FORM_JS = r"""
(function () {
  var form = document.getElementById('form');
  var btn = document.getElementById('btn');
  var msg = document.getElementById('message');
  var maxLeads = parseInt(form.getAttribute('data-max-leads'), 10) || 1000;

  var params = new URLSearchParams(window.location.search);
  var urlCities = params.getAll('city').map(function (c) { try { return decodeURIComponent(c); } catch (e) { return c; } });
  var urlNiches = params.getAll('niche').map(function (n) { try { return decodeURIComponent(n); } catch (e) { return n; } });
  var urlMax = parseInt(params.get('max_leads'), 10);
  var cityEl = document.getElementById('city');
  var nicheEl = document.getElementById('niche');
  var maxEl = document.getElementById('max_leads');
  if (urlCities.length > 0 && cityEl) {
    Array.from(cityEl.options).forEach(function (opt) { opt.selected = urlCities.indexOf(opt.value) !== -1; });
  }
  if (urlNiches.length > 0 && nicheEl) {
    Array.from(nicheEl.options).forEach(function (opt) { opt.selected = urlNiches.indexOf(opt.value) !== -1; });
  }
  if (!isNaN(urlMax) && urlMax >= 1 && urlMax <= maxLeads && maxEl) {
    maxEl.value = urlMax;
  }

  function showMessage(text, type) {
    msg.textContent = text;
    msg.className = type || 'info';
    msg.style.display = 'block';
  }

  function setLoading(loading) {
    btn.disabled = loading;
    btn.textContent = loading ? 'Collecting leads...' : 'Get leads';
  }

  form.addEventListener('submit', function (e) {
    e.preventDefault();
    var cityEl = document.getElementById('city');
    var nicheEl = document.getElementById('niche');
    var selectedCities = Array.from(cityEl.selectedOptions).map(function (o) { return o.value; });
    var selectedNiches = Array.from(nicheEl.selectedOptions).map(function (o) { return o.value; });
    var maxLeadsVal = parseInt(document.getElementById('max_leads').value, 10) || 10;
    if (selectedCities.length === 0 || selectedNiches.length === 0) {
      showMessage('Select at least one city and one niche.', 'error');
      return;
    }
    if (maxLeadsVal < 1 || maxLeadsVal > maxLeads) {
      showMessage('Please enter between 1 and ' + String(maxLeads) + ' leads.', 'error');
      return;
    }
    setLoading(true);
    showMessage('Starting...', 'info');
    fetch('/api/collect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        city: selectedCities,
        niche: selectedNiches,
        max_leads: maxLeadsVal
      })
    }).then(function (res) {
      var contentType = res.headers.get('content-type') || '';
      if (contentType.indexOf('text/csv') !== -1) {
        return res.text().then(function (text) {
          var a = document.createElement('a');
          a.href = URL.createObjectURL(new Blob([text], { type: 'text/csv' }));
          a.download = 'leads.csv';
          a.click();
          URL.revokeObjectURL(a.href);
          var count = Math.max(0, text.trim().split('\n').length - 1);
          if (count === 0) {
            showMessage('0 leads found. Check: API key is set in Vercel (GOOGLE_PLACES_API_KEY), Places API is enabled for your key, and try different city/niche. Only businesses with website + email are included.', 'error');
          } else {
            showMessage('Done! ' + count + ' leads. CSV downloaded.', 'success');
          }
          setLoading(false);
        });
      }
      return res.json().then(function (data) {
        if (!res.ok) {
          showMessage(data.error || 'Request failed', 'error');
          setLoading(false);
          return;
        }
        var jobId = data.job_id;
        showMessage('Collecting leads... This may take a few minutes.', 'info');
        function check() {
          fetch('/api/status/' + jobId).then(function (r) { return r.json(); }).then(function (s) {
            if (s.status === 'done') {
              showMessage('Done! ' + s.lead_count + ' leads. Download your CSV below.', 'success');
              msg.innerHTML = 'Done! ' + s.lead_count + ' leads. <a class="download" href="/api/download/' + jobId + '">Download CSV</a>';
              msg.className = 'success';
              setLoading(false);
              return;
            }
            if (s.status === 'error') {
              showMessage('Error: ' + (s.error || 'Unknown'), 'error');
              setLoading(false);
              return;
            }
            setTimeout(check, 2500);
          });
        }
        check();
      });
    }).catch(function (err) {
      showMessage('Network error: ' + err.message, 'error');
      setLoading(false);
    });
  });
})();
"""

from scrape_businesses import (
    COLUMNS,
    CITIES,
    NICHES,
    load_config,
    run_collection_for_cities_niches,
)

app = Flask(__name__)
jobs: dict = {}
MAX_LEADS_WEB = 1000
MAX_LEADS_VERCEL = 1000
VERCEL_MAX_TIME_SECONDS = 290
VERCEL = os.environ.get("VERCEL") == "1"


def _run_job(
    job_id: str,
    cities: list[str],
    niches: list[str],
    max_leads: int,
    api_key: str,
) -> None:
    max_time = None
    try:
        max_time = int(os.environ.get("MAX_RUN_SECONDS", 0)) or None
    except (TypeError, ValueError):
        pass
    try:
        leads = run_collection_for_cities_niches(
            api_key=api_key,
            cities=cities,
            niches=niches,
            max_leads=max_leads,
            extract_emails=True,
            sleep_api=0.2,
            sleep_web=0.1,
            max_time_seconds=max_time,
        )
        jobs[job_id]["status"] = "done"
        jobs[job_id]["leads"] = leads
    except Exception as e:
        logging.exception("Job %s failed", job_id)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)


@app.route("/")
def index():
    max_leads = MAX_LEADS_VERCEL if VERCEL else MAX_LEADS_WEB
    return render_template_string(
        INDEX_HTML,
        cities=CITIES,
        niches=NICHES,
        max_leads_web=max_leads,
        is_vercel=VERCEL,
    )


@app.route("/static/leads-form.js")
def leads_form_js():
    return Response(LEADS_FORM_JS.strip(), mimetype="application/javascript; charset=utf-8")


def _get_api_key():
    if VERCEL:
        key = os.environ.get("GOOGLE_PLACES_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not key or key == "YOUR_GOOGLE_API_KEY_HERE":
            return None
        return key
    try:
        config = load_config(_root / "config.json")
        return config.get("google_api_key")
    except SystemExit:
        return None


@app.route("/api/collect", methods=["POST"])
def api_collect():
    data = request.get_json() or {}
    raw_city = data.get("city")
    raw_niche = data.get("niche")
    cities = [raw_city] if isinstance(raw_city, str) else (raw_city or [])
    niches = [raw_niche] if isinstance(raw_niche, str) else (raw_niche or [])
    cities = [str(c).strip() for c in cities if c]
    niches = [str(n).strip() for n in niches if n]
    try:
        max_leads = int(data.get("max_leads", 10))
    except (TypeError, ValueError):
        max_leads = 10
    if not cities or not niches:
        return jsonify({"error": "Select at least one city and one niche"}), 400
    max_allowed = MAX_LEADS_VERCEL if VERCEL else MAX_LEADS_WEB
    if max_leads < 1 or max_leads > max_allowed:
        return jsonify({"error": f"Number of leads must be between 1 and {max_allowed}"}), 400
    api_key = _get_api_key()
    if not api_key:
        msg = (
            "Set GOOGLE_PLACES_API_KEY in Vercel project Environment Variables."
            if VERCEL
            else "API key not configured. Add config.json with google_api_key or set GOOGLE_PLACES_API_KEY."
        )
        return jsonify({"error": msg}), 500
    if VERCEL:
        try:
            leads = run_collection_for_cities_niches(
                api_key=api_key,
                cities=cities,
                niches=niches,
                max_leads=max_leads,
                extract_emails=True,
                sleep_api=0.2,
                sleep_web=0.1,
                max_time_seconds=VERCEL_MAX_TIME_SECONDS,
            )
        except Exception as e:
            logging.exception("Collect failed")
            return jsonify({"error": str(e)}), 500
        buf = io.StringIO()
        writer = csv_module.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
        csv_body = buf.getvalue()
        return Response(
            csv_body.encode("utf-8"),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=leads.csv"},
        )
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "running", "leads": [], "error": None}
    thread = Thread(
        target=_run_job,
        args=(job_id, cities, niches, max_leads, api_key),
    )
    thread.start()
    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def api_status(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404
    j = jobs[job_id]
    out = {"status": j["status"]}
    if j["status"] == "done":
        out["lead_count"] = len(j["leads"])
    if j["status"] == "error":
        out["error"] = j.get("error", "Unknown error")
    return jsonify(out)


@app.route("/api/download/<job_id>")
def api_download(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404
    j = jobs[job_id]
    if j["status"] != "done":
        return jsonify({"error": "Job not ready for download"}), 400
    buf = io.StringIO()
    writer = csv_module.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(j["leads"])
    buf.seek(0)
    filename = f"leads_{job_id[:8]}.csv"
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lead Dataset Builder</title>
  <style>
    * { box-sizing: border-box; }
    body {
      font-family: 'Segoe UI', system-ui, sans-serif;
      max-width: 480px;
      margin: 2rem auto;
      padding: 0 1rem;
      background: #0f0f12;
      color: #e4e4e7;
    }
    h1 {
      font-size: 1.5rem;
      font-weight: 600;
      margin-bottom: 1.5rem;
      color: #fafafa;
    }
    label {
      display: block;
      font-size: 0.875rem;
      font-weight: 500;
      margin-bottom: 0.35rem;
      color: #a1a1aa;
    }
    select, input[type="number"] {
      width: 100%;
      padding: 0.6rem 0.75rem;
      margin-bottom: 0.5rem;
      font-size: 1rem;
      background: #18181b;
      border: 1px solid #3f3f46;
      border-radius: 8px;
      color: #fafafa;
    }
    select[multiple] {
      min-height: 120px;
      padding: 0.5rem;
    }
    select[multiple] option {
      padding: 0.25rem 0;
    }
    .hint {
      font-size: 0.75rem;
      color: #71717a;
      margin-bottom: 1rem;
    }
    select:focus, input:focus {
      outline: none;
      border-color: #6366f1;
    }
    button {
      width: 100%;
      padding: 0.75rem 1rem;
      font-size: 1rem;
      font-weight: 500;
      background: #6366f1;
      color: white;
      border: none;
      border-radius: 8px;
      cursor: pointer;
      margin-top: 0.5rem;
    }
    button:hover { background: #4f46e5; }
    button:disabled {
      background: #3f3f46;
      cursor: not-allowed;
      color: #71717a;
    }
    #message {
      margin-top: 1rem;
      padding: 0.75rem;
      border-radius: 8px;
      font-size: 0.875rem;
      display: none;
    }
    #message.info { background: #1e1b4b; color: #a5b4fc; display: block; }
    #message.success { background: #14532d; color: #86efac; display: block; }
    #message.error { background: #450a0a; color: #fca5a5; display: block; }
    a.download {
      display: inline-block;
      margin-top: 0.5rem;
      color: #818cf8;
      text-decoration: none;
    }
    a.download:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <h1>Lead Dataset Builder</h1>
  <form id="form" data-max-leads="{{ max_leads_web | default(1000) | int }}">
    <label for="city">Cities</label>
    <select id="city" name="city" multiple size="8">
      {% for c in cities %}
      <option value="{{ c | e }}">{{ c | e }}</option>
      {% endfor %}
    </select>
    <p class="hint">Hold Ctrl (Windows) or Cmd (Mac) to select multiple cities.</p>
    <label for="niche">Niches</label>
    <select id="niche" name="niche" multiple size="8">
      {% for n in niches %}
      <option value="{{ n | e }}">{{ n | e }}</option>
      {% endfor %}
    </select>
    <p class="hint">Hold Ctrl (Windows) or Cmd (Mac) to select multiple niches.</p>
    <label for="max_leads">How many leads?</label>
    <input type="number" id="max_leads" name="max_leads" min="1" max="{{ max_leads_web }}" value="10" required>
    <small style="color:#71717a; font-size:0.8rem;">Max {{ max_leads_web }} per run.{% if is_vercel %} (Vercel: runs up to ~5 min; you get all leads collected in that time.){% endif %}</small>
    <button type="submit" id="btn">Get leads</button>
  </form>
  <div id="message" role="status"></div>

  <script src="/static/leads-form.js"></script>
</body>
</html>
"""
