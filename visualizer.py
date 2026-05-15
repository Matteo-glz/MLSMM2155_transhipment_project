"""
GlobalFlow Unified Visualizer
==============================
Reads phase-1 baseline and all phase-2 + S4 scenario/strategy xlsx files,
then writes a single self-contained interactive HTML map.

Usage
-----
  python visualizer.py            # writes globalflow_map.html
  python visualizer.py mymap.html # custom output path
"""

import json
import math
import os
import sys

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
INSTANCE_FILE = "data/globalflow_instance.xlsx"
BASELINE_FILE = "phase1/results/baseline_solution.xlsx"
PHASE2_DIR    = "phase2/results"
S4_DIR        = "phase3/results"          
OUTPUT_FILE   = sys.argv[1] if len(sys.argv) > 1 else "globalflow_map.html"

PRODUCTS      = ["A_Fertilizers", "B_Semiconductors", "C_BatteryComponents"]
SCENARIO_KEYS = ["T1", "T2", "T3", "S1", "S2", "S3"]
STRATEGIES    = ["R", "A", "F"]

# ── Load nodes ────────────────────────────────────────────────────────────────
print("Loading instance data…")
nodes_df = pd.read_excel(INSTANCE_FILE, sheet_name="Nodes")
nodes = nodes_df.to_dict("records")

# ── Solution loader ───────────────────────────────────────────────────────────
def load_solution(path: str) -> dict:
    xl = pd.ExcelFile(path)
    result: dict = {
        "flows": [],
        "warehouses": [],
        "arc_activations": [],
        "emergency_arcs": [],
        "summary": {},
    }

    for p in PRODUCTS:
        sheet = p.replace("_", " ")
        if sheet in xl.sheet_names:
            df = pd.read_excel(path, sheet_name=sheet)
            result["flows"].extend(df.to_dict("records"))

    for key, sheet in (
        ("warehouses",      "Warehouses"),
        ("arc_activations", "Arc Activations"),
        ("emergency_arcs",  "Emergency Arcs"),
    ):
        if sheet in xl.sheet_names:
            result[key] = pd.read_excel(path, sheet_name=sheet).to_dict("records")

    if "Summary" in xl.sheet_names:
        df = pd.read_excel(path, sheet_name="Summary")
        result["summary"] = {
            str(row["Metric"]): row["Value"]
            for _, row in df.iterrows()
            if str(row["Metric"]).strip()
        }

    return result


# ── Load all data ─────────────────────────────────────────────────────────────
print("Loading baseline…")
baseline = load_solution(BASELINE_FILE)

print("Loading phase-2 scenarios…")
scenarios: dict = {}
for key in SCENARIO_KEYS:
    scenarios[key] = {}
    for strat in STRATEGIES:
        path = os.path.join(PHASE2_DIR, f"scenario_ArcCosts_{key}",
                            f"strategy_{strat}.xlsx")
        if os.path.exists(path):
            print(f"  {key}/{strat}")
            scenarios[key][strat] = load_solution(path)

print("Loading S4 (Suez crisis)…")
scenarios["S4"] = {}
for strat in STRATEGIES:
    path = os.path.join(S4_DIR, f"S4_strategy_{strat}.xlsx")
    if os.path.exists(path):
        print(f"  S4/{strat}")
        scenarios["S4"][strat] = load_solution(path)
    else:
        print(f"  [skip] S4/{strat} not found: {path}")

# ── Clean NaN for JSON ────────────────────────────────────────────────────────
def clean(obj):
    if isinstance(obj, dict):
        return {k: clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean(v) for v in obj]
    if isinstance(obj, float) and math.isnan(obj):
        return None
    try:
        return obj.item()
    except AttributeError:
        return obj

data_json = json.dumps(
    clean({"nodes": nodes, "baseline": baseline, "scenarios": scenarios}),
    ensure_ascii=False, default=str,
)

# ── HTML template ─────────────────────────────────────────────────────────────
TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>GlobalFlow — Network Visualization</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #F0F4F8;
  display: flex; flex-direction: column; height: 100vh;
}
/* ── Header ── */
header {
  background: linear-gradient(135deg, #1565C0 0%, #0D47A1 100%);
  color: #fff; padding: 10px 20px;
  display: flex; align-items: center; gap: 14px;
  box-shadow: 0 2px 6px rgba(0,0,0,.25);
  z-index: 10;
}
header h1 { font-size: 17px; font-weight: 700; letter-spacing: .02em; }
header span { font-size: 12px; opacity: .75; }

/* ── Layout ── */
#app { display: flex; flex: 1; overflow: hidden; }
#sidebar {
  width: 270px; min-width: 270px;
  background: #fff; overflow-y: auto;
  border-right: 1px solid #DDE3EA;
  display: flex; flex-direction: column; gap: 12px;
  padding: 14px 12px 20px;
}
#mapbox { flex: 1; position: relative; }
#map   { width: 100%; height: 100%; }

/* ── Section titles ── */
.sec-title {
  font-size: 10px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .09em; color: #90A4AE; margin-bottom: 6px;
}
.card {
  background: #F7F9FB; border-radius: 8px;
  padding: 10px 11px;
}

/* ── Radio / Checkbox ── */
.radio-grp, .check-grp {
  display: flex; flex-direction: column; gap: 7px;
}
.radio-grp label, .check-grp label {
  display: flex; align-items: center; gap: 8px;
  font-size: 13px; cursor: pointer; color: #212121;
}
.radio-grp input[type=radio],
.check-grp input[type=checkbox] { cursor: pointer; accent-color: #1565C0; }

/* ── Select ── */
select {
  width: 100%; padding: 6px 8px;
  border: 1px solid #CFD8DC; border-radius: 6px;
  font-size: 13px; background: #fff; cursor: pointer;
  color: #212121;
}

/* ── Product pills ── */
.pill-row { display: flex; flex-wrap: wrap; gap: 5px; }
.pill {
  display: inline-flex; align-items: center; gap: 5px;
  border: 2px solid #CFD8DC; border-radius: 20px;
  padding: 4px 10px; font-size: 12px; cursor: pointer;
  background: #fff; color: #546E7A; transition: all .15s;
  user-select: none;
}
.pill:hover { border-color: #90A4AE; }
.pill.on { color: #fff; border-color: transparent; }
.pill-A.on { background: #FF9800; }
.pill-B.on { background: #2196F3; }
.pill-C.on { background: #4CAF50; }

/* ── Toggle switch ── */
.toggle-row {
  display: flex; align-items: center; gap: 10px; font-size: 13px; color: #212121;
}
.sw { position: relative; display: inline-block; width: 38px; height: 22px; }
.sw input { opacity: 0; width: 0; height: 0; }
.sl {
  position: absolute; cursor: pointer;
  inset: 0; background: #CFD8DC; border-radius: 22px; transition: .25s;
}
.sl:before {
  content: ""; position: absolute;
  width: 16px; height: 16px; left: 3px; bottom: 3px;
  background: #fff; border-radius: 50%; transition: .25s;
}
input:checked + .sl { background: #F44336; }
input:checked + .sl:before { transform: translateX(16px); }

/* ── Info box ── */
#info {
  background: #E8F4FD; border-left: 3px solid #1565C0;
  border-radius: 6px; padding: 10px 12px;
  font-size: 12px; line-height: 1.65; color: #0D47A1;
}

/* ── Swatch ── */
.sw2 {
  width: 11px; height: 11px; border-radius: 2px;
  display: inline-block; vertical-align: middle;
}
.sw-A { background: #FF9800; }
.sw-B { background: #2196F3; }
.sw-C { background: #4CAF50; }
.sw-E { background: #F44336; }
</style>
</head>
<body>

<header>
  <h1>GlobalFlow &mdash; Network Visualization</h1>
  <span>Phase 1 baseline &bull; Phase 2 disruption scenarios &bull; S4 Suez crisis</span>
</header>

<div id="app">
<div id="sidebar">

  <!-- ── Case ─────────────────────────────── -->
  <div>
    <div class="sec-title">Case</div>
    <div class="card">
      <div class="radio-grp">
        <label><input type="radio" name="case" value="baseline" checked>
          Baseline (Phase&nbsp;1)</label>
        <label><input type="radio" name="case" value="scenario">
          Scenario (Phase&nbsp;2 / S4)</label>
      </div>
    </div>
  </div>

  <!-- ── Scenario config ────────────────────── -->
  <div id="scen-cfg" style="display:none">
    <div class="sec-title">Scenario</div>
    <div class="card" style="display:flex;flex-direction:column;gap:10px">

      <div>
        <div style="font-size:11px;color:#78909C;margin-bottom:4px">
          Disruption scenario
        </div>
        <select id="scen-sel">
          <optgroup label="Phase 2 — Assigned scenarios">
            <option value="T1">T1 — Tariff shock (+40% transatlantic/transpacific)</option>
            <option value="T2">T2 — Energy crisis (sea +10%, road +20%)</option>
            <option value="T3">T3 — Hub surcharge +30% (Q4 seasonal)</option>
            <option value="S1">S1 — H3 Singapore closed (Taiwan Strait)</option>
            <option value="S2">S2 — Korea–Europe corridor blocked</option>
            <option value="S3">S3 — Combined: T2 + S1</option>
          </optgroup>
          <optgroup label="Phase 3 — Extended analysis">
            <option value="S4">S4 — Suez Crisis / Red Sea (Houthi 2024)</option>
          </optgroup>
        </select>
      </div>

      <div>
        <div style="font-size:11px;color:#78909C;margin-bottom:4px">
          Strategy &mdash; tick to overlay multiple
        </div>
        <div class="check-grp">
          <label>
            <input type="checkbox" name="strat" value="R" checked>
            <b style="font-family:monospace">R</b> &mdash; Reroute (locked baseline)
          </label>
          <label>
            <input type="checkbox" name="strat" value="A">
            <b style="font-family:monospace">A</b> &mdash; Adapt (all decisions free)
          </label>
          <label>
            <input type="checkbox" name="strat" value="F">
            <b style="font-family:monospace">F</b> &mdash; Full redesign (greenfield)
          </label>
        </div>
      </div>

    </div>
  </div>

  <!-- ── Products ───────────────────────────── -->
  <div>
    <div class="sec-title">Products (click to toggle)</div>
    <div class="card">
      <div class="pill-row">
        <div class="pill pill-A on" data-prod="A_Fertilizers">
          <span class="sw2 sw-A"></span> A &mdash; Fertilizers
        </div>
        <div class="pill pill-B on" data-prod="B_Semiconductors">
          <span class="sw2 sw-B"></span> B &mdash; Semicon.
        </div>
        <div class="pill pill-C on" data-prod="C_BatteryComponents">
          <span class="sw2 sw-C"></span> C &mdash; Battery
        </div>
      </div>
    </div>
  </div>

  <!-- ── Emergency arcs ────────────────────── -->
  <div>
    <div class="sec-title">Emergency Arcs</div>
    <div class="card" style="display:flex;flex-direction:column;gap:7px">
      <div class="toggle-row">
        <label class="sw">
          <input type="checkbox" id="emg-toggle" checked>
          <span class="sl"></span>
        </label>
        <span>Highlight emergency arcs</span>
      </div>
      <div style="font-size:11px;color:#78909C">
        <span class="sw2 sw-E"></span>
        Emergency arcs shown in red (Strategy&nbsp;F only)
      </div>
    </div>
  </div>

  <!-- ── Info ───────────────────────────────── -->
  <div id="info">Select a case and filters to explore the network.</div>

</div><!-- /sidebar -->

<div id="mapbox"><div id="map"></div></div>
</div><!-- /app -->

<script>
// ════════════════════════════════════════════════════════════════════
// DATA (injected by Python)
// ════════════════════════════════════════════════════════════════════
const DATA = __DATA__;

// ── Constants ────────────────────────────────────────────────────────
const PROD_COLOR = {
  A_Fertilizers:       '#FF9800',
  B_Semiconductors:    '#2196F3',
  C_BatteryComponents: '#4CAF50',
};
const PROD_LABEL = {
  A_Fertilizers:       'A — Fertilizers',
  B_Semiconductors:    'B — Semiconductors',
  C_BatteryComponents: 'C — Battery Components',
};
const STRAT_DASH  = { R: 'solid', A: 'dash', F: 'dot'  };
const STRAT_WIDTH = { R: 1.8,     A: 2.1,    F: 2.4    };
const STRAT_LABEL = {
  R: 'R — Reroute',
  A: 'A — Adapt',
  F: 'F — Full redesign',
};

const NODE_CFG = {
  SU:  { label: 'Supplier',           color: '#FF5722', symbol: 'square',  size: 10 },
  HUB: { label: 'International Hub',  color: '#9C27B0', symbol: 'diamond', size: 14 },
  WH:  { label: 'Regional Warehouse', color: '#4CAF50', symbol: 'circle',  size: 9  },
  CU:  { label: 'Customer',           color: '#607D8B', symbol: 'circle',  size: 7  },
};

const nodeMap = {};
DATA.nodes.forEach(n => nodeMap[n.node_id] = n);

// ── UI state ─────────────────────────────────────────────────────────
const state = {
  mode:       'baseline',
  scenario:   'T1',
  strategies: ['R'],
  products:   ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents'],
  showEmg:    true,
};

// ── DOM ───────────────────────────────────────────────────────────────
const $caseRadios  = document.querySelectorAll('input[name="case"]');
const $scenCfg     = document.getElementById('scen-cfg');
const $scenSel     = document.getElementById('scen-sel');
const $stratChecks = document.querySelectorAll('input[name="strat"]');
const $prodPills   = document.querySelectorAll('.pill[data-prod]');
const $emgToggle   = document.getElementById('emg-toggle');
const $info        = document.getElementById('info');

// ── Event listeners ───────────────────────────────────────────────────
$caseRadios.forEach(r => r.addEventListener('change', () => {
  state.mode = r.value;
  $scenCfg.style.display = r.value === 'scenario' ? '' : 'none';
  render();
}));

$scenSel.addEventListener('change', () => {
  state.scenario = $scenSel.value;
  render();
});

$stratChecks.forEach(cb => cb.addEventListener('change', () => {
  state.strategies = [...document.querySelectorAll('input[name="strat"]:checked')]
                       .map(c => c.value);
  if (state.strategies.length === 0) {
    cb.checked = true;
    state.strategies = [cb.value];
  }
  render();
}));

$prodPills.forEach(pill => pill.addEventListener('click', () => {
  const p = pill.dataset.prod;
  if (state.products.includes(p)) {
    if (state.products.length === 1) return;
    state.products = state.products.filter(x => x !== p);
    pill.classList.remove('on');
  } else {
    state.products.push(p);
    pill.classList.add('on');
  }
  render();
}));

$emgToggle.addEventListener('change', () => {
  state.showEmg = $emgToggle.checked;
  render();
});

// ── Helpers ───────────────────────────────────────────────────────────
function getActiveSolutions() {
  if (state.mode === 'baseline') {
    return [{ sol: DATA.baseline, strategy: null }];
  }
  const scen = DATA.scenarios[state.scenario] || {};
  return state.strategies
    .filter(s => scen[s])
    .map(s => ({ sol: scen[s], strategy: s }));
}

function fmtCost(v) {
  if (v == null) return '—';
  return '$' + Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function lineCoords(srcId, tgtId) {
  const s = nodeMap[srcId], t = nodeMap[tgtId];
  if (!s || !t) return null;
  return {
    lats: [s.latitude,  t.latitude,  null],
    lons: [s.longitude, t.longitude, null],
  };
}

// ── Build Plotly traces ───────────────────────────────────────────────
function buildTraces(solutions) {
  const traces = [];

  solutions.forEach(({ sol, strategy }) => {
    const emgIds = new Set((sol.emergency_arcs || []).map(e => e.arc_id));

    // ── Flow traces (one per active product) ──
    const byProd = {};
    sol.flows.forEach(f => {
      if (!state.products.includes(f.product)) return;
      if (!byProd[f.product]) byProd[f.product] = [];
      byProd[f.product].push(f);
    });

    state.products.forEach(prod => {
      const flows = (byProd[prod] || []).filter(f => !emgIds.has(f.arc_id));
      if (!flows.length) return;

      const lats = [], lons = [], texts = [];
      flows.forEach(f => {
        const c = lineCoords(f.source, f.target);
        if (!c) return;
        lats.push(...c.lats); lons.push(...c.lons);
        const hov =
          `<b>${f.arc_id}</b>  ${f.source} → ${f.target}<br>` +
          `${PROD_LABEL[prod]}<br>` +
          `Flow: <b>${f.flow}</b> units<br>` +
          `Mode: ${f.transport_mode || '?'}<br>` +
          `Cost: ${fmtCost(f.flow_cost)}` +
          (strategy ? `<br>Strategy: <b>${STRAT_LABEL[strategy]}</b>` : '');
        texts.push(hov, '', '');
      });

      traces.push({
        type: 'scattergeo',
        lat: lats, lon: lons,
        mode: 'lines',
        line: {
          width: strategy ? STRAT_WIDTH[strategy] : 1.8,
          color: PROD_COLOR[prod],
          dash:  strategy ? STRAT_DASH[strategy]  : 'solid',
        },
        opacity: 0.70,
        name: strategy ? `${PROD_LABEL[prod]} [${strategy}]` : PROD_LABEL[prod],
        text: texts,
        hovertemplate: '%{text}<extra></extra>',
      });
    });

    // ── Emergency arc traces ──
    if (state.showEmg && sol.emergency_arcs && sol.emergency_arcs.length > 0) {
      const active = sol.emergency_arcs.filter(e =>
        e.flow > 0 && state.products.includes(e.product));
      if (active.length) {
        const lats = [], lons = [], texts = [];
        active.forEach(e => {
          const c = lineCoords(e.source, e.target);
          if (!c) return;
          lats.push(...c.lats); lons.push(...c.lons);
          const hov =
            `<b>⚠ EMERGENCY</b>  ${e.arc_id}<br>` +
            `${e.source} → ${e.target}<br>` +
            `${PROD_LABEL[e.product] || e.product}<br>` +
            `Flow: <b>${e.flow}</b> units<br>` +
            `Unit cost: ${fmtCost(e.unit_cost)} (×${e.premium_x} premium)`;
          texts.push(hov, '', '');
        });
        traces.push({
          type: 'scattergeo',
          lat: lats, lon: lons,
          mode: 'lines',
          line: { width: 3.2, color: '#F44336', dash: 'dashdot' },
          opacity: 0.90,
          name: strategy ? `⚠ Emergency [${strategy}]` : '⚠ Emergency',
          text: texts,
          hovertemplate: '%{text}<extra></extra>',
        });
      }
    }
  });

  // ── Node traces (always visible) ──
  const whOpen = {}, whUtil = {};
  solutions.forEach(({ sol }) => {
    (sol.warehouses || []).forEach(w => {
      if (w.open) whOpen[w.warehouse_id] = true;
      if (w['utilization_%'] != null) {
        whUtil[w.warehouse_id] = (whUtil[w.warehouse_id] || []);
        whUtil[w.warehouse_id].push(w['utilization_%']);
      }
    });
  });

  Object.entries(NODE_CFG).forEach(([ntype, cfg]) => {
    const subset = DATA.nodes.filter(n => n.type === ntype);
    const lats = [], lons = [], texts = [], colors = [];

    subset.forEach(n => {
      lats.push(n.latitude); lons.push(n.longitude);
      if (ntype === 'WH') {
        const open = whOpen[n.node_id] || false;
        const utils = whUtil[n.node_id];
        const utilStr = utils
          ? utils.map(u => u.toFixed(1) + '%').join(' / ')
          : '';
        const status = open
          ? (' ✓ OPEN' + (utilStr ? `  util: ${utilStr}` : ''))
          : ' (closed)';
        texts.push(`<b>${n.node_id}</b> — ${n.name}${status}`);
        colors.push(open ? cfg.color : '#BDBDBD');
      } else {
        texts.push(
          `<b>${n.node_id}</b> — ${n.name}<br>` +
          `Type: ${n.type}  Region: ${n.region}`
        );
        colors.push(cfg.color);
      }
    });

    traces.push({
      type: 'scattergeo',
      lat: lats, lon: lons,
      mode: 'markers+text',
      marker: {
        size: cfg.size,
        color: colors,
        symbol: cfg.symbol,
        line: { width: 1, color: 'white' },
      },
      text: subset.map(n => n.node_id),
      textposition: 'top center',
      textfont: { size: 8 },
      name: cfg.label,
      hovertext: texts,
      hoverinfo: 'text',
    });
  });

  return traces;
}

// ── Build info box HTML ───────────────────────────────────────────────
function buildInfo(solutions) {
  if (!solutions.length) return 'No data for current selection.';

  let html = '';
  solutions.forEach(({ sol, strategy }) => {
    const s = sol.summary || {};
    const label = strategy
      ? `Strategy <b>${STRAT_LABEL[strategy]}</b>`
      : '<b>Baseline</b>';
    const cost  = s['Logistics Cost ($)'] ?? s['Total Cost ($)'];
    const delta = s['ΔZ Disruption Cost ($)'];
    const pct   = s['ΔZ (%)'];
    const unmet = s['Units Unserved'];
    const emgActive = (sol.emergency_arcs || []).filter(e => e.flow > 0);

    html += `${label}<br>`;
    if (cost != null)  html += `Cost: <b>${fmtCost(cost)}</b><br>`;
    if (delta != null) {
      const sign = delta >= 0 ? '+' : '';
      html += `ΔZ vs baseline: ${sign}${fmtCost(delta)} ` +
              `(${sign}${Number(pct).toFixed(1)}%)<br>`;
    }
    if (unmet != null && unmet > 0)
      html += `⚠ Unserved: <b>${unmet}</b> units<br>`;
    if (emgActive.length)
      html += `🚨 Emergency arcs active: <b>${emgActive.length}</b><br>`;
    html += '<br>';
  });
  return html.trimEnd();
}

// ── Title ─────────────────────────────────────────────────────────────
function buildTitle(solutions) {
  if (state.mode === 'baseline') {
    const cost = solutions[0]?.sol?.summary?.['Total Cost ($)'];
    return 'GlobalFlow — Baseline Solution' +
           (cost ? `  |  ${fmtCost(cost)}` : '');
  }
  const strats = state.strategies.join(' + ');
  const costs = solutions
    .map(({ sol, strategy }) => {
      const c = sol.summary?.['Logistics Cost ($)'];
      return c != null ? `${strategy}: ${fmtCost(c)}` : null;
    })
    .filter(Boolean).join('  ');
  return `GlobalFlow — Scenario ${state.scenario}` +
         `  |  Strategy: ${strats}` +
         (costs ? `  |  ${costs}` : '');
}

// ── Render ────────────────────────────────────────────────────────────
function render() {
  const solutions = getActiveSolutions();
  const traces    = buildTraces(solutions);

  $info.innerHTML = buildInfo(solutions);

  Plotly.react('map', traces, {
    title: { text: buildTitle(solutions), x: 0.5, font: { size: 14 } },
    showlegend: true,
    legend: {
      x: 0.01, y: 0.99,
      bgcolor: 'rgba(255,255,255,0.88)',
      font: { size: 11 },
    },
    geo: {
      projection_type: 'natural earth',
      showland:        true,  landcolor:      '#F5F5F5',
      showocean:       true,  oceancolor:     '#E3F2FD',
      showcountries:   true,  countrycolor:   '#BDBDBD',
      showcoastlines:  true,  coastlinecolor: '#90A4AE',
      showframe:       false,
    },
    margin: { l: 0, r: 0, t: 46, b: 0 },
    paper_bgcolor: '#F0F4F8',
  }, { responsive: true, scrollZoom: true });
}

// Initial render
render();
</script>
</body>
</html>
"""

html = TEMPLATE.replace("__DATA__", data_json)

with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
    fh.write(html)

print(f"\nDone — map saved to: {OUTPUT_FILE}")