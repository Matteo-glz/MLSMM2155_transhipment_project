"""
Generate an interactive HTML map of the optimal GlobalFlow solution.
Reads from globalflow_solution.xlsx (multi-sheet export from global.py).
"""

import pandas as pd
import plotly.graph_objects as go

SOLUTION_FILE  = 'globalflow_solution.xlsx'
INSTANCE_FILE  = 'globalflow_instance.xlsx'
PRODUCTS       = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']

# ── Load instance geometry ─────────────────────────────────────────────────────

nodes_df = pd.read_excel(INSTANCE_FILE, sheet_name='Nodes')
nodes    = {row['node_id']: row for _, row in nodes_df.iterrows()}

# ── Load solution sheets ───────────────────────────────────────────────────────

wh_df  = pd.read_excel(SOLUTION_FILE, sheet_name='Warehouses')
arc_df = pd.read_excel(SOLUTION_FILE, sheet_name='Arc Activations')

# Combine per-product flow sheets into one dataframe
flow_frames = []
for product in PRODUCTS:
    sheet = product.replace('_', ' ')
    try:
        df = pd.read_excel(SOLUTION_FILE, sheet_name=sheet)
        flow_frames.append(df)
    except Exception:
        pass
flows = pd.concat(flow_frames, ignore_index=True) if flow_frames else pd.DataFrame()

# ── Derived sets ───────────────────────────────────────────────────────────────

open_wh        = set(wh_df[wh_df['open'] == 1]['warehouse_id'])
active_arc_ids = set(arc_df[arc_df['activated'] == 1]['arc_id'])

# Total flow per arc (sum across products)
arc_total_flow = flows.groupby('arc_id')['flow'].sum().to_dict() if not flows.empty else {}

# All arcs to draw: activated optional arcs + always-active arcs that carry flow
drawn_arc_ids = active_arc_ids | set(arc_total_flow.keys())

# Build arc endpoint lookup from flows (source/target columns are present)
# and fall back to arc_df for optional arcs with 0 flow
arc_endpoints = {}
if not flows.empty:
    for _, row in flows[['arc_id', 'source', 'target']].drop_duplicates().iterrows():
        arc_endpoints[row['arc_id']] = (row['source'], row['target'])
for _, row in arc_df[['arc_id', 'source', 'target']].drop_duplicates().iterrows():
    if row['arc_id'] not in arc_endpoints:
        arc_endpoints[row['arc_id']] = (row['source'], row['target'])

# ── Helper ─────────────────────────────────────────────────────────────────────

def coord(node_id, field):
    return nodes[node_id][field] if node_id in nodes else None

# ── Arc traces ─────────────────────────────────────────────────────────────────

arc_lats, arc_lons, arc_texts = [], [], []

for arc_id in drawn_arc_ids:
    if arc_id not in arc_endpoints:
        continue
    src, tgt = arc_endpoints[arc_id]
    lat0, lon0 = coord(src, 'latitude'), coord(src, 'longitude')
    lat1, lon1 = coord(tgt, 'latitude'), coord(tgt, 'longitude')
    if None in (lat0, lon0, lat1, lon1):
        continue
    flow = arc_total_flow.get(arc_id, 0)
    arc_lats += [lat0, lat1, None]
    arc_lons += [lon0, lon1, None]
    arc_texts += [f"{arc_id}: {src}→{tgt} ({flow:.0f} units)", "", ""]

arc_trace = go.Scattergeo(
    lat=arc_lats,
    lon=arc_lons,
    mode='lines',
    line=dict(width=1.2, color='#2196F3'),
    opacity=0.6,
    name='Active Arcs',
    hoverinfo='skip',
)

# ── Node traces ────────────────────────────────────────────────────────────────

type_config = {
    'SU':  dict(label='Supplier',           color='#FF5722', symbol='square',  size=10),
    'HUB': dict(label='International Hub',  color='#9C27B0', symbol='diamond', size=14),
    'WH':  dict(label='Regional Warehouse', color='#4CAF50', symbol='circle',  size=9),
    'CU':  dict(label='Customer',           color='#607D8B', symbol='circle',  size=7),
}

node_traces = []
for ntype, cfg in type_config.items():
    subset = nodes_df[nodes_df['type'] == ntype]
    lats, lons, texts, colors = [], [], [], []

    for _, row in subset.iterrows():
        nid   = row['node_id']
        label = row['name']

        if ntype == 'WH':
            wh_row  = wh_df[wh_df['warehouse_id'] == nid]
            opened  = bool(wh_row.iloc[0]['open']) if not wh_row.empty else False
            inflow  = wh_row.iloc[0]['total_inflow'] if not wh_row.empty else 0
            cap     = wh_row.iloc[0]['capacity']     if not wh_row.empty else '?'
            util    = wh_row.iloc[0]['utilization_%'] if not wh_row.empty else None
            status  = f' ✓ OPEN  {inflow:.0f}/{cap} ({util:.1f}%)' if opened and util is not None else (' ✓ OPEN' if opened else ' (closed)')
            colors.append(cfg['color'] if opened else '#BDBDBD')
        else:
            status = ''
            colors.append(cfg['color'])

        texts.append(f"<b>{nid}</b> — {label}{status}")
        lats.append(row['latitude'])
        lons.append(row['longitude'])

    node_traces.append(go.Scattergeo(
        lat=lats,
        lon=lons,
        mode='markers+text',
        marker=dict(
            size=cfg['size'],
            color=colors,
            symbol=cfg['symbol'],
            line=dict(width=1, color='white'),
        ),
        text=[r['node_id'] for _, r in subset.iterrows()],
        textposition='top center',
        textfont=dict(size=8),
        name=cfg['label'],
        hovertext=texts,
        hoverinfo='text',
    ))

# ── Layout ─────────────────────────────────────────────────────────────────────

# Pull scenario name from Summary sheet if available
try:
    summary_df = pd.read_excel(SOLUTION_FILE, sheet_name='Summary')
    scenario_row = summary_df[summary_df['Metric'] == 'Scenario']
    scenario = scenario_row.iloc[0]['Value'] if not scenario_row.empty else 'Baseline'
    cost_row = summary_df[summary_df['Metric'] == 'Total Cost ($)']
    total_cost = f"  |  Total Cost: ${float(cost_row.iloc[0]['Value']):,.0f}" if not cost_row.empty else ''
except Exception:
    scenario, total_cost = 'Baseline', ''

fig = go.Figure(data=[arc_trace] + node_traces)

fig.update_layout(
    title=dict(
        text=f'GlobalFlow — Optimal Network Solution ({scenario}){total_cost}',
        x=0.5,
        font=dict(size=18),
    ),
    showlegend=True,
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)'),
    geo=dict(
        projection_type='natural earth',
        showland=True,
        landcolor='#F5F5F5',
        showocean=True,
        oceancolor='#E3F2FD',
        showcountries=True,
        countrycolor='#BDBDBD',
        showcoastlines=True,
        coastlinecolor='#90A4AE',
        showframe=False,
    ),
    margin=dict(l=0, r=0, t=50, b=0),
    height=650,
)

output_file = 'globalflow_optimal_map.html'
fig.write_html(output_file)
print(f'Map saved to {output_file}')
