"""
Generate an interactive HTML map of the optimal GlobalFlow solution.
Uses the same scattergeo Plotly style as globalflow_network_map.html.
"""

import pandas as pd
import plotly.graph_objects as go

# ── Load data ─────────────────────────────────────────────────────────────────

nodes_df   = pd.read_excel('globalflow_instance.xlsx', sheet_name='Nodes')
arcs_df    = pd.read_excel('globalflow_instance.xlsx', sheet_name='Arcs')
solution   = pd.read_csv('globalflow_solution.csv')

# Node lookup: id → row
nodes = {row['node_id']: row for _, row in nodes_df.iterrows()}

# Arc lookup: arc_id → (from, to)
arc_endpoints = {row['arc_id']: (row['from_id'], row['to_id'])
                 for _, row in arcs_df.iterrows()}

# Solution subsets
flows       = solution[solution['type'] == 'flow'].copy()
warehouses  = solution[solution['type'] == 'warehouse'].copy()
activations = solution[solution['type'] == 'arc_activation'].copy()

flows['value'] = flows['value'].astype(float)

# Open warehouses
open_wh = set(warehouses[warehouses['value'] == 1]['warehouse_id'])

# Activated optional arcs (y=1) + always-active arcs that carry flow
active_arc_ids = set(activations[activations['value'] == 1]['arc_id'])
arcs_with_flow = set(flows['arc_id'].unique())
drawn_arcs = active_arc_ids | arcs_with_flow

# Total flow per arc (sum across products)
arc_total_flow = flows.groupby('arc_id')['value'].sum().to_dict()
arc_max_flow = max(arc_total_flow.values()) if arc_total_flow else 1

# ── Helper ────────────────────────────────────────────────────────────────────

def get_coord(node_id, field):
    return nodes[node_id][field] if node_id in nodes else None

# ── Build arc traces (one line per arc with flow) ─────────────────────────────

arc_lats, arc_lons, arc_texts = [], [], []

for arc_id in drawn_arcs:
    if arc_id not in arc_endpoints:
        continue
    src, tgt = arc_endpoints[arc_id]
    lat0, lon0 = get_coord(src, 'latitude'), get_coord(src, 'longitude')
    lat1, lon1 = get_coord(tgt, 'latitude'), get_coord(tgt, 'longitude')
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

# ── Node traces by type ───────────────────────────────────────────────────────

type_config = {
    'SU': dict(label='Supplier',            color='#FF5722', symbol='square',   size=10),
    'HUB': dict(label='International Hub',  color='#9C27B0', symbol='diamond',  size=14),
    'WH': dict(label='Regional Warehouse',  color='#4CAF50', symbol='circle',   size=9),
    'CU': dict(label='Customer',            color='#607D8B', symbol='circle',   size=7),
}

node_traces = []
for ntype, cfg in type_config.items():
    subset = nodes_df[nodes_df['type'] == ntype]
    lats, lons, texts = [], [], []
    for _, row in subset.iterrows():
        nid = row['node_id']
        label = row['name']
        extra = ''
        if ntype == 'WH':
            extra = ' ✓ OPEN' if nid in open_wh else ' (closed)'
        texts.append(f"<b>{nid}</b> — {label}{extra}")
        lats.append(row['latitude'])
        lons.append(row['longitude'])

    # Dim closed warehouses
    if ntype == 'WH':
        marker_colors = [
            cfg['color'] if nodes_df[nodes_df['node_id'] == r['node_id']].iloc[0]['node_id'] in open_wh
            else '#BDBDBD'
            for _, r in subset.iterrows()
        ]
    else:
        marker_colors = cfg['color']

    node_traces.append(go.Scattergeo(
        lat=lats,
        lon=lons,
        mode='markers+text',
        marker=dict(
            size=cfg['size'],
            color=marker_colors,
            symbol=cfg['symbol'],
            line=dict(width=1, color='white'),
        ),
        text=[nodes_df[nodes_df['node_id'] == r['node_id']].iloc[0]['node_id'] for _, r in subset.iterrows()],
        textposition='top center',
        textfont=dict(size=8),
        name=cfg['label'],
        hovertext=texts,
        hoverinfo='text',
    ))

# ── Layout ────────────────────────────────────────────────────────────────────

fig = go.Figure(data=[arc_trace] + node_traces)

fig.update_layout(
    title=dict(
        text='GlobalFlow — Optimal Network Solution (Baseline)',
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
