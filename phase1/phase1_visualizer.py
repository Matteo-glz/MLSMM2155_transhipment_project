"""
Generate an interactive HTML map of the optimal GlobalFlow solution.
Product flows are split into three coloured arc traces.
Filter buttons let you show all products, a single one, or any pair.
"""

import pandas as pd
import plotly.graph_objects as go

SOLUTION_FILE = 'phase1/result/baseline_solution.xlsx'
INSTANCE_FILE = 'data/globalflow_instance.xlsx'

PRODUCTS = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']

PRODUCT_COLOR = {
    'A_Fertilizers':       '#FF9800',   # amber
    'B_Semiconductors':    '#2196F3',   # blue
    'C_BatteryComponents': '#4CAF50',   # green
}
PRODUCT_LABEL = {
    'A_Fertilizers':       'A — Fertilizers',
    'B_Semiconductors':    'B — Semiconductors',
    'C_BatteryComponents': 'C — Battery Components',
}

# =============================================================================
# Load data
# =============================================================================

nodes_df = pd.read_excel(INSTANCE_FILE, sheet_name='Nodes')
nodes    = {row['node_id']: row for _, row in nodes_df.iterrows()}

wh_df    = pd.read_excel(SOLUTION_FILE, sheet_name='Warehouses')
arc_df   = pd.read_excel(SOLUTION_FILE, sheet_name='Arc Activations')

flow_frames = []
for p in PRODUCTS:
    try:
        df = pd.read_excel(SOLUTION_FILE, sheet_name=p.replace('_', ' '))
        flow_frames.append(df)
    except Exception:
        pass
flows = pd.concat(flow_frames, ignore_index=True) if flow_frames else pd.DataFrame()

# =============================================================================
# Derived lookups
# =============================================================================

open_wh = set(wh_df[wh_df['open'] == 1]['warehouse_id'])

# Arc endpoint lookup: arc_id → (source, target)
arc_endpoints = {}
if not flows.empty:
    for _, row in flows[['arc_id', 'source', 'target']].drop_duplicates().iterrows():
        arc_endpoints[row['arc_id']] = (row['source'], row['target'])
for _, row in arc_df[['arc_id', 'source', 'target']].drop_duplicates().iterrows():
    if row['arc_id'] not in arc_endpoints:
        arc_endpoints[row['arc_id']] = (row['source'], row['target'])

def coord(node_id, field):
    return nodes[node_id][field] if node_id in nodes else None

# =============================================================================
# Arc traces — one per product
# =============================================================================
# Trace order matters for the visibility arrays in the buttons below:
#   index 0 → A arcs
#   index 1 → B arcs
#   index 2 → C arcs

arc_traces = []

for p in PRODUCTS:
    p_flows = flows[flows['product'] == p] if not flows.empty else pd.DataFrame()
    flow_by_arc = p_flows.groupby('arc_id')['flow'].sum().to_dict() if not p_flows.empty else {}

    lats, lons, hovers = [], [], []
    for arc_id, flow_val in flow_by_arc.items():
        if arc_id not in arc_endpoints:
            continue
        src, tgt = arc_endpoints[arc_id]
        lat0, lon0 = coord(src, 'latitude'), coord(src, 'longitude')
        lat1, lon1 = coord(tgt, 'latitude'), coord(tgt, 'longitude')
        if None in (lat0, lon0, lat1, lon1):
            continue
        lats  += [lat0, lat1, None]
        lons  += [lon0, lon1, None]
        hovers += [f"{arc_id}: {src}→{tgt} | {flow_val:.0f} units", "", ""]

    arc_traces.append(go.Scattergeo(
        lat=lats,
        lon=lons,
        mode='lines',
        line=dict(width=1.5, color=PRODUCT_COLOR[p]),
        opacity=0.65,
        name=PRODUCT_LABEL[p],
        hoverinfo='skip',
    ))

# =============================================================================
# Node traces — one per node type
# =============================================================================
# These are always visible regardless of the product filter.
# Indices 3-6 in the full trace list.

NODE_CFG = {
    'SU':  dict(label='Supplier',           color='#FF5722', symbol='square',  size=10),
    'HUB': dict(label='International Hub',  color='#9C27B0', symbol='diamond', size=14),
    'WH':  dict(label='Regional Warehouse', color='#4CAF50', symbol='circle',  size=9),
    'CU':  dict(label='Customer',           color='#607D8B', symbol='circle',  size=7),
}

node_traces = []
for ntype, cfg in NODE_CFG.items():
    subset = nodes_df[nodes_df['type'] == ntype]
    lats, lons, texts, colors = [], [], [], []

    for _, row in subset.iterrows():
        nid = row['node_id']
        if ntype == 'WH':
            wh_row = wh_df[wh_df['warehouse_id'] == nid]
            opened = bool(wh_row.iloc[0]['open'])       if not wh_row.empty else False
            inflow = wh_row.iloc[0]['total_inflow']     if not wh_row.empty else 0
            cap    = wh_row.iloc[0]['capacity']         if not wh_row.empty else '?'
            util   = wh_row.iloc[0]['utilization_%']    if not wh_row.empty else None
            if opened and util is not None:
                status = f' ✓ OPEN  {inflow:.0f}/{cap} ({util:.1f}%)'
            elif opened:
                status = ' ✓ OPEN'
            else:
                status = ' (closed)'
            colors.append(cfg['color'] if opened else '#BDBDBD')
        else:
            status = ''
            colors.append(cfg['color'])

        texts.append(f"<b>{nid}</b> — {row['name']}{status}")
        lats.append(row['latitude'])
        lons.append(row['longitude'])

    node_traces.append(go.Scattergeo(
        lat=lats,
        lon=lons,
        mode='markers+text',
        marker=dict(size=cfg['size'], color=colors, symbol=cfg['symbol'],
                    line=dict(width=1, color='white')),
        text=[r['node_id'] for _, r in subset.iterrows()],
        textposition='top center',
        textfont=dict(size=8),
        name=cfg['label'],
        hovertext=texts,
        hoverinfo='text',
    ))

# =============================================================================
# Filter buttons
# =============================================================================
# Total trace count: 3 arc traces + 4 node traces = 7.
# Visibility list: [arc_A, arc_B, arc_C, SU, HUB, WH, CU]
# Nodes are always True.

NODES_ON = [True] * len(node_traces)   # 4 × True

def vis(a=True, b=True, c=True):
    """Return full visibility list for given arc combination."""
    return [a, b, c] + NODES_ON

buttons = [
    dict(label='All products', method='update',
         args=[{'visible': vis(True,  True,  True)}]),
    dict(label='A — Fertilizers', method='update',
         args=[{'visible': vis(True,  False, False)}]),
    dict(label='B — Semiconductors', method='update',
         args=[{'visible': vis(False, True,  False)}]),
    dict(label='C — Battery Comp.', method='update',
         args=[{'visible': vis(False, False, True)}]),
    dict(label='A + B', method='update',
         args=[{'visible': vis(True,  True,  False)}]),
    dict(label='A + C', method='update',
         args=[{'visible': vis(True,  False, True)}]),
    dict(label='B + C', method='update',
         args=[{'visible': vis(False, True,  True)}]),
]

# =============================================================================
# Layout and export
# =============================================================================

try:
    summary_df  = pd.read_excel(SOLUTION_FILE, sheet_name='Summary')
    scen_row    = summary_df[summary_df['Metric'] == 'Scenario']
    cost_row    = summary_df[summary_df['Metric'] == 'Total Cost ($)']
    scenario    = scen_row.iloc[0]['Value']  if not scen_row.empty  else 'Baseline'
    total_cost  = f"  |  Total Cost: ${float(cost_row.iloc[0]['Value']):,.0f}" if not cost_row.empty else ''
except Exception:
    scenario, total_cost = 'Baseline', ''

fig = go.Figure(data=arc_traces + node_traces)

fig.update_layout(
    title=dict(
        text=f'GlobalFlow — Optimal Network Solution ({scenario}){total_cost}',
        x=0.5,
        font=dict(size=17),
    ),
    showlegend=True,
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.85)'),
    updatemenus=[dict(
        type='buttons',
        direction='right',
        x=0.5,
        xanchor='center',
        y=1.08,
        yanchor='top',
        showactive=True,
        buttons=buttons,
        bgcolor='white',
        bordercolor='#BDBDBD',
        font=dict(size=12),
    )],
    geo=dict(
        projection_type='natural earth',
        showland=True,      landcolor='#F5F5F5',
        showocean=True,     oceancolor='#E3F2FD',
        showcountries=True, countrycolor='#BDBDBD',
        showcoastlines=True,coastlinecolor='#90A4AE',
        showframe=False,
    ),
    margin=dict(l=0, r=0, t=80, b=0),
    height=680,
)

output_file = 'phase1/result/baseline_optimal_map.html'
fig.write_html(output_file)
print(f'Map saved to {output_file}')
