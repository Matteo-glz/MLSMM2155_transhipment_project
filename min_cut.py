"""
GlobalFlow — Minimum Cut Analysis
==================================
Identifies the structurally critical arcs and nodes of the baseline network
using max-flow / min-cut theory (Ford-Fulkerson theorem):
    max flow from supply to demand  =  capacity of the minimum cut

Three analyses are performed:

  1. GLOBAL MIN-CUT — single super-source (all suppliers) to super-sink
     (all customers), shared capacity.  Finds the bottleneck of the
     entire network regardless of product.

  2. PER-PRODUCT MIN-CUT — same structure but the graph is built using
     only the arcs and supply/demand relevant to each product separately.
     Reveals product-specific vulnerabilities.

  3. NODE CRITICALITY — for each hub and warehouse, temporarily remove
     the node (and its incident arcs) and recompute the global max-flow.
     The drop in max-flow measures the node's criticality.

Outputs
-------
  mincut_analysis.xlsx   — three sheets: GlobalCut, ProductCuts, NodeCriticality
  printed summary to console
"""

import pandas as pd
import networkx as nx
import os

EXCEL_FILE  = '/Users/matteogalizia/Documents/GitHub/MLSMM2155_transhipment_project/data/globalflow_instance.xlsx'
OUTPUT_FILE = 'phase2/results/mincut_analysis.xlsx'
PRODUCTS    = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# =============================================================================
# 1. LOAD DATA
# =============================================================================

print("Loading data...")

arcs_df      = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
nodes_df     = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
supply_df    = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
demand_df    = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
warehouses_df= pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')

suppliers  = set(supply_df['supplier_id'].unique())
customers  = set(demand_df['customer_id'].unique())
hubs       = set(nodes_df[nodes_df['type'] == 'HUB']['node_id'])
warehouses = set(warehouses_df['warehouse_id'])

total_supply = supply_df['supply'].sum()
total_demand = demand_df['demand'].sum()

print(f"  Nodes : {len(nodes_df)} total  "
      f"({len(suppliers)} suppliers, {len(hubs)} hubs, "
      f"{len(warehouses)} warehouses, {len(customers)} customers)")
print(f"  Arcs  : {len(arcs_df)}")
print(f"  Total supply : {total_supply}   Total demand : {total_demand}")


# =============================================================================
# HELPER — build directed graph with super-source and super-sink
# =============================================================================

def build_graph(arc_rows, sup_dict, dem_dict):
    """
    arc_rows : iterable of (from_id, to_id, capacity)
    sup_dict : {node: supply_capacity}   — edges from SUPER_SOURCE
    dem_dict : {node: demand}            — edges to SUPER_SINK
    Returns a DiGraph with 'SUPER_SRC' and 'SUPER_SNK' nodes.
    """
    G = nx.DiGraph()

    # Real arcs
    for from_id, to_id, cap in arc_rows:
        if G.has_edge(from_id, to_id):
            G[from_id][to_id]['capacity'] += cap   # parallel arcs → sum
        else:
            G.add_edge(from_id, to_id, capacity=cap)

    # Super-source → each supplier
    for node, cap in sup_dict.items():
        G.add_edge('SUPER_SRC', node, capacity=cap)

    # Each customer → super-sink
    for node, cap in dem_dict.items():
        G.add_edge(node, 'SUPER_SNK', capacity=cap)

    return G


def arc_rows_baseline():
    """Yields (from_id, to_id, shared_capacity) for all baseline arcs."""
    for _, row in arcs_df.iterrows():
        yield row['from_id'], row['to_id'], row['shared_capacity']


def run_mincut(G):
    """Returns (cut_value, cut_arcs) where cut_arcs is a list of (u,v) edge tuples."""
    cut_value, (reachable, non_reachable) = nx.minimum_cut(G, 'SUPER_SRC', 'SUPER_SNK')
    cut_arcs = [
        (u, v)
        for u in reachable
        for v in G.successors(u)
        if v in non_reachable
    ]
    return cut_value, cut_arcs, reachable, non_reachable


# =============================================================================
# 2. GLOBAL MIN-CUT (all products, shared capacity)
# =============================================================================

print("\n" + "=" * 60)
print("ANALYSIS 1 — Global min-cut (all products combined)")
print("=" * 60)

sup_all = {s: supply_df[supply_df['supplier_id'] == s]['supply'].sum()
           for s in suppliers}
dem_all = {c: demand_df[demand_df['customer_id'] == c]['demand'].sum()
           for c in customers}

G_global = build_graph(arc_rows_baseline(), sup_all, dem_all)
cut_val_global, cut_arcs_global, reach_g, non_reach_g = run_mincut(G_global)

print(f"  Max-flow (= min-cut capacity) : {cut_val_global:.0f}")
print(f"  Total demand                  : {total_demand}")
print(f"  Bottleneck ratio              : {cut_val_global / total_demand * 100:.1f}% of demand satisfiable")
print(f"\n  Cut arcs ({len(cut_arcs_global)}):")

global_rows = []
for u, v in sorted(cut_arcs_global):
    cap = G_global[u][v]['capacity']
    arc_id = arcs_df[(arcs_df['from_id'] == u) & (arcs_df['to_id'] == v)]['arc_id'].values
    arc_id_str = arc_id[0] if len(arc_id) > 0 else 'SUPER'
    zone_from = arcs_df[(arcs_df['from_id'] == u) & (arcs_df['to_id'] == v)]['zone_from'].values
    zone_to   = arcs_df[(arcs_df['from_id'] == u) & (arcs_df['to_id'] == v)]['zone_to'].values
    zf = zone_from[0] if len(zone_from) > 0 else ''
    zt = zone_to[0]   if len(zone_to)   > 0 else ''
    print(f"    {arc_id_str:6s}  {u:6s} → {v:6s}  cap={cap:>6.0f}  ({zf} → {zt})")
    global_rows.append({
        'arc_id': arc_id_str, 'from': u, 'to': v,
        'capacity': cap, 'zone_from': zf, 'zone_to': zt,
    })

global_df = pd.DataFrame(global_rows)


# =============================================================================
# 3. PER-PRODUCT MIN-CUT
# =============================================================================

print("\n" + "=" * 60)
print("ANALYSIS 2 — Per-product min-cut")
print("=" * 60)

product_rows_all = []

for p in PRODUCTS:
    sup_p = {row['supplier_id']: row['supply']
             for _, row in supply_df[supply_df['product'] == p].iterrows()}
    dem_p = {row['customer_id']: row['demand']
             for _, row in demand_df[demand_df['product'] == p].iterrows()}
    total_dem_p = sum(dem_p.values())

    G_p = build_graph(arc_rows_baseline(), sup_p, dem_p)
    cut_val_p, cut_arcs_p, _, _ = run_mincut(G_p)

    print(f"\n  {p}")
    print(f"    Min-cut capacity : {cut_val_p:.0f}  /  demand {total_dem_p}"
          f"  ({cut_val_p / total_dem_p * 100:.1f}%)")
    print(f"    Cut arcs ({len(cut_arcs_p)}):")

    for u, v in sorted(cut_arcs_p):
        cap = G_p[u][v]['capacity']
        arc_id = arcs_df[(arcs_df['from_id'] == u) & (arcs_df['to_id'] == v)]['arc_id'].values
        arc_id_str = arc_id[0] if len(arc_id) > 0 else 'SUPER'
        print(f"      {arc_id_str:6s}  {u:5s} → {v:5s}  cap={cap:>6.0f}")
        product_rows_all.append({
            'product': p, 'arc_id': arc_id_str,
            'from': u, 'to': v, 'capacity': cap,
            'total_demand': total_dem_p, 'cut_value': cut_val_p,
            'bottleneck_%': round(cut_val_p / total_dem_p * 100, 1),
        })

product_df = pd.DataFrame(product_rows_all)


# =============================================================================
# 4. NODE CRITICALITY (hubs and warehouses)
# =============================================================================

print("\n" + "=" * 60)
print("ANALYSIS 3 — Node criticality (flow drop when node removed)")
print("=" * 60)

node_rows = []

for node in sorted(hubs | warehouses):
    # Remove node and all its incident arcs
    arcs_without = arcs_df[
        (arcs_df['from_id'] != node) & (arcs_df['to_id'] != node)
    ]
    G_no_node = build_graph(
        ((r['from_id'], r['to_id'], r['shared_capacity'])
         for _, r in arcs_without.iterrows()),
        sup_all, dem_all
    )
    try:
        cut_val_no, _, _, _ = run_mincut(G_no_node)
    except Exception:
        cut_val_no = 0.0

    flow_drop = cut_val_global - cut_val_no
    drop_pct  = flow_drop / cut_val_global * 100 if cut_val_global > 0 else 0
    node_type = 'HUB' if node in hubs else 'WH'

    print(f"  {node:4s} ({node_type}) : max-flow without = {cut_val_no:>7.0f}"
          f"  drop = {flow_drop:>6.0f}  ({drop_pct:+.1f}%)")

    node_rows.append({
        'node_id':       node,
        'type':          node_type,
        'maxflow_without': round(cut_val_no, 1),
        'flow_drop':     round(flow_drop, 1),
        'drop_%':        round(drop_pct, 2),
        'critical':      'YES' if flow_drop > 0 else 'no',
    })

node_df = (pd.DataFrame(node_rows)
           .sort_values('flow_drop', ascending=False)
           .reset_index(drop=True))

print(f"\n  Most critical nodes:")
print(node_df[node_df['flow_drop'] > 0]
      [['node_id', 'type', 'flow_drop', 'drop_%']].to_string(index=False))


# =============================================================================
# 5. SUMMARY AND INTERPRETATION
# =============================================================================

print("\n" + "=" * 60)
print("SUMMARY — Strategic implications")
print("=" * 60)

print(f"\n  Global min-cut = {cut_val_global:.0f} units")
print(f"  The network can theoretically satisfy {cut_val_global/total_demand*100:.1f}% of total demand.")
print(f"\n  Critical cut arcs (appear in global min-cut):")
for _, row in global_df[~global_df['arc_id'].str.startswith('SUPER')].iterrows():
    print(f"    {row['arc_id']}  {row['from']} → {row['to']}  cap={row['capacity']:.0f}")

critical_nodes = node_df[node_df['flow_drop'] > 0]
if not critical_nodes.empty:
    print(f"\n  Critical nodes (removal reduces max-flow):")
    for _, row in critical_nodes.iterrows():
        print(f"    {row['node_id']} ({row['type']}) : -{row['flow_drop']:.0f} units ({row['drop_%']:+.1f}%)")

print(f"\n  Scenario design recommendation:")
print(f"  The arcs in the global min-cut and the nodes with highest")
print(f"  flow_drop are the most impactful targets for adversarial scenarios.")
print(f"  A Suez Canal closure would remove H1↔H2 and H2↔H3 sea arcs,")
print(f"  which together form the Europe–MiddleEast–Asia backbone.")


# =============================================================================
# 6. EXPORT TO EXCEL
# =============================================================================

print(f"\nExporting to {OUTPUT_FILE} ...")

summary_rows = [
    ('Global min-cut value',          round(cut_val_global, 0)),
    ('Total demand',                   total_demand),
    ('Bottleneck ratio (%)',           round(cut_val_global / total_demand * 100, 1)),
    ('Number of cut arcs (global)',    len(global_df)),
    ('Critical nodes (flow drop > 0)', len(critical_nodes)),
]
summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    summary_df.to_excel(writer,    sheet_name='Summary',         index=False)
    global_df.to_excel(writer,     sheet_name='GlobalCut',       index=False)
    product_df.to_excel(writer,    sheet_name='ProductCuts',     index=False)
    node_df.to_excel(writer,       sheet_name='NodeCriticality', index=False)

print(f"Done — {OUTPUT_FILE}")