"""
GlobalFlow Network Design Optimization
=======================================
Flat, procedural script — easy to read and modify.
Structure mirrors the mathematical model (model_base_current_version.tex):
  1. Configuration
  2. Data loading
  3. Sets and parameters
  4. Decision variables
  5. Objective function
  6. Constraints (C1 – C7)
  7. Solve
  8. Report
  9. Export to Excel

To switch scenario: change SCENARIO below.
"""

import pandas as pd
import xpress as xp
import time

xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')

# =============================================================================
# 1. CONFIGURATION  ← only section you need to edit between runs
# =============================================================================

EXCEL_FILE = 'globalflow_instance.xlsx'

# Available scenarios (sheet names in the Excel file):
#   Baseline   : ArcCosts_Baseline
#   Tariff shocks : T1, T2, T3
#   Supply shocks : S1, S2, S3
SCENARIO = 'ArcCosts_Baseline'

PRODUCTS      = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME = 300   # seconds

OUTPUT_FILE   = f'testglobalflow_solution.xlsx'

# =============================================================================
# 2. DATA LOADING
# =============================================================================

print("=" * 70)
print(f"GlobalFlow — scenario: {SCENARIO}")
print("=" * 70)
print("\nLoading data...")

nodes_df     = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
arcs_df      = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
suppliers_df = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
demand_df    = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
supply_df    = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
costs_df     = pd.read_excel(EXCEL_FILE, sheet_name=SCENARIO)
tariffs_df   = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')

# =============================================================================
# 3. SETS AND PARAMETERS
# =============================================================================

# --- Node sets ----------------------------------------------------------------
# P = {A_Fertilizers, B_Semiconductors, C_BatteryComponents}  (defined above)
# S = all suppliers
S = set(suppliers_df['supplier_id'])

# H = hubs, W = warehouses, C = customers
H = set(nodes_df[nodes_df['type'] == 'HUB']['node_id'])
W = set(warehouses_df['warehouse_id'])
C = set(demand_df['customer_id'].unique())

# S^p — suppliers that produce product p (used in C2 constraint)
# e.g.  S_p['A_Fertilizers'] = {S1, S2, S3}
S_p = {}              # S_p[product]  = set of supplier IDs that produce it
supplier_prods = {}   # supplier_prods[supplier] = set of products it produces
for _, row in supply_df.iterrows():
    S_p.setdefault(row['product'], set()).add(row['supplier_id'])
    supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])

# --- Arc dictionaries ---------------------------------------------------------
# Pre-compute per-arc lookups so constraint loops stay clean (no nested search)

arc_src  = {}   # arc_id → source node
arc_tgt  = {}   # arc_id → target node
arc_cap  = {}   # arc_id → shared capacity
arc_fc   = {}   # arc_id → fixed activation cost
arc_mode = {}   # arc_id → transport mode
arc_dist = {}   # arc_id → distance (km)

# arcs_from[node] and arcs_into[node] = sets of arc_ids touching that node
arcs_from = {n: set() for n in nodes_df['node_id']}
arcs_into = {n: set() for n in nodes_df['node_id']}

for _, row in arcs_df.iterrows():
    a = row['arc_id']
    arc_src[a]  = row['from_id']
    arc_tgt[a]  = row['to_id']
    arc_cap[a]  = row['shared_capacity']
    arc_fc[a]   = row['fixed_activation_cost']
    arc_mode[a] = row['transport_mode']
    arc_dist[a] = row['distance_km']
    arcs_from[row['from_id']].add(a)
    arcs_into[row['to_id']].add(a)

# A_fixed   = optional arcs (need activation variable y)
# A_always  = always-active arcs (no activation variable needed)
A_fixed  = {a for a in arc_src if arc_fc[a] > 0}
A_always = {a for a in arc_src if arc_fc[a] == 0}

# --- Parameters ---------------------------------------------------------------

# Dem[c, p]  — customer demand
Dem = {(row['customer_id'], row['product']): row['demand']
       for _, row in demand_df.iterrows()}

# Sup[s, p]  — supplier capacity
Sup = {(row['supplier_id'], row['product']): row['supply']
       for _, row in supply_df.iterrows()}

# Warehouse parameters
wh_cap  = {row['warehouse_id']: row['capacity']     for _, row in warehouses_df.iterrows()}
wh_cost = {row['warehouse_id']: row['opening_cost'] for _, row in warehouses_df.iterrows()}

# Tariff rates: (zone_from, zone_to) → rate
tariff_rate = {(row['zone_pair_from'], row['zone_pair_to']): row['interzonal_tariff_rate']
               for _, row in tariffs_df.iterrows()}

# Zone pair per arc (from arcs sheet)
arc_zone_from = dict(zip(arcs_df['arc_id'], arcs_df['zone_from']))
arc_zone_to   = dict(zip(arcs_df['arc_id'], arcs_df['zone_to']))

# Variable base costs from the scenario sheet
var_cost = {(row['arc_id'], row['product']): row['variable_cost']
            for _, row in costs_df.iterrows()}

# TotalCost[a, p] = (1 + TariffRate[zone_from, zone_to]) * VarCost[a, p]
total_cost = {}
for (a, p), vc in var_cost.items():
    rate = tariff_rate.get((arc_zone_from.get(a), arc_zone_to.get(a)), 0.0)
    total_cost[(a, p)] = (1 + rate) * vc

print(f"  Suppliers: {len(S)}  |  Hubs: {len(H)}  |  Warehouses: {len(W)}  |  Customers: {len(C)}")
print(f"  Arcs: {len(arc_src)} total  ({len(A_fixed)} optional, {len(A_always)} always-active)")
print(f"  Products: {len(PRODUCTS)}  |  (arc, product) pairs: {len(total_cost)}")

# =============================================================================
# 4. DECISION VARIABLES
# =============================================================================

print("\nCreating decision variables...")

prob = xp.problem()
prob.setControl('MAXTIME', MAX_SOLVE_TIME)
prob.setControl('OUTPUTLOG', 1)

# x[a, p] >= 0  — flow of product p on arc a
# Restriction: for supplier arcs, only create x if that supplier produces p.
# This prevents phantom flows (a supplier "shipping" a product it doesn't make).
x = {}
for (a, p) in total_cost:
    src = arc_src[a]
    # For supplier arcs: only create the variable if that supplier actually produces p.
    # supplier_prods[src] = set of products src makes (e.g. {A_Fertilizers} for S1).
    # Using S_p here would be wrong — S_p is indexed by product, not by supplier.
    if src in S and p not in supplier_prods.get(src, set()):
        continue   # supplier does not produce p → skip variable
    x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

# openWarehouse[w] ∈ {0,1}  — 1 if warehouse w is open
openWarehouse = {w: prob.addVariable(name=f'open_{w}', vartype=xp.binary) for w in W}

# arc[a] ∈ {0,1}  — 1 if optional arc a is activated  (only for A_fixed)
arc_act = {a: prob.addVariable(name=f'arc_{a}', vartype=xp.binary) for a in A_fixed}

print(f"  Flow variables x:          {len(x)}")
print(f"  Warehouse variables open:  {len(openWarehouse)}")
print(f"  Arc activation variables:  {len(arc_act)}")

# =============================================================================
# 5. OBJECTIVE FUNCTION
# =============================================================================
# min  Σ_w  OpenCost_w * openWarehouse_w
#    + Σ_{a ∈ A_fixed}  FixActCost_a * arc_a
#    + Σ_{a,p}  TotalCost_{a,p} * x_{a,p}

print("\nBuilding objective function...")

obj = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)                    # warehouse opening
obj += xp.Sum(arc_fc[a] * arc_act[a] for a in A_fixed)                     # arc activation
obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)             # variable transport

prob.setObjective(obj, sense=xp.minimize)

# =============================================================================
# 6. CONSTRAINTS
# =============================================================================

print("Adding constraints...")

# Convenient inline helpers — return a Xpress sum expression for arcs touching node n
def inflow(node, product):
    """Sum of x[a, p] for all arcs a ending at node, for given product."""
    return xp.Sum(x[(a, product)] for a in arcs_into.get(node, []) if (a, product) in x)

def outflow(node, product):
    """Sum of x[a, p] for all arcs a starting at node, for given product."""
    return xp.Sum(x[(a, product)] for a in arcs_from.get(node, []) if (a, product) in x)

# (C1) Demand satisfaction — inflow to each customer equals demand
# Σ_{a ∈ S_target^c}  x_{a,p}  =  Dem_{c,p}    ∀ p ∈ P, c ∈ C
for (c, p), d in Dem.items():
    prob.addConstraint(xp.constraint(inflow(c, p) == d, name=f'C1_{c}_{p}'))

# (C2) Supply availability — outflow from each supplier ≤ available supply
# Σ_{a ∈ S_source^s}  x_{a,p}  ≤  Sup_{s,p}    ∀ p ∈ P, s ∈ S^p
# Inequality (≤) handles the slight supply > demand imbalance in the data.
for p, sup_set in S_p.items():
    for s in sup_set:
        prob.addConstraint(xp.constraint(outflow(s, p) <= Sup[(s, p)], name=f'C2_{s}_{p}'))

# (C3) Arc capacity — always-active arcs
# Σ_p  x_{a,p}  ≤  CapArc_a    ∀ a ∈ A_always
for a in A_always:
    flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
    prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))

# (C4) Arc capacity — optional arcs (capacity only if activated)
# Σ_p  x_{a,p}  ≤  CapArc_a * arc_a    ∀ a ∈ A_fixed
for a in A_fixed:
    flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
    prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a], name=f'C4_{a}'))

# (C5) Warehouse capacity — inflow ≤ capacity * open binary
# Σ_{a ∈ S_target^w} Σ_p  x_{a,p}  ≤  CapWare_w * openWarehouse_w    ∀ w ∈ W
for w in W:
    total_in = xp.Sum(x[(a, p)] for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
    prob.addConstraint(xp.constraint(total_in <= wh_cap[w] * openWarehouse[w], name=f'C5_{w}'))

# (C6) Flow conservation at warehouses — what enters must exit, per product
# Σ_{a ∈ S_source^w}  x_{a,p}  =  Σ_{a ∈ S_target^w}  x_{a,p}    ∀ p ∈ P, w ∈ W
for w in W:
    for p in PRODUCTS:
        prob.addConstraint(xp.constraint(inflow(w, p) == outflow(w, p), name=f'C6_{w}_{p}'))

# (C7) Flow conservation at hubs — same rule (hubs are uncapacitated transit points)
# Σ_{a ∈ S_source^h}  x_{a,p}  =  Σ_{a ∈ S_target^h}  x_{a,p}    ∀ p ∈ P, h ∈ H
for h in H:
    for p in PRODUCTS:
        prob.addConstraint(xp.constraint(inflow(h, p) == outflow(h, p), name=f'C7_{h}_{p}'))

# (C8/C9) Non-negativity and integrality are enforced by variable declarations above.

print("  Constraints added.")

# =============================================================================
# 7. SOLVE
# =============================================================================

print(f"\nSolving (time limit: {MAX_SOLVE_TIME}s)...\n")

t0 = time.time()
prob.solve()
solve_time = time.time() - t0

# =============================================================================
# 8. REPORT
# =============================================================================

status = prob.attributes.solstatus
print(f"\n{'=' * 70}")
print(f"SOLUTION REPORT  ({SCENARIO})")
print(f"{'=' * 70}")
print(f"Status     : {status}")
print(f"Solve time : {solve_time:.2f}s")

if status not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
    print("No feasible solution found — check data or constraints.")
    raise SystemExit(1)

obj_val = prob.getObjVal()

# Cost decomposition
fixed_wh  = sum(wh_cost[w]  * round(prob.getSolution(openWarehouse[w])) for w in W)
fixed_arc = sum(arc_fc[a]   * round(prob.getSolution(arc_act[a]))       for a in A_fixed)
var_trans = obj_val - fixed_wh - fixed_arc

print(f"\nCost breakdown:")
print(f"  Warehouse opening costs  : ${fixed_wh:>14,.2f}")
print(f"  Arc activation costs     : ${fixed_arc:>14,.2f}")
print(f"  Variable transport costs : ${var_trans:>14,.2f}")
print(f"  {'─' * 42}")
print(f"  TOTAL                    : ${obj_val:>14,.2f}")

# Open warehouses
open_wh = [w for w in W if round(prob.getSolution(openWarehouse[w])) == 1]
print(f"\nOpen warehouses ({len(open_wh)} / {len(W)}): {sorted(open_wh)}")

# Activated optional arcs
active_arcs = [a for a in A_fixed if round(prob.getSolution(arc_act[a])) == 1]
print(f"Activated optional arcs   ({len(active_arcs)} / {len(A_fixed)}): {sorted(active_arcs)}")

# Demand fulfillment per product
print(f"\nDemand fulfilment:")
for p in PRODUCTS:
    delivered  = sum(prob.getSolution(x[(a, p)]) for a in arcs_into.get(c, [])
                     for c in C if (a, p) in x and arc_tgt[a] in C)
    total_dem  = sum(v for (_, pp), v in Dem.items() if pp == p)
    print(f"  {p:35s}: {delivered:>7.1f} / {total_dem:.0f}")

# =============================================================================
# 9. EXPORT TO EXCEL
# =============================================================================

print(f"\nExporting solution to {OUTPUT_FILE}...")

# Arc metadata table (used in multiple sheets)
arc_meta = pd.DataFrame({
    'arc_id':         list(arc_src.keys()),
    'source':         [arc_src[a]  for a in arc_src],
    'target':         [arc_tgt[a]  for a in arc_src],
    'capacity':       [arc_cap[a]  for a in arc_src],
    'fixed_cost':     [arc_fc[a]   for a in arc_src],
    'transport_mode': [arc_mode[a] for a in arc_src],
    'distance_km':    [arc_dist[a] for a in arc_src],
}).set_index('arc_id')

# --- Per-product flow sheets --------------------------------------------------
product_dfs = {}
for p in PRODUCTS:
    rows = []
    for (a, pp) in x:
        if pp != p:
            continue
        flow = round(prob.getSolution(x[(a, p)]), 2)
        if flow <= 0.01:
            continue
        cap = arc_cap[a]
        rows.append({
            'arc_id':         a,
            'source':         arc_src[a],
            'target':         arc_tgt[a],
            'product':        p,
            'flow':           flow,
            'capacity':       cap,
            'utilization_%':  round(flow / cap * 100, 1),
            'var_cost':       round(var_cost.get((a, p), 0), 4),
            'total_cost':     round(total_cost.get((a, p), 0), 4),
            'flow_cost':      round(flow * total_cost.get((a, p), 0), 2),
            'transport_mode': arc_mode[a],
            'distance_km':    arc_dist[a],
        })
    product_dfs[p] = pd.DataFrame(rows).sort_values('arc_id')

# --- Warehouse sheet ----------------------------------------------------------
wh_rows = []
for w in sorted(W):
    opened    = round(prob.getSolution(openWarehouse[w]))
    total_in  = round(sum(
        prob.getSolution(x[(a, p)])
        for a in arcs_into[w] for p in PRODUCTS if (a, p) in x
    ), 2)
    cap = wh_cap[w]
    wh_rows.append({
        'warehouse_id':  w,
        'open':          opened,
        'opening_cost':  wh_cost[w],
        'capacity':      cap,
        'total_inflow':  total_in,
        'utilization_%': round(total_in / cap * 100, 1) if opened else None,
    })
wh_df = pd.DataFrame(wh_rows).sort_values(['open', 'warehouse_id'], ascending=[False, True])

# --- Arc activation sheet (optional arcs only) --------------------------------
arc_rows = []
for a in sorted(A_fixed):
    activated  = round(prob.getSolution(arc_act[a]))
    total_flow = round(sum(prob.getSolution(x[(a, p)]) for p in PRODUCTS if (a, p) in x), 2)
    cap = arc_cap[a]
    arc_rows.append({
        'arc_id':         a,
        'activated':      activated,
        'source':         arc_src[a],
        'target':         arc_tgt[a],
        'total_flow':     total_flow,
        'capacity':       cap,
        'utilization_%':  round(total_flow / cap * 100, 1) if activated else None,
        'fixed_cost':     arc_fc[a],
        'transport_mode': arc_mode[a],
        'distance_km':    arc_dist[a],
    })
arc_df = pd.DataFrame(arc_rows).sort_values(['activated', 'arc_id'], ascending=[False, True])

# --- Summary sheet ------------------------------------------------------------
summary_rows = [
    ('Scenario',                     SCENARIO),
    ('Solve Time (s)',                round(solve_time, 3)),
    ('Total Cost ($)',                round(obj_val, 2)),
    ('  Warehouse Opening Cost ($)',  round(fixed_wh, 2)),
    ('  Arc Activation Cost ($)',     round(fixed_arc, 2)),
    ('  Variable Transport Cost ($)', round(var_trans, 2)),
    ('', ''),
    ('Warehouses Open',               len(open_wh)),
    ('Warehouses Total',              len(W)),
    ('Optional Arcs Activated',       len(active_arcs)),
    ('Optional Arcs Total',           len(A_fixed)),
    ('', ''),
]
for p in PRODUCTS:
    delivered = sum(
        prob.getSolution(x[(a, p)])
        for c in C for a in arcs_into.get(c, [])
        if (a, p) in x
    )
    total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
    summary_rows.append((f'Demand Met — {p}', f'{delivered:.0f} / {total_dem:.0f}'))

summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

# --- Write workbook -----------------------------------------------------------
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    summary_df.to_excel(writer, sheet_name='Summary', index=False)
    for p, df in product_dfs.items():
        df.to_excel(writer, sheet_name=p.replace('_', ' ')[:31], index=False)
    wh_df.to_excel(writer, sheet_name='Warehouses', index=False)
    arc_df.to_excel(writer, sheet_name='Arc Activations', index=False)

print(f"Done — results in {OUTPUT_FILE}")
