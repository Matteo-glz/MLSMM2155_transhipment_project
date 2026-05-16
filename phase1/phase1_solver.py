"""
GlobalFlow Network Design Optimization
=======================================
Flat, procedural script — easy to read and modify.
Structure mirrors the mathematical model:
  1.  Configuration
  2.  Data loading
  3.  Sets and parameters
  4.  Decision variables
  5.  Objective function
  6.  Constraints (C1 – C7)
  7.  Solve (MIP)
  8.  Report
  9.  Export to Excel
  10. LP Relaxation analysis (aggregated vs. disaggregated C4)

To switch scenario: change SCENARIO below.
"""

import os
import pandas as pd
import xpress as xp
import time

xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')

# =============================================================================
# 1. CONFIGURATION  ← only section you need to edit between runs
# =============================================================================

EXCEL_FILE     = 'data/globalflow_instance.xlsx'
SCENARIO       = 'ArcCosts_Baseline'
PRODUCTS       = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME = 300   # seconds
OUTPUT_FILE    = 'phase1/results/baseline_solution.xlsx'

# =============================================================================
# 2. DATA LOADING
# =============================================================================

print("=" * 70)
print(f"GlobalFlow — scenario: {SCENARIO}")
print("=" * 70)
print("\nLoading data...")

nodes_df      = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
arcs_df       = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
suppliers_df  = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
demand_df     = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
supply_df     = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
costs_df      = pd.read_excel(EXCEL_FILE, sheet_name=SCENARIO)
tariffs_df    = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')

# =============================================================================
# 3. SETS AND PARAMETERS
# =============================================================================

S = set(suppliers_df['supplier_id'])
H = set(nodes_df[nodes_df['type'] == 'HUB']['node_id'])
W = set(warehouses_df['warehouse_id'])
C = set(demand_df['customer_id'].unique())

S_p            = {}   # S_p[product]   = set of supplier IDs that produce it
supplier_prods = {}   # supplier_prods[supplier] = set of products it produces
for _, row in supply_df.iterrows():
    S_p.setdefault(row['product'], set()).add(row['supplier_id'])
    supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])

arc_src  = {}
arc_tgt  = {}
arc_cap  = {}
arc_fc   = {}
arc_mode = {}
arc_dist = {}

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

_cap_overrides = {}
for _, _sup_row in supply_df.iterrows():
    _s, _sup_vol = _sup_row['supplier_id'], _sup_row['supply']
    _s_arcs = [a for a, src in arc_src.items() if src == _s]
    if len(_s_arcs) == 1:                       # single outgoing arc only
        _a = _s_arcs[0]
        if arc_cap[_a] < _sup_vol:              # cap genuinely below supply
            arc_cap[_a] = _sup_vol
            _cap_overrides[_a] = (_sup_vol, _sup_row['supply'])
if _cap_overrides:
    print(f"  [data fix] Arc capacity raised to match supplier supply: {_cap_overrides}")
    
A_fixed  = {a for a in arc_src if arc_fc[a] > 0}
A_always = {a for a in arc_src if arc_fc[a] == 0}

Dem = {(row['customer_id'], row['product']): row['demand']
       for _, row in demand_df.iterrows()}
Sup = {(row['supplier_id'], row['product']): row['supply']
       for _, row in supply_df.iterrows()}
wh_cap  = {row['warehouse_id']: row['capacity']     for _, row in warehouses_df.iterrows()}
wh_cost = {row['warehouse_id']: row['opening_cost'] for _, row in warehouses_df.iterrows()}

tariff_rate   = {(row['zone_pair_from'], row['zone_pair_to']): row['interzonal_tariff_rate']
                 for _, row in tariffs_df.iterrows()}
arc_zone_from = dict(zip(arcs_df['arc_id'], arcs_df['zone_from']))
arc_zone_to   = dict(zip(arcs_df['arc_id'], arcs_df['zone_to']))

var_cost   = {(row['arc_id'], row['product']): row['variable_cost']
              for _, row in costs_df.iterrows()}
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
prob.setControl('MAXTIME',    MAX_SOLVE_TIME)
prob.setControl('OUTPUTLOG',  1)

# x[a, p] >= 0 — flow of product p on arc a
x = {}
for (a, p) in total_cost:
    src = arc_src[a]
    if src in S and p not in supplier_prods.get(src, set()):
        continue
    x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

# openWarehouse[w] ∈ {0,1}
openWarehouse = {w: prob.addVariable(name=f'open_{w}', vartype=xp.binary) for w in W}

# arc_act[a] ∈ {0,1}  — only for optional arcs
arc_act = {a: prob.addVariable(name=f'arc_{a}', vartype=xp.binary) for a in A_fixed}

print(f"  Flow variables x:          {len(x)}")
print(f"  Warehouse variables open:  {len(openWarehouse)}")
print(f"  Arc activation variables:  {len(arc_act)}")

# =============================================================================
# 5. OBJECTIVE FUNCTION
# =============================================================================
# min  Σ_w  OpenCost_w * open_w
#    + Σ_{a ∈ A_fixed}  FixActCost_a * arc_a
#    + Σ_{a,p}  TotalCost_{a,p} * x_{a,p}

print("\nBuilding objective function...")

obj  = xp.Sum(wh_cost[w]        * openWarehouse[w] for w in W)
obj += xp.Sum(arc_fc[a]         * arc_act[a]        for a in A_fixed)
obj += xp.Sum(total_cost[(a,p)] * x[(a, p)]         for (a, p) in x)
prob.setObjective(obj, sense=xp.minimize)

# =============================================================================
# 6. CONSTRAINTS
# =============================================================================

print("Adding constraints...")

def inflow(node, product):
    return xp.Sum(x[(a, product)] for a in arcs_into.get(node, []) if (a, product) in x)

def outflow(node, product):
    return xp.Sum(x[(a, product)] for a in arcs_from.get(node, []) if (a, product) in x)

# C1 — demand satisfaction
for (c, p), d in Dem.items():
    prob.addConstraint(xp.constraint(inflow(c, p) == d, name=f'C1_{c}_{p}'))

# C2 — supply availability
for p, sup_set in S_p.items():
    for s in sup_set:
        prob.addConstraint(xp.constraint(outflow(s, p) <= Sup[(s, p)], name=f'C2_{s}_{p}'))

# C3 — arc capacity (always-on)
for a in A_always:
    flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
    prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))

# C4 — arc capacity (optional, aggregated: one constraint per arc)
# Σ_p x[a,p] ≤ CapArc_a * arc_a   ∀ a ∈ A_fixed
for a in A_fixed:
    flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
    prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a], name=f'C4_{a}'))

# C5 — warehouse capacity
for w in W:
    total_in = xp.Sum(x[(a, p)] for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
    prob.addConstraint(xp.constraint(total_in <= wh_cap[w] * openWarehouse[w], name=f'C5_{w}'))

# C6 — flow conservation at warehouses
for w in W:
    for p in PRODUCTS:
        prob.addConstraint(xp.constraint(inflow(w, p) == outflow(w, p), name=f'C6_{w}_{p}'))

# C7 — flow conservation at hubs
for h in H:
    for p in PRODUCTS:
        prob.addConstraint(xp.constraint(inflow(h, p) == outflow(h, p), name=f'C7_{h}_{p}'))

print("  Constraints added.")

# =============================================================================
# 7. SOLVE (MIP)
# =============================================================================

print(f"\nSolving MIP (time limit: {MAX_SOLVE_TIME}s)...\n")

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
print(f"Solve time : {solve_time:.3f}s")

if status not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
    print("No feasible solution found — check data or constraints.")
    raise SystemExit(1)

obj_val   = prob.attributes.objval
fixed_wh  = sum(wh_cost[w] * round(prob.getSolution(openWarehouse[w])) for w in W)
fixed_arc = sum(arc_fc[a]  * round(prob.getSolution(arc_act[a]))       for a in A_fixed)
var_trans = obj_val - fixed_wh - fixed_arc

print(f"\nCost breakdown:")
print(f"  Warehouse opening costs  : ${fixed_wh:>14,.2f}")
print(f"  Arc activation costs     : ${fixed_arc:>14,.2f}")
print(f"  Variable transport costs : ${var_trans:>14,.2f}")
print(f"  {'─' * 42}")
print(f"  TOTAL Z*                 : ${obj_val:>14,.2f}")

open_wh     = sorted(w for w in W      if round(prob.getSolution(openWarehouse[w])) == 1)
active_arcs = sorted(a for a in A_fixed if round(prob.getSolution(arc_act[a]))       == 1)
print(f"\nOpen warehouses ({len(open_wh)} / {len(W)}): {open_wh}")
print(f"Activated optional arcs ({len(active_arcs)} / {len(A_fixed)}): {active_arcs}")

print(f"\nDemand fulfilment:")
for p in PRODUCTS:
    # Correct loop order: iterate customers, then their incoming arcs
    delivered = sum(
        prob.getSolution(x[(a, p)])
        for c in C
        for a in arcs_into.get(c, [])
        if (a, p) in x
    )
    total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
    print(f"  {p:35s}: {delivered:>7.1f} / {total_dem:.0f}")

# =============================================================================
# 9. EXPORT TO EXCEL  (MIP solution)
# =============================================================================

print(f"\nExporting MIP solution to {OUTPUT_FILE}...")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Per-product flow sheets
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

# Warehouse sheet
wh_rows = []
for w in sorted(W):
    opened   = round(prob.getSolution(openWarehouse[w]))
    total_in = round(sum(
        prob.getSolution(x[(a, p)])
        for a in arcs_into[w] for p in PRODUCTS if (a, p) in x), 2)
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

# Arc activation sheet
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

# Summary sheet
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

# Write MIP workbook (LP sheet added below after LP solve)
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    summary_df.to_excel(writer, sheet_name='Summary',         index=False)
    for p, df in product_dfs.items():
        df.to_excel(writer, sheet_name=p.replace('_', ' ')[:31], index=False)
    wh_df.to_excel(writer,  sheet_name='Warehouses',          index=False)
    arc_df.to_excel(writer, sheet_name='Arc Activations',     index=False)

print(f"  MIP solution written.")

# =============================================================================
# 10. LP RELAXATION — formulation quality analysis
#     Compares two formulations:
#       Aggregated (C4)     : sum_p x[a,p] <= cap[a] * z[a]  — 47 constraints
#       Disaggregated (C4') : x[a,p]       <= cap[a] * z[a]  — 141 constraints
#
#     Both LP problems are identical to the MIP above except that the
#     binary variables y_w and z_a are relaxed to continuous [0, 1].
#     The LP-IP gap = (Z* - Z_LP*) / Z* measures formulation tightness.
# =============================================================================

print(f"\n{'=' * 70}")
print("LP RELAXATION  (formulation quality — aggregated vs. disaggregated C4)")
print(f"{'=' * 70}")

def solve_lp_relaxation(disaggregated: bool) -> tuple:
    """
    Build and solve the LP relaxation of the Phase 1 model.

    Parameters
    ----------
    disaggregated : bool
        If False  → aggregated C4  : sum_p x[a,p] <= cap[a] * z[a]
        If True   → disaggregated  : x[a,p]       <= cap[a] * z[a]  per product

    Returns
    -------
    (Z_LP, frac_w, frac_a, elapsed)
        Z_LP    : LP objective value (None if infeasible)
        frac_w  : list of (warehouse_id, lp_value) for fractional y_w vars
        frac_a  : list of (arc_id, lp_value) for fractional z_a vars
        elapsed : wall-clock solve time (seconds)
    """
    label = "Disaggregated C4'" if disaggregated else "Aggregated C4"

    lp = xp.problem()
    lp.setControl('OUTPUTLOG', 0)

    # Flow variables — identical to MIP
    xlp = {}
    for (a, p) in total_cost:
        src = arc_src[a]
        if src in S and p not in supplier_prods.get(src, set()):
            continue
        xlp[(a, p)] = lp.addVariable(name=f'xlp_{a}_{p}', lb=0,
                                      vartype=xp.continuous)

    # Binary variables RELAXED to continuous [0, 1]
    yw = {w: lp.addVariable(name=f'yw_{w}', lb=0, ub=1, vartype=xp.continuous)
          for w in W}
    za = {a: lp.addVariable(name=f'za_{a}', lb=0, ub=1, vartype=xp.continuous)
          for a in A_fixed}

    # Objective (same formula as MIP)
    obj_lp  = xp.Sum(wh_cost[w]        * yw[w]        for w in W)
    obj_lp += xp.Sum(arc_fc[a]         * za[a]         for a in A_fixed)
    obj_lp += xp.Sum(total_cost[(a,p)] * xlp[(a,p)]    for (a,p) in xlp)
    lp.setObjective(obj_lp, sense=xp.minimize)

    # Inline flow helpers for the LP problem
    def lp_in(node, product):
        return xp.Sum(xlp[(a, product)]
                      for a in arcs_into.get(node, []) if (a, product) in xlp)
    def lp_out(node, product):
        return xp.Sum(xlp[(a, product)]
                      for a in arcs_from.get(node, []) if (a, product) in xlp)

    # C1 — demand satisfaction
    for (c, p), d in Dem.items():
        lp.addConstraint(xp.constraint(lp_in(c, p) == d))

    # C2 — supply availability
    for p, sup_set in S_p.items():
        for s in sup_set:
            if (s, p) in Sup:
                lp.addConstraint(xp.constraint(lp_out(s, p) <= Sup[(s, p)]))

    # C3 — arc capacity (always-on, unchanged)
    for a in A_always:
        flow = xp.Sum(xlp[(a, p)] for p in PRODUCTS if (a, p) in xlp)
        lp.addConstraint(xp.constraint(flow <= arc_cap[a]))

    # C4 / C4' — arc capacity (optional arcs)
    if not disaggregated:
        # Aggregated: one constraint per arc (47 total)
        for a in A_fixed:
            flow = xp.Sum(xlp[(a, p)] for p in PRODUCTS if (a, p) in xlp)
            lp.addConstraint(xp.constraint(flow <= arc_cap[a] * za[a]))
    else:
        # Disaggregated: one constraint per (arc, product) — up to 141 total
        for a in A_fixed:
            for p in PRODUCTS:
                if (a, p) in xlp:
                    lp.addConstraint(xp.constraint(xlp[(a, p)] <= arc_cap[a] * za[a]))

    # C5 — warehouse capacity
    for w in W:
        total_in = xp.Sum(xlp[(a, p)]
                          for a in arcs_into[w] for p in PRODUCTS if (a, p) in xlp)
        lp.addConstraint(xp.constraint(total_in <= wh_cap[w] * yw[w]))

    # C6 — flow conservation at warehouses
    for w in W:
        for p in PRODUCTS:
            lp.addConstraint(xp.constraint(lp_in(w, p) == lp_out(w, p)))

    # C7 — flow conservation at hubs
    for h in H:
        for p in PRODUCTS:
            lp.addConstraint(xp.constraint(lp_in(h, p) == lp_out(h, p)))

    # Solve
    t0 = time.time()
    lp.solve()
    elapsed = time.time() - t0

    lp_status = lp.attributes.solstatus
    if lp_status not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        print(f"  [{label}] LP solve failed: {lp_status}")
        return None, [], [], elapsed

    Z_LP = lp.attributes.objval

    # Identify fractional binary variables in the LP solution
    frac_w = [(w, round(lp.getSolution(yw[w]), 6))
              for w in W
              if abs(lp.getSolution(yw[w]) % 1) > 1e-6]
    frac_a = [(a, round(lp.getSolution(za[a]), 6))
              for a in A_fixed
              if abs(lp.getSolution(za[a]) % 1) > 1e-6]

    return Z_LP, frac_w, frac_a, elapsed


# --- Solve both formulations --------------------------------------------------
Z_LP_agg, frac_w_agg, frac_a_agg, t_agg = solve_lp_relaxation(disaggregated=False)
Z_LP_dis, frac_w_dis, frac_a_dis, t_dis = solve_lp_relaxation(disaggregated=True)

# --- Console report -----------------------------------------------------------
lp_rows = []

for label, Z_LP, frac_w, frac_a, t_solve, n_C4 in [
    ("Aggregated (C4)",      Z_LP_agg, frac_w_agg, frac_a_agg, t_agg,
     len(A_fixed)),
    ("Disaggregated (C4')",  Z_LP_dis, frac_w_dis, frac_a_dis, t_dis,
     len(A_fixed) * len(PRODUCTS)),
]:
    if Z_LP is None:
        print(f"\n  [{label}] — LP solve failed.")
        continue

    gap_abs = obj_val - Z_LP
    gap_pct = gap_abs / obj_val * 100

    print(f"\n  Formulation  : {label}  ({n_C4} arc-capacity constraints)")
    print(f"  Z_LP*        : ${Z_LP:>14,.2f}  ({t_solve:.4f}s)")
    print(f"  Z* (MIP)     : ${obj_val:>14,.2f}")
    print(f"  Gap ($)      : ${gap_abs:>14,.2f}")
    print(f"  Gap (%)      : {gap_pct:.4f}%")
    print(f"  Frac. WH vars: {len(frac_w):>3d}  |  Frac. arc vars: {len(frac_a):>3d}")
    if frac_w:
        print(f"  Examples WH  : {frac_w[:4]}")
    if frac_a:
        print(f"  Examples arc : {frac_a[:4]}")

    lp_rows.append({
        'Formulation':              label,
        'C4 constraints':           n_C4,
        'Z_LP* ($)':                round(Z_LP, 2),
        'Z* MIP ($)':               round(obj_val, 2),
        'LP-IP Gap ($)':            round(gap_abs, 2),
        'LP-IP Gap (%)':            round(gap_pct, 6),
        'Fractional WH vars':       len(frac_w),
        'Fractional arc vars':      len(frac_a),
        'LP Solve Time (s)':        round(t_solve, 4),
    })

# --- Tightening summary -------------------------------------------------------
if Z_LP_agg is not None and Z_LP_dis is not None:
    tightening     = Z_LP_dis - Z_LP_agg
    gap_agg        = (obj_val - Z_LP_agg) / obj_val * 100
    gap_dis        = (obj_val - Z_LP_dis) / obj_val * 100
    gap_improvement = gap_agg - gap_dis

    print(f"\n  {'─' * 50}")
    print(f"  Tightening (Z_LP_dis − Z_LP_agg) : ${tightening:>+,.2f}")
    print(f"  Gap reduction                    : {gap_agg:.4f}% → {gap_dis:.4f}%"
          f"  (Δ = {gap_improvement:.4f} pp)")

    if tightening > 1.0:
        print("  => Disaggregated C4' produces a tighter LP bound.")
        print("     Worth considering if MIP solve time becomes a bottleneck.")
    elif tightening < -1.0:
        print("  => Aggregated C4 produces an equal or tighter bound on this instance.")
    else:
        print("  => Both formulations produce equivalent LP bounds (< $1 difference).")

    lp_rows.append({
        'Formulation':         'Gap reduction (Disagg. vs Agg.)',
        'C4 constraints':      f"{len(A_fixed)} → {len(A_fixed)*len(PRODUCTS)}",
        'Z_LP* ($)':           '',
        'Z* MIP ($)':          '',
        'LP-IP Gap ($)':       '',
        'LP-IP Gap (%)':       round(gap_improvement, 6),
        'Fractional WH vars':  '',
        'Fractional arc vars': '',
        'LP Solve Time (s)':   '',
    })

# --- Append LP sheet to existing workbook ------------------------------------
lp_df = pd.DataFrame(lp_rows)
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl', mode='a',
                    if_sheet_exists='replace') as writer:
    lp_df.to_excel(writer, sheet_name='LP Relaxation', index=False)

print(f"\n  LP results saved -> {OUTPUT_FILE}  (sheet: LP Relaxation)")
print(f"\nDone — all results in {OUTPUT_FILE}")
