"""
GlobalFlow — Scenario S4: Suez Crisis (Combined Shock)
=======================================================
Implements ONE scenario × 3 strategies = 3 solver runs.

Scenario S4 — Red Sea / Suez Crisis (Houthi 2024)
--------------------------------------------------
  Affected arcs : all sea-mode arcs on Suez-corridor zone pairs
                  (Europe↔Asia, Europe↔MiddleEast, MiddleEast↔Asia,
                   EastEurope→MiddleEast)
  Cost shock    : variable cost × 1.80 on affected arcs
  Capacity shock: shared capacity × 0.40 on affected arcs
  Both applied simultaneously (peak-crisis configuration)

All three strategies are fully feasible (no hub removed, no supplier
stranded). Hard demand equality C1 is enforced throughout.

Strategies
----------
  R  Baseline W* and A* held fixed; only flow re-optimised.
     Fixed costs are SUNK — not in objective, but added back at
     reporting time so Cost_R is comparable to Z*.

  A  Warehouse/arc decisions freed. Only NEW openings/activations
     beyond the baseline are charged fixed costs.

  F  Full greenfield re-solve. ALL fixed costs paid from scratch.
     Lower bound on achievable cost.

Cost model
----------
  total_cost[a,p] = baseline_var[a,p] × factor[a] × (1 + tariff[zone_from,zone_to])
  where factor[a] = 1.80 if a in Suez arcs, else 1.0

Output
------
  S4_strategy_R.xlsx
  S4_strategy_A.xlsx
  S4_strategy_F.xlsx
  (written to the current working directory by default)

Usage
-----
  python s4_suez_combined.py

  Optional overrides via environment variables:
    XPRESS_LICENSE   full path to xpauth.xpr
    EXCEL_FILE       path to globalflow_instance.xlsx
    BASELINE_FILE    path to baseline_solution.xlsx
    OUTPUT_DIR       directory for result workbooks
"""

import os
import sys
import time
import pandas as pd
import xpress as xp

# =============================================================================
# 0. XPRESS INITIALISATION
# =============================================================================

_lic = os.environ.get('XPRESS_LICENSE',
                      '/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')
try:
    xp.init(_lic)
except Exception:
    pass   # Fall back to community / environment licence

# =============================================================================
# 1. FILE PATHS
# =============================================================================

_CWD = os.getcwd()

def _find(candidates):
    for p in candidates:
        if os.path.exists(p):
            return p
    return None

EXCEL_FILE = os.environ.get('EXCEL_FILE') or _find([
    os.path.join(_CWD, 'data', 'globalflow_instance.xlsx'),
    os.path.join(_CWD, 'globalflow_instance.xlsx'),
    os.path.join(os.path.dirname(__file__), '..', 'data', 'globalflow_instance.xlsx'),
])

BASELINE_FILE = os.environ.get('BASELINE_FILE') or _find([
    os.path.join(_CWD, 'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_CWD, 'baseline_solution.xlsx'),
    os.path.join(os.path.dirname(__file__), '..', 'phase1', 'results',
                 'baseline_solution.xlsx'),
])
OUTPUT_DIR = os.path.join(_CWD, 'phase3', 'results')
os.makedirs(OUTPUT_DIR, exist_ok=True)

if not EXCEL_FILE:
    sys.exit("ERROR: globalflow_instance.xlsx not found. Set EXCEL_FILE env var.")
if not BASELINE_FILE:
    sys.exit("ERROR: baseline_solution.xlsx not found. Set BASELINE_FILE env var.")

print("=" * 70)
print("GlobalFlow — S4: Suez Crisis (cost ×1.80 + capacity ×0.40)")
print("=" * 70)
print(f"  Instance : {EXCEL_FILE}")
print(f"  Baseline : {BASELINE_FILE}")
print(f"  Outputs  : {OUTPUT_DIR}")

# =============================================================================
# 2. SHARED DATA LOADING
# =============================================================================

print("\nLoading data...")

# --- Network sheets ---
nodes_df      = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
arcs_df       = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
suppliers_df  = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
demand_df     = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
supply_df     = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
tariffs_df    = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')
baseline_costs_df = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')

# --- Baseline solution ---
_bl_wh  = pd.read_excel(BASELINE_FILE, sheet_name='Warehouses').dropna(
    subset=['warehouse_id', 'open'])
_bl_arc = pd.read_excel(BASELINE_FILE, sheet_name='Arc Activations').dropna(
    subset=['arc_id', 'activated'])
_bl_sum = pd.read_excel(BASELINE_FILE, sheet_name='Summary')

BASELINE_OPEN_WH    = dict(zip(_bl_wh['warehouse_id'],  _bl_wh['open'].astype(int)))
BASELINE_ACTIVE_ARC = dict(zip(_bl_arc['arc_id'],       _bl_arc['activated'].astype(int)))
BASELINE_OPEN_WH_SET    = {w for w, v in BASELINE_OPEN_WH.items()    if v == 1}
BASELINE_ACTIVE_ARC_SET = {a for a, v in BASELINE_ACTIVE_ARC.items() if v == 1}

_cost_row = _bl_sum[_bl_sum['Metric'] == 'Total Cost ($)']['Value']
Z_STAR = float(_cost_row.iloc[0]) if not _cost_row.empty else 0.0

print(f"  Baseline: {len(BASELINE_OPEN_WH_SET)} open WH, "
      f"{len(BASELINE_ACTIVE_ARC_SET)} active arcs, Z*=${Z_STAR:,.2f}")

# --- Static network parameters ---
PRODUCTS = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']

baseline_var_cost = {(r['arc_id'], r['product']): r['variable_cost']
                     for _, r in baseline_costs_df.iterrows()}
tariff_lookup = {(r['zone_pair_from'], r['zone_pair_to']): r['interzonal_tariff_rate']
                 for _, r in tariffs_df.iterrows()}

arc_zone_from_all = dict(zip(arcs_df['arc_id'], arcs_df['zone_from']))
arc_zone_to_all   = dict(zip(arcs_df['arc_id'], arcs_df['zone_to']))

WH_COST_ALL = {r['warehouse_id']: r['opening_cost']  for _, r in warehouses_df.iterrows()}
ARC_FC_ALL  = {r['arc_id']:       r['fixed_activation_cost'] for _, r in arcs_df.iterrows()}

SUNK_WH_COST  = sum(WH_COST_ALL.get(w, 0) for w in BASELINE_OPEN_WH_SET)
SUNK_ARC_COST = sum(ARC_FC_ALL.get(a, 0)  for a in BASELINE_ACTIVE_ARC_SET)

# =============================================================================
# 3. SUEZ ARC IDENTIFICATION
# =============================================================================

SUEZ_ZONE_PAIRS = {
    ('Europe',      'Asia'),        ('Asia',        'Europe'),
    ('Europe',      'MiddleEast'),  ('MiddleEast',  'Europe'),
}

suez_arc_ids = {
    row['arc_id']
    for _, row in arcs_df.iterrows()
    if str(row.get('transport_mode', '')).lower() == 'sea'
    and (row['zone_from'], row['zone_to']) in SUEZ_ZONE_PAIRS
}

print(f"\n  Suez-corridor sea arcs identified: {len(suez_arc_ids)}")
assert 30 <= len(suez_arc_ids) <= 70, (
    f"Unexpected Suez arc count: {len(suez_arc_ids)} — check zone_from/zone_to in Arcs sheet.")

# =============================================================================
# 4. SCENARIO PARAMETERS
# =============================================================================

COST_FACTOR     = 1.80   # variable cost multiplier on Suez arcs
CAP_FACTOR      = 0.40   # capacity multiplier on Suez arcs
MAX_SOLVE_TIME  = 300    # seconds per solve

# =============================================================================
# 5. BUILD STRUCTURAL LOOKUPS (full network, no arcs removed)
# =============================================================================

arc_src, arc_tgt, arc_cap_bl, arc_fc, arc_mode, arc_dist = {}, {}, {}, {}, {}, {}
all_nodes = set(nodes_df['node_id'])
arcs_from = {n: set() for n in all_nodes}
arcs_into = {n: set() for n in all_nodes}

for _, row in arcs_df.iterrows():
    a = row['arc_id']
    arc_src[a]    = row['from_id']
    arc_tgt[a]    = row['to_id']
    arc_cap_bl[a] = row['shared_capacity']      # baseline capacity
    arc_fc[a]     = row['fixed_activation_cost']
    arc_mode[a]   = row['transport_mode']
    arc_dist[a]   = row['distance_km']
    arcs_from[row['from_id']].add(a)
    arcs_into[row['to_id']].add(a)

_cap_overrides = {}
for _, _sup_row in supply_df.iterrows():
    _s, _sup_vol = _sup_row['supplier_id'], _sup_row['supply']
    _s_arcs = [a for a, src in arc_src.items() if src == _s]
    if len(_s_arcs) == 1:
        _a = _s_arcs[0]
        if arc_cap_bl[_a] < _sup_vol:
            arc_cap_bl[_a] = _sup_vol
            _cap_overrides[_a] = _sup_vol
if _cap_overrides:
    print(f"  [data fix] Arc capacity raised to match supplier supply: {_cap_overrides}")

A_fixed  = {a for a in arc_src if arc_fc[a] > 0}
A_always = {a for a in arc_src if arc_fc[a] == 0}

# Scenario capacity: apply CAP_FACTOR to Suez arcs
arc_cap_scen = {
    a: (cap * CAP_FACTOR if a in suez_arc_ids else cap)
    for a, cap in arc_cap_bl.items()
}

# Scenario total cost (with tariff): apply COST_FACTOR to Suez arcs
total_cost = {}
for (a, p), bv in baseline_var_cost.items():
    if a not in arc_src:
        continue
    sv     = bv * COST_FACTOR if a in suez_arc_ids else bv
    factor = sv / bv if bv > 0 else 1.0
    tariff = tariff_lookup.get(
        (arc_zone_from_all.get(a, ''), arc_zone_to_all.get(a, '')), 0.0)
    total_cost[(a, p)] = bv * factor * (1.0 + tariff)

# Node sets
S = set(suppliers_df['supplier_id'])
H = set(nodes_df[nodes_df['type'] == 'HUB']['node_id'])
W = set(warehouses_df['warehouse_id'])
C = set(demand_df['customer_id'].unique())

S_p, supplier_prods = {}, {}
for _, row in supply_df.iterrows():
    S_p.setdefault(row['product'], set()).add(row['supplier_id'])
    supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])

Dem = {(r['customer_id'], r['product']): r['demand']    for _, r in demand_df.iterrows()}
Sup = {(r['supplier_id'], r['product']): r['supply']    for _, r in supply_df.iterrows()}
wh_cap  = {r['warehouse_id']: r['capacity']     for _, r in warehouses_df.iterrows()}
wh_cost = {r['warehouse_id']: r['opening_cost'] for _, r in warehouses_df.iterrows()}

# Baseline sets that remain alive (full network — nothing removed)
baseline_wh_alive   = BASELINE_OPEN_WH_SET    & W
baseline_arcs_alive = BASELINE_ACTIVE_ARC_SET & A_fixed
new_wh_set   = W       - baseline_wh_alive
new_arcs_set = A_fixed - baseline_arcs_alive

print(f"  Active hubs  : {sorted(H)}")
print(f"  Arcs         : {len(arc_src)} total  "
      f"({len(A_fixed)} optional, {len(A_always)} always-on)")
print(f"  Cost pairs   : {len(total_cost)}")

# =============================================================================
# 6. MAIN LOOP: 3 STRATEGIES
# =============================================================================

all_results = []   # accumulates one dict per strategy for the summary print

for STRATEGY in ['R', 'A', 'F']:

    OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'S4_strategy_{STRATEGY}.xlsx')

    print(f"\n{'=' * 70}")
    print(f"  S4 — Suez crisis   Strategy: {STRATEGY}")
    print("=" * 70)

    # -------------------------------------------------------------------------
    # 6a. Decision variables
    # -------------------------------------------------------------------------
    prob = xp.problem()
    prob.setControl('MAXTIME',    MAX_SOLVE_TIME)
    prob.setControl('OUTPUTLOG',  0)
    prob.setControl('MIPRELSTOP', 1e-7)

    # Flow variables x[a, p] ≥ 0
    # Supplier arcs: only create x if that supplier produces p
    x = {}
    for (a, p) in total_cost:
        src = arc_src[a]
        if src in S and p not in supplier_prods.get(src, set()):
            continue
        x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

    # Warehouse opening (locked to baseline for R)
    openWarehouse = {}
    for w in W:
        if STRATEGY == 'R' and w in BASELINE_OPEN_WH:
            v = float(BASELINE_OPEN_WH[w])
            openWarehouse[w] = prob.addVariable(name=f'open_{w}', lb=v, ub=v,
                                                vartype=xp.continuous)
        else:
            openWarehouse[w] = prob.addVariable(name=f'open_{w}', vartype=xp.binary)

    # Arc activation (locked to baseline for R)
    arc_act = {}
    for a in A_fixed:
        if STRATEGY == 'R' and a in BASELINE_ACTIVE_ARC:
            v = float(BASELINE_ACTIVE_ARC[a])
            arc_act[a] = prob.addVariable(name=f'arc_{a}', lb=v, ub=v,
                                          vartype=xp.continuous)
        else:
            arc_act[a] = prob.addVariable(name=f'arc_{a}', vartype=xp.binary)

    # -------------------------------------------------------------------------
    # 6b. Objective (strategy-specific fixed-cost accounting)
    # -------------------------------------------------------------------------
    # R : variable cost only (sunk fixed costs added back at reporting time)
    # A : variable + fixed costs for NEW openings/activations only
    # F : variable + ALL fixed costs from scratch (greenfield)
    if STRATEGY == 'R':
        obj = xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

    elif STRATEGY == 'A':
        obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in new_wh_set)
        obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in new_arcs_set)
        obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

    else:  # F
        obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)
        obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in A_fixed)
        obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

    prob.setObjective(obj, sense=xp.minimize)

    # -------------------------------------------------------------------------
    # 6c. Constraints C1–C7
    # -------------------------------------------------------------------------
    def inflow(node, product):
        """Sum of x[a,p] for all arcs a entering node."""
        return xp.Sum(x[(a, product)]
                      for a in arcs_into.get(node, []) if (a, product) in x)

    def outflow(node, product):
        """Sum of x[a,p] for all arcs a leaving node."""
        return xp.Sum(x[(a, product)]
                      for a in arcs_from.get(node, []) if (a, product) in x)

    # C1 — demand satisfaction (hard equality; scenario is fully feasible)
    for (c, p), d in Dem.items():
        prob.addConstraint(xp.constraint(inflow(c, p) == d, name=f'C1_{c}_{p}'))

    # C2 — supply availability (inequality: supply surplus allowed)
    for p, sup_set in S_p.items():
        for s in sup_set:
            if (s, p) in Sup:
                prob.addConstraint(
                    xp.constraint(outflow(s, p) <= Sup[(s, p)], name=f'C2_{s}_{p}'))

    # C3 — arc capacity (always-active arcs, scenario-modified capacity)
    for a in A_always:
        flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(xp.constraint(flow <= arc_cap_scen[a], name=f'C3_{a}'))

    # C4 — arc capacity (optional arcs, gated by activation variable)
    for a in A_fixed:
        flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(
            xp.constraint(flow <= arc_cap_scen[a] * arc_act[a], name=f'C4_{a}'))

    # C5 — warehouse capacity (gated by opening variable)
    for w in W:
        total_in = xp.Sum(x[(a, p)]
                          for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(
            xp.constraint(total_in <= wh_cap[w] * openWarehouse[w], name=f'C5_{w}'))

    # C6 — flow conservation at warehouses (per product)
    for w in W:
        for p in PRODUCTS:
            prob.addConstraint(
                xp.constraint(inflow(w, p) == outflow(w, p), name=f'C6_{w}_{p}'))

    # C7 — flow conservation at hubs (per product; hubs are uncapacitated)
    for h in H:
        for p in PRODUCTS:
            prob.addConstraint(
                xp.constraint(inflow(h, p) == outflow(h, p), name=f'C7_{h}_{p}'))

    # -------------------------------------------------------------------------
    # 6d. Solve
    # -------------------------------------------------------------------------
    print(f"  Solving (limit: {MAX_SOLVE_TIME}s)...")
    t0 = time.time()
    prob.solve()
    solve_time = time.time() - t0

    status = prob.attributes.solstatus
    print(f"  Status: {status}  |  {solve_time:.2f}s")

    if status not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        print("  ERROR: No feasible solution found. Check instance data.")
        continue

    # -------------------------------------------------------------------------
    # 6e. Cost extraction (same methodology as phase2_solver.py)
    # -------------------------------------------------------------------------
    obj_val = prob.getObjVal()

    open_wh_sol     = {w for w in W      if round(prob.getSolution(openWarehouse[w])) == 1}
    active_arcs_sol = {a for a in A_fixed if round(prob.getSolution(arc_act[a]))       == 1}

    var_trans_cost = sum(total_cost[(a, p)] * prob.getSolution(x[(a, p)])
                         for (a, p) in x)

    # Fixed cost accounting
    fixed_wh_full  = sum(wh_cost[w] for w in open_wh_sol)
    fixed_arc_full = sum(arc_fc[a]  for a in active_arcs_sol)

    if STRATEGY == 'R':
        sunk_wh   = sum(wh_cost[w] for w in (BASELINE_OPEN_WH_SET    & W))
        sunk_arcs = sum(arc_fc[a]  for a in (BASELINE_ACTIVE_ARC_SET & A_fixed))
        fixed_wh_charged  = 0.0
        fixed_arc_charged = 0.0
    elif STRATEGY == 'A':
        sunk_wh   = sum(wh_cost[w] for w in (open_wh_sol & baseline_wh_alive))
        sunk_arcs = sum(arc_fc[a]  for a in (active_arcs_sol & baseline_arcs_alive))
        fixed_wh_charged  = sum(wh_cost[w] for w in (open_wh_sol & new_wh_set))
        fixed_arc_charged = sum(arc_fc[a]  for a in (active_arcs_sol & new_arcs_set))
    else:  # F
        sunk_wh   = 0.0
        sunk_arcs = 0.0
        fixed_wh_charged  = fixed_wh_full
        fixed_arc_charged = fixed_arc_full

    # Logistics cost (comparable to Z* — includes sunk for R/A)
    if STRATEGY == 'R':
        logistics_cost   = sunk_wh + sunk_arcs + var_trans_cost
        optimisation_obj = var_trans_cost
    elif STRATEGY == 'A':
        logistics_cost   = (sunk_wh + sunk_arcs
                            + fixed_wh_charged + fixed_arc_charged
                            + var_trans_cost)
        optimisation_obj = fixed_wh_charged + fixed_arc_charged + var_trans_cost
    else:
        logistics_cost   = fixed_wh_full + fixed_arc_full + var_trans_cost
        optimisation_obj = logistics_cost

    disruption_cost = logistics_cost - Z_STAR if Z_STAR > 0 else None
    disruption_pct  = (disruption_cost / Z_STAR * 100
                       if disruption_cost is not None else None)

    # Network changes vs baseline
    wh_opened_new    = sorted(open_wh_sol - BASELINE_OPEN_WH_SET)
    wh_closed        = sorted(BASELINE_OPEN_WH_SET - open_wh_sol)
    arc_activated_new = sorted(active_arcs_sol - BASELINE_ACTIVE_ARC_SET)
    arc_deactivated  = sorted(BASELINE_ACTIVE_ARC_SET - active_arcs_sol)

    # ---- Console summary ----
    print(f"  Variable transport cost      : ${var_trans_cost:>14,.2f}")
    print(f"  Sunk fixed costs (WH + arcs) : ${sunk_wh + sunk_arcs:>14,.2f}")
    print(f"  New fixed costs (WH + arcs)  : ${fixed_wh_charged + fixed_arc_charged:>14,.2f}")
    print(f"  ---")
    print(f"  Logistics cost (vs Z*)       : ${logistics_cost:>14,.2f}")
    if disruption_cost is not None:
        print(f"  DeltaZ                       : ${disruption_cost:>+14,.2f}"
              f"  ({disruption_pct:+.2f}%)")
    print(f"  Flex value (ZR-ZA)           : computed across strategies after loop")
    print(f"  Open warehouses ({len(open_wh_sol)}/{len(W)})  : {sorted(open_wh_sol)}")
    if wh_opened_new:   print(f"  WH newly opened vs baseline  : {wh_opened_new}")
    if wh_closed:       print(f"  WH closed vs baseline        : {wh_closed}")
    if arc_activated_new: print(f"  Arcs newly activated         : {arc_activated_new}")
    if arc_deactivated:   print(f"  Arcs deactivated             : {arc_deactivated}")

    print("  Demand fulfilment:")
    for p in PRODUCTS:
        delivered = sum(prob.getSolution(x[(a, p)])
                        for c in C for a in arcs_into.get(c, []) if (a, p) in x)
        total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
        print(f"    {p:35s}: {delivered:>7.1f} / {total_dem:.0f}"
              f"  ({100 * delivered / total_dem:.1f}%)")

    all_results.append({
        'strategy':        STRATEGY,
        'logistics_cost':  logistics_cost,
        'disruption_cost': disruption_cost,
        'disruption_pct':  disruption_pct,
        'var_trans_cost':  var_trans_cost,
        'sunk_wh':         sunk_wh,
        'sunk_arcs':       sunk_arcs,
        'fixed_wh_charged':  fixed_wh_charged,
        'fixed_arc_charged': fixed_arc_charged,
        'fixed_wh_full':   fixed_wh_full,
        'fixed_arc_full':  fixed_arc_full,
        'open_wh_sol':     open_wh_sol,
        'active_arcs_sol': active_arcs_sol,
    })

    # -------------------------------------------------------------------------
    # 6f. Export Excel workbook
    # -------------------------------------------------------------------------
    print(f"  Exporting → {OUTPUT_FILE}")

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
            bvc = baseline_var_cost.get((a, p))
            rows.append({
                'arc_id':            a,
                'source':            arc_src[a],
                'target':            arc_tgt[a],
                'product':           p,
                'flow':              flow,
                'capacity_scenario': round(arc_cap_scen[a], 1),
                'capacity_baseline': arc_cap_bl[a],
                'utilization_%':     round(flow / arc_cap_scen[a] * 100, 1),
                'baseline_var_cost': round(bvc, 4) if bvc is not None else None,
                'scenario_var_cost': round(total_cost.get((a, p), 0), 4),
                'cost_delta_pu':     (round(total_cost.get((a, p), 0) - bvc, 4)
                                      if bvc is not None else None),
                'flow_cost':         round(flow * total_cost.get((a, p), 0), 2),
                'transport_mode':    arc_mode[a],
                'distance_km':       arc_dist[a],
                'suez_affected':     'YES' if a in suez_arc_ids else 'no',
            })
        df = pd.DataFrame(rows)
        product_dfs[p] = df.sort_values('arc_id') if not df.empty else df

    # Warehouses sheet
    wh_rows = []
    for w in sorted(W):
        opened   = round(prob.getSolution(openWarehouse[w]))
        total_in = round(sum(prob.getSolution(x[(a, p)])
                             for a in arcs_into[w] for p in PRODUCTS
                             if (a, p) in x), 2)
        was_open = 1 if w in BASELINE_OPEN_WH_SET else 0
        status_vs_bl = ('kept'          if opened == 1 and was_open == 1 else
                         'newly_opened'  if opened == 1 and was_open == 0 else
                         'closed'        if opened == 0 and was_open == 1 else
                         'unused')
        cost_charged = (0.0            if STRATEGY == 'R' else
                        wh_cost[w]     if (opened == 1 and was_open == 0) else
                        0.0            if STRATEGY == 'A' else
                        wh_cost[w]     if opened == 1 else 0.0)
        wh_rows.append({
            'warehouse_id':       w,
            'open_baseline':      was_open,
            'open_scenario':      opened,
            'status_vs_baseline': status_vs_bl,
            'opening_cost':       wh_cost[w],
            'cost_charged':       round(cost_charged, 2),
            'capacity':           wh_cap[w],
            'total_inflow':       total_in,
            'utilization_%':      (round(total_in / wh_cap[w] * 100, 1)
                                   if opened and wh_cap[w] > 0 else None),
        })
    wh_df = pd.DataFrame(wh_rows).sort_values(
        ['open_scenario', 'warehouse_id'], ascending=[False, True])

    # Arc activations sheet
    arc_rows = []
    for a in sorted(A_fixed):
        activated  = round(prob.getSolution(arc_act[a]))
        total_flow = round(
            sum(prob.getSolution(x[(a, p)]) for p in PRODUCTS if (a, p) in x), 2)
        was_active = 1 if a in BASELINE_ACTIVE_ARC_SET else 0
        status_vs_bl = ('kept'             if activated == 1 and was_active == 1 else
                         'newly_activated'  if activated == 1 and was_active == 0 else
                         'deactivated'      if activated == 0 and was_active == 1 else
                         'unused')
        cost_charged = (0.0       if STRATEGY == 'R' else
                        arc_fc[a] if (activated == 1 and was_active == 0) else
                        0.0       if STRATEGY == 'A' else
                        arc_fc[a] if activated == 1 else 0.0)
        arc_rows.append({
            'arc_id':              a,
            'activated_baseline':  was_active,
            'activated_scenario':  activated,
            'status_vs_baseline':  status_vs_bl,
            'source':              arc_src[a],
            'target':              arc_tgt[a],
            'total_flow':          total_flow,
            'capacity_scenario':   round(arc_cap_scen[a], 1),
            'capacity_baseline':   arc_cap_bl[a],
            'fixed_cost':          arc_fc[a],
            'cost_charged':        round(cost_charged, 2),
            'transport_mode':      arc_mode[a],
            'distance_km':         arc_dist[a],
            'suez_affected':       'YES' if a in suez_arc_ids else 'no',
        })
    arc_df = pd.DataFrame(arc_rows).sort_values(
        ['activated_scenario', 'arc_id'], ascending=[False, True])

    # Summary sheet
    summary_rows = [
        ('Scenario',                     'S4 — Suez Crisis (Houthi 2024)'),
        ('Strategy',                     STRATEGY),
        ('Cost Factor (Suez arcs)',      COST_FACTOR),
        ('Capacity Factor (Suez arcs)',  CAP_FACTOR),
        ('Suez Arcs Affected',           len(suez_arc_ids)),
        ('Solve Time (s)',               round(solve_time, 3)),
        ('Solver Status',                str(status)),
        ('', ''),
        ('=== Cost breakdown (comparable to Z*) ===', ''),
        ('Logistics Cost ($)',           round(logistics_cost, 2)),
        ('  Sunk WH cost ($)',           round(sunk_wh, 2)),
        ('  Sunk arc cost ($)',          round(sunk_arcs, 2)),
        ('  New WH opening cost ($)',    round(fixed_wh_charged, 2)),
        ('  New arc activation cost ($)',round(fixed_arc_charged, 2)),
        ('  Variable transport cost ($)',round(var_trans_cost, 2)),
        ('', ''),
        ('=== Transport costs by product ===', ''),
    ]
    for p in PRODUCTS:
        prod_cost = sum(total_cost.get((a, pp), 0) * prob.getSolution(x[(a, pp)])
                        for (a, pp) in x if pp == p)
        summary_rows.append((f'  {p} ($)', round(prod_cost, 2)))
    summary_rows += [
        ('', ''),
        ('=== vs Baseline ===', ''),
        ('Z* Baseline Cost ($)',         round(Z_STAR, 2)),
        ('DeltaZ ($)',                   round(disruption_cost, 2) if disruption_cost else 'N/A'),
        ('DeltaZ (%)',                   round(disruption_pct, 2)  if disruption_pct  else 'N/A'),
        ('', ''),
        ('=== Network vs Baseline ===', ''),
        ('Warehouses Open (baseline)',   len(BASELINE_OPEN_WH_SET)),
        ('Warehouses Open (scenario)',   len(open_wh_sol)),
        ('WH opened vs baseline',        str(wh_opened_new) if wh_opened_new else 'none'),
        ('WH closed vs baseline',        str(wh_closed)     if wh_closed     else 'none'),
        ('Arcs newly activated',         str(arc_activated_new) if arc_activated_new else 'none'),
        ('Arcs deactivated',             str(arc_deactivated)   if arc_deactivated   else 'none'),
        ('', ''),
        ('=== Demand ===', ''),
    ]
    for p in PRODUCTS:
        delivered = sum(prob.getSolution(x[(a, p)])
                        for c in C for a in arcs_into.get(c, []) if (a, p) in x)
        total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
        summary_rows.append((f'Demand Met — {p}', f'{delivered:.0f} / {total_dem:.0f}'))

    summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

    # Write workbook
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        for p, df in product_dfs.items():
            sheet_name = p.replace('_', ' ')[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        wh_df.to_excel(writer, sheet_name='Warehouses', index=False)
        arc_df.to_excel(writer, sheet_name='Arc Activations', index=False)

    print(f"  Done → {OUTPUT_FILE}")

# =============================================================================
# 7. CROSS-STRATEGY SUMMARY
# =============================================================================

print(f"\n{'=' * 70}")
print("  CROSS-STRATEGY SUMMARY — Scenario S4 (Suez combined)")
print(f"{'=' * 70}")
print(f"  Z* baseline = ${Z_STAR:,.2f}")
print(f"  {'Strategy':10s}  {'Logistics Cost ($)':>20s}  {'DeltaZ ($)':>14s}  {'DeltaZ (%)':>10s}")
print(f"  {'-'*10}  {'-'*20}  {'-'*14}  {'-'*10}")
costs_by_strat = {}
for r in all_results:
    print(f"  {r['strategy']:10s}  ${r['logistics_cost']:>19,.2f}  "
          f"${r['disruption_cost']:>+13,.2f}  {r['disruption_pct']:>+9.2f}%")
    costs_by_strat[r['strategy']] = r['logistics_cost']

if 'R' in costs_by_strat and 'A' in costs_by_strat:
    flex = costs_by_strat['R'] - costs_by_strat['A']
    print(f"\n  Flexibility value ZR - ZA = ${flex:,.2f}")
    if abs(flex) < 1:
        print("  → Baseline already optimal; adaptation and rerouting are equivalent.")

print(f"\n  Suez arcs affected : {len(suez_arc_ids)}")
print(f"  Cost factor        : ×{COST_FACTOR:.2f}")
print(f"  Capacity factor    : ×{CAP_FACTOR:.2f}")
print("\nAll runs complete.")
