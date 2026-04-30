"""
GlobalFlow Phase 2 — Adversarial Scenario Analysis
====================================================
For each of the six scenarios (T1–T3, S1–S3) plus a custom S4, this script
evaluates three response strategies:

  R — Rerouting only  : baseline network fixed (W*, A*); re-optimise flows only.
  A — Adaptation      : allow opening/closing warehouses and activating/deactivating arcs.
  F — Full redesign   : solve Phase 1 from scratch with scenario parameters.

Structure
---------
  1. Configuration
  2. Data loading helpers
  3. Core solver (generic MIP builder)
  4. Strategy builders (R, A, F wrappers)
  5. Scenario loop
  6. Phase 1 baseline solve
  7. Analysis & report
  8. Export to Excel
"""

import os
import time
import pandas as pd
import xpress as xp

xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')

# =============================================================================
# 1. CONFIGURATION
# =============================================================================

EXCEL_FILE    = 'data/globalflow_instance.xlsx'
OUTPUT_FILE   = 'result/globalflow_phase2_solution.xlsx'
PRODUCTS      = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME = 300   # seconds per sub-problem

# Scenario definitions
# Each entry: (scenario_id, cost_sheet, arcs_removed_sheet, nodes_removed_sheet)
SCENARIOS = [
    ('T1', 'ArcCosts_T1', None,             None),
    ('T2', 'ArcCosts_T2', None,             None),
    ('T3', 'ArcCosts_T3', None,             None),
    ('S1', 'ArcCosts_S1', 'ArcsRemoved_S1', 'NodesRemoved_S1'),
    ('S2', 'ArcCosts_S2', 'ArcsRemoved_S2', None),
    ('S3', 'ArcCosts_S3', 'ArcsRemoved_S3', 'NodesRemoved_S3'),
    ('S4', 'ArcCosts_S4', 'ArcsRemoved_S4', None),   # Custom scenario — see Section 5
]

# =============================================================================
# 2. DATA LOADING
# =============================================================================

def load_base_data():
    """Load all sheets that are constant across scenarios."""
    nodes_df      = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
    arcs_df       = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
    warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
    suppliers_df  = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
    demand_df     = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
    supply_df     = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
    tariffs_df    = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')
    return nodes_df, arcs_df, warehouses_df, suppliers_df, demand_df, supply_df, tariffs_df


def build_instance(nodes_df, arcs_df, warehouses_df, suppliers_df,
                   demand_df, supply_df, tariffs_df,
                   costs_df, removed_arcs=None, removed_nodes=None):
    """
    Convert raw DataFrames into the dictionaries expected by the solver.

    Parameters
    ----------
    removed_arcs  : set of arc_ids to exclude (node/arc disruption scenarios)
    removed_nodes : set of node_ids to exclude
    """
    removed_arcs  = removed_arcs  or set()
    removed_nodes = removed_nodes or set()

    # Sets
    S = set(suppliers_df['supplier_id'])
    H = set(nodes_df[nodes_df['type'] == 'HUB']['node_id']) - removed_nodes
    W = set(warehouses_df['warehouse_id'])
    C = set(demand_df['customer_id'].unique())

    S_p = {}
    supplier_prods = {}
    for _, row in supply_df.iterrows():
        S_p.setdefault(row['product'], set()).add(row['supplier_id'])
        supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])

    # Arc dictionaries (filter out removed arcs / arcs touching removed nodes)
    arc_src, arc_tgt, arc_cap, arc_fc = {}, {}, {}, {}
    arc_mode, arc_dist = {}, {}
    arc_zone_from, arc_zone_to = {}, {}
    arcs_from = {n: set() for n in nodes_df['node_id']}
    arcs_into = {n: set() for n in nodes_df['node_id']}

    for _, row in arcs_df.iterrows():
        a = row['arc_id']
        src, tgt = row['from_id'], row['to_id']
        if a in removed_arcs or src in removed_nodes or tgt in removed_nodes:
            continue
        arc_src[a]       = src
        arc_tgt[a]       = tgt
        arc_cap[a]       = row['shared_capacity']
        arc_fc[a]        = row['fixed_activation_cost']
        arc_mode[a]      = row['transport_mode']
        arc_dist[a]      = row['distance_km']
        arc_zone_from[a] = row['zone_from']
        arc_zone_to[a]   = row['zone_to']
        arcs_from[src].add(a)
        arcs_into[tgt].add(a)

    A_fixed  = {a for a in arc_src if arc_fc[a] > 0}
    A_always = {a for a in arc_src if arc_fc[a] == 0}

    # Parameters
    Dem = {(row['customer_id'], row['product']): row['demand']
           for _, row in demand_df.iterrows()}
    Sup = {(row['supplier_id'], row['product']): row['supply']
           for _, row in supply_df.iterrows()}
    wh_cap  = {row['warehouse_id']: row['capacity']     for _, row in warehouses_df.iterrows()}
    wh_cost = {row['warehouse_id']: row['opening_cost'] for _, row in warehouses_df.iterrows()}
    tariff_rate = {(row['zone_pair_from'], row['zone_pair_to']): row['interzonal_tariff_rate']
                   for _, row in tariffs_df.iterrows()}
    var_cost = {(row['arc_id'], row['product']): row['variable_cost']
                for _, row in costs_df.iterrows()
                if row['arc_id'] in arc_src}   # only arcs not removed

    total_cost = {}
    for (a, p), vc in var_cost.items():
        rate = tariff_rate.get((arc_zone_from.get(a), arc_zone_to.get(a)), 0.0)
        total_cost[(a, p)] = vc * (1 + rate)

    return dict(
        S=S, H=H, W=W, C=C,
        S_p=S_p, supplier_prods=supplier_prods,
        arc_src=arc_src, arc_tgt=arc_tgt, arc_cap=arc_cap, arc_fc=arc_fc,
        arc_mode=arc_mode, arc_dist=arc_dist,
        arcs_from=arcs_from, arcs_into=arcs_into,
        A_fixed=A_fixed, A_always=A_always,
        Dem=Dem, Sup=Sup, wh_cap=wh_cap, wh_cost=wh_cost,
        var_cost=var_cost, total_cost=total_cost,
    )


# =============================================================================
# 3. CORE SOLVER
# =============================================================================

def solve_network(inst, strategy, baseline_open_wh=None, baseline_active_arcs=None):
    """
    Build and solve the MIP for a given instance and strategy.

    strategy : 'R' | 'A' | 'F'
        R — flow variables only; warehouse and arc binaries fixed to baseline.
        A — warehouse and arc binaries free; fixed costs are paid again.
        F — identical to A but typically called on a fresh (scenario) instance
            without sunk-cost considerations (same formulation, different framing).

    baseline_open_wh     : set of open warehouse IDs from Phase 1 (used in R).
    baseline_active_arcs : set of activated optional arc IDs from Phase 1 (used in R).

    Returns dict with objective value, cost components, and decision values.
    """
    prob = xp.problem()
    prob.setControl('MAXTIME', MAX_SOLVE_TIME)
    prob.setControl('OUTPUTLOG', 0)

    arc_src       = inst['arc_src']
    arc_tgt       = inst['arc_tgt']
    arc_cap       = inst['arc_cap']
    arc_fc        = inst['arc_fc']
    arcs_from     = inst['arcs_from']
    arcs_into     = inst['arcs_into']
    A_fixed       = inst['A_fixed']
    A_always      = inst['A_always']
    W             = inst['W']
    S             = inst['S']
    C             = inst['C']
    Dem           = inst['Dem']
    Sup           = inst['Sup']
    wh_cap        = inst['wh_cap']
    wh_cost       = inst['wh_cost']
    total_cost    = inst['total_cost']
    supplier_prods = inst['supplier_prods']

    # ── Decision variables ──────────────────────────────────────────────────
    x = {}
    for (a, p) in total_cost:
        src = arc_src[a]
        if src in S and p not in supplier_prods.get(src, set()):
            continue
        x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

    # Warehouse open binaries
    if strategy == 'R':
        # Fixed to baseline: open → UB=LB=1, closed → UB=LB=0
        openWarehouse = {}
        for w in W:
            val = 1 if (baseline_open_wh and w in baseline_open_wh) else 0
            openWarehouse[w] = prob.addVariable(
                name=f'open_{w}', lb=val, ub=val, vartype=xp.binary)
    else:
        openWarehouse = {w: prob.addVariable(name=f'open_{w}', vartype=xp.binary) for w in W}

    # Arc activation binaries (optional arcs only)
    if strategy == 'R':
        arc_act = {}
        for a in A_fixed:
            val = 1 if (baseline_active_arcs and a in baseline_active_arcs) else 0
            arc_act[a] = prob.addVariable(
                name=f'arc_{a}', lb=val, ub=val, vartype=xp.binary)
    else:
        arc_act = {a: prob.addVariable(name=f'arc_{a}', vartype=xp.binary) for a in A_fixed}

    # ── Objective ────────────────────────────────────────────────────────────
    if strategy == 'R':
        # Sunk costs already paid; only minimise variable transport
        obj = xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)
    else:
        obj  = xp.Sum(wh_cost[w]  * openWarehouse[w] for w in W)
        obj += xp.Sum(arc_fc[a]   * arc_act[a]       for a in A_fixed)
        obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

    prob.setObjective(obj, sense=xp.minimize)

    # ── Helpers ──────────────────────────────────────────────────────────────
    def inflow(node, product):
        return xp.Sum(x[(a, product)] for a in arcs_into.get(node, []) if (a, product) in x)

    def outflow(node, product):
        return xp.Sum(x[(a, product)] for a in arcs_from.get(node, []) if (a, product) in x)

    # ── Constraints ──────────────────────────────────────────────────────────
    for (c, p), d in Dem.items():
        prob.addConstraint(xp.constraint(inflow(c, p) == d, name=f'C1_{c}_{p}'))

    for p, sup_set in inst['S_p'].items():
        for s in sup_set:
            prob.addConstraint(xp.constraint(outflow(s, p) <= Sup[(s, p)], name=f'C2_{s}_{p}'))

    for a in A_always:
        flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))

    for a in A_fixed:
        flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a], name=f'C4_{a}'))

    for w in W:
        total_in = xp.Sum(x[(a, p)] for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(xp.constraint(total_in <= wh_cap[w] * openWarehouse[w], name=f'C5_{w}'))

    for w in W:
        for p in PRODUCTS:
            prob.addConstraint(xp.constraint(inflow(w, p) == outflow(w, p), name=f'C6_{w}_{p}'))

    for h in inst['H']:
        for p in PRODUCTS:
            prob.addConstraint(xp.constraint(inflow(h, p) == outflow(h, p), name=f'C7_{h}_{p}'))

    # ── Solve ─────────────────────────────────────────────────────────────────
    t0 = time.time()
    prob.solve()
    solve_time = time.time() - t0

    status = prob.attributes.solstatus
    if status not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        return None   # infeasible / timeout without solution

    obj_val = prob.getObjVal()

    # Retrieve binary solutions
    open_wh_sol      = {w: round(prob.getSolution(openWarehouse[w])) for w in W}
    arc_act_sol      = {a: round(prob.getSolution(arc_act[a]))       for a in A_fixed}
    x_sol            = {k: prob.getSolution(v) for k, v in x.items()}

    # Cost decomposition
    fixed_wh_cost  = sum(wh_cost[w]  * open_wh_sol[w]  for w in W)
    fixed_arc_cost = sum(arc_fc[a]   * arc_act_sol[a]  for a in A_fixed)
    var_trans_cost = sum(total_cost[k] * v for k, v in x_sol.items())

    return dict(
        status=status,
        solve_time=solve_time,
        obj_val=obj_val,
        fixed_wh_cost=fixed_wh_cost,
        fixed_arc_cost=fixed_arc_cost,
        var_trans_cost=var_trans_cost,
        open_wh=open_wh_sol,
        arc_act=arc_act_sol,
        x=x_sol,
    )


# =============================================================================
# 4. STRATEGY WRAPPERS
# =============================================================================

def run_strategy_R(scenario_inst, baseline_open_wh, baseline_active_arcs):
    """
    Strategy R — Rerouting only.
    Network configuration locked; re-optimise flows under scenario costs/capacities.
    The reported cost adds sunk fixed costs (already paid in Phase 1) back on top
    of the variable-only objective so that ΔZ comparisons are fair.
    """
    result = solve_network(scenario_inst, 'R',
                           baseline_open_wh=baseline_open_wh,
                           baseline_active_arcs=baseline_active_arcs)
    if result is None:
        return None
    # Add sunk fixed costs back so total cost is comparable
    sunk_wh  = sum(scenario_inst['wh_cost'][w]  for w in baseline_open_wh)
    sunk_arc = sum(scenario_inst['arc_fc'].get(a, 0) for a in baseline_active_arcs)
    result['sunk_wh_cost']  = sunk_wh
    result['sunk_arc_cost'] = sunk_arc
    result['total_cost_comparable'] = result['var_trans_cost'] + sunk_wh + sunk_arc
    return result


def run_strategy_A(scenario_inst):
    """
    Strategy A — Adaptation.
    Network can be partially reconfigured; fixed costs are paid for any open
    warehouse or activated arc under the scenario.
    """
    result = solve_network(scenario_inst, 'A')
    if result is None:
        return None
    result['total_cost_comparable'] = result['obj_val']
    return result


def run_strategy_F(scenario_inst):
    """
    Strategy F — Full redesign.
    Identical to Strategy A in formulation but represents solving Phase 1 fresh.
    Gives the true cost lower bound under scenario parameters.
    """
    result = solve_network(scenario_inst, 'F')
    if result is None:
        return None
    result['total_cost_comparable'] = result['obj_val']
    return result
# =============================================================================
# 5. CUSTOM SCENARIO S4 — Red Sea / Suez Crisis
# =============================================================================
#
# Motivation: In late 2023 and throughout 2024, Houthi attacks in the Red Sea
# forced container shipping to re-route around the Cape of Good Hope, adding
# ~10-14 days and 20-25% to sea freight costs on routes between Asia/Middle East
# and Europe. The disruption is ongoing and geopolitically credible.
#
# Implementation:
#   • All sea-mode arcs whose zone_from is Asia, MiddleEast, or Africa and
#     zone_to is Europe (or vice-versa) receive a +25% cost uplift.
#   • This is realised by generating a synthetic ArcCosts_S4 sheet in the Excel
#     file and an empty ArcsRemoved_S4 sheet (no arcs are removed, only costs
#     rise).  The code below creates those sheets if they don't already exist.

def build_s4_sheets():
    """
    Create ArcCosts_S4 and ArcsRemoved_S4 sheets inside the Excel file if absent.
    Returns (cost_df, removed_arcs_df).
    """
    xl = pd.ExcelFile(EXCEL_FILE)
    if 'ArcCosts_S4' in xl.sheet_names:
        costs_df    = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_S4')
        removed_df  = pd.read_excel(EXCEL_FILE, sheet_name='ArcsRemoved_S4') \
                      if 'ArcsRemoved_S4' in xl.sheet_names else pd.DataFrame(columns=['arc_id', 'from_id', 'to_id', 'reason'])
        return costs_df, removed_df

    baseline_costs = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')
    arcs_df        = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')

    # Identify arcs crossing via Red Sea / Suez corridor
    # Proxy: sea-mode arcs between (Asia | MiddleEast | Africa) and Europe
    SUEZ_ZONES_FROM = {'Asia', 'MiddleEast', 'Africa'}
    SUEZ_ZONES_TO   = {'Europe'}
    suez_arcs = set(
        arcs_df.loc[
            (arcs_df['transport_mode'] == 'sea') &
            (
                (arcs_df['zone_from'].isin(SUEZ_ZONES_FROM) & arcs_df['zone_to'].isin(SUEZ_ZONES_TO)) |
                (arcs_df['zone_from'].isin(SUEZ_ZONES_TO)   & arcs_df['zone_to'].isin(SUEZ_ZONES_FROM))
            ),
            'arc_id'
        ]
    )

    s4_costs = baseline_costs.copy()
    mask = s4_costs['arc_id'].isin(suez_arcs)
    s4_costs.loc[mask, 'variable_cost'] = s4_costs.loc[mask, 'variable_cost'] * 1.25
    s4_costs['cost_multiplier'] = s4_costs['arc_id'].apply(lambda a: 1.25 if a in suez_arcs else 1.0)

    removed_df = pd.DataFrame(columns=['arc_id', 'from_id', 'to_id', 'reason'])

    # Append new sheets to the workbook
    from openpyxl import load_workbook
    with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl', mode='a') as writer:
        s4_costs.to_excel(writer,   sheet_name='ArcCosts_S4',   index=False)
        removed_df.to_excel(writer, sheet_name='ArcsRemoved_S4', index=False)

    print(f"  S4: {mask.sum()} (arc, product) pairs affected by +25% Suez surcharge "
          f"({len(suez_arcs)} unique arcs).")
    return s4_costs, removed_df


# =============================================================================
# 6. MAIN — PHASE 1 BASELINE + PHASE 2 SCENARIO LOOP
# =============================================================================

print("=" * 70)
print("GlobalFlow Phase 2 — Adversarial Scenario Analysis")
print("=" * 70)

# -- Build S4 data (idempotent) ------------------------------------------------
print("\nPreparing custom scenario S4 (Red Sea / Suez crisis)...")
build_s4_sheets()

# -- Load base data ------------------------------------------------------------
print("Loading base data...")
nodes_df, arcs_df, warehouses_df, suppliers_df, demand_df, supply_df, tariffs_df = load_base_data()

# -- Phase 1 baseline ----------------------------------------------------------
print("\n--- PHASE 1: Solving baseline (ArcCosts_Baseline) ---")
baseline_costs_df = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')
baseline_inst     = build_instance(nodes_df, arcs_df, warehouses_df, suppliers_df,
                                   demand_df, supply_df, tariffs_df, baseline_costs_df)

baseline_result = solve_network(baseline_inst, 'F')   # 'F' = unconstrained solve = Phase 1
if baseline_result is None:
    raise SystemExit("Phase 1 baseline infeasible — check data.")

Z_star             = baseline_result['obj_val']
baseline_open_wh   = {w for w, v in baseline_result['open_wh'].items() if v == 1}
baseline_act_arcs  = {a for a, v in baseline_result['arc_act'].items() if v == 1}

print(f"  Baseline cost Z* = ${Z_star:,.2f}")
print(f"  Open warehouses  : {sorted(baseline_open_wh)}")
print(f"  Activated arcs   : {len(baseline_act_arcs)}")

# -- Phase 2 scenario loop -----------------------------------------------------
all_results = {}   # scenario_id → {'R': ..., 'A': ..., 'F': ...}

for (scen_id, cost_sheet, arcs_removed_sheet, nodes_removed_sheet) in SCENARIOS:
    print(f"\n{'─' * 70}")
    print(f"Scenario {scen_id}  (cost sheet: {cost_sheet})")

    # Load removed sets
    removed_arcs  = set()
    removed_nodes = set()
    if arcs_removed_sheet:
        df = pd.read_excel(EXCEL_FILE, sheet_name=arcs_removed_sheet)
        removed_arcs = set(df['arc_id'])
    if nodes_removed_sheet:
        df = pd.read_excel(EXCEL_FILE, sheet_name=nodes_removed_sheet)
        removed_nodes = set(df['node_id'])

    # Load scenario costs
    costs_df = pd.read_excel(EXCEL_FILE, sheet_name=cost_sheet)

    # Build scenario instance
    scen_inst = build_instance(nodes_df, arcs_df, warehouses_df, suppliers_df,
                               demand_df, supply_df, tariffs_df, costs_df,
                               removed_arcs=removed_arcs, removed_nodes=removed_nodes)

    scen_results = {}

    # Strategy R
    print(f"  [R] Rerouting only...", end=' ', flush=True)
    r = run_strategy_R(scen_inst, baseline_open_wh, baseline_act_arcs)
    if r:
        scen_results['R'] = r
        print(f"${r['total_cost_comparable']:,.2f}  (ΔZ = ${r['total_cost_comparable'] - Z_star:+,.2f})")
    else:
        scen_results['R'] = None
        print("INFEASIBLE")

    # Strategy A
    print(f"  [A] Adaptation...",    end=' ', flush=True)
    a = run_strategy_A(scen_inst)
    if a:
        scen_results['A'] = a
        print(f"${a['total_cost_comparable']:,.2f}  (ΔZ = ${a['total_cost_comparable'] - Z_star:+,.2f})")
    else:
        scen_results['A'] = None
        print("INFEASIBLE")

    # Strategy F
    print(f"  [F] Full redesign...", end=' ', flush=True)
    f = run_strategy_F(scen_inst)
    if f:
        scen_results['F'] = f
        print(f"${f['total_cost_comparable']:,.2f}  (ΔZ = ${f['total_cost_comparable'] - Z_star:+,.2f})")
    else:
        scen_results['F'] = None
        print("INFEASIBLE")

    all_results[scen_id] = scen_results

# =============================================================================
# 7. ANALYSIS — DETERMINE BEST STRATEGY AND DISRUPTION COSTS
# =============================================================================

print(f"\n{'=' * 70}")
print("PHASE 2 SUMMARY")
print(f"{'=' * 70}")
print(f"{'Scenario':<10} {'R ($)':>14} {'A ($)':>14} {'F ($)':>14} {'Best':>6} {'ΔZ ($)':>14}")
print(f"{'─' * 70}")

summary_data = []
for scen_id, res in all_results.items():
    costs = {}
    for strat in ('R', 'A', 'F'):
        if res[strat] is not None:
            costs[strat] = res[strat]['total_cost_comparable']

    if not costs:
        print(f"{scen_id:<10} {'INFEASIBLE':>48}")
        continue

    best_strat = min(costs, key=costs.get)
    best_cost  = costs[best_strat]
    delta_z    = best_cost - Z_star

    r_str = f"${costs.get('R', float('nan')):>12,.2f}" if 'R' in costs else f"{'INFEAS':>14}"
    a_str = f"${costs.get('A', float('nan')):>12,.2f}" if 'A' in costs else f"{'INFEAS':>14}"
    f_str = f"${costs.get('F', float('nan')):>12,.2f}" if 'F' in costs else f"{'INFEAS':>14}"

    print(f"{scen_id:<10} {r_str} {a_str} {f_str} {best_strat:>6} ${delta_z:>12,.2f}")
    summary_data.append(dict(
        scenario_id=scen_id,
        cost_R=costs.get('R'),
        cost_A=costs.get('A'),
        cost_F=costs.get('F'),
        best_strategy=best_strat,
        best_cost=best_cost,
        delta_z=delta_z,
    ))

# =============================================================================
# 8. EXPORT TO EXCEL
# =============================================================================

print(f"\nExporting to {OUTPUT_FILE}...")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Helper: build per-scenario per-strategy detail rows
def flow_rows(result, inst, strategy_label, scenario_id):
    rows = []
    if result is None:
        return rows
    for (a, p), flow in result['x'].items():
        flow = round(flow, 2)
        if flow <= 0.01:
            continue
        cap = inst['arc_cap'].get(a, None)
        rows.append(dict(
            scenario=scenario_id,
            strategy=strategy_label,
            arc_id=a,
            source=inst['arc_src'].get(a),
            target=inst['arc_tgt'].get(a),
            product=p,
            flow=flow,
            capacity=cap,
            utilization_pct=round(flow / cap * 100, 1) if cap else None,
            var_cost=round(inst['var_cost'].get((a, p), 0), 4),
            total_cost_unit=round(inst['total_cost'].get((a, p), 0), 4),
            flow_cost=round(flow * inst['total_cost'].get((a, p), 0), 2),
            transport_mode=inst['arc_mode'].get(a),
            distance_km=inst['arc_dist'].get(a),
        ))
    return rows


# Re-build scenario instances for export (they were not cached)
scen_instances = {}
for (scen_id, cost_sheet, arcs_removed_sheet, nodes_removed_sheet) in SCENARIOS:
    removed_arcs  = set()
    removed_nodes = set()
    if arcs_removed_sheet:
        df = pd.read_excel(EXCEL_FILE, sheet_name=arcs_removed_sheet)
        removed_arcs = set(df['arc_id'])
    if nodes_removed_sheet:
        df = pd.read_excel(EXCEL_FILE, sheet_name=nodes_removed_sheet)
        removed_nodes = set(df['node_id'])
    costs_df = pd.read_excel(EXCEL_FILE, sheet_name=cost_sheet)
    scen_instances[scen_id] = build_instance(
        nodes_df, arcs_df, warehouses_df, suppliers_df,
        demand_df, supply_df, tariffs_df, costs_df,
        removed_arcs=removed_arcs, removed_nodes=removed_nodes,
    )

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:

    # ── Sheet 1: Comparison Summary ──────────────────────────────────────────
    sum_rows = []
    for d in summary_data:
        sum_rows.append({
            'Scenario':              d['scenario_id'],
            'Cost R ($)':            round(d['cost_R'],  2) if d['cost_R']  is not None else 'INFEASIBLE',
            'Cost A ($)':            round(d['cost_A'],  2) if d['cost_A']  is not None else 'INFEASIBLE',
            'Cost F ($)':            round(d['cost_F'],  2) if d['cost_F']  is not None else 'INFEASIBLE',
            'Best Strategy':         d['best_strategy'],
            'Best Cost ($)':         round(d['best_cost'], 2),
            'ΔZ ($)':                round(d['delta_z'],   2),
            'ΔZ (%)':                round(d['delta_z'] / Z_star * 100, 2),
        })
    pd.DataFrame(sum_rows).to_excel(writer, sheet_name='Comparison Summary', index=False)

    # ── Sheet 2: Baseline Summary ─────────────────────────────────────────────
    baseline_rows = [
        ('Scenario',                    'Baseline'),
        ('Total Cost Z* ($)',            round(Z_star, 2)),
        ('  Warehouse Opening Cost ($)', round(baseline_result['fixed_wh_cost'], 2)),
        ('  Arc Activation Cost ($)',    round(baseline_result['fixed_arc_cost'], 2)),
        ('  Variable Transport Cost ($)',round(baseline_result['var_trans_cost'], 2)),
        ('', ''),
        ('Open Warehouses',             ', '.join(sorted(baseline_open_wh))),
        ('Activated Optional Arcs',     len(baseline_act_arcs)),
    ]
    for p in PRODUCTS:
        delivered = sum(
            v for (a, pp), v in baseline_result['x'].items()
            if pp == p and baseline_inst['arc_tgt'].get(a) in baseline_inst['C']
        )
        total_dem = sum(v for (_, pp), v in baseline_inst['Dem'].items() if pp == p)
        baseline_rows.append((f'Demand Met — {p}', f'{delivered:.0f} / {total_dem:.0f}'))
    pd.DataFrame(baseline_rows, columns=['Metric', 'Value']).to_excel(
        writer, sheet_name='Baseline', index=False)

    # ── Sheets 3–N: Per-scenario detail ──────────────────────────────────────
    for scen_id, res in all_results.items():
        inst = scen_instances[scen_id]

        # Cost decomposition per strategy
        decomp_rows = []
        for strat in ('R', 'A', 'F'):
            r = res[strat]
            if r is None:
                decomp_rows.append({
                    'Strategy': strat, 'Status': 'INFEASIBLE',
                    'Warehouse Fixed ($)': None, 'Arc Fixed ($)': None,
                    'Variable Transport ($)': None, 'Total ($)': None, 'ΔZ ($)': None,
                })
                continue
            decomp_rows.append({
                'Strategy':               strat,
                'Status':                 'OK',
                'Warehouse Fixed ($)':    round(r.get('sunk_wh_cost',  r['fixed_wh_cost']),  2),
                'Arc Fixed ($)':          round(r.get('sunk_arc_cost', r['fixed_arc_cost']), 2),
                'Variable Transport ($)': round(r['var_trans_cost'], 2),
                'Total ($)':              round(r['total_cost_comparable'], 2),
                'ΔZ ($)':                 round(r['total_cost_comparable'] - Z_star, 2),
            })
        pd.DataFrame(decomp_rows).to_excel(
            writer, sheet_name=f'{scen_id} Decomposition', index=False)

        # Flow details for best strategy
        best_strat = next((d['best_strategy'] for d in summary_data if d['scenario_id'] == scen_id), None)
        if best_strat and res[best_strat] is not None:
            rows = flow_rows(res[best_strat], inst, best_strat, scen_id)
            if rows:
                pd.DataFrame(rows).to_excel(
                    writer, sheet_name=f'{scen_id} Best Flows', index=False)

    # ── Sheet: Product Impact ─────────────────────────────────────────────────
    impact_rows = []
    for d in summary_data:
        scen_id    = d['scenario_id']
        best_strat = d['best_strategy']
        res        = all_results[scen_id][best_strat]
        inst       = scen_instances[scen_id]
        if res is None:
            continue
        for p in PRODUCTS:
            delivered = sum(
                v for (a, pp), v in res['x'].items()
                if pp == p and inst['arc_tgt'].get(a) in inst['C']
            )
            total_dem  = sum(v for (_, pp), v in inst['Dem'].items() if pp == p)
            base_cost  = sum(
                v * baseline_inst['total_cost'].get((a, pp), 0)
                for (a, pp), v in baseline_result['x'].items() if pp == p
            )
            scen_cost  = sum(
                v * inst['total_cost'].get((a, pp), 0)
                for (a, pp), v in res['x'].items() if pp == p
            )
            impact_rows.append({
                'Scenario':              scen_id,
                'Product':               p,
                'Demand Met':            round(delivered, 1),
                'Total Demand':          round(total_dem, 1),
                'Fill Rate (%)':         round(delivered / total_dem * 100, 1) if total_dem else None,
                'Baseline Flow Cost ($)': round(base_cost, 2),
                'Scenario Flow Cost ($)': round(scen_cost, 2),
                'ΔFlow Cost ($)':        round(scen_cost - base_cost, 2),
            })
    pd.DataFrame(impact_rows).to_excel(writer, sheet_name='Product Impact', index=False)

print(f"Done — results in {OUTPUT_FILE}")