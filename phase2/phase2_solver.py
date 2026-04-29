"""
phase2_solver.py
================
Master solver for Phase 2: Robustness analysis across all scenarios and strategies.

For each of 6 scenarios (T1, T2, T3, S1, S2, S3), solves three strategies:
  - R (Rerouting only): baseline network fixed, flows re-optimized
  - A (Adaptation): full network re-optimization
  - F (Full redesign): greenfield re-solve (same as Phase 1 with scenario costs)

Outputs:
  - Individual scenario sheets: phase2/results/scenario_{SCENARIO}/strategy_{STRATEGY}.xlsx
  - Master summary: phase2/results/summary_all_scenarios.xlsx
"""

import os
import pandas as pd
import xpress as xp
import time
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# =============================================================================
# CONFIGURATION
# =============================================================================

EXCEL_FILE = 'data/globalflow_instance.xlsx'
BASELINE_SOLUTION = 'phase1/results/baseline_solution.xlsx'

SCENARIOS = ['ArcCosts_T1', 'ArcCosts_T2', 'ArcCosts_T3',
             #'ArcCosts_S1', 
             'ArcCosts_S2'
             #, 'ArcCosts_S3'
             ]

STRATEGIES = ['R', 'A', 'F']

MAX_SOLVE_TIME = 300  # seconds per solve

PRODUCTS = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']

# Output directories
RESULTS_DIR = 'phase2/results'
os.makedirs(RESULTS_DIR, exist_ok=True)

# =============================================================================
# HELPER: Load baseline network configuration
# =============================================================================

def load_baseline_config():
    """
    Read Phase 1 baseline solution to extract:
    - open_wh_baseline: set of open warehouse IDs
    - active_arcs_baseline: set of active arc IDs
    - baseline_total_cost: total cost from baseline
    """
    print("Loading baseline configuration...")
    
    wh_df = pd.read_excel(BASELINE_SOLUTION, sheet_name='Warehouses')
    arc_df = pd.read_excel(BASELINE_SOLUTION, sheet_name='Arc Activations')
    summary_df = pd.read_excel(BASELINE_SOLUTION, sheet_name='Summary')
    
    open_wh = set(wh_df[wh_df['open'] == 1]['warehouse_id'])
    active_arcs = set(arc_df[arc_df['activated'] == 1]['arc_id'])
    baseline_cost = float(
        summary_df[summary_df['Metric'] == 'Total Cost ($)']['Value'].iloc[0]
    )
    
    return open_wh, active_arcs, baseline_cost

# =============================================================================
# HELPER: Load and prepare data for a scenario
# =============================================================================

def load_scenario_data(scenario_name):
    """
    Load all static data and scenario-specific costs.
    Returns a dict with all parameters needed to build the model.
    """
    print(f"  Loading data for scenario {scenario_name}...")
    
    nodes_df = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
    arcs_df = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
    warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
    suppliers_df = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
    demand_df = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
    supply_df = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
    baseline_costs_df = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')
    tariffs_df        = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')
    
    # Load scenario-specific costs
    scenario_costs_df = pd.read_excel(EXCEL_FILE, sheet_name=scenario_name)
    
    # Check if scenario removes nodes (S1, S3 scenarios)
    try:
        nodes_removed_df = pd.read_excel(EXCEL_FILE, sheet_name=f'NodesRemoved_{scenario_name.split("_")[1]}')
        removed_nodes = set(nodes_removed_df['node_id'].unique())
        print(f"    WARNING: Scenario {scenario_name} removes nodes: {removed_nodes}")
    except:
        removed_nodes = set()
    
    # Check if scenario removes arcs (S1, S2, S3 scenarios)
    try:
        arcs_removed_df = pd.read_excel(EXCEL_FILE, sheet_name=f'ArcsRemoved_{scenario_name.split("_")[1]}')
        removed_arcs = set(arcs_removed_df['arc_id'].unique())
        print(f"    WARNING: Scenario {scenario_name} removes arcs: {removed_arcs}")
    except:
        removed_arcs = set()
    
    data = {
        'nodes_df': nodes_df,
        'arcs_df': arcs_df,
        'warehouses_df': warehouses_df,
        'suppliers_df': suppliers_df,
        'demand_df': demand_df,
        'supply_df': supply_df,
        'baseline_costs_df': baseline_costs_df,
        'tariffs_df': tariffs_df,
        'scenario_costs_df': scenario_costs_df,
        'removed_nodes': removed_nodes,
        'removed_arcs': removed_arcs,
    }
    
    return data

# =============================================================================
# CORE: Build model (shared by all strategies)
# =============================================================================

def build_model(data, fixed_wh=None, fixed_arcs=None, scenario_name='Baseline'):
    """
    Build the multi-commodity flow model.
    
    Parameters:
    -----------
    data : dict
        Output from load_scenario_data()
    fixed_wh : set or None
        If not None, fix openWarehouse[w] to 1 for w in fixed_wh, 0 otherwise (Strategy R)
    fixed_arcs : set or None
        If not None, fix arc_a to 1 for a in fixed_arcs, 0 otherwise (Strategy R)
    scenario_name : str
        For logging
    
    Returns:
    --------
    (prob, x, openWarehouse, arc_act, arc_src, arc_tgt, arc_cap, arc_fc,
     total_cost, wh_cost, S, H, W, C, arcs_from, arcs_into)
    """
    
    nodes_df = data['nodes_df']
    arcs_df = data['arcs_df']
    warehouses_df = data['warehouses_df']
    suppliers_df = data['suppliers_df']
    demand_df = data['demand_df']
    supply_df = data['supply_df']
    baseline_costs_df = data['baseline_costs_df']
    scenario_costs_df = data['scenario_costs_df']
    tariffs_df = data['tariffs_df']
    removed_nodes = data['removed_nodes']
    removed_arcs = data['removed_arcs']
    
    # --- Sets ---
    S = set(suppliers_df['supplier_id']) - removed_nodes
    H = set(nodes_df[nodes_df['type'] == 'HUB']['node_id']) - removed_nodes
    W = set(warehouses_df['warehouse_id']) - removed_nodes
    C = set(demand_df['customer_id'].unique()) - removed_nodes
    
    # S_p[product] = set of suppliers that produce it
    S_p = {}
    supplier_prods = {}
    for _, row in supply_df.iterrows():
        if row['supplier_id'] in removed_nodes:
            continue
        S_p.setdefault(row['product'], set()).add(row['supplier_id'])
        supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])
    
    # --- Arc lookups ---
    arc_src = {}
    arc_tgt = {}
    arc_cap = {}
    arc_fc = {}
    arc_mode = {}
    arc_dist = {}
    arcs_from = {n: set() for n in nodes_df['node_id'] if n not in removed_nodes}
    arcs_into = {n: set() for n in nodes_df['node_id'] if n not in removed_nodes}
    
    for _, row in arcs_df.iterrows():
        a = row['arc_id']
        if a in removed_arcs:
            continue
        arc_src[a] = row['from_id']
        arc_tgt[a] = row['to_id']
        arc_cap[a] = row['shared_capacity']
        arc_fc[a] = row['fixed_activation_cost']
        arc_mode[a] = row['transport_mode']
        arc_dist[a] = row['distance_km']
        arcs_from[row['from_id']].add(a)
        arcs_into[row['to_id']].add(a)
    
    # --- Arc partitions ---
    A_fixed = {a for a in arc_src if arc_fc[a] > 0}
    A_always = {a for a in arc_src if arc_fc[a] == 0}
    
    # --- Parameters ---
    Dem = {(row['customer_id'], row['product']): row['demand']
           for _, row in demand_df.iterrows()
           if row['customer_id'] not in removed_nodes}
    
    Sup = {(row['supplier_id'], row['product']): row['supply']
           for _, row in supply_df.iterrows()
           if row['supplier_id'] not in removed_nodes}
    
    wh_cap = {row['warehouse_id']: row['capacity']
              for _, row in warehouses_df.iterrows()
              if row['warehouse_id'] not in removed_nodes}
    
    wh_cost = {row['warehouse_id']: row['opening_cost']
               for _, row in warehouses_df.iterrows()
               if row['warehouse_id'] not in removed_nodes}
    
        # Build helper lookups
    tariff_lookup = {(r['zone_pair_from'], r['zone_pair_to']): r['interzonal_tariff_rate']
                    for _, r in tariffs_df.iterrows()}
    arc_zone_from = dict(zip(arcs_df['arc_id'], arcs_df['zone_from']))
    arc_zone_to   = dict(zip(arcs_df['arc_id'], arcs_df['zone_to']))

    base_var = {(r['arc_id'], r['product']): r['variable_cost']
                for _, r in baseline_costs_df.iterrows()}
    scen_var = {(r['arc_id'], r['product']): r['variable_cost']
                for _, r in scenario_costs_df.iterrows()}

    # total_cost = base_var * scenario_factor * (1 + baseline_tariff)
    total_cost = {}
    for (a, p), bv in base_var.items():
        if a in removed_arcs:
            continue
        sv = scen_var.get((a, p), bv)               # scenario cost (or base if missing)
        factor = sv / bv if bv > 0 else 1.0          # scenario shock factor
        tariff = tariff_lookup.get((arc_zone_from[a], arc_zone_to[a]), 0.0)
        total_cost[(a, p)] = bv * factor * (1.0 + tariff)
    
    # --- Problem ---
    prob = xp.problem()
    prob.setControl('MAXTIME', MAX_SOLVE_TIME)
    prob.setControl('OUTPUTLOG', 0)  # Suppress solver output
    prob.setControl('MIPRELSTOP', 1e-7)
    
    # --- Variables ---
    x = {}
    for (a, p) in total_cost:
        src = arc_src[a]
        if src in S and p not in supplier_prods.get(src, set()):
            continue  # Supplier doesn't produce this product
        x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)
    
    openWarehouse = {w: prob.addVariable(name=f'open_{w}', vartype=xp.binary) for w in W}
    arc_act = {a: prob.addVariable(name=f'arc_{a}', vartype=xp.binary) for a in A_fixed}
    
    # --- Constraints ---
    
    # Helper functions
    def inflow(node, product):
        return xp.Sum(x[(a, product)] for a in arcs_into.get(node, []) if (a, product) in x)
    
    def outflow(node, product):
        return xp.Sum(x[(a, product)] for a in arcs_from.get(node, []) if (a, product) in x)
    
    # C1: Demand satisfaction
    for (c, p), d in Dem.items():
        prob.addConstraint(xp.constraint(inflow(c, p) == d, name=f'C1_{c}_{p}'))
    
    # C2: Supply availability
    for p, sup_set in S_p.items():
        for s in sup_set:
            prob.addConstraint(xp.constraint(outflow(s, p) <= Sup[(s, p)], name=f'C2_{s}_{p}'))
    
    # C3: Arc capacity (always-active)
    for a in A_always:
        flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))
    
    # C4: Arc capacity (optional, with activation)
    for a in A_fixed:
        flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a], name=f'C4_{a}'))
    
    # C5: Warehouse capacity (with opening)
    for w in W:
        total_in = xp.Sum(x[(a, p)] for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
        prob.addConstraint(xp.constraint(total_in <= wh_cap[w] * openWarehouse[w], name=f'C5_{w}'))
    
    # C6: Flow conservation at warehouses
    for w in W:
        for p in PRODUCTS:
            prob.addConstraint(xp.constraint(inflow(w, p) == outflow(w, p), name=f'C6_{w}_{p}'))
    
    # C7: Flow conservation at hubs
    for h in H:
        for p in PRODUCTS:
            prob.addConstraint(xp.constraint(inflow(h, p) == outflow(h, p), name=f'C7_{h}_{p}'))
    
    # --- STRATEGY R: Fix warehouse and arc decisions ---
    if fixed_wh is not None:
        for w in W:
            if w in fixed_wh:
                prob.addConstraint(openWarehouse[w] == 1)
            else:
                prob.addConstraint(openWarehouse[w] == 0)
    
    if fixed_arcs is not None:
        for a in A_fixed:
            if a in fixed_arcs:
                prob.addConstraint(arc_act[a] == 1)
            else:
                prob.addConstraint(arc_act[a] == 0)
    
    return {
        'prob': prob,
        'x': x,
        'openWarehouse': openWarehouse,
        'arc_act': arc_act,
        'arc_src': arc_src,
        'arc_tgt': arc_tgt,
        'arc_cap': arc_cap,
        'arc_fc': arc_fc,
        'total_cost': total_cost,
        'wh_cost': wh_cost,
        'S': S,
        'H': H,
        'W': W,
        'C': C,
        'A_fixed': A_fixed,
        'A_always': A_always,
        'arcs_from': arcs_from,
        'arcs_into': arcs_into,
        'Dem': Dem,
        'Sup': Sup,
        'S_p': S_p,
    }

# =============================================================================
# STRATEGY SOLVERS
# =============================================================================

def solve_strategy_R(data, open_wh_baseline, active_arcs_baseline):
    """
    Strategy R: Rerouting only.
    - Warehouse and arc decisions FIXED to baseline
    - Only flow is re-optimized
    - Decision objective: variable costs only (sunk fixed costs are constant)
    - REPORTED cost: variable + sunk baseline fixed costs, so it's comparable to A, F, Z*
    """
    print(f"    Strategy R (Rerouting)...", end=' ', flush=True)

    model = build_model(data, fixed_wh=open_wh_baseline, fixed_arcs=active_arcs_baseline)
    prob = model['prob']
    x = model['x']
    total_cost = model['total_cost']
    wh_cost = model['wh_cost']
    arc_fc = model['arc_fc']
    W = model['W']
    A_fixed = model['A_fixed']

    # Decision objective: only variable costs (fixed are sunk, don't influence routing).
    obj = xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)
    prob.setObjective(obj, sense=xp.minimize)

    t0 = time.time()
    prob.solve()
    elapsed = time.time() - t0

    if prob.attributes.solstatus not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        raise RuntimeError(f"Strategy R did not find feasible solution (status: {prob.attributes.solstatus})")

    variable_cost = prob.getObjVal()

    # Reporting: add the sunk fixed costs from the baseline so the number is
    # on the same scale as Z*, Cost_A, Cost_F. Intersect with the live sets
    # because the scenario may have removed some baseline warehouses/arcs.
    sunk_wh   = sum(wh_cost[w] for w in (open_wh_baseline & W))
    sunk_arcs = sum(arc_fc[a]  for a in (active_arcs_baseline & A_fixed))
    cost_full = variable_cost + sunk_wh + sunk_arcs

    print(f"${cost_full:.2f} ({elapsed:.1f}s)")

    return cost_full, model, prob

'''def solve_strategy_A_odl(data):
    """
    Strategy A: Adaptation.
    - All decisions (warehouses, arcs, flows) are FREE
    - Full objective with fixed + variable costs
    """
    print(f"    Strategy A (Adaptation)...", end=' ', flush=True)
    
    model = build_model(data)
    prob = model['prob']
    x = model['x']
    openWarehouse = model['openWarehouse']
    arc_act = model['arc_act']
    wh_cost = model['wh_cost']
    arc_fc = model['arc_fc']
    total_cost = model['total_cost']
    A_fixed = model['A_fixed']
    W = model['W']
    
    # Objective: full cost
    obj = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)
    obj += xp.Sum(arc_fc[a] * arc_act[a] for a in A_fixed)
    obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)
    prob.setObjective(obj, sense=xp.minimize)
    
    t0 = time.time()
    prob.solve()
    elapsed = time.time() - t0
    
    if prob.attributes.solstatus not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        raise RuntimeError(f"Strategy A did not find feasible solution (status: {prob.attributes.solstatus})")
    
    cost = prob.getObjVal()
    print(f"${cost:.2f} ({elapsed:.1f}s)")
    
    return cost, model, prob'''

def solve_strategy_A(data, open_wh_baseline, active_arcs_baseline):
    """
    Strategy A: Adaptation.
    - All decisions (warehouses, arcs, flows) are FREE
    - Fixed costs are only charged for NEW openings/activations
      (warehouses already open in baseline and arcs already active in baseline
       are sunk: no cost if kept, no recovery if dropped).
    """
    print(f"    Strategy A (Adaptation)...", end=' ', flush=True)

    model = build_model(data)
    prob = model['prob']
    x = model['x']
    openWarehouse = model['openWarehouse']
    arc_act = model['arc_act']
    wh_cost = model['wh_cost']
    arc_fc = model['arc_fc']
    total_cost = model['total_cost']
    A_fixed = model['A_fixed']
    W = model['W']

    # Adaptation accounting: only charge fixed costs for changes from baseline.
    # - Warehouses in baseline (W*) are sunk — no charge whether kept or dropped.
    # - Optional arcs in baseline (A*) are sunk — same treatment.
    # - Newly opened warehouses (not in W*): pay full opening cost.
    # - Newly activated arcs (not in A*): pay full activation cost.
    #
    # NB: open_wh_baseline / active_arcs_baseline may include items that the
    # scenario removed (deleted nodes/arcs). We intersect with the live sets
    # W and A_fixed so we don't reference variables that don't exist.
    baseline_wh_alive   = open_wh_baseline    & W
    baseline_arcs_alive = active_arcs_baseline & A_fixed
    new_wh   = W       - baseline_wh_alive
    new_arcs = A_fixed - baseline_arcs_alive

    obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in new_wh)
    obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in new_arcs)
    obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

    prob.setObjective(obj, sense=xp.minimize)

    t0 = time.time()
    prob.solve()
    elapsed = time.time() - t0

    if prob.attributes.solstatus not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        raise RuntimeError(f"Strategy A did not find feasible solution (status: {prob.attributes.solstatus})")

    cost = prob.getObjVal()
    print(f"${cost:.2f} ({elapsed:.1f}s)")

    return cost, model, prob
def solve_strategy_F(data):
    """
    Strategy F: Full redesign (greenfield).
    - All decisions free
    - Pays ALL fixed costs from scratch (warehouse openings and arc activations),
      ignoring what was already paid in baseline. This is the theoretical
      lower bound that ignores sunk and transition costs.
    """
    print(f"    Strategy F (Full redesign)...", end=' ', flush=True)

    model = build_model(data)
    prob = model['prob']
    x = model['x']
    openWarehouse = model['openWarehouse']
    arc_act = model['arc_act']
    wh_cost = model['wh_cost']
    arc_fc = model['arc_fc']
    total_cost = model['total_cost']
    A_fixed = model['A_fixed']
    W = model['W']

    obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)
    obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in A_fixed)
    obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

    prob.setObjective(obj, sense=xp.minimize)

    t0 = time.time()
    prob.solve()
    elapsed = time.time() - t0

    if prob.attributes.solstatus not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        raise RuntimeError(f"Strategy F did not find feasible solution (status: {prob.attributes.solstatus})")

    cost = prob.getObjVal()
    print(f"${cost:.2f} ({elapsed:.1f}s)")

    return cost, model, prob

# =============================================================================
# EXPORT: Extract and save solution
# =============================================================================

def extract_solution(model, prob):
    """
    Extract solution from Xpress model and return structured dict.
    """
    x = model['x']
    openWarehouse = model['openWarehouse']
    arc_act = model['arc_act']
    arc_src = model['arc_src']
    arc_tgt = model['arc_tgt']
    arc_cap = model['arc_cap']
    arc_fc = model['arc_fc']
    wh_cost = model['wh_cost']
    wh_cap_dict = {w: model['prob'].getProbStatus() for w in model['W']}  # Placeholder
    total_cost = model['total_cost']
    
    # Actually, let's get wh capacity from the data we loaded earlier
    # This is a bit messy; let me restructure the return
    
    return {
        'prob': prob,
        'model': model,
        'obj_value': prob.getObjVal(),
        'x': x,
        'openWarehouse': openWarehouse,
        'arc_act': arc_act,
    }

def export_scenario_solution(scenario_name, strategy, model, prob, baseline_cost):
    """
    Export solution to Excel file for a specific scenario and strategy.
    """
    x = model['x']
    openWarehouse = model['openWarehouse']
    arc_act = model['arc_act']
    arc_src = model['arc_src']
    arc_tgt = model['arc_tgt']
    arc_cap = model['arc_cap']
    arc_fc = model['arc_fc']
    arc_mode = {a: 'air' for a in arc_src}  # Placeholder; load from data if needed
    arc_dist = {a: 0 for a in arc_src}  # Placeholder
    wh_cost = model['wh_cost']
    total_cost = model['total_cost']
    W = model['W']
    A_fixed = model['A_fixed']
    
    obj_value = prob.getObjVal()
    disruption = obj_value - baseline_cost
    
    output_dir = os.path.join(RESULTS_DIR, f'scenario_{scenario_name}')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'strategy_{strategy}.xlsx')
    
    # --- Per-product flow sheets ---
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
                'arc_id': a,
                'source': arc_src[a],
                'target': arc_tgt[a],
                'product': p,
                'flow': flow,
                'capacity': cap,
                'utilization_%': round(flow / cap * 100, 1) if cap > 0 else 0,
                'var_cost_per_unit': round(total_cost.get((a, p), 0), 4),
                'flow_cost': round(flow * total_cost.get((a, p), 0), 2),
            })
        product_dfs[p] = pd.DataFrame(rows).sort_values('arc_id')
    
    # --- Warehouse sheet ---
    wh_rows = []
    for w in sorted(W):
        opened = round(prob.getSolution(openWarehouse[w]))
        total_in = round(sum(
            prob.getSolution(x[(a, p)])
            for a in model['arcs_into'][w] for p in PRODUCTS if (a, p) in x
        ), 2)
        # Load actual capacity — for now use placeholder
        cap = 10000  # Placeholder
        wh_rows.append({
            'warehouse_id': w,
            'open': opened,
            'opening_cost': wh_cost[w],
            'capacity': cap,
            'total_inflow': total_in,
            'utilization_%': round(total_in / cap * 100, 1) if opened and cap > 0 else None,
        })
    wh_df = pd.DataFrame(wh_rows).sort_values(['open', 'warehouse_id'], ascending=[False, True])
    
    # --- Arc activation sheet ---
    arc_rows = []
    for a in sorted(A_fixed):
        activated = round(prob.getSolution(arc_act[a]))
        total_flow = round(sum(prob.getSolution(x[(a, p)]) for p in PRODUCTS if (a, p) in x), 2)
        cap = arc_cap[a]
        arc_rows.append({
            'arc_id': a,
            'activated': activated,
            'source': arc_src[a],
            'target': arc_tgt[a],
            'total_flow': total_flow,
            'capacity': cap,
            'utilization_%': round(total_flow / cap * 100, 1) if activated else None,
            'fixed_cost': arc_fc[a],
        })
    arc_df = pd.DataFrame(arc_rows).sort_values(['activated', 'arc_id'], ascending=[False, True])
    
    # --- Summary sheet ---
    summary_rows = [
        ('Scenario', scenario_name),
        ('Strategy', strategy),
        ('Total Cost ($)', round(obj_value, 2)),
        ('Disruption Cost ($)', round(disruption, 2)),
        ('Disruption (%)', round(disruption / baseline_cost * 100, 1)),
        ('', ''),
        ('Warehouses Open', sum(1 for w in W if round(prob.getSolution(openWarehouse[w])) == 1)),
        ('Optional Arcs Activated', sum(1 for a in A_fixed if round(prob.getSolution(arc_act[a])) == 1)),
    ]
    summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])
    
    # --- Write workbook ---
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        for p, df in product_dfs.items():
            df.to_excel(writer, sheet_name=p.replace('_', ' ')[:31], index=False)
        wh_df.to_excel(writer, sheet_name='Warehouses', index=False)
        arc_df.to_excel(writer, sheet_name='Arc Activations', index=False)
    
    print(f"      → Exported to {output_file}")
    
    return {
        'scenario': scenario_name,
        'strategy': strategy,
        'cost': obj_value,
        'disruption': disruption,
    }

# =============================================================================
# MAIN: Run all scenarios and strategies
# =============================================================================

def main():
    print("=" * 80)
    print("PHASE 2: ROBUSTNESS ANALYSIS")
    print("=" * 80)
    
    # Load baseline
    open_wh_baseline, active_arcs_baseline, baseline_cost = load_baseline_config()
    print(f"\nBaseline configuration:")
    print(f"  Open warehouses: {len(open_wh_baseline)}")
    print(f"  Active arcs: {len(active_arcs_baseline)}")
    print(f"  Total cost: ${baseline_cost:.2f}")
    
    # Master results table
    summary_table = []
    
    for scenario_name in SCENARIOS:
        print(f"\n{'=' * 80}")
        print(f"SCENARIO: {scenario_name}")
        print(f"{'=' * 80}")
        
        data = load_scenario_data(scenario_name)
        scenario_results = {}
        
        # Strategy R
        try:
            cost_R, model_R, prob_R = solve_strategy_R(data, open_wh_baseline, active_arcs_baseline)
            result_R = export_scenario_solution(scenario_name, 'R', model_R, prob_R, baseline_cost)
            scenario_results['R'] = result_R
        except Exception as e:
            print(f"      ERROR: {e}")
            scenario_results['R'] = {'cost': None, 'disruption': None}
        
        # Strategy A
        try:
            cost_A, model_A, prob_A = solve_strategy_A(data, open_wh_baseline, active_arcs_baseline)
            result_A = export_scenario_solution(scenario_name, 'A', model_A, prob_A, baseline_cost)
            scenario_results['A'] = result_A
        except Exception as e:
            print(f"      ERROR: {e}")
            scenario_results['A'] = {'cost': None, 'disruption': None}
        
        # Strategy F
        try:
            cost_F, model_F, prob_F = solve_strategy_F(data)
            result_F = export_scenario_solution(scenario_name, 'F', model_F, prob_F, baseline_cost)
            scenario_results['F'] = result_F
        except Exception as e:
            print(f"      ERROR: {e}")
            scenario_results['F'] = {'cost': None, 'disruption': None}
        
        # Compute comparisons
        cost_R = scenario_results['R'].get('cost')
        cost_A = scenario_results['A'].get('cost')
        cost_F = scenario_results['F'].get('cost')
        
        if cost_R and cost_A:
            flex_value = cost_R - cost_A
        else:
            flex_value = None
        
        if cost_A and cost_F:
            sunk_cost = cost_A - cost_F
        else:
            sunk_cost = None
        
        best_strategy = None
        if cost_R and cost_A and cost_F:
            costs = {'R': cost_R, 'A': cost_A, 'F': cost_F}
            best_strategy = min(costs, key=costs.get)
        
        # Add to master summary
        summary_table.append({
            'Scenario': scenario_name,
            'Cost_R': round(cost_R, 2) if cost_R else None,
            'Cost_A': round(cost_A, 2) if cost_A else None,
            'Cost_F': round(cost_F, 2) if cost_F else None,
            'Disruption_R': round(scenario_results['R'].get('disruption', 0), 2),
            'Disruption_A': round(scenario_results['A'].get('disruption', 0), 2),
            'Disruption_F': round(scenario_results['F'].get('disruption', 0), 2),
            'Best_Strategy': best_strategy,
            'Flex_Value_R_to_A': round(flex_value, 2) if flex_value else None,
            'Sunk_Cost_A_to_F': round(sunk_cost, 2) if sunk_cost else None,
        })
    
    # --- Write master summary ---
    summary_df = pd.DataFrame(summary_table)
    master_file = os.path.join(RESULTS_DIR, 'summary_all_scenarios.xlsx')
    summary_df.to_excel(master_file, index=False)
    
    print(f"\n{'=' * 80}")
    print(f"MASTER SUMMARY")
    print(f"{'=' * 80}")
    print(summary_df.to_string(index=False))
    print(f"\nExported to {master_file}")

if __name__ == '__main__':
    main()