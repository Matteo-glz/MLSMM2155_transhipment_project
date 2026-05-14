"""
GlobalFlow Phase 2 Extension — Canal Disruption Scenarios
==========================================================
Implements 6 canal-disruption scenarios × 3 strategies = 18 runs.

Scenarios
---------
  S4.1  Suez cost shock only          (+80% variable cost on Suez sea arcs)
  S4.2  Suez capacity reduction only  (×0.40 capacity on Suez sea arcs)
  S4.3  Suez combined shock           (cost ×1.80 + capacity ×0.40)
  S5.1  Panama cost shock only        (+80% variable cost on Panama sea arcs)
  S5.2  Panama capacity reduction only(×0.40 capacity on Panama sea arcs)
  S5.3  Panama combined shock         (cost ×1.80 + capacity ×0.40)

All six scenarios are structurally feasible (no nodes removed, no suppliers
stranded). Demand C1 is enforced as a hard equality for all strategies.
No penalty slack or emergency arcs are used.

Strategy semantics (identical to phase2_solver.py)
--------------------------------------------------
  R  Baseline W* and A* held fixed; only flow re-optimised under new
     costs / capacities. Fixed costs are SUNK — not charged in the
     objective but added back at reporting time so Cost_R is on the
     same scale as Z*.

  A  Warehouse and arc activation decisions freed. Only NEW openings /
     activations beyond the baseline are charged fixed costs. Baseline
     facilities kept are sunk (no charge). Cost_A ≥ Cost_F (sunk burden).

  F  Full greenfield re-solve. ALL fixed costs paid from scratch.
     Cost_F is the lower bound on achievable cost under the scenario.

Cost model (identical to phase2_solver.py)
------------------------------------------
  total_cost[a,p] = baseline_var[a,p] * factor[a] * (1 + tariff[zone_from, zone_to])
  where factor[a] = cost_factor  if arc a is in the affected canal set
                  = 1.0          otherwise

Outputs
-------
  phase3/results/scenario_{SCENARIO_KEY}/strategy_{STRATEGY}.xlsx   (per run)
  phase3/results/summary_canal_scenarios.xlsx                        (master)

Each per-run workbook includes the same sheets as phase2_solver.py
(minus Unmet Demand and Emergency Arcs, which are not needed here):
  - Summary
  - A Fertilizers / B Semiconductors / C BatteryComponents
  - Warehouses
  - Arc Activations
"""

import os
import pandas as pd
import xpress as xp
import time

xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')

# =============================================================================
# 1. CONFIGURATION
# =============================================================================

_HERE = os.path.dirname(os.path.abspath(__file__))   # phase2/
_ROOT = os.path.dirname(_HERE)                        # project root
_CWD  = os.getcwd()                                   # wherever the user launched python

# Baseline solution: try both 'result' and 'results' spellings (folder was renamed)
_BASELINE_CANDIDATES = [
    os.path.join(_CWD,  'phase1', 'result',  'baseline_solution.xlsx'),
    os.path.join(_ROOT, 'phase1', 'result',  'baseline_solution.xlsx'),
    os.path.join(_HERE, '..', 'phase1', 'result',  'baseline_solution.xlsx'),
    os.path.join(_CWD,  'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_ROOT, 'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_HERE, '..', 'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_CWD,  'baseline_solution.xlsx'),
]
BASELINE_SOLUTION_FILE = next((p for p in _BASELINE_CANDIDATES if os.path.exists(p)), None)

_EXCEL_CANDIDATES = [
    os.path.join(_CWD,  'data', 'globalflow_instance.xlsx'),
    os.path.join(_ROOT, 'data', 'globalflow_instance.xlsx'),
    os.path.join(_HERE, 'data', 'globalflow_instance.xlsx'),
    os.path.join(_HERE, '..', 'data', 'globalflow_instance.xlsx'),
    os.path.join(_CWD,  'globalflow_instance.xlsx'),
]
EXCEL_FILE = next((p for p in _EXCEL_CANDIDATES if os.path.exists(p)), None)

RESULTS_DIR = os.path.join(_CWD, 'phase3', 'results')

CANAL_SCENARIO_KEYS = ['S4.1', 'S4.2', 'S4.3', 'S5.1', 'S5.2', 'S5.3']
STRATEGIES          = ['R', 'A', 'F']
PRODUCTS            = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME      = 300     # seconds per solve

os.makedirs(RESULTS_DIR, exist_ok=True)

# =============================================================================
# 2. DATA LOADING
# =============================================================================

print("Loading shared data...")
print(f"  CWD          : {_CWD}")
print(f"  Script dir   : {_HERE}")

if EXCEL_FILE is None:
    raise FileNotFoundError(
        "\n\nInstance Excel file NOT FOUND. Tried:\n  - "
        + "\n  - ".join(_EXCEL_CANDIDATES)
        + "\n\nEither move globalflow_instance.xlsx to one of those paths or edit "
          "_EXCEL_CANDIDATES at the top of this script.\n"
    )
print(f"  Instance file: {EXCEL_FILE}")

if BASELINE_SOLUTION_FILE is None:
    raise FileNotFoundError(
        "\n\nBaseline solution file NOT FOUND. Tried:\n  - "
        + "\n  - ".join(_BASELINE_CANDIDATES)
        + "\n\nStrategies R and A require the Phase 1 baseline (W*, A*, Z*).\n"
          "Move baseline_solution.xlsx to one of those paths or edit "
          "_BASELINE_CANDIDATES.\n"
    )
print(f"  Baseline file: {BASELINE_SOLUTION_FILE}")
print(f"  Results dir  : {RESULTS_DIR}")

# ----- Baseline decisions and Z* -----
_bl_wh  = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Warehouses')
_bl_arc = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Arc Activations')
_bl_sum = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Summary')

for col in ('warehouse_id', 'open'):
    if col not in _bl_wh.columns:
        raise ValueError(f"Baseline 'Warehouses' sheet missing column '{col}'.")
for col in ('arc_id', 'activated'):
    if col not in _bl_arc.columns:
        raise ValueError(f"Baseline 'Arc Activations' sheet missing column '{col}'.")

_bl_wh  = _bl_wh.dropna(subset=['warehouse_id', 'open']).copy()
_bl_arc = _bl_arc.dropna(subset=['arc_id', 'activated']).copy()

baseline_open_wh    = dict(zip(_bl_wh['warehouse_id'],  _bl_wh['open'].astype(int)))
baseline_active_arc = dict(zip(_bl_arc['arc_id'],       _bl_arc['activated'].astype(int)))

BASELINE_OPEN_WH_SET    = {w for w, v in baseline_open_wh.items()    if v == 1}
BASELINE_ACTIVE_ARC_SET = {a for a, v in baseline_active_arc.items() if v == 1}

_cost_row = _bl_sum[_bl_sum['Metric'] == 'Total Cost ($)']['Value']
Z_STAR = float(_cost_row.iloc[0]) if not _cost_row.empty else 0.0

if not BASELINE_OPEN_WH_SET:
    raise ValueError("Baseline has ZERO open warehouses — check the Warehouses sheet.")
if Z_STAR <= 0:
    print("  WARNING: Z* is 0 or missing. DeltaZ values will not be computed.")

# ----- Baseline variable costs and tariffs -----
baseline_costs_df = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')
tariffs_df        = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')

baseline_var_cost = {(r['arc_id'], r['product']): r['variable_cost']
                     for _, r in baseline_costs_df.iterrows()}
tariff_lookup     = {(r['zone_pair_from'], r['zone_pair_to']): r['interzonal_tariff_rate']
                     for _, r in tariffs_df.iterrows()}

# ----- Static network sheets -----
nodes_df_all  = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
arcs_df_all   = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
suppliers_df  = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
demand_df     = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
supply_df     = pd.read_excel(EXCEL_FILE, sheet_name='Supply')

arc_zone_from_all = dict(zip(arcs_df_all['arc_id'], arcs_df_all['zone_from']))
arc_zone_to_all   = dict(zip(arcs_df_all['arc_id'], arcs_df_all['zone_to']))

WH_COST_ALL = {row['warehouse_id']: row['opening_cost']
               for _, row in warehouses_df.iterrows()}
ARC_FC_ALL  = {row['arc_id']: row['fixed_activation_cost']
               for _, row in arcs_df_all.iterrows()}

SUNK_BASELINE_WH_COST  = sum(WH_COST_ALL.get(w, 0) for w in BASELINE_OPEN_WH_SET)
SUNK_BASELINE_ARC_COST = sum(ARC_FC_ALL.get(a, 0)  for a in BASELINE_ACTIVE_ARC_SET)

print(f"  Baseline loaded : {len(BASELINE_OPEN_WH_SET)} open WH, "
      f"{len(BASELINE_ACTIVE_ARC_SET)} active arcs, Z*=${Z_STAR:,.2f}")
print(f"  Sunk WH cost    : ${SUNK_BASELINE_WH_COST:,.2f}")
print(f"  Sunk arc cost   : ${SUNK_BASELINE_ARC_COST:,.2f}")

# =============================================================================
# 3. CANAL ARC IDENTIFICATION
# =============================================================================

SUEZ_ZONE_PAIRS = {
    ('Europe',      'Asia'),        ('Asia',        'Europe'),
    ('Europe',      'MiddleEast'),  ('MiddleEast',  'Europe')
       
}

PANAMA_ZONE_PAIRS = {
    ('Americas',     'Asia'),         ('Asia',         'Americas'),
    ('SouthAmerica', 'Asia'),         ('Asia',         'SouthAmerica'),
    ('Americas',     'SouthAmerica'), ('SouthAmerica', 'Americas')
}

suez_arc_ids   = set()
panama_arc_ids = set()

for _, row in arcs_df_all.iterrows():
    if str(row.get('transport_mode', '')).lower() != 'sea':
        continue
    pair = (row['zone_from'], row['zone_to'])
    if pair in SUEZ_ZONE_PAIRS:
        suez_arc_ids.add(row['arc_id'])
    if pair in PANAMA_ZONE_PAIRS:
        panama_arc_ids.add(row['arc_id'])

print(f"\n  Suez-corridor sea arcs   : {len(suez_arc_ids)}")
print(f"  Panama-corridor sea arcs : {len(panama_arc_ids)}")


# Print minimum remaining capacity for capacity-reduction scenarios (sanity check)
all_arc_cap_bl = dict(zip(arcs_df_all['arc_id'], arcs_df_all['shared_capacity']))

for label, arc_set in (('Suez', suez_arc_ids), ('Panama', panama_arc_ids)):
    caps = [all_arc_cap_bl[a] * 0.40 for a in arc_set if a in all_arc_cap_bl]
    if caps:
        print(f"  {label} capacity after ×0.40 reduction: "
              f"min={min(caps):.1f}  max={max(caps):.1f}  "
              f"(all > 0: {all(c > 0 for c in caps)})")

# =============================================================================
# 4. SCENARIO DEFINITIONS
# =============================================================================

CANAL_SCENARIOS = {
    'S4.1': {
        'label':           'Suez — cost shock (+80%)',
        'canal':           'suez',
        'cost_factor':     1.80,
        'capacity_factor': 1.00,
        'shock_type':      'Cost only',
    },
    'S4.2': {
        'label':           'Suez — capacity reduction (−60%)',
        'canal':           'suez',
        'cost_factor':     1.00,
        'capacity_factor': 0.40,
        'shock_type':      'Capacity only',
    },
    'S4.3': {
        'label':           'Suez — combined shock (cost +80%, capacity −60%)',
        'canal':           'suez',
        'cost_factor':     1.80,
        'capacity_factor': 0.40,
        'shock_type':      'Cost + Capacity',
    },
    'S5.1': {
        'label':           'Panama — cost shock (+80%)',
        'canal':           'panama',
        'cost_factor':     1.80,
        'capacity_factor': 1.00,
        'shock_type':      'Cost only',
    },
    'S5.2': {
        'label':           'Panama — capacity reduction (−60%)',
        'canal':           'panama',
        'cost_factor':     1.00,
        'capacity_factor': 0.40,
        'shock_type':      'Capacity only',
    },
    'S5.3': {
        'label':           'Panama — combined shock (cost +80%, capacity −60%)',
        'canal':           'panama',
        'cost_factor':     1.80,
        'capacity_factor': 0.40,
        'shock_type':      'Cost + Capacity',
    },
}

# =============================================================================
# 5. STATIC ARC / NODE SETUP (no arc removals — built once)
# =============================================================================
# For canal scenarios the full network is always available; only costs and
# capacities differ per scenario.  We build the structural lookups once here
# and derive scenario-specific capacity / cost inside the main loop.

arc_src_bl  = {}
arc_tgt_bl  = {}
arc_cap_bl  = {}   # baseline capacity (modified per scenario)
arc_fc_bl   = {}
arc_mode_bl = {}
arc_dist_bl = {}

all_node_ids = set(nodes_df_all['node_id'])
arcs_from_bl = {n: set() for n in all_node_ids}
arcs_into_bl = {n: set() for n in all_node_ids}

for _, row in arcs_df_all.iterrows():
    a = row['arc_id']
    arc_src_bl[a]  = row['from_id']
    arc_tgt_bl[a]  = row['to_id']
    arc_cap_bl[a]  = row['shared_capacity']
    arc_fc_bl[a]   = row['fixed_activation_cost']
    arc_mode_bl[a] = row['transport_mode']
    arc_dist_bl[a] = row['distance_km']
    arcs_from_bl[row['from_id']].add(a)
    arcs_into_bl[row['to_id']].add(a)

A_fixed_bl  = {a for a in arc_src_bl if arc_fc_bl[a] > 0}
A_always_bl = {a for a in arc_src_bl if arc_fc_bl[a] == 0}

S_all = set(suppliers_df['supplier_id'])
H_all = set(nodes_df_all[nodes_df_all['type'] == 'HUB']['node_id'])
W_all = set(warehouses_df['warehouse_id'])
C_all = set(demand_df['customer_id'].unique())

S_p_all = {}
supplier_prods_all = {}
for _, row in supply_df.iterrows():
    S_p_all.setdefault(row['product'], set()).add(row['supplier_id'])
    supplier_prods_all.setdefault(row['supplier_id'], set()).add(row['product'])

Dem_all = {(r['customer_id'], r['product']): r['demand']  for _, r in demand_df.iterrows()}
Sup_all = {(r['supplier_id'], r['product']): r['supply']  for _, r in supply_df.iterrows()}
wh_cap  = {r['warehouse_id']: r['capacity']     for _, r in warehouses_df.iterrows()}
wh_cost = {r['warehouse_id']: r['opening_cost'] for _, r in warehouses_df.iterrows()}

# Master results accumulator
master_rows = []
all_run_costs: dict = {}   # (scenario_key, strategy) -> logistics_cost for post-validation

# =============================================================================
# 6. MAIN LOOP  (scenario × strategy)
# =============================================================================

for SCENARIO_KEY in CANAL_SCENARIO_KEYS:
    scen_cfg = CANAL_SCENARIOS[SCENARIO_KEY]

    affected_arc_ids = (suez_arc_ids if scen_cfg['canal'] == 'suez'
                        else panama_arc_ids)
    cost_factor     = scen_cfg['cost_factor']
    cap_factor      = scen_cfg['capacity_factor']

    for STRATEGY in STRATEGIES:

        OUTPUT_DIR  = os.path.join(RESULTS_DIR, f'scenario_{SCENARIO_KEY}')
        OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'strategy_{STRATEGY}.xlsx')
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        print("\n" + "=" * 70)
        print(f"  Scenario: {SCENARIO_KEY}   Strategy: {STRATEGY}")
        print(f"  {scen_cfg['label']}")
        print("=" * 70)
        print(f"  Affected canal arcs : {len(affected_arc_ids)}"
              f"  cost×{cost_factor:.2f}  capacity×{cap_factor:.2f}")

        # -----------------------------------------------------------------
        # 4a. Build scenario-specific capacity and cost arrays
        # -----------------------------------------------------------------

        # Scenario capacity: apply cap_factor to affected arcs
        arc_cap_scen = {
            a: (cap * cap_factor if a in affected_arc_ids else cap)
            for a, cap in arc_cap_bl.items()
        }

        # Scenario total cost: apply cost_factor to affected arcs, then tariff
        total_cost: dict = {}
        for (a, p), bv in baseline_var_cost.items():
            if a not in arc_src_bl:
                continue
            # scenario variable cost per unit
            sv     = bv * cost_factor if a in affected_arc_ids else bv
            factor = sv / bv if bv > 0 else 1.0
            tariff = tariff_lookup.get(
                (arc_zone_from_all.get(a, ''), arc_zone_to_all.get(a, '')), 0.0)
            total_cost[(a, p)] = bv * factor * (1.0 + tariff)

        # Use the full (unmodified) structural sets — no nodes or arcs removed
        arc_src  = arc_src_bl
        arc_tgt  = arc_tgt_bl
        arc_cap  = arc_cap_scen     # scenario-modified
        arc_fc   = arc_fc_bl
        arc_mode = arc_mode_bl
        arc_dist = arc_dist_bl
        arcs_from = arcs_from_bl
        arcs_into = arcs_into_bl
        A_fixed  = A_fixed_bl
        A_always = A_always_bl
        S        = S_all
        H        = H_all
        W        = W_all
        C        = C_all
        S_p      = S_p_all
        supplier_prods = supplier_prods_all
        Dem      = Dem_all
        Sup      = Sup_all

        print(f"  Active hubs : {sorted(H)}")
        print(f"  Arcs        : {len(arc_src)} total  "
              f"({len(A_fixed)} optional, {len(A_always)} always-on)")
        print(f"  Cost pairs  : {len(total_cost)}")

        # -----------------------------------------------------------------
        # 4b. Build Xpress model
        # -----------------------------------------------------------------

        # Live baseline sets (all survive — no nodes removed)
        baseline_wh_alive   = BASELINE_OPEN_WH_SET    & W
        baseline_arcs_alive = BASELINE_ACTIVE_ARC_SET & A_fixed
        new_wh_set          = W       - baseline_wh_alive
        new_arcs_set        = A_fixed - baseline_arcs_alive

        prob = xp.problem()
        prob.setControl('MAXTIME',    MAX_SOLVE_TIME)
        prob.setControl('OUTPUTLOG',  0)
        prob.setControl('MIPRELSTOP', 1e-7)

        # Flow variables
        x: dict = {}
        for (a, p) in total_cost:
            src = arc_src[a]
            if src in S and p not in supplier_prods.get(src, set()):
                continue
            x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0,
                                          vartype=xp.continuous)

        # Warehouse opening (locked for R)
        openWarehouse: dict = {}
        for w in W:
            if STRATEGY == 'R' and w in baseline_open_wh:
                v = float(baseline_open_wh[w])
                openWarehouse[w] = prob.addVariable(name=f'open_{w}',
                                                     lb=v, ub=v,
                                                     vartype=xp.continuous)
            else:
                openWarehouse[w] = prob.addVariable(name=f'open_{w}',
                                                     vartype=xp.binary)

        # Arc activation (locked for R)
        arc_act: dict = {}
        for a in A_fixed:
            if STRATEGY == 'R' and a in baseline_active_arc:
                v = float(baseline_active_arc[a])
                arc_act[a] = prob.addVariable(name=f'arc_{a}',
                                               lb=v, ub=v,
                                               vartype=xp.continuous)
            else:
                arc_act[a] = prob.addVariable(name=f'arc_{a}',
                                               vartype=xp.binary)

        # Suez capacity scenarios (S4.2, S4.3) can be infeasible for ALL strategies:
        # the 60% capacity cut on Suez corridors creates bottlenecks that cannot
        # be fully bypassed even with full network flexibility, because the Suez
        # arcs carry a disproportionate share of semiconductor and fertiliser flow.
        # Panama capacity scenarios (S5.2, S5.3) are not affected — sufficient
        # alternative routing exists.  Mirror phase2_solver.py's S1/S3 handling:
        # add unmet-demand penalty slack so the model still solves and produces a
        # result (penalty units are excluded from reported logistics cost).
        is_infeasible = (scen_cfg['canal'] == 'suez' and cap_factor < 1.0)

        if is_infeasible:
            print(f"  [NOTE] Suez capacity reduction: adding unmet-demand slack "
                  f"(network may be infeasible under {cap_factor:.0%} Suez capacity, "
                  f"strategy {STRATEGY})")

        print(f"  Variables: {len(x)} flow, {len(openWarehouse)} WH, "
              f"{len(arc_act)} arc_act")

        # Unmet demand slack (R with capacity reduction only)
        PENALTY_M = 1_000_000
        unmet: dict = {}
        if is_infeasible:
            for (c, p), d in Dem.items():
                unmet[(c, p)] = prob.addVariable(name=f'unmet_{c}_{p}', lb=0, ub=d,
                                                 vartype=xp.continuous)

        # -----------------------------------------------------------------
        # Objective (strategy-specific fixed-cost accounting)
        # -----------------------------------------------------------------
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

        # Penalty for unmet demand (R + capacity reduction)
        if is_infeasible:
            obj += xp.Sum(PENALTY_M * unmet[(c, p)] for (c, p) in unmet)

        prob.setObjective(obj, sense=xp.minimize)

        # -----------------------------------------------------------------
        # Constraints C1–C7
        # -----------------------------------------------------------------
        def inflow(node, product):
            return xp.Sum(x[(a, product)]
                          for a in arcs_into.get(node, []) if (a, product) in x)

        def outflow(node, product):
            return xp.Sum(x[(a, product)]
                          for a in arcs_from.get(node, []) if (a, product) in x)

        # C1 — demand satisfaction (soft with penalty when locked network may be infeasible)
        for (c, p), d in Dem.items():
            if is_infeasible:
                prob.addConstraint(xp.constraint(inflow(c, p) + unmet[(c, p)] == d,
                                                 name=f'C1_{c}_{p}'))
            else:
                prob.addConstraint(xp.constraint(inflow(c, p) == d,
                                                 name=f'C1_{c}_{p}'))

        # C2 — supply capacity
        for p, sup_set in S_p.items():
            for s in sup_set:
                if (s, p) in Sup:
                    prob.addConstraint(xp.constraint(outflow(s, p) <= Sup[(s, p)],
                                                     name=f'C2_{s}_{p}'))

        # C3 — arc capacity (always-on arcs, scenario capacity)
        for a in A_always:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))

        # C4 — arc capacity (optional arcs, gated by activation, scenario capacity)
        for a in A_fixed:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a],
                                             name=f'C4_{a}'))

        # C5 — warehouse capacity (gated by opening)
        for w in W:
            total_in = xp.Sum(x[(a, p)]
                              for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(total_in <= wh_cap[w] * openWarehouse[w],
                                             name=f'C5_{w}'))

        # C6 — flow conservation at warehouses
        for w in W:
            for p in PRODUCTS:
                prob.addConstraint(xp.constraint(inflow(w, p) == outflow(w, p),
                                                 name=f'C6_{w}_{p}'))

        # C7 — flow conservation at hubs
        for h in H:
            for p in PRODUCTS:
                prob.addConstraint(xp.constraint(inflow(h, p) == outflow(h, p),
                                                 name=f'C7_{h}_{p}'))

        # -----------------------------------------------------------------
        # 4c. Solve
        # -----------------------------------------------------------------
        print(f"  Solving (limit: {MAX_SOLVE_TIME}s)...")
        t0 = time.time()
        prob.solve()
        solve_time = time.time() - t0

        status = prob.attributes.solstatus
        print(f"  Status: {status}  |  {solve_time:.1f}s")

        if status not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
            print("  No feasible solution found.")
            master_rows.append({
                'scenario_key': SCENARIO_KEY, 'strategy': STRATEGY,
                'canal':        scen_cfg['canal'],
                'shock_type':   scen_cfg['shock_type'],
                'affected_arcs_count': len(affected_arc_ids),
                'status': str(status),
                'logistics_cost': None, 'optimisation_obj': None,
                'disruption_cost': None, 'disruption_pct': None,
                'open_wh': None, 'active_arcs': None,
                'solve_time_s': round(solve_time, 1),
            })
            continue

        # -----------------------------------------------------------------
        # 4d. Extract and report costs
        # -----------------------------------------------------------------
        obj_val = prob.getObjVal()

        open_wh_sol     = {w for w in W      if round(prob.getSolution(openWarehouse[w])) == 1}
        active_arcs_sol = {a for a in A_fixed if round(prob.getSolution(arc_act[a]))       == 1}

        var_trans_cost = sum(
            total_cost[(a, p)] * prob.getSolution(x[(a, p)]) for (a, p) in x)

        # Penalty and unmet units (R + capacity reduction only)
        if is_infeasible:
            total_unmet_units = sum(prob.getSolution(unmet[(c, p)]) for (c, p) in unmet)
            penalty_cost      = PENALTY_M * total_unmet_units
        else:
            total_unmet_units = 0.0
            penalty_cost      = 0.0

        # Fixed cost accounting (same methodology as phase2_solver.py)
        fixed_wh_full  = sum(wh_cost[w] for w in open_wh_sol)
        fixed_arc_full = sum(arc_fc[a]  for a in active_arcs_sol)

        if STRATEGY == 'R':
            fixed_wh_charged  = 0.0
            fixed_arc_charged = 0.0
            sunk_wh   = sum(wh_cost[w] for w in (BASELINE_OPEN_WH_SET    & W))
            sunk_arcs = sum(arc_fc[a]  for a in (BASELINE_ACTIVE_ARC_SET & A_fixed))
        elif STRATEGY == 'A':
            fixed_wh_charged  = sum(wh_cost[w] for w in (open_wh_sol & new_wh_set))
            fixed_arc_charged = sum(arc_fc[a]  for a in (active_arcs_sol & new_arcs_set))
            sunk_wh   = sum(wh_cost[w] for w in (open_wh_sol & baseline_wh_alive))
            sunk_arcs = sum(arc_fc[a]  for a in (active_arcs_sol & baseline_arcs_alive))
        else:  # F
            fixed_wh_charged  = fixed_wh_full
            fixed_arc_charged = fixed_arc_full
            sunk_wh   = 0.0
            sunk_arcs = 0.0

        # Logistics cost (comparable to Z*)
        if STRATEGY == 'R':
            logistics_cost   = sunk_wh + sunk_arcs + var_trans_cost
            optimisation_obj = var_trans_cost
        elif STRATEGY == 'A':
            logistics_cost   = (sunk_wh + sunk_arcs
                                + fixed_wh_charged + fixed_arc_charged
                                + var_trans_cost)
            optimisation_obj = fixed_wh_charged + fixed_arc_charged + var_trans_cost
        else:  # F
            logistics_cost   = fixed_wh_full + fixed_arc_full + var_trans_cost
            optimisation_obj = logistics_cost

        disruption_cost = logistics_cost - Z_STAR if Z_STAR > 0 else None
        disruption_pct  = (disruption_cost / Z_STAR * 100
                           if (disruption_cost is not None and Z_STAR > 0) else None)

        if STRATEGY == 'R':
            baseline_var_trans = (Z_STAR - SUNK_BASELINE_WH_COST
                                  - SUNK_BASELINE_ARC_COST if Z_STAR > 0 else None)
            delta_var_R = (var_trans_cost - baseline_var_trans
                           if baseline_var_trans is not None else None)
        else:
            baseline_var_trans = None
            delta_var_R = None

        # Network changes vs baseline
        wh_opened_new    = sorted(open_wh_sol - BASELINE_OPEN_WH_SET)
        wh_closed        = sorted(BASELINE_OPEN_WH_SET - open_wh_sol)
        wh_kept          = sorted(open_wh_sol & BASELINE_OPEN_WH_SET)
        arc_activated_new = sorted(active_arcs_sol - BASELINE_ACTIVE_ARC_SET)
        arc_deactivated  = sorted(BASELINE_ACTIVE_ARC_SET - active_arcs_sol)
        arc_kept         = sorted(active_arcs_sol & BASELINE_ACTIVE_ARC_SET)

        new_wh_cost_val  = sum(wh_cost.get(w, 0) for w in wh_opened_new)
        new_arc_cost_val = sum(arc_fc.get(a, 0)  for a in arc_activated_new)

        # ---- Print summary (same format as phase2_solver.py) ----
        print(f"  Warehouse opening costs (charged) : ${fixed_wh_charged:>14,.2f}")
        print(f"  Warehouse opening costs (full)    : ${fixed_wh_full:>14,.2f}")
        print(f"  Arc activation costs (charged)    : ${fixed_arc_charged:>14,.2f}")
        print(f"  Arc activation costs (full)       : ${fixed_arc_full:>14,.2f}")
        print(f"  Variable transport cost           : ${var_trans_cost:>14,.2f}")
        print(f"  ----------------------------------")
        print(f"  Optimisation objective (raw)      : ${optimisation_obj:>14,.2f}")
        print(f"  Logistics total (vs Z*)           : ${logistics_cost:>14,.2f}")
        print(f"  OBJECTIVE (xpress, with penalty)  : ${obj_val:>14,.2f}")
        if is_infeasible:
            print(f"  Penalty (unmet demand)            : ${penalty_cost:>14,.2f}"
                  f"  ({total_unmet_units:.1f} units × M={PENALTY_M:,})")
        if disruption_cost is not None:
            print(f"  DeltaZ (vs Z*=${Z_STAR:,.2f})       : ${disruption_cost:>+14,.2f}"
                  f"  ({disruption_pct:+.1f}%)")
        if delta_var_R is not None:
            print(f"  Variable cost delta vs baseline   : ${delta_var_R:>+14,.2f}")

        print(f"  Open warehouses ({len(open_wh_sol)}/{len(W)})     : {sorted(open_wh_sol)}")
        print(f"  Active opt. arcs ({len(active_arcs_sol)}/{len(A_fixed)})  : {len(active_arcs_sol)} arcs")
        if wh_opened_new:
            print(f"  WH opened vs baseline  : {wh_opened_new}  (+${new_wh_cost_val:,.2f})")
        if wh_closed:
            print(f"  WH closed vs baseline  : {wh_closed}")
        if arc_activated_new:
            print(f"  Arcs activated vs baseline : {len(arc_activated_new)} arcs  "
                  f"(+${new_arc_cost_val:,.2f})")
        if arc_deactivated:
            print(f"  Arcs deactivated vs baseline: {len(arc_deactivated)} arcs")

        print("  Demand fulfilment:")
        for p in PRODUCTS:
            delivered = sum(
                prob.getSolution(x[(a, p)])
                for c in C for a in arcs_into.get(c, []) if (a, p) in x)
            unmet_p = sum(prob.getSolution(unmet[(c, pp)])
                          for (c, pp) in unmet if pp == p) if unmet else 0.0
            total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
            if is_infeasible:
                print(f"    {p:35s}: {delivered:>7.1f} / {total_dem:.0f}"
                      f"  unmet={unmet_p:.1f}  ({100 * delivered / total_dem:.1f}%)")
            else:
                print(f"    {p:35s}: {delivered:>7.1f} / {total_dem:.0f}"
                      f"  ({100 * delivered / total_dem:.1f}%)")

        all_run_costs[(SCENARIO_KEY, STRATEGY)] = logistics_cost

        # -----------------------------------------------------------------
        # 4e. Export Excel workbook
        # -----------------------------------------------------------------
        print(f"  Exporting -> {OUTPUT_FILE}")

        # Per-product flow sheets
        product_dfs: dict = {}
        for p in PRODUCTS:
            rows = []
            for (a, pp) in x:
                if pp != p:
                    continue
                flow = round(prob.getSolution(x[(a, p)]), 2)
                if flow <= 0.01:
                    continue
                cap = arc_cap[a]
                bvc = baseline_var_cost.get((a, p), None)
                rows.append({
                    'arc_id':            a,
                    'source':            arc_src[a],
                    'target':            arc_tgt[a],
                    'product':           p,
                    'flow':              flow,
                    'capacity':          cap,
                    'utilization_%':     round(flow / cap * 100, 1) if cap < 99999 else None,
                    'baseline_var_cost': round(bvc, 4) if bvc is not None else None,
                    'scenario_var_cost': round(total_cost.get((a, p), 0), 4),
                    'cost_delta_pu':     (round(total_cost.get((a, p), 0) - bvc, 4)
                                          if bvc is not None else None),
                    'flow_cost':         round(flow * total_cost.get((a, p), 0), 2),
                    'transport_mode':    arc_mode[a],
                    'distance_km':       arc_dist[a],
                    'canal_affected':    ('YES' if a in affected_arc_ids else 'no'),
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
            cap = wh_cap[w]
            was_open = 1 if w in BASELINE_OPEN_WH_SET else 0
            if opened == 1 and was_open == 1:
                status_vs_bl = 'kept'
            elif opened == 1 and was_open == 0:
                status_vs_bl = 'newly_opened'
            elif opened == 0 and was_open == 1:
                status_vs_bl = 'closed'
            else:
                status_vs_bl = 'unused'
            if STRATEGY == 'R':
                cost_charged = 0.0
            elif STRATEGY == 'A':
                cost_charged = wh_cost[w] if (opened == 1 and was_open == 0) else 0.0
            else:
                cost_charged = wh_cost[w] if opened == 1 else 0.0
            wh_rows.append({
                'warehouse_id':       w,
                'open_baseline':      was_open,
                'open_scenario':      opened,
                'status_vs_baseline': status_vs_bl,
                'opening_cost':       wh_cost[w],
                'cost_charged':       round(cost_charged, 2),
                'capacity':           cap,
                'total_inflow':       total_in,
                'utilization_%':      (round(total_in / cap * 100, 1)
                                       if opened and cap > 0 else None),
                'locked_by_R':        ('YES' if STRATEGY == 'R'
                                       and w in baseline_open_wh else 'no'),
            })
        wh_df = pd.DataFrame(wh_rows).sort_values(
            ['open_scenario', 'warehouse_id'], ascending=[False, True])

        # Arc activations sheet
        arc_rows = []
        for a in sorted(A_fixed):
            activated  = round(prob.getSolution(arc_act[a]))
            total_flow = round(
                sum(prob.getSolution(x[(a, p)]) for p in PRODUCTS if (a, p) in x), 2)
            cap = arc_cap[a]
            was_active = 1 if a in BASELINE_ACTIVE_ARC_SET else 0
            if activated == 1 and was_active == 1:
                status_vs_bl = 'kept'
            elif activated == 1 and was_active == 0:
                status_vs_bl = 'newly_activated'
            elif activated == 0 and was_active == 1:
                status_vs_bl = 'deactivated'
            else:
                status_vs_bl = 'unused'
            if STRATEGY == 'R':
                cost_charged = 0.0
            elif STRATEGY == 'A':
                cost_charged = arc_fc[a] if (activated == 1 and was_active == 0) else 0.0
            else:
                cost_charged = arc_fc[a] if activated == 1 else 0.0
            arc_rows.append({
                'arc_id':              a,
                'activated_baseline':  was_active,
                'activated_scenario':  activated,
                'status_vs_baseline':  status_vs_bl,
                'source':              arc_src[a],
                'target':              arc_tgt[a],
                'total_flow':          total_flow,
                'capacity':            cap,
                'utilization_%':       (round(total_flow / cap * 100, 1)
                                        if activated and cap > 0 else None),
                'fixed_cost':          arc_fc[a],
                'cost_charged':        round(cost_charged, 2),
                'transport_mode':      arc_mode[a],
                'distance_km':         arc_dist[a],
                'canal_affected':      ('YES' if a in affected_arc_ids else 'no'),
                'locked_by_R':         ('YES' if STRATEGY == 'R'
                                        and a in baseline_active_arc else 'no'),
            })
        arc_df = pd.DataFrame(arc_rows).sort_values(
            ['activated_scenario', 'arc_id'], ascending=[False, True])

        # Summary sheet
        summary_rows = [
            ('=== Identification ===',                    ''),
            ('Scenario Key',                              SCENARIO_KEY),
            ('Scenario Label',                            scen_cfg['label']),
            ('Canal',                                     scen_cfg['canal'].capitalize()),
            ('Shock Type',                                scen_cfg['shock_type']),
            ('Cost Factor',                               cost_factor),
            ('Capacity Factor',                           cap_factor),
            ('Affected Canal Arcs',                       len(affected_arc_ids)),
            ('Strategy',                                  STRATEGY),
            ('Solve Time (s)',                            round(solve_time, 3)),
            ('Status',                                    str(status)),
            ('', ''),
            ('=== Cost breakdown (reported, comparable to Z*) ===', ''),
            ('Logistics Cost ($)',                        round(logistics_cost, 2)),
            ('  Sunk baseline WH cost ($)',               round(sunk_wh, 2)),
            ('  Sunk baseline arc cost ($)',              round(sunk_arcs, 2)),
            ('  New WH opening cost ($)',                 round(fixed_wh_charged, 2)),
            ('  New arc activation cost ($)',             round(fixed_arc_charged, 2)),
            ('  Variable transport cost ($)',             round(var_trans_cost, 2)),
            ('', ''),
            ('=== Optimisation objective (raw) ===',     ''),
            ('Optimisation Objective ($)',                round(optimisation_obj, 2)),
            ('  (R: variable only; A: new fixed+var; F: full fixed+var)', ''),
            ('', ''),
            ('=== Cost breakdown (full — if all fixed paid) ===', ''),
            ('Full WH cost (all open) ($)',               round(fixed_wh_full, 2)),
            ('Full arc cost (all active) ($)',            round(fixed_arc_full, 2)),
            ('Total fixed cost (full) ($)',               round(fixed_wh_full + fixed_arc_full, 2)),
            ('', ''),
            ('Objective (raw, $)',                        round(obj_val, 2)),
        ]
        if is_infeasible:
            summary_rows += [
                ('', ''),
                ('=== Penalty (unmet demand) ===',       ''),
                ('Penalty Cost ($)',                     round(penalty_cost, 2)),
                ('  Units Unserved',                     round(total_unmet_units, 1)),
                ('  Penalty per unit (M)',               PENALTY_M),
                ('Objective (logistics+penalty)',        round(obj_val, 2)),
            ]
        summary_rows += [
            ('', ''),
            ('=== Disruption vs Baseline ===',           ''),
            ('Z* Baseline Cost ($)',                      round(Z_STAR, 2) if Z_STAR > 0 else 'N/A'),
        ]
        if disruption_cost is not None:
            summary_rows += [
                ('DeltaZ Disruption Cost ($)',  round(disruption_cost, 2)),
                ('DeltaZ (%)',                  round(disruption_pct, 2)),
            ]
        if baseline_var_trans is not None:
            summary_rows += [
                ('Baseline variable cost (imputed) ($)', round(baseline_var_trans, 2)),
            ]
        if delta_var_R is not None:
            summary_rows += [
                ('Variable cost delta (R only) ($)', round(delta_var_R, 2)),
            ]
        summary_rows += [
            ('', ''),
            ('=== Network changes vs Baseline ===',      ''),
            ('Warehouses Open (baseline)',                len(BASELINE_OPEN_WH_SET)),
            ('Warehouses Open (scenario)',                len(open_wh_sol)),
            ('  WH kept',      f"{len(wh_kept)}: {wh_kept}"       if wh_kept       else '0'),
            ('  WH newly opened', f"{len(wh_opened_new)}: {wh_opened_new}" if wh_opened_new else '0'),
            ('  WH closed',    f"{len(wh_closed)}: {wh_closed}"   if wh_closed     else '0'),
            ('  New WH opening cost ($)',                 round(new_wh_cost_val, 2)),
            ('', ''),
            ('Optional arcs active (baseline)',           len(BASELINE_ACTIVE_ARC_SET)),
            ('Optional arcs active (scenario)',           len(active_arcs_sol)),
            ('  Arcs kept',                              len(arc_kept)),
            ('  Arcs newly activated',                   len(arc_activated_new)),
            ('  Arcs deactivated',                       len(arc_deactivated)),
            ('  New arc activation cost ($)',             round(new_arc_cost_val, 2)),
            ('', ''),
            ('=== Demand fulfilment ===',                ''),
        ]
        for p in PRODUCTS:
            delivered = sum(prob.getSolution(x[(a, p)])
                            for c in C for a in arcs_into.get(c, []) if (a, p) in x)
            unmet_p   = sum(prob.getSolution(unmet[(c, pp)])
                            for (c, pp) in unmet if pp == p) if unmet else 0.0
            total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
            val = (f'{delivered:.0f} / {total_dem:.0f}  (unmet: {unmet_p:.0f})'
                   if is_infeasible else f'{delivered:.0f} / {total_dem:.0f}')
            summary_rows.append((f'Demand Met - {p}', val))

        summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

        # Unmet demand sheet (R + capacity reduction only)
        unmet_rows = []
        if is_infeasible:
            for (c, p), d in Dem.items():
                u = round(prob.getSolution(unmet[(c, p)]), 2)
                unmet_rows.append({
                    'customer_id': c,
                    'product':     p,
                    'demand':      d,
                    'delivered':   round(d - u, 2),
                    'unmet':       u,
                    'fulfil_%':    round((1 - u / d) * 100, 1) if d > 0 else 100.0,
                })
        unmet_df = (pd.DataFrame(unmet_rows)
                    .sort_values(['product', 'unmet'], ascending=[True, False])
                    if unmet_rows else pd.DataFrame(unmet_rows))

        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            for p, df in product_dfs.items():
                df.to_excel(writer, sheet_name=p.replace('_', ' ')[:31], index=False)
            wh_df.to_excel(writer,  sheet_name='Warehouses',      index=False)
            arc_df.to_excel(writer, sheet_name='Arc Activations', index=False)
            if not unmet_df.empty:
                unmet_df.to_excel(writer, sheet_name='Unmet Demand', index=False)

        print(f"  Done -- {OUTPUT_FILE}")

        # ---- Append to master summary ----
        master_rows.append({
            'scenario_key':        SCENARIO_KEY,
            'scenario_label':      scen_cfg['label'],
            'canal':               scen_cfg['canal'].capitalize(),
            'shock_type':          scen_cfg['shock_type'],
            'cost_factor':         cost_factor,
            'capacity_factor':     cap_factor,
            'affected_arcs_count': len(affected_arc_ids),
            'strategy':            STRATEGY,
            'status':              str(status),
            'logistics_cost':      round(logistics_cost, 2),
            'optimisation_obj':    round(optimisation_obj, 2),
            'sunk_wh_cost':        round(sunk_wh, 2),
            'sunk_arc_cost':       round(sunk_arcs, 2),
            'new_wh_cost':         round(fixed_wh_charged, 2),
            'new_arc_cost':        round(fixed_arc_charged, 2),
            'var_trans_cost':      round(var_trans_cost, 2),
            'penalty_cost':        round(penalty_cost, 2),
            'unmet_units':         round(total_unmet_units, 1),
            'obj_val':             round(obj_val, 2),
            'disruption_cost':     round(disruption_cost, 2) if disruption_cost is not None else None,
            'disruption_pct':      round(disruption_pct, 2)  if disruption_pct  is not None else None,
            'open_wh':             len(open_wh_sol),
            'wh_newly_opened':     len(wh_opened_new),
            'wh_closed':           len(wh_closed),
            'active_arcs':         len(active_arcs_sol),
            'arcs_newly_activated':len(arc_activated_new),
            'arcs_deactivated':    len(arc_deactivated),
            'solve_time_s':        round(solve_time, 1),
        })

# =============================================================================
# 7. POST-SOLVE VALIDATION
# =============================================================================

print("\n" + "=" * 70)
print("  POST-SOLVE VALIDATION")
print("=" * 70)

validation_ok = True

for key in CANAL_SCENARIO_KEYS:
    cost_R = all_run_costs.get((key, 'R'))
    cost_A = all_run_costs.get((key, 'A'))
    cost_F = all_run_costs.get((key, 'F'))

    # Check Cost_F <= Cost_A (F is greenfield lower bound; A carries sunk burden)
    if cost_F is not None and cost_A is not None:
        if cost_A < cost_F - 1.0:
            print(f"  WARNING [{key}]: Cost_A ({cost_A:,.2f}) < Cost_F ({cost_F:,.2f}) "
                  f"by {cost_F - cost_A:,.2f}  — unexpected (A should be ≥ F).")
            validation_ok = False
        else:
            print(f"  [{key}] Cost_F ≤ Cost_A  OK  "
                  f"(F={cost_F:,.2f}, A={cost_A:,.2f}, diff={cost_A - cost_F:,.2f})")

    # Check Cost_R >= Cost_F (R cannot beat greenfield)
    if cost_R is not None and cost_F is not None:
        if cost_F > cost_R + 1.0:
            print(f"  WARNING [{key}]: Cost_F ({cost_F:,.2f}) > Cost_R ({cost_R:,.2f}) "
                  f"— greenfield should not exceed locked-network cost.")
            validation_ok = False
        else:
            print(f"  [{key}] Cost_R ≥ Cost_F  OK  "
                  f"(R={cost_R:,.2f}, F={cost_F:,.2f})")

if validation_ok:
    print("  All validation checks PASSED.")

# =============================================================================
# 8. MASTER SUMMARY
# =============================================================================

master_df   = pd.DataFrame(master_rows)
master_file = os.path.join(RESULTS_DIR, 'summary_canal_scenarios.xlsx')

# Wide comparison table: one row per scenario
wide_rows = []
for key in CANAL_SCENARIO_KEYS:
    block = master_df[master_df['scenario_key'] == key]
    if block.empty:
        continue
    first = block.iloc[0]
    row = {
        'Scenario':            key,
        'Canal':               first['canal'],
        'Shock_Type':          first['shock_type'],
        'Cost_Factor':         first['cost_factor'],
        'Capacity_Factor':     first['capacity_factor'],
        'Affected_Arcs_Count': first['affected_arcs_count'],
    }
    cost_by_strat: dict = {}
    for _, r in block.iterrows():
        st = r['strategy']
        row[f'Cost_{st}']     = r['logistics_cost']
        row[f'DeltaZ_{st}']   = r['disruption_cost']
        row[f'DeltaZ_%_{st}'] = r['disruption_pct']
        cost_by_strat[st]     = r['logistics_cost']

    # Flexibility value: Cost_R - Cost_A (savings from adapting vs. staying put)
    if all(cost_by_strat.get(s) is not None for s in ('R', 'A')):
        row['Flex_Value_R_minus_A'] = round(cost_by_strat['R'] - cost_by_strat['A'], 2)

    # Sunk burden: Cost_A - Cost_F (cost of being locked into baseline)
    if all(cost_by_strat.get(s) is not None for s in ('A', 'F')):
        row['Sunk_Cost_A_minus_F'] = round(cost_by_strat['A'] - cost_by_strat['F'], 2)

    # Best strategy (minimum reported logistics cost)
    valid = {s: v for s, v in cost_by_strat.items() if v is not None}
    if valid:
        row['Best_Strategy'] = min(valid, key=valid.get)

    wide_rows.append(row)

wide_df = pd.DataFrame(wide_rows)

# Reorder wide columns for clarity
wide_col_order = [
    'Scenario', 'Canal', 'Shock_Type', 'Cost_Factor', 'Capacity_Factor',
    'Affected_Arcs_Count',
    'Cost_R',    'Cost_A',    'Cost_F',
    'DeltaZ_R',  'DeltaZ_A',  'DeltaZ_F',
    'DeltaZ_%_R','DeltaZ_%_A','DeltaZ_%_F',
    'Flex_Value_R_minus_A', 'Sunk_Cost_A_minus_F', 'Best_Strategy',
]
wide_df = wide_df[[c for c in wide_col_order if c in wide_df.columns]]

with pd.ExcelWriter(master_file, engine='openpyxl') as writer:
    master_df.to_excel(writer, sheet_name='Detailed (long)',        index=False)
    wide_df.to_excel(writer,   sheet_name='Cost comparison (wide)', index=False)

print(f"\nMaster summary written -> {master_file}")
print("Canal scenario runs complete.")