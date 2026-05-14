"""
GlobalFlow Phase 2 — Complete Solver (CORRECTED)
==================================================
Solves all 6 scenarios x 3 strategies = 18 runs.

Scenarios
---------
  T1, T2, T3, S2   Feasible under the standard formulation.
  S1, S3            Remove hub H3 (Singapore), stranding suppliers S4-S6, S9.
                    Detected automatically at runtime via arc-reachability check.

Strategy semantics (per report Section 4.2 / 4.3)
-------------------------------------------------
  R (Rerouting only)
      Baseline infrastructure (W*, A*) held fixed; only flow is re-optimised.
      FIXED COSTS ARE SUNK -- not re-incurred in the optimisation objective,
      but ADDED BACK in the reported cost so the number is on the same scale
      as Z*, Cost_A, Cost_F.
      Infeasible scenarios (S1, S3): C1 relaxed to inequality; unmet demand
      penalised at M per unit (penalty excluded from logistics cost reporting).

  A (Adaptation)
      Warehouse opening and arc activation decisions are freed.
      ONLY NEW openings/activations are charged a fixed cost; baseline
      facilities/arcs that are kept are sunk (no charge); baseline facilities
      that are dropped get no cost recovery either.
      Infeasible scenarios (S1, S3): same C1 relaxation + penalty.

  F (Full redesign)
      Greenfield re-solve: ALL fixed costs paid from scratch, ignoring sunk.
      Lower bound on achievable cost.
      Infeasible scenarios (S1, S3): emergency supply arcs injected from each
      stranded supplier to its geographically closest remaining hub, at a
      premium cost (gamma * cost-per-km * estimated rerouting distance).

Cost model
----------
  total_cost[a,p] = baseline_var[a,p] * scenario_factor[a,p] * (1 + tariff[zone_from, zone_to])

Outputs
-------
  phase2/results/scenario_ArcCosts_{KEY}/strategy_{STRATEGY}.xlsx   (per run)
  phase2/results/summary_all_scenarios.xlsx                          (master)

Each per-run workbook includes:
  - Summary sheet with full cost breakdown AND baseline comparison
    (which warehouses opened/closed vs baseline, new activation costs,
     variable cost delta, total disruption cost DeltaZ)
  - Per-product flow sheets
  - Warehouses sheet (open status + comparison vs baseline)
  - Arc Activations sheet (activated status + comparison vs baseline)
  - Unmet Demand sheet (R, A on S1/S3 only)
  - Emergency Arcs sheet (F on S1/S3 only)
"""

import os
import pandas as pd
import xpress as xp
import time

xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')

# =============================================================================
# CONFIGURATION
# =============================================================================

_HERE = os.path.dirname(os.path.abspath(__file__))   # phase2/
_ROOT = os.path.dirname(_HERE)                        # project root
_CWD  = os.getcwd()                                   # wherever the user launched python

# Candidate paths for the baseline solution (first found wins).
# Order matters: most likely first.
_BASELINE_CANDIDATES = [
    os.path.join(_CWD,  'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_ROOT, 'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_HERE, 'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_HERE, '..', 'phase1', 'results', 'baseline_solution.xlsx'),
    os.path.join(_CWD,  'baseline_solution.xlsx'),
]
BASELINE_SOLUTION_FILE = next((p for p in _BASELINE_CANDIDATES if os.path.exists(p)), None)

# Candidate paths for the instance Excel file
_EXCEL_CANDIDATES = [
    os.path.join(_CWD,  'data', 'globalflow_instance.xlsx'),
    os.path.join(_ROOT, 'data', 'globalflow_instance.xlsx'),
    os.path.join(_HERE, 'data', 'globalflow_instance.xlsx'),
    os.path.join(_HERE, '..', 'data', 'globalflow_instance.xlsx'),
    os.path.join(_CWD,  'globalflow_instance.xlsx'),
]
EXCEL_FILE = next((p for p in _EXCEL_CANDIDATES if os.path.exists(p)), None)

# Results dir: relative to the launching CWD (so behaviour matches what the user expects)
RESULTS_DIR = os.path.join(_CWD, 'phase2', 'results')

ALL_SCENARIO_KEYS = ['T1', 'T2', 'T3', 'S1', 'S2', 'S3']
STRATEGIES        = ['R', 'A', 'F']

PRODUCTS          = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME    = 300        # seconds per solve
PENALTY_M         = 1000000  # cost per unit of unmet demand (R and A on infeasible)
EMERGENCY_PREMIUM = 2.5        # gamma: spot-market procurement premium (F infeasible)
EMG_TOP_K         = 1          # connect each stranded supplier to top-K closest hubs

os.makedirs(RESULTS_DIR, exist_ok=True)

# =============================================================================
# ONE-TIME DATA LOADS
# =============================================================================

print("Loading shared data...")

# ----- Baseline decisions and Z* -----
print(f"  CWD              : {_CWD}")
print(f"  Script dir       : {_HERE}")
print(f"  Looking for baseline solution + data file...")

if EXCEL_FILE is None:
    raise FileNotFoundError(
        f"\n\nInstance Excel file NOT FOUND. Tried:\n  - " +
        "\n  - ".join(_EXCEL_CANDIDATES) +
        f"\n\nEither move/copy globalflow_instance.xlsx to one of those paths,\n"
        f"or edit the EXCEL_FILE / _EXCEL_CANDIDATES block at the top of this script.\n"
    )
print(f"  Instance file    : {EXCEL_FILE}")

if BASELINE_SOLUTION_FILE is None:
    raise FileNotFoundError(
        f"\n\nBaseline solution file NOT FOUND. Tried:\n  - " +
        "\n  - ".join(_BASELINE_CANDIDATES) +
        f"\n\nPhase 2 strategies R and A REQUIRE the Phase 1 baseline (W*, A*, Z*).\n"
        f"Either move/copy baseline_solution.xlsx to one of those paths,\n"
        f"or edit the BASELINE_SOLUTION_FILE / _BASELINE_CANDIDATES block at the top of this script.\n"
    )
print(f"  Baseline file    : {BASELINE_SOLUTION_FILE}")
print(f"  Results dir      : {RESULTS_DIR}")

_bl_wh  = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Warehouses')
_bl_arc = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Arc Activations')
_bl_sum = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Summary')

# Validate expected columns
for col in ('warehouse_id', 'open'):
    if col not in _bl_wh.columns:
        raise ValueError(
            f"Baseline 'Warehouses' sheet missing column '{col}'. "
            f"Found columns: {list(_bl_wh.columns)}"
        )
for col in ('arc_id', 'activated'):
    if col not in _bl_arc.columns:
        raise ValueError(
            f"Baseline 'Arc Activations' sheet missing column '{col}'. "
            f"Found columns: {list(_bl_arc.columns)}"
        )

# Drop "tail" rows (totals, footers, blanks) that have NaN in the key columns.
# These are common in human-edited Excel exports.
_n_wh_before  = len(_bl_wh)
_n_arc_before = len(_bl_arc)
_bl_wh  = _bl_wh.dropna(subset=['warehouse_id', 'open']).copy()
_bl_arc = _bl_arc.dropna(subset=['arc_id', 'activated']).copy()
if len(_bl_wh) < _n_wh_before:
    print(f"  Note: dropped {_n_wh_before - len(_bl_wh)} blank/footer row(s) from Warehouses sheet")
if len(_bl_arc) < _n_arc_before:
    print(f"  Note: dropped {_n_arc_before - len(_bl_arc)} blank/footer row(s) from Arc Activations sheet")

baseline_open_wh    = dict(zip(_bl_wh['warehouse_id'],  _bl_wh['open'].astype(int)))
baseline_active_arc = dict(zip(_bl_arc['arc_id'],       _bl_arc['activated'].astype(int)))

# Sets of baseline-open warehouses and baseline-active arcs
BASELINE_OPEN_WH_SET    = {w for w, v in baseline_open_wh.items()    if v == 1}
BASELINE_ACTIVE_ARC_SET = {a for a, v in baseline_active_arc.items() if v == 1}

_cost_row = _bl_sum[_bl_sum['Metric'] == 'Total Cost ($)']['Value']
Z_STAR = float(_cost_row.iloc[0]) if not _cost_row.empty else 0.0

if not BASELINE_OPEN_WH_SET:
    raise ValueError(
        f"Baseline has ZERO open warehouses -- this can't be right.\n"
        f"  Check {BASELINE_SOLUTION_FILE} -> Warehouses sheet -> 'open' column.\n"
        f"  Values found: {sorted(set(_bl_wh['open']))}"
    )
if Z_STAR <= 0:
    print(f"  WARNING: Z* baseline cost is 0 or missing. "
          f"DeltaZ disruption costs will not be computed.")

print(f"  Baseline loaded  : {len(BASELINE_OPEN_WH_SET)} open WH "
      f"{sorted(BASELINE_OPEN_WH_SET)}")
print(f"                     {len(BASELINE_ACTIVE_ARC_SET)} active arcs")
print(f"                     Z* = ${Z_STAR:,.2f}")

# ----- Baseline variable costs and tariffs (shared across scenarios) -----
baseline_costs_df = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')
tariffs_df        = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')

baseline_var_cost = {(r['arc_id'], r['product']): r['variable_cost']
                     for _, r in baseline_costs_df.iterrows()}
tariff_lookup     = {(r['zone_pair_from'], r['zone_pair_to']): r['interzonal_tariff_rate']
                     for _, r in tariffs_df.iterrows()}

# ----- Static sheets shared across all scenarios -----
nodes_df_all      = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
arcs_df_all       = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
warehouses_df     = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
suppliers_df      = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
demand_df         = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
supply_df         = pd.read_excel(EXCEL_FILE, sheet_name='Supply')

arc_zone_from_all = dict(zip(arcs_df_all['arc_id'], arcs_df_all['zone_from']))
arc_zone_to_all   = dict(zip(arcs_df_all['arc_id'], arcs_df_all['zone_to']))

# Warehouse cost lookup (for full set, used in baseline-comparison reporting)
WH_COST_ALL = {row['warehouse_id']: row['opening_cost']
               for _, row in warehouses_df.iterrows()}
# Arc fixed-cost lookup (for full set)
ARC_FC_ALL  = {row['arc_id']: row['fixed_activation_cost']
               for _, row in arcs_df_all.iterrows()}

# Hub-to-hub distance lookup (used for emergency arc cost estimation)
_all_hubs = set(nodes_df_all[nodes_df_all['type'] == 'HUB']['node_id'])
hub_hub_dist: dict = {}   # (from_hub, to_hub) -> distance_km
for _, _r in arcs_df_all.iterrows():
    if _r['from_id'] in _all_hubs and _r['to_id'] in _all_hubs:
        hub_hub_dist[(_r['from_id'], _r['to_id'])] = _r['distance_km']

# ----- Baseline total cost decomposition (for accurate sunk-cost accounting) ---
# Sunk warehouse cost: sum of opening costs of baseline-open warehouses
SUNK_BASELINE_WH_COST  = sum(WH_COST_ALL.get(w, 0) for w in BASELINE_OPEN_WH_SET)
# Sunk arc activation cost: sum of fixed activation costs of baseline-active arcs
SUNK_BASELINE_ARC_COST = sum(ARC_FC_ALL.get(a, 0)  for a in BASELINE_ACTIVE_ARC_SET)

print(f"  Sunk baseline WH cost  : ${SUNK_BASELINE_WH_COST:,.2f}")
print(f"  Sunk baseline arc cost : ${SUNK_BASELINE_ARC_COST:,.2f}")

# Master results accumulator
master_rows = []

# =============================================================================
# MAIN LOOP
# =============================================================================

for SCENARIO_KEY in ALL_SCENARIO_KEYS:
    for STRATEGY in STRATEGIES:

        SCENARIO    = f'ArcCosts_{SCENARIO_KEY}'
        OUTPUT_DIR  = os.path.join(RESULTS_DIR, f'scenario_ArcCosts_{SCENARIO_KEY}')
        OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'strategy_{STRATEGY}.xlsx')
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        print("\n" + "=" * 70)
        print(f"  Scenario: {SCENARIO_KEY}   Strategy: {STRATEGY}")
        print("=" * 70)

        # ---------------------------------------------------------------------
        # 1. Load scenario-specific sheets
        # ---------------------------------------------------------------------
        costs_df = pd.read_excel(EXCEL_FILE, sheet_name=SCENARIO)

        try:
            removed_nodes_df = pd.read_excel(EXCEL_FILE, sheet_name=f'NodesRemoved_{SCENARIO_KEY}')
            removed_node_ids = set(removed_nodes_df['node_id'])
        except Exception:
            removed_node_ids = set()

        try:
            removed_arcs_df = pd.read_excel(EXCEL_FILE, sheet_name=f'ArcsRemoved_{SCENARIO_KEY}')
            removed_arc_ids = set(removed_arcs_df['arc_id'])
        except Exception:
            removed_arc_ids = set()

        if removed_node_ids:
            print(f"  Removed nodes : {sorted(removed_node_ids)}")
        if removed_arc_ids:
            print(f"  Removed arcs  : {len(removed_arc_ids)}")

        # Active arcs/nodes after removals
        arcs_df      = arcs_df_all[~arcs_df_all['arc_id'].isin(removed_arc_ids)].copy()
        nodes_active = nodes_df_all[~nodes_df_all['node_id'].isin(removed_node_ids)].copy()

        # ---------------------------------------------------------------------
        # 2. Sets
        # ---------------------------------------------------------------------
        S = set(suppliers_df['supplier_id'])  - removed_node_ids
        H = set(nodes_active[nodes_active['type'] == 'HUB']['node_id'])
        W = set(warehouses_df['warehouse_id']) - removed_node_ids
        C = set(demand_df['customer_id'].unique()) - removed_node_ids

        S_p = {}
        supplier_prods = {}
        for _, row in supply_df.iterrows():
            if row['supplier_id'] in removed_node_ids:
                continue
            S_p.setdefault(row['product'], set()).add(row['supplier_id'])
            supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])

        # ---------------------------------------------------------------------
        # 3. Arc lookups
        # ---------------------------------------------------------------------
        arc_src = {}; arc_tgt = {}; arc_cap = {}; arc_fc = {}
        arc_mode = {}; arc_dist = {}

        all_node_ids = set(nodes_df_all['node_id'])
        arcs_from = {n: set() for n in all_node_ids}
        arcs_into = {n: set() for n in all_node_ids}

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

        A_fixed  = {a for a in arc_src if arc_fc[a] > 0}   # optional arcs (need activation)
        A_always = {a for a in arc_src if arc_fc[a] == 0}  # always-on arcs

        # ---------------------------------------------------------------------
        # 4. Parameters
        # ---------------------------------------------------------------------
        Dem = {(row['customer_id'], row['product']): row['demand']
               for _, row in demand_df.iterrows()
               if row['customer_id'] not in removed_node_ids}
        Sup = {(row['supplier_id'], row['product']): row['supply']
               for _, row in supply_df.iterrows()
               if row['supplier_id'] not in removed_node_ids}
        wh_cap  = {row['warehouse_id']: row['capacity']
                   for _, row in warehouses_df.iterrows()
                   if row['warehouse_id'] not in removed_node_ids}
        wh_cost = {row['warehouse_id']: row['opening_cost']
                   for _, row in warehouses_df.iterrows()
                   if row['warehouse_id'] not in removed_node_ids}

        # ---------------------------------------------------------------------
        # 5. Tariff-aware variable cost: total_cost[a,p] = bv * factor * (1 + tariff)
        # ---------------------------------------------------------------------
        scen_var = {(row['arc_id'], row['product']): row['variable_cost']
                    for _, row in costs_df.iterrows()
                    if row['arc_id'] not in removed_arc_ids}

        total_cost = {}
        for (a, p), bv in baseline_var_cost.items():
            if a in removed_arc_ids or a not in arc_src:
                continue
            sv     = scen_var.get((a, p), bv)
            factor = sv / bv if bv > 0 else 1.0
            tariff = tariff_lookup.get(
                (arc_zone_from_all.get(a, ''), arc_zone_to_all.get(a, '')), 0.0)
            total_cost[(a, p)] = bv * factor * (1.0 + tariff)

        print(f"  Active hubs : {sorted(H)}")
        print(f"  Arcs        : {len(arc_src)} total  "
              f"({len(A_fixed)} optional, {len(A_always)} always-on)")
        print(f"  Cost pairs  : {len(total_cost)}")

        # ---------------------------------------------------------------------
        # 6. Detect stranded suppliers (structural infeasibility)
        # ---------------------------------------------------------------------
        stranded = {}
        for s in S:
            s_arcs_full   = set(arcs_df_all[arcs_df_all['from_id'] == s]['arc_id'])
            s_arcs_active = set(arcs_df[arcs_df['from_id'] == s]['arc_id'])
            if s_arcs_full and not s_arcs_active:
                stranded[s] = supplier_prods.get(s, set())

        is_infeasible = bool(stranded)

        if is_infeasible:
            print(f"\n  [INFEASIBLE] Stranded suppliers detected: {sorted(stranded.keys())}")
            for s, prods in stranded.items():
                for p in prods:
                    mask = (supply_df['supplier_id'] == s) & (supply_df['product'] == p)
                    if mask.any():
                        qty = supply_df.loc[mask, 'supply'].values[0]
                        print(f"    {s} - {p}: {qty} units stranded")

        # ---------------------------------------------------------------------
        # 7. Emergency arcs for Strategy F on infeasible scenarios
        # ---------------------------------------------------------------------
        emergency_arcs = {}
        emg_var_cost   = {}

        if STRATEGY == 'F' and is_infeasible:
            print(f"  [F] Injecting geographic emergency arcs "
                  f"(x{EMERGENCY_PREMIUM} premium, top-{EMG_TOP_K} closest hub(s))...")

            for s, prods in stranded.items():
                s_orig_rows = arcs_df_all[arcs_df_all['from_id'] == s]

                # Cost-per-km from the supplier's original baseline arc(s)
                cost_per_km: dict = {}
                for _, orig_row in s_orig_rows.iterrows():
                    d = orig_row['distance_km']
                    if d <= 0:
                        continue
                    for p in prods:
                        bv = baseline_var_cost.get((orig_row['arc_id'], p))
                        if bv is not None and p not in cost_per_km:
                            cost_per_km[p] = bv / d   # $/unit/km

                # Estimate s -> h distance for every active hub
                hub_dist_est: dict = {}
                for h in H:
                    direct = s_orig_rows[s_orig_rows['to_id'] == h]['distance_km']
                    if not direct.empty:
                        hub_dist_est[h] = direct.iloc[0]
                        continue
                    best = None
                    for _, orig_row in s_orig_rows.iterrows():
                        via = orig_row['to_id']
                        if via not in removed_node_ids:
                            continue
                        d_via_h = hub_hub_dist.get((via, h))
                        if d_via_h is None:
                            continue
                        est = orig_row['distance_km'] + d_via_h
                        if best is None or est < best:
                            best = est
                    if best is not None:
                        hub_dist_est[h] = best

                # Fallback if no distance estimates
                if not hub_dist_est:
                    s_arc_ids = set(s_orig_rows['arc_id'])
                    base_costs = [v for (a, p), v in baseline_var_cost.items()
                                  if a in s_arc_ids and p in prods]
                    fb_cost = round(EMERGENCY_PREMIUM * (min(base_costs) if base_costs else 500.0), 4)
                    print(f"    WARNING: {s} - no distance estimates, "
                          f"falling back to uniform cost {fb_cost:.2f} for all hubs")
                    for h in sorted(H):
                        eid = f'EMG_{s}_{h}'
                        arc_src[eid]  = s;  arc_tgt[eid]  = h
                        arc_cap[eid]  = 99999; arc_fc[eid] = 0
                        arc_mode[eid] = 'emergency'; arc_dist[eid] = 0
                        arcs_from[s].add(eid); arcs_into[h].add(eid)
                        A_always.add(eid); emergency_arcs[eid] = True
                        for p in prods:
                            emg_var_cost[(eid, p)] = fb_cost
                            total_cost[(eid, p)]   = fb_cost
                    continue

                # Select the EMG_TOP_K closest hubs
                chosen = sorted(hub_dist_est.items(), key=lambda kv: kv[1])[:EMG_TOP_K]

                for h, est_km in chosen:
                    eid = f'EMG_{s}_{h}'
                    arc_src[eid]  = s
                    arc_tgt[eid]  = h
                    arc_cap[eid]  = 99999
                    arc_fc[eid]   = 0
                    arc_mode[eid] = 'emergency'
                    arc_dist[eid] = round(est_km, 1)
                    arcs_from[s].add(eid)
                    arcs_into[h].add(eid)
                    A_always.add(eid)
                    emergency_arcs[eid] = True

                    cost_parts = []
                    for p in prods:
                        cpk = cost_per_km.get(p)
                        if cpk is not None:
                            emg_uc = round(EMERGENCY_PREMIUM * cpk * est_km, 4)
                        else:
                            s_arc_ids = set(s_orig_rows['arc_id'])
                            base_p = [v for (a, pp), v in baseline_var_cost.items()
                                      if a in s_arc_ids and pp == p]
                            emg_uc = round(EMERGENCY_PREMIUM * (min(base_p) if base_p else 500.0), 4)
                        emg_var_cost[(eid, p)] = emg_uc
                        total_cost[(eid, p)]   = emg_uc
                        cost_parts.append(f"{p.split('_')[0]}={emg_uc:.2f}")

                    print(f"    {s} ({sorted(prods)}) -> {h}  "
                          f"est_dist={est_km:.0f} km  "
                          f"unit_costs=[{', '.join(cost_parts)}]")

        # ---------------------------------------------------------------------
        # 8. Decision variables
        # ---------------------------------------------------------------------
        prob = xp.problem()
        prob.setControl('MAXTIME', MAX_SOLVE_TIME)
        prob.setControl('OUTPUTLOG', 0)
        prob.setControl('MIPRELSTOP', 1e-7)

        # Flow variables
        x = {}
        for (a, p) in total_cost:
            src = arc_src[a]
            if src in S and p not in supplier_prods.get(src, set()):
                continue
            x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

        # Warehouse opening (locked to baseline for strategy R)
        openWarehouse = {}
        for w in W:
            if STRATEGY == 'R' and w in baseline_open_wh:
                v = float(baseline_open_wh[w])
                openWarehouse[w] = prob.addVariable(name=f'open_{w}', lb=v, ub=v,
                                                    vartype=xp.continuous)
            else:
                openWarehouse[w] = prob.addVariable(name=f'open_{w}', vartype=xp.binary)

        # Arc activation (locked to baseline for strategy R)
        arc_act = {}
        for a in A_fixed:
            if STRATEGY == 'R' and a in baseline_active_arc:
                v = float(baseline_active_arc[a])
                arc_act[a] = prob.addVariable(name=f'arc_{a}', lb=v, ub=v,
                                              vartype=xp.continuous)
            else:
                arc_act[a] = prob.addVariable(name=f'arc_{a}', vartype=xp.binary)

        # Unmet demand slack (infeasible scenarios, R and A only)
        unmet = {}
        if is_infeasible and STRATEGY in ('R', 'A'):
            for (c, p), d in Dem.items():
                unmet[(c, p)] = prob.addVariable(name=f'unmet_{c}_{p}', lb=0, ub=d,
                                                 vartype=xp.continuous)

        print(f"  Variables: {len(x)} flow, {len(openWarehouse)} WH, "
              f"{len(arc_act)} arc_act, {len(unmet)} unmet")

        # ---------------------------------------------------------------------
        # 9. Objective function -- strategy-specific cost accounting
        # ---------------------------------------------------------------------
        #
        # R: variable costs only (sunk fixed costs are constant, dropped from
        #    optimisation and ADDED BACK at reporting time).
        # A: variable costs + fixed costs ONLY for NEW openings / activations
        #    (baseline ones are sunk).
        # F: variable costs + ALL fixed costs from scratch (greenfield).
        # ---------------------------------------------------------------------

        # Live baseline sets intersected with feasible W / A_fixed
        # (a scenario may have removed some baseline warehouses or arcs)
        baseline_wh_alive   = BASELINE_OPEN_WH_SET    & W
        baseline_arcs_alive = BASELINE_ACTIVE_ARC_SET & A_fixed
        new_wh_set          = W       - baseline_wh_alive    # candidates for new openings (A)
        new_arcs_set        = A_fixed - baseline_arcs_alive  # candidates for new activations (A)

        if STRATEGY == 'R':
            # Variable cost only -- fixed costs are sunk and constant.
            obj  = xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

        elif STRATEGY == 'A':
            # Variable cost + fixed cost only for NEW decisions.
            obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in new_wh_set)
            obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in new_arcs_set)
            obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

        else:  # STRATEGY == 'F'
            # Full greenfield: pay ALL fixed costs.
            obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)
            obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in A_fixed)
            obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

        # Penalty for unmet demand (infeasible scenarios, R and A)
        if is_infeasible and STRATEGY in ('R', 'A'):
            obj += xp.Sum(PENALTY_M * unmet[(c, p)] for (c, p) in unmet)

        prob.setObjective(obj, sense=xp.minimize)

        # ---------------------------------------------------------------------
        # 10. Constraints
        # ---------------------------------------------------------------------
        def inflow(node, product):
            return xp.Sum(x[(a, product)] for a in arcs_into.get(node, []) if (a, product) in x)

        def outflow(node, product):
            return xp.Sum(x[(a, product)] for a in arcs_from.get(node, []) if (a, product) in x)

        # C1 -- demand satisfaction
        for (c, p), d in Dem.items():
            if is_infeasible and STRATEGY in ('R', 'A'):
                prob.addConstraint(xp.constraint(inflow(c, p) + unmet[(c, p)] == d,
                                                 name=f'C1_{c}_{p}'))
            else:
                prob.addConstraint(xp.constraint(inflow(c, p) == d,
                                                 name=f'C1_{c}_{p}'))

        # C2 -- supply capacity
        for p, sup_set in S_p.items():
            for s in sup_set:
                if (s, p) in Sup:
                    prob.addConstraint(xp.constraint(outflow(s, p) <= Sup[(s, p)],
                                                     name=f'C2_{s}_{p}'))

        # C3 -- arc capacity (always-on)
        for a in A_always:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))

        # C4 -- arc capacity (optional, gated by activation)
        for a in A_fixed:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a], name=f'C4_{a}'))

        # C5 -- warehouse capacity (gated by opening)
        for w in W:
            total_in = xp.Sum(x[(a, p)] for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(total_in <= wh_cap[w] * openWarehouse[w],
                                             name=f'C5_{w}'))

        # C6 -- flow conservation at warehouses
        for w in W:
            for p in PRODUCTS:
                prob.addConstraint(xp.constraint(inflow(w, p) == outflow(w, p),
                                                 name=f'C6_{w}_{p}'))

        # C7 -- flow conservation at hubs
        for h in H:
            for p in PRODUCTS:
                prob.addConstraint(xp.constraint(inflow(h, p) == outflow(h, p),
                                                 name=f'C7_{h}_{p}'))

        # ---------------------------------------------------------------------
        # 11. Solve
        # ---------------------------------------------------------------------
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
                'infeasible_scenario': 'YES' if is_infeasible else 'no',
                'status': str(status),
                'logistics_cost': None, 'penalty_cost': None, 'obj_val': None,
                'disruption_cost': None, 'disruption_pct': None,
                'unmet_units': None, 'open_wh': None, 'active_arcs': None,
                'solve_time_s': round(solve_time, 1),
            })
            continue

        # ---------------------------------------------------------------------
        # 12. Cost extraction and reconciliation
        # ---------------------------------------------------------------------
        obj_val = prob.getObjVal()

        # Identify open/active in the solution
        open_wh_sol     = {w for w in W      if round(prob.getSolution(openWarehouse[w])) == 1}
        active_arcs_sol = {a for a in A_fixed if round(prob.getSolution(arc_act[a]))       == 1}

        # Variable transport cost: always Sum(total_cost * x), independent of strategy
        var_trans_cost = sum(total_cost[(a, p)] * prob.getSolution(x[(a, p)])
                             for (a, p) in x)

        # Penalty (only for R/A on infeasible)
        if is_infeasible and STRATEGY in ('R', 'A'):
            total_unmet_units = sum(prob.getSolution(unmet[(c, p)]) for (c, p) in unmet)
            penalty_cost      = PENALTY_M * total_unmet_units
        else:
            total_unmet_units = 0.0
            penalty_cost      = 0.0

        # ---- Fixed cost accounting (the heart of the corrected methodology) ----

        # Full fixed cost actually incurred at solution
        fixed_wh_full    = sum(wh_cost[w] for w in open_wh_sol)
        fixed_arc_full   = sum(arc_fc[a]  for a in active_arcs_sol)

        # Fixed costs charged in the optimisation objective (depends on strategy)
        if STRATEGY == 'R':
            fixed_wh_charged   = 0.0
            fixed_arc_charged  = 0.0
            # Sunk costs are the baseline ones that survived (i.e., still in W/A_fixed)
            sunk_wh   = sum(wh_cost[w] for w in (BASELINE_OPEN_WH_SET    & W))
            sunk_arcs = sum(arc_fc[a]  for a in (BASELINE_ACTIVE_ARC_SET & A_fixed))
        elif STRATEGY == 'A':
            fixed_wh_charged   = sum(wh_cost[w] for w in (open_wh_sol & new_wh_set))
            fixed_arc_charged  = sum(arc_fc[a]  for a in (active_arcs_sol & new_arcs_set))
            # Sunk: baseline-open that survived scenario AND are kept open in the new solution
            sunk_wh   = sum(wh_cost[w] for w in (open_wh_sol & baseline_wh_alive))
            sunk_arcs = sum(arc_fc[a]  for a in (active_arcs_sol & baseline_arcs_alive))
        else:  # F
            fixed_wh_charged   = fixed_wh_full
            fixed_arc_charged  = fixed_arc_full
            sunk_wh   = 0.0
            sunk_arcs = 0.0

        # ---------------------------------------------------------------------
        # REPORTED COSTS -- two views
        # ---------------------------------------------------------------------
        # (a) logistics_cost  : "comparable to Z*" view -- always includes
        #     ALL fixed costs that are economically incurred + variable cost.
        #     This is what makes Z(R), Z(A), Z(F), Z* comparable on one scale.
        #
        #     R: sunk (baseline still alive) + variable          -> comparable to Z*
        #     A: sunk (baseline kept) + new openings/activations + variable
        #        Note: warehouses/arcs in baseline that the scenario REMOVED
        #              are NOT in sunk_wh / sunk_arcs (we lost them).
        #     F: full (all open/active in solution) + variable   -> greenfield, comparable to Z*
        #
        # (b) optimisation_obj : raw value of the optimisation objective
        #     (what solve_Tx_S2.py historically reports for A and F).
        #     For A this excludes sunk costs from the baseline; for R it
        #     excludes ALL fixed costs (variable only). Useful for understanding
        #     the marginal cost of adaptation, NOT for comparing to Z*.
        # ---------------------------------------------------------------------

        if STRATEGY == 'R':
            logistics_cost   = sunk_wh + sunk_arcs + var_trans_cost
            optimisation_obj = var_trans_cost            # matches solve_Tx_S2.py R objective
        elif STRATEGY == 'A':
            logistics_cost   = sunk_wh + sunk_arcs + fixed_wh_charged + fixed_arc_charged + var_trans_cost
            optimisation_obj = fixed_wh_charged + fixed_arc_charged + var_trans_cost  # matches solve_Tx_S2.py A objective
        else:  # F
            logistics_cost   = fixed_wh_full + fixed_arc_full + var_trans_cost
            optimisation_obj = logistics_cost            # F objective == logistics cost (full greenfield)

        # Disruption cost vs baseline
        disruption_cost = logistics_cost - Z_STAR if Z_STAR > 0 else None
        disruption_pct  = (disruption_cost / Z_STAR * 100) if (disruption_cost is not None and Z_STAR > 0) else None

        # Baseline variable transport (for comparing "what would the same network
        # have cost without the scenario shock"). Note: this requires the baseline
        # solution's flows, which we don't have here -- so we report only the
        # CURRENT variable cost and the CURRENT fixed costs vs baseline-imputed.
        # Delta_var_R: how much more (or less) the SAME network costs to route under shock.
        # This is meaningful for R only (network unchanged).
        if STRATEGY == 'R':
            # Approximate baseline variable cost = Z* - SUNK_BASELINE_WH_COST - SUNK_BASELINE_ARC_COST
            baseline_var_trans = Z_STAR - SUNK_BASELINE_WH_COST - SUNK_BASELINE_ARC_COST if Z_STAR > 0 else None
            delta_var_R = var_trans_cost - baseline_var_trans if baseline_var_trans is not None else None
        else:
            baseline_var_trans = None
            delta_var_R = None

        # Warehouse changes vs baseline
        wh_opened_new   = sorted(open_wh_sol - BASELINE_OPEN_WH_SET)        # added
        wh_closed       = sorted(BASELINE_OPEN_WH_SET - open_wh_sol)        # removed (or stranded)
        wh_kept         = sorted(open_wh_sol & BASELINE_OPEN_WH_SET)        # unchanged
        # Arc changes vs baseline
        arc_activated_new = sorted(active_arcs_sol - BASELINE_ACTIVE_ARC_SET)
        arc_deactivated   = sorted(BASELINE_ACTIVE_ARC_SET - active_arcs_sol)
        arc_kept          = sorted(active_arcs_sol & BASELINE_ACTIVE_ARC_SET)

        # Cost of newly opened warehouses (full opening cost, charged only by A and F)
        new_wh_cost  = sum(wh_cost.get(w, 0) for w in wh_opened_new)
        # Cost of newly activated arcs
        new_arc_cost = sum(arc_fc.get(a, 0)  for a in arc_activated_new)

        # ---- Print summary ----
        print(f"  Warehouse opening costs (charged) : ${fixed_wh_charged:>14,.2f}")
        print(f"  Warehouse opening costs (full)    : ${fixed_wh_full:>14,.2f}")
        print(f"  Arc activation costs (charged)    : ${fixed_arc_charged:>14,.2f}")
        print(f"  Arc activation costs (full)       : ${fixed_arc_full:>14,.2f}")
        print(f"  Variable transport cost           : ${var_trans_cost:>14,.2f}")
        print(f"  ----------------------------------")
        print(f"  Optimisation objective (raw)      : ${optimisation_obj:>14,.2f}")
        print(f"  Logistics total (vs Z*)           : ${logistics_cost:>14,.2f}")
        if is_infeasible and STRATEGY in ('R', 'A'):
            print(f"  Penalty (unmet demand)            : ${penalty_cost:>14,.2f}"
                  f"  ({total_unmet_units:.1f} units x M={PENALTY_M:,})")
        print(f"  OBJECTIVE (xpress, with penalty)  : ${obj_val:>14,.2f}")
        if disruption_cost is not None:
            print(f"  DeltaZ (vs Z*=${Z_STAR:,.2f})       : ${disruption_cost:>+14,.2f}"
                  f"  ({disruption_pct:+.1f}%)")
        if delta_var_R is not None:
            print(f"  Variable cost delta vs baseline   : ${delta_var_R:>+14,.2f}")

        print(f"  Open warehouses ({len(open_wh_sol)}/{len(W)})       : {sorted(open_wh_sol)}")
        print(f"  Active opt. arcs ({len(active_arcs_sol)}/{len(A_fixed)})    : {len(active_arcs_sol)} arcs")
        if wh_opened_new:
            print(f"  WH opened vs baseline    : {wh_opened_new}  (+${new_wh_cost:,.2f})")
        if wh_closed:
            print(f"  WH closed vs baseline    : {wh_closed}")
        if arc_activated_new:
            print(f"  Arcs activated vs baseline : {len(arc_activated_new)} arcs  (+${new_arc_cost:,.2f})")
        if arc_deactivated:
            print(f"  Arcs deactivated vs baseline: {len(arc_deactivated)} arcs")

        print("  Demand fulfilment:")
        for p in PRODUCTS:
            delivered = sum(prob.getSolution(x[(a, p)])
                            for c in C for a in arcs_into.get(c, []) if (a, p) in x)
            unmet_p   = sum(prob.getSolution(unmet[(c, pp)])
                            for (c, pp) in unmet if pp == p) if unmet else 0.0
            total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
            print(f"    {p:35s}: {delivered:>7.1f} / {total_dem:.0f}"
                  f"  unmet={unmet_p:.1f}  ({100*delivered/total_dem:.1f}%)")

        if STRATEGY == 'F' and emergency_arcs:
            print("  Emergency arc usage:")
            for eid in sorted(emergency_arcs):
                for p in PRODUCTS:
                    if (eid, p) in x:
                        fv = round(prob.getSolution(x[(eid, p)]), 2)
                        if fv > 0.01:
                            print(f"    {eid}  {p}  flow={fv:.1f}"
                                  f"  unit={emg_var_cost.get((eid,p),0):.4f}")

        # ---------------------------------------------------------------------
        # 13. Export to Excel
        # ---------------------------------------------------------------------
        print(f"  Exporting -> {OUTPUT_FILE}")

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
                # Baseline variable cost for the same (a, p) -- for comparison
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
                    'cost_delta_pu':     round(total_cost.get((a, p), 0) - bvc, 4) if bvc is not None else None,
                    'flow_cost':         round(flow * total_cost.get((a, p), 0), 2),
                    'transport_mode':    arc_mode[a],
                    'distance_km':       arc_dist[a],
                    'emergency':         'YES' if a in emergency_arcs else 'no',
                })
            df = pd.DataFrame(rows)
            product_dfs[p] = df.sort_values('arc_id') if not df.empty else df

        # Warehouse sheet with baseline comparison
        wh_rows = []
        for w in sorted(W):
            opened   = round(prob.getSolution(openWarehouse[w]))
            total_in = round(sum(prob.getSolution(x[(a, p)])
                                 for a in arcs_into[w] for p in PRODUCTS if (a, p) in x), 2)
            cap = wh_cap[w]
            was_open_in_baseline = 1 if w in BASELINE_OPEN_WH_SET else 0
            # Status vs baseline:
            #   "kept"          -- open in both
            #   "newly_opened"  -- not in baseline, opened now
            #   "closed"        -- open in baseline, closed now
            #   "unused"        -- not in baseline, not opened now
            if opened == 1 and was_open_in_baseline == 1:
                status_vs_bl = 'kept'
            elif opened == 1 and was_open_in_baseline == 0:
                status_vs_bl = 'newly_opened'
            elif opened == 0 and was_open_in_baseline == 1:
                status_vs_bl = 'closed'
            else:
                status_vs_bl = 'unused'

            # Cost charged by the objective (R: 0; A: new only; F: all)
            if STRATEGY == 'R':
                cost_charged = 0.0
            elif STRATEGY == 'A':
                cost_charged = wh_cost[w] if (opened == 1 and was_open_in_baseline == 0) else 0.0
            else:  # F
                cost_charged = wh_cost[w] if opened == 1 else 0.0

            wh_rows.append({
                'warehouse_id':       w,
                'open_baseline':      was_open_in_baseline,
                'open_scenario':      opened,
                'status_vs_baseline': status_vs_bl,
                'opening_cost':       wh_cost[w],
                'cost_charged':       round(cost_charged, 2),
                'capacity':           cap,
                'total_inflow':       total_in,
                'utilization_%':      round(total_in / cap * 100, 1) if opened and cap > 0 else None,
                'locked_by_R':        'YES' if STRATEGY == 'R' and w in baseline_open_wh else 'no',
            })
        wh_df = pd.DataFrame(wh_rows).sort_values(
            ['open_scenario', 'warehouse_id'], ascending=[False, True])

        # Arc activation sheet with baseline comparison
        arc_rows = []
        for a in sorted(A_fixed):
            activated  = round(prob.getSolution(arc_act[a]))
            total_flow = round(
                sum(prob.getSolution(x[(a, p)]) for p in PRODUCTS if (a, p) in x), 2)
            cap = arc_cap[a]
            was_active_in_baseline = 1 if a in BASELINE_ACTIVE_ARC_SET else 0
            if activated == 1 and was_active_in_baseline == 1:
                status_vs_bl = 'kept'
            elif activated == 1 and was_active_in_baseline == 0:
                status_vs_bl = 'newly_activated'
            elif activated == 0 and was_active_in_baseline == 1:
                status_vs_bl = 'deactivated'
            else:
                status_vs_bl = 'unused'

            if STRATEGY == 'R':
                cost_charged = 0.0
            elif STRATEGY == 'A':
                cost_charged = arc_fc[a] if (activated == 1 and was_active_in_baseline == 0) else 0.0
            else:  # F
                cost_charged = arc_fc[a] if activated == 1 else 0.0

            arc_rows.append({
                'arc_id':              a,
                'activated_baseline':  was_active_in_baseline,
                'activated_scenario':  activated,
                'status_vs_baseline':  status_vs_bl,
                'source':              arc_src[a],
                'target':              arc_tgt[a],
                'total_flow':          total_flow,
                'capacity':            cap,
                'utilization_%':       round(total_flow / cap * 100, 1) if activated else None,
                'fixed_cost':          arc_fc[a],
                'cost_charged':        round(cost_charged, 2),
                'transport_mode':      arc_mode[a],
                'distance_km':         arc_dist[a],
                'locked_by_R':         'YES' if STRATEGY == 'R' and a in baseline_active_arc else 'no',
            })
        arc_df = pd.DataFrame(arc_rows).sort_values(
            ['activated_scenario', 'arc_id'], ascending=[False, True])

        # Unmet demand sheet (infeasible scenarios, R and A only)
        unmet_rows = []
        if is_infeasible and STRATEGY in ('R', 'A'):
            for (c, p), _ in Dem.items():
                u = round(prob.getSolution(unmet[(c, p)]), 2)
                d = Dem[(c, p)]
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

        # Emergency arcs sheet (infeasible scenarios, F only)
        emg_rows = []
        if STRATEGY == 'F' and emergency_arcs:
            for eid in sorted(emergency_arcs):
                for p in PRODUCTS:
                    if (eid, p) in x:
                        fv = round(prob.getSolution(x[(eid, p)]), 2)
                        emg_rows.append({
                            'arc_id':    eid,
                            'source':    arc_src[eid],
                            'target':    arc_tgt[eid],
                            'product':   p,
                            'flow':      fv,
                            'unit_cost': emg_var_cost.get((eid, p), 0),
                            'flow_cost': round(fv * emg_var_cost.get((eid, p), 0), 2),
                            'premium_x': EMERGENCY_PREMIUM,
                            'distance_km': arc_dist.get(eid, 0),
                        })
        emg_df = pd.DataFrame(emg_rows)

        # ---- Summary sheet -- enriched with baseline comparison ----
        summary_rows = [
            ('=== Identification ===',            ''),
            ('Scenario Key',                      SCENARIO_KEY),
            ('Strategy',                          STRATEGY),
            ('Infeasible Scenario',               'YES' if is_infeasible else 'no'),
            ('Solve Time (s)',                    round(solve_time, 3)),
            ('Status',                            str(status)),
            ('', ''),
            ('=== Cost breakdown (reported, comparable to Z*) ===', ''),
            ('Logistics Cost ($)',                round(logistics_cost, 2)),
            ('  Sunk baseline WH cost ($)',       round(sunk_wh, 2)),
            ('  Sunk baseline arc cost ($)',      round(sunk_arcs, 2)),
            ('  New WH opening cost ($)',         round(fixed_wh_charged, 2)),
            ('  New arc activation cost ($)',     round(fixed_arc_charged, 2)),
            ('  Variable transport cost ($)',     round(var_trans_cost, 2)),
            ('', ''),
            ('=== Optimisation objective (raw) ===', ''),
            ('Optimisation Objective ($)',         round(optimisation_obj, 2)),
            ('  (R: variable only; A: new fixed + variable; F: full fixed + variable)', ''),
            ('', ''),
            ('=== Cost breakdown (full -- if all fixed paid) ===', ''),
            ('Full WH cost (all open) ($)',       round(fixed_wh_full, 2)),
            ('Full arc cost (all active) ($)',    round(fixed_arc_full, 2)),
            ('Total fixed cost (full) ($)',       round(fixed_wh_full + fixed_arc_full, 2)),
        ]

        if is_infeasible and STRATEGY in ('R', 'A'):
            summary_rows += [
                ('', ''),
                ('=== Penalty (unmet demand) ===',   ''),
                ('Penalty Cost ($)',                 round(penalty_cost, 2)),
                ('  Units Unserved',                 round(total_unmet_units, 1)),
                ('  Penalty per unit (M)',           PENALTY_M),
                ('Objective (logistics+penalty)',    round(obj_val, 2)),
            ]
        else:
            summary_rows += [
                ('', ''),
                ('Objective (raw, $)',               round(obj_val, 2)),
            ]

        summary_rows += [
            ('', ''),
            ('=== Disruption vs Baseline ===',    ''),
            ('Z* Baseline Cost ($)',              round(Z_STAR, 2) if Z_STAR > 0 else 'N/A'),
        ]
        if disruption_cost is not None:
            summary_rows += [
                ('DeltaZ Disruption Cost ($)',    round(disruption_cost, 2)),
                ('DeltaZ (%)',                    round(disruption_pct, 2)),
            ]
        if baseline_var_trans is not None:
            summary_rows += [
                ('Baseline variable cost (imputed) ($)', round(baseline_var_trans, 2)),
            ]
        if delta_var_R is not None:
            summary_rows += [
                ('Variable cost delta (R only) ($)',     round(delta_var_R, 2)),
            ]

        summary_rows += [
            ('', ''),
            ('=== Network changes vs Baseline ===', ''),
            ('Removed Nodes',                ', '.join(sorted(removed_node_ids)) or 'none'),
            ('Removed Arcs',                 len(removed_arc_ids)),
            ('Stranded Suppliers',           ', '.join(sorted(stranded.keys())) or 'none'),
            ('', ''),
            ('Warehouses Open (baseline)',   len(BASELINE_OPEN_WH_SET)),
            ('Warehouses Open (scenario)',   len(open_wh_sol)),
            ('  WH kept',                    f"{len(wh_kept)}: {wh_kept}" if wh_kept else '0'),
            ('  WH newly opened',            f"{len(wh_opened_new)}: {wh_opened_new}" if wh_opened_new else '0'),
            ('  WH closed',                  f"{len(wh_closed)}: {wh_closed}" if wh_closed else '0'),
            ('  New WH opening cost ($)',    round(new_wh_cost, 2)),
            ('', ''),
            ('Optional arcs active (baseline)', len(BASELINE_ACTIVE_ARC_SET)),
            ('Optional arcs active (scenario)', len(active_arcs_sol)),
            ('  Arcs kept',                  len(arc_kept)),
            ('  Arcs newly activated',       len(arc_activated_new)),
            ('  Arcs deactivated',           len(arc_deactivated)),
            ('  New arc activation cost ($)',round(new_arc_cost, 2)),
            ('', ''),
            ('=== Demand fulfilment ===',    ''),
        ]
        for p in PRODUCTS:
            delivered = sum(prob.getSolution(x[(a, p)])
                            for c in C for a in arcs_into.get(c, []) if (a, p) in x)
            unmet_p   = sum(prob.getSolution(unmet[(c, pp)])
                            for (c, pp) in unmet if pp == p) if unmet else 0.0
            total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
            val = (f'{delivered:.0f} / {total_dem:.0f}  (unmet: {unmet_p:.0f})'
                   if (is_infeasible and STRATEGY in ('R', 'A'))
                   else f'{delivered:.0f} / {total_dem:.0f}')
            summary_rows.append((f'Demand Met - {p}', val))

        summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

        # ---- Write workbook ----
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            for p, df in product_dfs.items():
                df.to_excel(writer, sheet_name=p.replace('_', ' ')[:31], index=False)
            wh_df.to_excel(writer, sheet_name='Warehouses', index=False)
            arc_df.to_excel(writer, sheet_name='Arc Activations', index=False)
            if not unmet_df.empty:
                unmet_df.to_excel(writer, sheet_name='Unmet Demand', index=False)
            if not emg_df.empty:
                emg_df.to_excel(writer, sheet_name='Emergency Arcs', index=False)

        print(f"  Done -- {OUTPUT_FILE}")

        # ---- Append to master summary ----
        master_rows.append({
            'scenario_key':         SCENARIO_KEY,
            'strategy':             STRATEGY,
            'infeasible_scenario':  'YES' if is_infeasible else 'no',
            'status':               str(status),
            'logistics_cost':       round(logistics_cost, 2),
            'optimisation_obj':     round(optimisation_obj, 2),
            'sunk_wh_cost':         round(sunk_wh, 2),
            'sunk_arc_cost':        round(sunk_arcs, 2),
            'new_wh_cost':          round(fixed_wh_charged, 2),
            'new_arc_cost':         round(fixed_arc_charged, 2),
            'var_trans_cost':       round(var_trans_cost, 2),
            'penalty_cost':         round(penalty_cost, 2),
            'obj_val':              round(obj_val, 2),
            'disruption_cost':      round(disruption_cost, 2) if disruption_cost is not None else None,
            'disruption_pct':       round(disruption_pct, 2) if disruption_pct is not None else None,
            'unmet_units':          round(total_unmet_units, 1),
            'open_wh':              len(open_wh_sol),
            'wh_newly_opened':      len(wh_opened_new),
            'wh_closed':            len(wh_closed),
            'active_arcs':          len(active_arcs_sol),
            'arcs_newly_activated': len(arc_activated_new),
            'arcs_deactivated':     len(arc_deactivated),
            'solve_time_s':         round(solve_time, 1),
        })

# =============================================================================
# MASTER SUMMARY
# =============================================================================

master_df   = pd.DataFrame(master_rows)
master_file = os.path.join(RESULTS_DIR, 'summary_all_scenarios.xlsx')

# Also build a "wide" comparison table: one row per scenario, columns for R/A/F costs
wide_rows = []
for scen in ALL_SCENARIO_KEYS:
    block = master_df[master_df['scenario_key'] == scen]
    if block.empty:
        continue
    row = {'Scenario': scen,
           'Infeasible': block['infeasible_scenario'].iloc[0]}
    cost_by_strat = {}
    for _, r in block.iterrows():
        st = r['strategy']
        row[f'Cost_{st}']       = r['logistics_cost']
        row[f'DeltaZ_{st}']     = r['disruption_cost']
        row[f'DeltaZ_%_{st}']   = r['disruption_pct']
        row[f'Unmet_{st}']      = r['unmet_units']
        cost_by_strat[st]       = r['logistics_cost']
    # Flexibility value (R cost - A cost): how much adapting saves vs sticking with baseline
    if 'R' in cost_by_strat and 'A' in cost_by_strat \
       and cost_by_strat['R'] is not None and cost_by_strat['A'] is not None:
        row['Flex_Value_R_minus_A'] = round(cost_by_strat['R'] - cost_by_strat['A'], 2)
    # Sunk cost burden (A cost - F cost): theoretical regret from being locked in
    if 'A' in cost_by_strat and 'F' in cost_by_strat \
       and cost_by_strat['A'] is not None and cost_by_strat['F'] is not None:
        row['Sunk_Cost_A_minus_F'] = round(cost_by_strat['A'] - cost_by_strat['F'], 2)
    # Best strategy (min cost)
    valid = {k: v for k, v in cost_by_strat.items() if v is not None}
    if valid:
        row['Best_Strategy'] = min(valid, key=valid.get)
    wide_rows.append(row)
wide_df = pd.DataFrame(wide_rows)

with pd.ExcelWriter(master_file, engine='openpyxl') as writer:
    master_df.to_excel(writer, sheet_name='Detailed (long)', index=False)
    wide_df.to_excel(writer,   sheet_name='Cost comparison (wide)', index=False)

print("\n" + "=" * 70)
print("MASTER SUMMARY -- detailed (long format)")
print("=" * 70)
print(master_df.to_string(index=False))

print("\n" + "=" * 70)
print("MASTER SUMMARY -- cost comparison (wide format)")
print("=" * 70)
print(wide_df.to_string(index=False))

print(f"\nWritten to {master_file}")