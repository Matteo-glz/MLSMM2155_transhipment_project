"""
GlobalFlow Phase 2 — Complete Solver
======================================
Solves all 6 scenarios × 3 strategies = 18 runs.

Scenarios
─────────
  T1, T2, T3, S2   Feasible under the standard formulation.
  S1, S3            Remove hub H3 (Singapore), stranding suppliers S4–S6, S9.
                    Detected automatically at runtime via arc-reachability check.

Strategy semantics
──────────────────
  R (Reroute)   Baseline warehouse/arc decisions locked.
                Infeasible scenarios: C1 relaxed to inequality, unmet demand
                penalised at M per unit.
  A (Adapt)     All decisions free.
                Infeasible scenarios: same C1 relaxation + penalty.
  F (Fight)     All decisions free, hard demand equality (C1 = demand).
                Infeasible scenarios: emergency supply arcs injected at a
                premium cost so full demand can be restored.
                Feasible scenarios: standard greenfield re-solve.

Cost model
──────────
  total_cost[a,p] = baseline_var[a,p] × scenario_factor[a,p] × (1 + tariff[zone_from, zone_to])

Outputs
───────
  phase2/results/scenario_ArcCosts_{KEY}/strategy_{STRATEGY}.xlsx   (per run)
  phase2/results/summary_all_scenarios.xlsx                          (master)
"""

import os
import pandas as pd
import xpress as xp
import time

xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')

# =============================================================================
# CONFIGURATION  ← edit here
# =============================================================================

EXCEL_FILE             = 'data/globalflow_instance.xlsx'
BASELINE_SOLUTION_FILE = 'phase1/results/baseline_solution.xlsx'
RESULTS_DIR            = 'phase2/results'

ALL_SCENARIO_KEYS = ['T1', 'T2', 'T3', 'S1', 'S2', 'S3']
STRATEGIES        = ['R', 'A', 'F']

PRODUCTS          = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME    = 300        # seconds per solve
PENALTY_M         = 1_000_000  # cost per unit of unmet demand (R and A on infeasible)
EMERGENCY_PREMIUM = 2.5        # multiplier applied to distance-scaled cost for emergency arcs (F infeasible)
EMG_TOP_K         = 1          # connect each stranded supplier to this many closest hubs (1 or 2)

os.makedirs(RESULTS_DIR, exist_ok=True)

# =============================================================================
# ONE-TIME DATA LOADS
# =============================================================================

print("Loading shared data...")

# Baseline decisions (needed by strategy R)
try:
    _bl_wh  = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Warehouses')
    _bl_arc = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Arc Activations')
    _bl_sum = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Summary')
    baseline_open_wh    = dict(zip(_bl_wh['warehouse_id'],  _bl_wh['open'].astype(int)))
    baseline_active_arc = dict(zip(_bl_arc['arc_id'],       _bl_arc['activated'].astype(int)))
    _cost_row = _bl_sum[_bl_sum['Metric'] == 'Total Cost ($)']['Value']
    Z_STAR = float(_cost_row.iloc[0]) if not _cost_row.empty else 0.0
    print(f"  Baseline: {sum(baseline_open_wh.values())} open WH, "
          f"{sum(baseline_active_arc.values())} active arcs, Z*=${Z_STAR:,.2f}")
except Exception as e:
    print(f"  WARNING: could not load baseline ({e}). Strategy R unconstrained; Z*=0.")
    baseline_open_wh    = {}
    baseline_active_arc = {}
    Z_STAR = 0.0

# Baseline variable costs and tariffs (used for all scenarios)
baseline_costs_df = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')
tariffs_df        = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')

baseline_var_cost = {(r['arc_id'], r['product']): r['variable_cost']
                     for _, r in baseline_costs_df.iterrows()}
tariff_lookup     = {(r['zone_pair_from'], r['zone_pair_to']): r['interzonal_tariff_rate']
                     for _, r in tariffs_df.iterrows()}

# Static sheets shared across all scenarios
nodes_df_all      = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
arcs_df_all       = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
warehouses_df     = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
suppliers_df      = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
demand_df         = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
supply_df         = pd.read_excel(EXCEL_FILE, sheet_name='Supply')

arc_zone_from_all = dict(zip(arcs_df_all['arc_id'], arcs_df_all['zone_from']))
arc_zone_to_all   = dict(zip(arcs_df_all['arc_id'], arcs_df_all['zone_to']))

# Hub-to-hub distance lookup built once from all baseline arcs (used for emergency arc estimation)
_all_hubs = set(nodes_df_all[nodes_df_all['type'] == 'HUB']['node_id'])
hub_hub_dist: dict = {}   # (from_hub, to_hub) → distance_km
for _, _r in arcs_df_all.iterrows():
    if _r['from_id'] in _all_hubs and _r['to_id'] in _all_hubs:
        hub_hub_dist[(_r['from_id'], _r['to_id'])] = _r['distance_km']

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

        # ── Load scenario-specific sheets ─────────────────────────────────────
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

        # ── Sets ─────────────────────────────────────────────────────────────
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

        # ── Arc lookups ───────────────────────────────────────────────────────
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

        A_fixed  = {a for a in arc_src if arc_fc[a] > 0}
        A_always = {a for a in arc_src if arc_fc[a] == 0}

        # ── Parameters ────────────────────────────────────────────────────────
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

        # ── Tariff-aware variable costs ───────────────────────────────────────
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

        # ── Detect stranded suppliers ─────────────────────────────────────────
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
                        print(f"    {s} — {p}: {qty} units stranded")

        # ── Strategy F infeasible: inject emergency arcs ───────────────────────
        emergency_arcs = {}
        emg_var_cost   = {}

        if STRATEGY == 'F' and is_infeasible:
            print(f"  [F] Injecting geographic emergency arcs "
                  f"(×{EMERGENCY_PREMIUM} premium, top-{EMG_TOP_K} closest hub(s))...")

            for s, prods in stranded.items():
                s_orig_rows = arcs_df_all[arcs_df_all['from_id'] == s]

                # ── Cost-per-km from the supplier's original baseline arc(s) ──
                # cost_per_km[p] = baseline_var_cost[original_arc, p] / distance_km
                cost_per_km: dict = {}
                for _, orig_row in s_orig_rows.iterrows():
                    d = orig_row['distance_km']
                    if d <= 0:
                        continue
                    for p in prods:
                        bv = baseline_var_cost.get((orig_row['arc_id'], p))
                        if bv is not None and p not in cost_per_km:
                            cost_per_km[p] = bv / d   # $/unit/km

                # ── Estimate s→h distance for every active hub ────────────────
                # Priority 1: direct arc in the baseline network.
                # Priority 2: proxy via a removed hub that s was originally wired to.
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
                            continue          # only use removed hubs as waypoints
                        d_via_h = hub_hub_dist.get((via, h))
                        if d_via_h is None:
                            continue
                        est = orig_row['distance_km'] + d_via_h
                        if best is None or est < best:
                            best = est
                    if best is not None:
                        hub_dist_est[h] = best
                    else:
                        print(f"    WARNING: no distance estimate for {s}→{h}, hub skipped")

                # ── Hard fallback: uniform cost to all hubs (original behaviour) ──
                if not hub_dist_est:
                    s_arc_ids = set(s_orig_rows['arc_id'])
                    base_costs = [v for (a, p), v in baseline_var_cost.items()
                                  if a in s_arc_ids and p in prods]
                    fb_cost = round(EMERGENCY_PREMIUM * (min(base_costs) if base_costs else 500.0), 4)
                    print(f"    WARNING: {s} — no distance estimates at all, "
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

                # ── Select the EMG_TOP_K closest hubs ────────────────────────
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
                            # No cost-per-km data for this product; fall back to flat premium
                            s_arc_ids = set(s_orig_rows['arc_id'])
                            base_p = [v for (a, pp), v in baseline_var_cost.items()
                                      if a in s_arc_ids and pp == p]
                            emg_uc = round(EMERGENCY_PREMIUM * (min(base_p) if base_p else 500.0), 4)
                        emg_var_cost[(eid, p)] = emg_uc
                        total_cost[(eid, p)]   = emg_uc
                        cost_parts.append(f"{p.split('_')[0]}={emg_uc:.2f}")

                    print(f"    {s} ({sorted(prods)}) → {h}  "
                          f"est_dist={est_km:.0f} km  "
                          f"unit_costs=[{', '.join(cost_parts)}]")

        # ── Decision variables ────────────────────────────────────────────────
        prob = xp.problem()
        prob.setControl('MAXTIME', MAX_SOLVE_TIME)
        prob.setControl('OUTPUTLOG', 1)
        prob.setControl('MIPRELSTOP', 1e-7)

        # Flow variables
        x = {}
        for (a, p) in total_cost:
            src = arc_src[a]
            if src in S and p not in supplier_prods.get(src, set()):
                continue
            x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

        # Warehouse opening (locked for strategy R)
        openWarehouse = {}
        for w in W:
            if STRATEGY == 'R' and w in baseline_open_wh:
                v = float(baseline_open_wh[w])
                openWarehouse[w] = prob.addVariable(name=f'open_{w}', lb=v, ub=v,
                                                    vartype=xp.continuous)
            else:
                openWarehouse[w] = prob.addVariable(name=f'open_{w}', vartype=xp.binary)

        # Arc activation (locked for strategy R)
        arc_act = {}
        for a in A_fixed:
            if STRATEGY == 'R' and a in baseline_active_arc:
                v = float(baseline_active_arc[a])
                arc_act[a] = prob.addVariable(name=f'arc_{a}', lb=v, ub=v,
                                              vartype=xp.continuous)
            else:
                arc_act[a] = prob.addVariable(name=f'arc_{a}', vartype=xp.binary)

        # Unmet demand slack (infeasible scenarios, strategies R and A only)
        unmet = {}
        if is_infeasible and STRATEGY in ('R', 'A'):
            for (c, p), d in Dem.items():
                unmet[(c, p)] = prob.addVariable(name=f'unmet_{c}_{p}', lb=0, ub=d,
                                                 vartype=xp.continuous)

        print(f"  Variables: {len(x)} flow, {len(openWarehouse)} WH, "
              f"{len(arc_act)} arc_act, {len(unmet)} unmet")

        # ── Objective ─────────────────────────────────────────────────────────
        obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)
        obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in A_fixed)
        obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)
        if is_infeasible and STRATEGY in ('R', 'A'):
            obj += xp.Sum(PENALTY_M * unmet[(c, p)] for (c, p) in unmet)
        prob.setObjective(obj, sense=xp.minimize)

        # ── Constraints ───────────────────────────────────────────────────────
        def inflow(node, product):
            return xp.Sum(x[(a, product)] for a in arcs_into.get(node, []) if (a, product) in x)

        def outflow(node, product):
            return xp.Sum(x[(a, product)] for a in arcs_from.get(node, []) if (a, product) in x)

        # C1 — demand satisfaction
        for (c, p), d in Dem.items():
            if is_infeasible and STRATEGY in ('R', 'A'):
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

        # C3 — arc capacity (always-on)
        for a in A_always:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))

        # C4 — arc capacity (optional, gated by activation)
        for a in A_fixed:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a], name=f'C4_{a}'))

        # C5 — warehouse capacity (gated by opening)
        for w in W:
            total_in = xp.Sum(x[(a, p)] for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
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

        # ── Solve ─────────────────────────────────────────────────────────────
        print(f"Solving (limit: {MAX_SOLVE_TIME}s)...")
        t0 = time.time()
        prob.solve()
        solve_time = time.time() - t0

        # ── Report ────────────────────────────────────────────────────────────
        status = prob.attributes.solstatus
        print(f"\nStatus: {status}  |  {solve_time:.1f}s")

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

        obj_val = prob.getObjVal()

        if is_infeasible and STRATEGY in ('R', 'A'):
            total_unmet_units = sum(prob.getSolution(unmet[(c, p)]) for (c, p) in unmet)
            penalty_cost      = PENALTY_M * total_unmet_units
            logistics_cost    = obj_val - penalty_cost
        else:
            total_unmet_units = 0.0
            penalty_cost      = 0.0
            logistics_cost    = obj_val

        fixed_wh_cost  = sum(wh_cost[w] * round(prob.getSolution(openWarehouse[w])) for w in W)
        fixed_arc_cost = sum(arc_fc[a]  * round(prob.getSolution(arc_act[a]))       for a in A_fixed)
        var_trans_cost = logistics_cost - fixed_wh_cost - fixed_arc_cost
        disruption_cost = logistics_cost - Z_STAR if Z_STAR > 0 else None

        print(f"  Warehouse opening costs  : ${fixed_wh_cost:>14,.2f}")
        print(f"  Arc activation costs     : ${fixed_arc_cost:>14,.2f}")
        print(f"  Variable transport costs : ${var_trans_cost:>14,.2f}")
        print(f"  Logistics total          : ${logistics_cost:>14,.2f}")
        if is_infeasible and STRATEGY in ('R', 'A'):
            print(f"  Penalty (unmet demand)   : ${penalty_cost:>14,.2f}"
                  f"  ({total_unmet_units:.1f} units × M={PENALTY_M:,})")
        print(f"  OBJECTIVE                : ${obj_val:>14,.2f}")
        if disruption_cost is not None:
            print(f"  ΔZ (vs Z*=${Z_STAR:,.2f})  : ${disruption_cost:>+14,.2f}"
                  f"  ({disruption_cost/Z_STAR*100:+.1f}%)")

        open_wh     = [w for w in W      if round(prob.getSolution(openWarehouse[w])) == 1]
        active_arcs = [a for a in A_fixed if round(prob.getSolution(arc_act[a]))       == 1]
        print(f"  Open warehouses ({len(open_wh)}/{len(W)})    : {sorted(open_wh)}")
        print(f"  Active opt. arcs ({len(active_arcs)}/{len(A_fixed)}) : {sorted(active_arcs)}")

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

        # ── Export to Excel ───────────────────────────────────────────────────
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
                cap = arc_cap[a]
                rows.append({
                    'arc_id':         a,
                    'source':         arc_src[a],
                    'target':         arc_tgt[a],
                    'product':        p,
                    'flow':           flow,
                    'capacity':       cap,
                    'utilization_%':  round(flow / cap * 100, 1) if cap < 99999 else None,
                    'total_cost':     round(total_cost.get((a, p), 0), 4),
                    'flow_cost':      round(flow * total_cost.get((a, p), 0), 2),
                    'transport_mode': arc_mode[a],
                    'distance_km':    arc_dist[a],
                    'emergency':      'YES' if a in emergency_arcs else 'no',
                })
            df = pd.DataFrame(rows)
            product_dfs[p] = df.sort_values('arc_id') if not df.empty else df

        # Warehouse sheet
        wh_rows = []
        for w in sorted(W):
            opened   = round(prob.getSolution(openWarehouse[w]))
            total_in = round(sum(prob.getSolution(x[(a, p)])
                                 for a in arcs_into[w] for p in PRODUCTS if (a, p) in x), 2)
            cap = wh_cap[w]
            wh_rows.append({
                'warehouse_id':  w,
                'open':          opened,
                'opening_cost':  wh_cost[w],
                'capacity':      cap,
                'total_inflow':  total_in,
                'utilization_%': round(total_in / cap * 100, 1) if opened and cap > 0 else None,
                'locked_by_R':   'YES' if STRATEGY == 'R' and w in baseline_open_wh else 'no',
            })
        wh_df = pd.DataFrame(wh_rows).sort_values(
            ['open', 'warehouse_id'], ascending=[False, True])

        # Arc activation sheet
        arc_rows = []
        for a in sorted(A_fixed):
            activated  = round(prob.getSolution(arc_act[a]))
            total_flow = round(
                sum(prob.getSolution(x[(a, p)]) for p in PRODUCTS if (a, p) in x), 2)
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
                'locked_by_R':    'YES' if STRATEGY == 'R' and a in baseline_active_arc else 'no',
            })
        arc_df = pd.DataFrame(arc_rows).sort_values(
            ['activated', 'arc_id'], ascending=[False, True])

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
                        })
        emg_df = pd.DataFrame(emg_rows)

        # Summary sheet
        summary_rows = [
            ('Scenario Key',                      SCENARIO_KEY),
            ('Strategy',                          STRATEGY),
            ('Infeasible Scenario',               'YES' if is_infeasible else 'no'),
            ('Solve Time (s)',                     round(solve_time, 3)),
            ('', ''),
            ('Logistics Cost ($)',                 round(logistics_cost, 2)),
            ('  Warehouse Opening Cost ($)',       round(fixed_wh_cost, 2)),
            ('  Arc Activation Cost ($)',          round(fixed_arc_cost, 2)),
            ('  Variable Transport Cost ($)',      round(var_trans_cost, 2)),
        ]
        if is_infeasible and STRATEGY in ('R', 'A'):
            summary_rows += [
                ('Penalty Cost ($)',               round(penalty_cost, 2)),
                ('  Units Unserved',               round(total_unmet_units, 1)),
                ('  Penalty per unit (M)',         PENALTY_M),
                ('Objective (logistics+penalty)',  round(obj_val, 2)),
            ]
        else:
            summary_rows += [('Objective ($)', round(obj_val, 2))]
        if disruption_cost is not None:
            summary_rows += [
                ('', ''),
                ('Z* Baseline Cost ($)',           round(Z_STAR, 2)),
                ('ΔZ Disruption Cost ($)',         round(disruption_cost, 2)),
                ('ΔZ (%)',                         round(disruption_cost / Z_STAR * 100, 2)),
            ]
        summary_rows += [
            ('', ''),
            ('Removed Nodes',        ', '.join(sorted(removed_node_ids)) or 'none'),
            ('Stranded Suppliers',   ', '.join(sorted(stranded.keys())) or 'none'),
            ('Warehouses Open',      len(open_wh)),
            ('Warehouses Total',     len(W)),
            ('Arcs Activated',       len(active_arcs)),
            ('Arcs Total (optional)',len(A_fixed)),
            ('', ''),
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
            summary_rows.append((f'Demand Met — {p}', val))

        summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

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

        print(f"  Done — {OUTPUT_FILE}")

        # Append to master summary
        master_rows.append({
            'scenario_key':       SCENARIO_KEY,
            'strategy':           STRATEGY,
            'infeasible_scenario':'YES' if is_infeasible else 'no',
            'status':             str(status),
            'logistics_cost':     round(logistics_cost, 2),
            'penalty_cost':       round(penalty_cost, 2),
            'obj_val':            round(obj_val, 2),
            'disruption_cost':    round(disruption_cost, 2) if disruption_cost is not None else None,
            'disruption_pct':     round(disruption_cost / Z_STAR * 100, 2) if disruption_cost and Z_STAR else None,
            'unmet_units':        round(total_unmet_units, 1),
            'open_wh':            len(open_wh),
            'active_arcs':        len(active_arcs),
            'solve_time_s':       round(solve_time, 1),
        })

# =============================================================================
# MASTER SUMMARY
# =============================================================================

master_df   = pd.DataFrame(master_rows)
master_file = os.path.join(RESULTS_DIR, 'summary_all_scenarios.xlsx')
master_df.to_excel(master_file, index=False)

print("\n" + "=" * 70)
print("MASTER SUMMARY (all scenarios × strategies)")
print("=" * 70)
print(master_df.to_string(index=False))
print(f"\nWritten to {master_file}")
