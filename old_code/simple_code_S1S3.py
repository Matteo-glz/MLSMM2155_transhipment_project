"""
GlobalFlow Network Design Optimization — S1 / S3 Infeasibility Handler
=======================================================================
Structural modification for scenarios S1 (East Asian hub closure) and S3
(combined S1 + T2), which are INFEASIBLE under the standard formulation
because suppliers S4, S5, S6, S9 route exclusively through H3 (Singapore).

Removing H3 strands:
  • 100 % of B_Semiconductors supply  (S4, S5, S6  → H3 only)
  •  33 % of C_BatteryComponents supply (S9          → H3 only)

Because C1 is an equality constraint (demand must be fully met), no feasible
solution exists.  This script solves the three response strategies explicitly:

  R (Respond)  — Re-optimise flows on the disrupted network.
                  C1 is RELAXED to an inequality; unmet demand is penalised
                  at a large cost M.  No structural changes are allowed.
                  Warehouses and optional arcs from the baseline solution
                  are FIXED (open_w and arc_a locked to baseline values).

  A (Adapt)    — Same relaxed C1 + penalty, but warehouses and optional arcs
                  are FREE to be re-optimised.  The solver may open / close
                  warehouses and activate / deactivate optional arcs.

  F (Fight)    — Full demand satisfaction is enforced (C1 as equality again).
                  Emergency supply arcs are added: each stranded supplier gets
                  a virtual arc to every remaining hub at a premium cost
                  (EMERGENCY_PREMIUM × cheapest available arc cost for that
                  supplier-product pair).  This models spot-market / airfreight
                  procurement needed to fully restore supply.

Key modelling changes vs simple_code.py
────────────────────────────────────────
  1. C1 relaxed to ≤  and  unmet[c,p] slack variable added          (R + A)
  2. Penalty term  M × Σ unmet[c,p]  added to objective             (R + A)
  3. Baseline warehouse / arc decisions read and locked as parameters (R only)
  4. Emergency supply arcs injected with premium variable cost       (F only)
  5. Disruption cost ΔZ = Z(scenario, strategy) − Z*  reported
  6. Unmet demand table exported to Excel

Structure mirrors simple_code.py exactly:
  1. Configuration
  2. Data loading
  3. Sets and parameters
  4. Decision variables
  5. Objective function
  6. Constraints (C1–C7 ± modifications)
  7. Solve
  8. Report
  9. Export to Excel
"""

import os
import sys
import pandas as pd
import xpress as xp
import time

xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')

# =============================================================================
# 1. CONFIGURATION  ← edit here
# =============================================================================

EXCEL_FILE = '/Users/matteogalizia/Documents/GitHub/MLSMM2155_transhipment_project/data/globalflow_instance.xlsx'

# Scenarios and strategies to run (all combinations will be solved)
SCENARIO_KEYS = ['S1', 'S3']
STRATEGIES    = ['R', 'A', 'F']

# Baseline optimal cost Z* (from Phase 1 / Baseline run) — used to compute ΔZ
Z_STAR = 0.0   # ← replace with your baseline total cost

# Penalty cost M per unit of unmet demand (R and A strategies).
PENALTY_M = 1_000_000

# Emergency supply premium multiplier for strategy F.
EMERGENCY_PREMIUM = 2.5

PRODUCTS       = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME = 300   # seconds

# Path to the baseline solution Excel (needed by strategy R to lock decisions)
BASELINE_SOLUTION_FILE = '/Users/matteogalizia/Documents/GitHub/MLSMM2155_transhipment_project/phase1/results/baseline_solution.xlsx'

# =============================================================================
# MAIN LOOP — iterate over every (scenario, strategy) combination
# =============================================================================

for SCENARIO_KEY in SCENARIO_KEYS:
    for STRATEGY in STRATEGIES:

        SCENARIO    = f'ArcCosts_{SCENARIO_KEY}'
        OUTPUT_FILE = f'phase2/results/scenario_ArcCosts_{SCENARIO_KEY}/strategy_{STRATEGY}.xlsx'

        # =====================================================================
        # 2. DATA LOADING
        # =====================================================================

        print("=" * 70)
        print(f"GlobalFlow — scenario: {SCENARIO_KEY}  |  strategy: {STRATEGY}")
        print("=" * 70)
        print("\nLoading data...")

        nodes_df      = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
        arcs_df_full  = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
        warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
        suppliers_df  = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
        demand_df     = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
        supply_df     = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
        costs_df      = pd.read_excel(EXCEL_FILE, sheet_name=SCENARIO)

        removed_nodes = pd.read_excel(EXCEL_FILE, sheet_name=f'NodesRemoved_{SCENARIO_KEY}')
        removed_arcs  = pd.read_excel(EXCEL_FILE, sheet_name=f'ArcsRemoved_{SCENARIO_KEY}')

        removed_node_ids = set(removed_nodes['node_id'])
        removed_arc_ids  = set(removed_arcs['arc_id'])

        arcs_df = arcs_df_full[
            ~arcs_df_full['arc_id'].isin(removed_arc_ids)
        ].copy()

        nodes_active = nodes_df[~nodes_df['node_id'].isin(removed_node_ids)].copy()

        print(f"  Removed nodes : {sorted(removed_node_ids)}")
        print(f"  Removed arcs  : {len(removed_arc_ids)}")

        # =====================================================================
        # 3. SETS AND PARAMETERS
        # =====================================================================

        S = set(suppliers_df['supplier_id'])
        H = set(nodes_active[nodes_active['type'] == 'HUB']['node_id'])
        W = set(warehouses_df['warehouse_id'])
        C = set(demand_df['customer_id'].unique())

        S_p = {}
        supplier_prods = {}
        for _, row in supply_df.iterrows():
            S_p.setdefault(row['product'], set()).add(row['supplier_id'])
            supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])

        stranded = {}
        for s in S:
            s_arcs_baseline = set(arcs_df_full[arcs_df_full['from_id'] == s]['arc_id'])
            s_arcs_active   = set(arcs_df[arcs_df['from_id'] == s]['arc_id'])
            if len(s_arcs_baseline) > 0 and len(s_arcs_active) == 0:
                stranded[s] = supplier_prods.get(s, set())

        print(f"\n  Stranded suppliers (no remaining arc): {sorted(stranded.keys())}")
        for s, prods in stranded.items():
            qty = {p: supply_df.loc[(supply_df['supplier_id']==s) &
                                     (supply_df['product']==p), 'supply'].values[0]
                   for p in prods}
            print(f"    {s}: {qty}")

        arc_src = {}; arc_tgt = {}; arc_cap = {}; arc_fc = {}
        arc_mode = {}; arc_dist = {}

        all_node_ids = set(nodes_df['node_id'])
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

        Dem = {(row['customer_id'], row['product']): row['demand']
               for _, row in demand_df.iterrows()}
        Sup = {(row['supplier_id'], row['product']): row['supply']
               for _, row in supply_df.iterrows()}

        wh_cap  = {row['warehouse_id']: row['capacity']     for _, row in warehouses_df.iterrows()}
        wh_cost = {row['warehouse_id']: row['opening_cost'] for _, row in warehouses_df.iterrows()}

        arc_zone_from = dict(zip(arcs_df['arc_id'], arcs_df['zone_from']))
        arc_zone_to   = dict(zip(arcs_df['arc_id'], arcs_df['zone_to']))

        var_cost = {(row['arc_id'], row['product']): row['variable_cost']
                    for _, row in costs_df.iterrows()
                    if row['arc_id'] not in removed_arc_ids}

        total_cost = {(a, p): vc for (a, p), vc in var_cost.items()}

        print(f"\n  Active hubs : {sorted(H)}")
        print(f"  Arcs: {len(arc_src)} total ({len(A_fixed)} optional, {len(A_always)} always-active)")
        print(f"  (arc, product) cost pairs: {len(total_cost)}")

        # ── Strategy R: read baseline open/activated decisions to lock ────────
        baseline_open_wh    = {}
        baseline_active_arc = {}

        if STRATEGY == 'R':
            print(f"\n  [R] Reading baseline decisions from {BASELINE_SOLUTION_FILE} ...")
            try:
                bl_wh  = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Warehouses')
                bl_arc = pd.read_excel(BASELINE_SOLUTION_FILE, sheet_name='Arc Activations')
                baseline_open_wh    = dict(zip(bl_wh['warehouse_id'],  bl_wh['open'].astype(int)))
                baseline_active_arc = dict(zip(bl_arc['arc_id'],        bl_arc['activated'].astype(int)))
                print(f"    Warehouses locked: {sum(baseline_open_wh.values())} open")
                print(f"    Optional arcs locked: {sum(baseline_active_arc.values())} activated")
            except Exception as e:
                print(f"  WARNING: could not read baseline solution ({e}).")
                print("           Strategy R will not lock warehouse / arc decisions.")

        # ── Strategy F: build emergency arcs for stranded suppliers ──────────
        emergency_arcs = {}
        emg_var_cost   = {}

        if STRATEGY == 'F':
            print(f"\n  [F] Injecting emergency supply arcs (premium ×{EMERGENCY_PREMIUM}) ...")
            baseline_var_cost = {(row['arc_id'], row['product']): row['variable_cost']
                                 for _, row in pd.read_excel(
                                     EXCEL_FILE, sheet_name='ArcCosts_Baseline').iterrows()}

            for s, prods in stranded.items():
                s_baseline_arcs = set(arcs_df_full[arcs_df_full['from_id'] == s]['arc_id'])
                baseline_costs_s = [v for (a, p), v in baseline_var_cost.items()
                                    if a in s_baseline_arcs and p in prods]
                base_cost = min(baseline_costs_s) if baseline_costs_s else 500.0
                emg_unit_cost = round(EMERGENCY_PREMIUM * base_cost, 4)

                for h in sorted(H):
                    eid = f'EMG_{s}_{h}'
                    emergency_arcs[eid] = {
                        'arc_id':         eid,
                        'from_id':        s,
                        'to_id':          h,
                        'shared_capacity': 99999,
                        'fixed_activation_cost': 0,
                        'transport_mode': 'emergency',
                        'distance_km':    0,
                    }
                    arc_src[eid]  = s
                    arc_tgt[eid]  = h
                    arc_cap[eid]  = 99999
                    arc_fc[eid]   = 0
                    arc_mode[eid] = 'emergency'
                    arc_dist[eid] = 0
                    arcs_from[s].add(eid)
                    arcs_into[h].add(eid)
                    A_always.add(eid)

                    for p in prods:
                        emg_var_cost[(eid, p)] = emg_unit_cost
                        total_cost[(eid, p)]   = emg_unit_cost

                print(f"    {s} ({sorted(prods)}): base={base_cost:.2f}  emergency={emg_unit_cost:.2f}"
                      f"  → {len(H)} virtual arcs added")

        # =====================================================================
        # 4. DECISION VARIABLES
        # =====================================================================

        print("\nCreating decision variables...")

        prob = xp.problem()
        prob.setControl('MAXTIME', MAX_SOLVE_TIME)
        prob.setControl('OUTPUTLOG', 1)

        x = {}
        for (a, p) in total_cost:
            src = arc_src[a]
            if src in S and p not in supplier_prods.get(src, set()):
                continue
            x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

        openWarehouse = {}
        for w in W:
            if STRATEGY == 'R' and w in baseline_open_wh:
                v = float(baseline_open_wh[w])
                openWarehouse[w] = prob.addVariable(name=f'open_{w}', lb=v, ub=v,
                                                    vartype=xp.continuous)
            else:
                openWarehouse[w] = prob.addVariable(name=f'open_{w}', vartype=xp.binary)

        arc_act = {}
        for a in A_fixed:
            if STRATEGY == 'R' and a in baseline_active_arc:
                v = float(baseline_active_arc[a])
                arc_act[a] = prob.addVariable(name=f'arc_{a}', lb=v, ub=v,
                                              vartype=xp.continuous)
            else:
                arc_act[a] = prob.addVariable(name=f'arc_{a}', vartype=xp.binary)

        unmet = {}
        if STRATEGY in ('R', 'A'):
            for (c, p), d in Dem.items():
                unmet[(c, p)] = prob.addVariable(name=f'unmet_{c}_{p}', lb=0, ub=d,
                                                 vartype=xp.continuous)

        print(f"  Flow variables x           : {len(x)}")
        print(f"  Warehouse open variables   : {len(openWarehouse)}")
        print(f"  Arc activation variables   : {len(arc_act)}")
        if unmet:
            print(f"  Unmet demand variables     : {len(unmet)}")

        # =====================================================================
        # 5. OBJECTIVE FUNCTION
        # =====================================================================

        print("\nBuilding objective function...")

        obj  = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)
        obj += xp.Sum(arc_fc[a]  * arc_act[a]       for a in A_fixed)
        obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)

        if STRATEGY in ('R', 'A'):
            obj += xp.Sum(PENALTY_M * unmet[(c, p)] for (c, p) in unmet)

        prob.setObjective(obj, sense=xp.minimize)

        # =====================================================================
        # 6. CONSTRAINTS
        # =====================================================================

        print("Adding constraints...")

        def inflow(node, product):
            return xp.Sum(x[(a, product)] for a in arcs_into.get(node, []) if (a, product) in x)

        def outflow(node, product):
            return xp.Sum(x[(a, product)] for a in arcs_from.get(node, []) if (a, product) in x)

        for (c, p), d in Dem.items():
            if STRATEGY == 'F':
                prob.addConstraint(xp.constraint(inflow(c, p) == d,
                                                 name=f'C1_{c}_{p}'))
            else:
                prob.addConstraint(xp.constraint(inflow(c, p) + unmet[(c, p)] == d,
                                                 name=f'C1_{c}_{p}'))

        for p, sup_set in S_p.items():
            for s in sup_set:
                if (s, p) in Sup:
                    prob.addConstraint(xp.constraint(outflow(s, p) <= Sup[(s, p)],
                                                     name=f'C2_{s}_{p}'))

        for a in A_always:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a], name=f'C3_{a}'))

        for a in A_fixed:
            flow = xp.Sum(x[(a, p)] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(flow <= arc_cap[a] * arc_act[a], name=f'C4_{a}'))

        for w in W:
            total_in = xp.Sum(x[(a, p)] for a in arcs_into[w] for p in PRODUCTS if (a, p) in x)
            prob.addConstraint(xp.constraint(total_in <= wh_cap[w] * openWarehouse[w],
                                             name=f'C5_{w}'))

        for w in W:
            for p in PRODUCTS:
                prob.addConstraint(xp.constraint(inflow(w, p) == outflow(w, p),
                                                 name=f'C6_{w}_{p}'))

        for h in H:
            for p in PRODUCTS:
                prob.addConstraint(xp.constraint(inflow(h, p) == outflow(h, p),
                                                 name=f'C7_{h}_{p}'))

        print("  Constraints added.")

        # =====================================================================
        # 7. SOLVE
        # =====================================================================

        print(f"\nSolving (time limit: {MAX_SOLVE_TIME}s)...\n")

        t0 = time.time()
        prob.solve()
        solve_time = time.time() - t0

        # =====================================================================
        # 8. REPORT
        # =====================================================================

        status = prob.attributes.solstatus
        print(f"\n{'=' * 70}")
        print(f"SOLUTION REPORT  —  Scenario: {SCENARIO_KEY}  |  Strategy: {STRATEGY}")
        print(f"{'=' * 70}")
        print(f"Status     : {status}")
        print(f"Solve time : {solve_time:.2f}s")

        if status not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
            print("No feasible solution found — check data or constraints.")
            continue

        obj_val = prob.getObjVal()

        if STRATEGY in ('R', 'A'):
            total_unmet_units = sum(prob.getSolution(unmet[(c, p)]) for (c, p) in unmet)
            penalty_cost      = PENALTY_M * total_unmet_units
            logistics_cost    = obj_val - penalty_cost
        else:
            total_unmet_units = 0.0
            penalty_cost      = 0.0
            logistics_cost    = obj_val

        fixed_wh  = sum(wh_cost[w] * round(prob.getSolution(openWarehouse[w])) for w in W)
        fixed_arc = sum(arc_fc[a]  * round(prob.getSolution(arc_act[a]))       for a in A_fixed)
        var_trans = logistics_cost - fixed_wh - fixed_arc

        disruption_cost = logistics_cost - Z_STAR if Z_STAR > 0 else None

        print(f"\nCost breakdown (logistics only — penalty excluded):")
        print(f"  Warehouse opening costs  : ${fixed_wh:>14,.2f}")
        print(f"  Arc activation costs     : ${fixed_arc:>14,.2f}")
        print(f"  Variable transport costs : ${var_trans:>14,.2f}")
        print(f"  {'─' * 42}")
        print(f"  Logistics total          : ${logistics_cost:>14,.2f}")
        if STRATEGY in ('R', 'A'):
            print(f"  Penalty (unmet demand)   : ${penalty_cost:>14,.2f}  "
                  f"({total_unmet_units:.1f} units unserved × M={PENALTY_M:,})")
        print(f"  OBJECTIVE (reported)     : ${obj_val:>14,.2f}")
        if disruption_cost is not None:
            print(f"\n  ΔZ = Z(scenario,strategy) − Z*")
            print(f"     = {logistics_cost:,.2f} − {Z_STAR:,.2f} = {disruption_cost:+,.2f}")

        open_wh     = [w for w in W      if round(prob.getSolution(openWarehouse[w])) == 1]
        active_arcs = [a for a in A_fixed if round(prob.getSolution(arc_act[a]))       == 1]
        print(f"\nOpen warehouses ({len(open_wh)}/{len(W)})           : {sorted(open_wh)}")
        print(f"Activated optional arcs ({len(active_arcs)}/{len(A_fixed)}): {sorted(active_arcs)}")

        print(f"\nDemand fulfilment:")
        for p in PRODUCTS:
            delivered   = sum(prob.getSolution(x[(a, p)])
                              for c in C for a in arcs_into.get(c, []) if (a, p) in x)
            total_unmet = sum(prob.getSolution(unmet[(c, pp)])
                              for (c, pp) in unmet if pp == p) if STRATEGY in ('R','A') else 0.0
            total_dem   = sum(v for (_, pp), v in Dem.items() if pp == p)
            print(f"  {p:35s}: delivered={delivered:>7.1f}  unmet={total_unmet:>6.1f}  "
                  f"demand={total_dem:.0f}  ({100*delivered/total_dem:.1f}%)")

        if STRATEGY == 'F' and emergency_arcs:
            print(f"\nEmergency arc usage:")
            for eid in sorted(emergency_arcs):
                src_e = arc_src[eid]
                tgt_e = arc_tgt[eid]
                for p in PRODUCTS:
                    if (eid, p) in x:
                        flow_e = round(prob.getSolution(x[(eid, p)]), 2)
                        if flow_e > 0.01:
                            cost_e = round(emg_var_cost.get((eid, p), 0), 4)
                            print(f"  {eid}: {src_e}→{tgt_e}  {p}  flow={flow_e:.1f}  "
                                  f"unit_cost={cost_e:.4f}")

        # =====================================================================
        # 9. EXPORT TO EXCEL
        # =====================================================================

        print(f"\nExporting solution to {OUTPUT_FILE}...")

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
                    'var_cost':       round(var_cost.get((a, p), emg_var_cost.get((a, p), 0)), 4),
                    'total_cost':     round(total_cost.get((a, p), 0), 4),
                    'flow_cost':      round(flow * total_cost.get((a, p), 0), 2),
                    'transport_mode': arc_mode[a],
                    'distance_km':    arc_dist[a],
                    'emergency':      'YES' if a in emergency_arcs else 'no',
                })
            df = pd.DataFrame(rows)
            product_dfs[p] = df.sort_values('arc_id') if not df.empty else df

        wh_rows = []
        for w in sorted(W):
            opened   = round(prob.getSolution(openWarehouse[w]))
            total_in = round(sum(
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
                'locked_by_R':   'YES' if STRATEGY == 'R' and w in baseline_open_wh else 'no',
            })
        wh_df = pd.DataFrame(wh_rows).sort_values(['open', 'warehouse_id'], ascending=[False, True])

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
                'locked_by_R':    'YES' if STRATEGY == 'R' and a in baseline_active_arc else 'no',
            })
        arc_df = pd.DataFrame(arc_rows).sort_values(['activated', 'arc_id'], ascending=[False, True])

        unmet_rows = []
        if STRATEGY in ('R', 'A'):
            for (c, p), _ in Dem.items():
                u = round(prob.getSolution(unmet[(c, p)]), 2)
                unmet_rows.append({
                    'customer_id':  c,
                    'product':      p,
                    'demand':       Dem[(c, p)],
                    'delivered':    round(Dem[(c, p)] - u, 2),
                    'unmet':        u,
                    'fulfil_%':     round((1 - u / Dem[(c, p)]) * 100, 1) if Dem[(c, p)] > 0 else 100.0,
                })
        unmet_df = pd.DataFrame(unmet_rows).sort_values(['product', 'unmet'], ascending=[True, False]) if unmet_rows else pd.DataFrame(unmet_rows)

        emg_rows = []
        if STRATEGY == 'F':
            for eid in sorted(emergency_arcs):
                for p in PRODUCTS:
                    if (eid, p) in x:
                        flow_e = round(prob.getSolution(x[(eid, p)]), 2)
                        emg_rows.append({
                            'arc_id':      eid,
                            'source':      arc_src[eid],
                            'target':      arc_tgt[eid],
                            'product':     p,
                            'flow':        flow_e,
                            'unit_cost':   emg_var_cost.get((eid, p), 0),
                            'flow_cost':   round(flow_e * emg_var_cost.get((eid, p), 0), 2),
                            'premium_x':   EMERGENCY_PREMIUM,
                        })
        emg_df = pd.DataFrame(emg_rows)

        summary_rows = [
            ('Scenario',                         SCENARIO_KEY),
            ('Strategy',                         STRATEGY),
            ('Solve Time (s)',                    round(solve_time, 3)),
            ('', ''),
            ('Logistics Cost ($)',                round(logistics_cost, 2)),
            ('  Warehouse Opening Cost ($)',      round(fixed_wh, 2)),
            ('  Arc Activation Cost ($)',         round(fixed_arc, 2)),
            ('  Variable Transport Cost ($)',     round(var_trans, 2)),
        ]
        if STRATEGY in ('R', 'A'):
            summary_rows += [
                ('Penalty Cost ($)',              round(penalty_cost, 2)),
                ('  Units Unserved (total)',      round(total_unmet_units, 1)),
                ('  Penalty per unit (M)',        PENALTY_M),
                ('Objective (logistics+penalty)', round(obj_val, 2)),
            ]
        else:
            summary_rows += [
                ('Objective ($)',                 round(obj_val, 2)),
                ('Emergency Premium (×)',         EMERGENCY_PREMIUM),
            ]
        if disruption_cost is not None:
            summary_rows += [
                ('', ''),
                ('Z* Baseline Cost ($)',          round(Z_STAR, 2)),
                ('ΔZ Disruption Cost ($)',        round(disruption_cost, 2)),
            ]
        summary_rows += [
            ('', ''),
            ('Removed Hub',                       ', '.join(sorted(removed_node_ids))),
            ('Stranded Suppliers',                ', '.join(sorted(stranded.keys()))),
            ('Warehouses Open',                   len(open_wh)),
            ('Warehouses Total',                  len(W)),
            ('Optional Arcs Activated',           len(active_arcs)),
            ('Optional Arcs Total',               len(A_fixed)),
            ('', ''),
        ]
        for p in PRODUCTS:
            delivered = sum(
                prob.getSolution(x[(a, p)])
                for c in C for a in arcs_into.get(c, []) if (a, p) in x
            )
            unmet_p = sum(prob.getSolution(unmet[(c, pp)])
                          for (c, pp) in unmet if pp == p) if STRATEGY in ('R','A') else 0.0
            total_dem = sum(v for (_, pp), v in Dem.items() if pp == p)
            summary_rows.append((
                f'Demand Met — {p}',
                f'{delivered:.0f} / {total_dem:.0f}  (unmet: {unmet_p:.0f})'
                if STRATEGY in ('R', 'A') else f'{delivered:.0f} / {total_dem:.0f}'
            ))

        summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

        os.makedirs(os.path.dirname(OUTPUT_FILE) if os.path.dirname(OUTPUT_FILE) else '.', exist_ok=True)
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

        print(f"Done — results in {OUTPUT_FILE}")
