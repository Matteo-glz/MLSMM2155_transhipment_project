"""
GlobalFlow — Resilient Baseline Analysis (v2 — Complete)
=========================================================
Identical to resilient_baseline.py but with two corrections:

  1. All four stranded suppliers receive a synthetic bypass arc to H2:
       SYN_S4_H2  Taipei    → Dubai
       SYN_S5_H2  Seoul     → Dubai
       SYN_S6_H2  Yokohama  → Dubai
       SYN_S9_H2  Chengdu   → Dubai

  2. PESSIMISTIC unit costs — each arc is priced at:
       vc_pess[s] = 1.5 × vc_estimated[s] × (1 + tariff_Asia_ME)
     where vc_estimated[s] = cost_per_km[s] × (dist_S_H3 + dist_H3_H2)
     and cost_per_km[s] is derived from the supplier's existing S→H3 arc.

     This overestimates the true maritime cost by ~50% on top of a
     conservative (via-H3) distance estimate, making the resilience
     case as hard as possible to make. Any saving that survives this
     pessimism is structural, not an artefact of optimistic pricing.

     Critically: for S9 (Chengdu, the cheapest supplier), the pessimistic
     cost is still below the γ=2.5 emergency cost, so deploying the arc
     remains beneficial even under worst-case assumptions.

Two experiments:

  EXPERIMENT 1 — Resilient Baseline (Z*_res)
     Phase 1 re-solved with 10 insurance arcs forced open and
     4 synthetic bypass arcs injected at pessimistic unit cost.

  EXPERIMENT 2 — S1 on Resilient Baseline (Strategy F)
     Scenario S1 (H3 closure) re-solved GREENFIELD on the resilient
     network. Strategy F is used (not R) so the solver can freely
     use the synthetic arcs — with H3 gone, they become the primary
     route for all four stranded suppliers, and C1 is a hard equality.

Output
------
  resilient_baseline_v2.xlsx
"""

import os
import sys
import time
import pandas as pd
import xpress as xp

# =============================================================================
# 0. XPRESS INIT
# =============================================================================
_lic = os.environ.get('XPRESS_LICENSE',
                      '/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')
try:
    xp.init(_lic)
except Exception:
    pass

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
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
os.makedirs(_RESULTS_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(
    os.environ.get('OUTPUT_DIR', _RESULTS_DIR), 'resilient_baseline.xlsx')

if not EXCEL_FILE:
    sys.exit("ERROR: globalflow_instance.xlsx not found. Set EXCEL_FILE env var.")

print("=" * 70)
print("GlobalFlow — Resilient Baseline v2 (4 synthetic arcs, pessimistic costs)")
print("=" * 70)
print(f"  Instance : {EXCEL_FILE}")
print(f"  Output   : {OUTPUT_FILE}")

# =============================================================================
# 2. DATA LOADING
# =============================================================================
print("\nLoading data...")

nodes_df      = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')
arcs_df       = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
warehouses_df = pd.read_excel(EXCEL_FILE, sheet_name='Warehouses')
suppliers_df  = pd.read_excel(EXCEL_FILE, sheet_name='Suppliers')
demand_df     = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
supply_df     = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
costs_df      = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_Baseline')
tariffs_df    = pd.read_excel(EXCEL_FILE, sheet_name='TariffZones')

PRODUCTS       = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME = 300

# =============================================================================
# 3. SHARED PARAMETERS
# =============================================================================
S = set(suppliers_df['supplier_id'])
H = set(nodes_df[nodes_df['type'] == 'HUB']['node_id'])
W = set(warehouses_df['warehouse_id'])
C = set(demand_df['customer_id'].unique())

S_p, supplier_prods = {}, {}
for _, row in supply_df.iterrows():
    S_p.setdefault(row['product'], set()).add(row['supplier_id'])
    supplier_prods.setdefault(row['supplier_id'], set()).add(row['product'])

Dem = {(r['customer_id'], r['product']): r['demand']  for _, r in demand_df.iterrows()}
Sup = {(r['supplier_id'], r['product']): r['supply']  for _, r in supply_df.iterrows()}
wh_cap  = {r['warehouse_id']: r['capacity']     for _, r in warehouses_df.iterrows()}
wh_cost = {r['warehouse_id']: r['opening_cost'] for _, r in warehouses_df.iterrows()}

tariff_rate = {(r['zone_pair_from'], r['zone_pair_to']): r['interzonal_tariff_rate']
               for _, r in tariffs_df.iterrows()}
arc_zone_from_all = dict(zip(arcs_df['arc_id'], arcs_df['zone_from']))
arc_zone_to_all   = dict(zip(arcs_df['arc_id'], arcs_df['zone_to']))
var_cost_baseline = {(r['arc_id'], r['product']): r['variable_cost']
                     for _, r in costs_df.iterrows()}

# =============================================================================
# 4. INSURANCE ARCS
# =============================================================================
INSURANCE_ARCS = ['A185', 'A188', 'A190', 'A199', 'A210',
                   'A223', 'A224', 'A241', 'A251', 'A317']

insurance_fc = {a: arcs_df[arcs_df['arc_id'] == a].iloc[0]['fixed_activation_cost']
                for a in INSURANCE_ARCS
                if not arcs_df[arcs_df['arc_id'] == a].empty}

INSURANCE_COST = sum(insurance_fc.values())
print(f"\n  Insurance arcs : {len(insurance_fc)}  total fc = ${INSURANCE_COST:,.2f}")

# =============================================================================
# 5. SYNTHETIC ARCS — pessimistic individual costs
# =============================================================================
# Distance H3→H2 (arc A020, used as conservative distance proxy)
dist_H3_H2 = float(arcs_df[arcs_df['arc_id'] == 'A020']['distance_km'].values[0])

# Tariff Asia → MiddleEast
tariff_asia_me = tariff_rate.get(('Asia', 'MiddleEast'), 0.05)

# Pessimism factor: applied individually per supplier
PESSIMISM_FACTOR = 1.5

# Original arcs used to derive cost-per-km for each supplier
ORIG_ARCS = {
    'S4': ('A006', 'B_Semiconductors'),
    'S5': ('A007', 'B_Semiconductors'),
    'S6': ('A008', 'B_Semiconductors'),
    'S9': ('A011', 'C_BatteryComponents'),
}

print(f"\n  Synthetic bypass arcs (all → H2/Dubai, pessimistic costs)")
print(f"  Formula: vc_pess = {PESSIMISM_FACTOR} × (cpkm × dist_S_H3_H2) × (1 + {tariff_asia_me:.0%})\n")

SYNTHETIC_ARCS = {}
SYN_VAR_COST   = {}

for sup_id, (orig_arc, prod) in ORIG_ARCS.items():
    orig_row  = arcs_df[arcs_df['arc_id'] == orig_arc].iloc[0]
    dist_s_h3 = float(orig_row['distance_km'])
    dist_total = dist_s_h3 + dist_H3_H2

    bv_orig    = var_cost_baseline.get((orig_arc, prod), 0.0)
    cpkm       = bv_orig / dist_s_h3 if dist_s_h3 > 0 else 0.0
    vc_est     = cpkm * dist_total
    vc_pess    = round(PESSIMISM_FACTOR * vc_est * (1 + tariff_asia_me), 4)
    vc_emg     = round(2.5 * cpkm * dist_total, 4)  # gamma=2.5 standard
    supply_vol = int(supply_df[supply_df['supplier_id'] == sup_id]['supply'].values[0])
    saving_pu  = vc_emg - vc_pess

    arc_id = f'SYN_{sup_id}_H2'
    SYNTHETIC_ARCS[arc_id] = {
        'src': sup_id, 'tgt': 'H2',
        'cap': 99999,  'fc':  0.0,
        'mode': 'sea', 'dist': round(dist_total, 1),
        'zone_from': 'Asia', 'zone_to': 'MiddleEast',
    }
    SYN_VAR_COST[(arc_id, prod)] = vc_pess

    print(f"  {arc_id}  dist≈{dist_total:.0f}km")
    print(f"    vc_estimated = {vc_est:.2f}  →  pess (×{PESSIMISM_FACTOR}×{1+tariff_asia_me:.2f})"
          f" = {vc_pess:.2f}  |  emergency γ=2.5 = {vc_emg:.2f}")
    print(f"    saving = {saving_pu:.2f}/unit × {supply_vol} = ${saving_pu*supply_vol:,.0f}")

# =============================================================================
# 6. NETWORK BUILD HELPER
# =============================================================================

def build_network(removed_arc_ids=None, removed_node_ids=None,
                  override_var_cost=None):
    """
    Build arc lookups and total_cost dict.
    override_var_cost: if provided, used for variable costs instead of baseline.
    """
    removed_arc_ids  = removed_arc_ids  or set()
    removed_node_ids = removed_node_ids or set()
    vc_source = override_var_cost if override_var_cost else var_cost_baseline

    arc_src, arc_tgt, arc_cap, arc_fc, arc_mode, arc_dist = {}, {}, {}, {}, {}, {}
    arc_zf, arc_zt = {}, {}
    all_nodes = set(nodes_df['node_id'])
    arcs_from = {n: set() for n in all_nodes}
    arcs_into = {n: set() for n in all_nodes}

    for _, row in arcs_df.iterrows():
        a = row['arc_id']
        if a in removed_arc_ids: continue
        if row['from_id'] in removed_node_ids or row['to_id'] in removed_node_ids: continue
        arc_src[a]  = row['from_id'];  arc_tgt[a]  = row['to_id']
        arc_cap[a]  = row['shared_capacity']
        arc_fc[a]   = row['fixed_activation_cost']
        arc_mode[a] = row['transport_mode']; arc_dist[a] = row['distance_km']
        arc_zf[a]   = row['zone_from'];      arc_zt[a]   = row['zone_to']
        arcs_from[row['from_id']].add(a);    arcs_into[row['to_id']].add(a)

    # Synthetic arcs (only if endpoints survive)
    for eid, spec in SYNTHETIC_ARCS.items():
        if spec['src'] in removed_node_ids or spec['tgt'] in removed_node_ids: continue
        arc_src[eid]  = spec['src'];  arc_tgt[eid]  = spec['tgt']
        arc_cap[eid]  = spec['cap'];  arc_fc[eid]   = spec['fc']
        arc_mode[eid] = spec['mode']; arc_dist[eid] = spec['dist']
        arc_zf[eid]   = spec['zone_from']; arc_zt[eid] = spec['zone_to']
        arcs_from[spec['src']].add(eid); arcs_into[spec['tgt']].add(eid)

    A_fixed  = {a for a in arc_src if arc_fc[a] > 0}
    A_always = {a for a in arc_src if arc_fc[a] == 0}

    total_cost = {}
    for (a, p), bv in vc_source.items():
        if a in removed_arc_ids or a not in arc_src: continue
        tariff = tariff_rate.get((arc_zf.get(a,''), arc_zt.get(a,'')), 0.0)
        total_cost[(a, p)] = bv * (1.0 + tariff)
    # Synthetic arc costs (already include tariff)
    for (eid, p), vc in SYN_VAR_COST.items():
        if eid in arc_src:
            total_cost[(eid, p)] = vc

    return (arc_src, arc_tgt, arc_cap, arc_fc, arc_mode, arc_dist,
            arcs_from, arcs_into, A_fixed, A_always, total_cost)

# =============================================================================
# 7. MIP SOLVE HELPER
# =============================================================================

def solve_mip(net, S_act, H_act, W_act, C_act, S_p_act, sp_act,
              Dem_act, Sup_act, forced_arcs=None, strategy='F',
              bl_wh_set=None, bl_arc_set=None):

    (arc_src, arc_tgt, arc_cap, arc_fc, arc_mode, arc_dist,
     arcs_from, arcs_into, A_fixed, A_always, total_cost) = net

    forced_arcs = forced_arcs or set()
    bl_wh_set   = bl_wh_set   or set()
    bl_arc_set  = bl_arc_set  or set()

    prob = xp.problem()
    prob.setControl('MAXTIME',    MAX_SOLVE_TIME)
    prob.setControl('OUTPUTLOG',  0)
    prob.setControl('MIPRELSTOP', 1e-7)

    x = {}
    for (a, p) in total_cost:
        src = arc_src[a]
        if src in S_act and p not in sp_act.get(src, set()): continue
        x[(a, p)] = prob.addVariable(name=f'x_{a}_{p}', lb=0, vartype=xp.continuous)

    openWH = {}
    for w in W_act:
        openWH[w] = prob.addVariable(name=f'open_{w}', vartype=xp.binary)

    arc_act = {}
    for a in A_fixed:
        if a in forced_arcs:
            arc_act[a] = prob.addVariable(name=f'arc_{a}', lb=1, ub=1,
                                          vartype=xp.continuous)
        else:
            arc_act[a] = prob.addVariable(name=f'arc_{a}', vartype=xp.binary)

    # Objective — strategy F: pay all fixed costs from scratch
    obj  = xp.Sum(wh_cost[w] * openWH[w]    for w in W_act  if w in openWH)
    obj += xp.Sum(arc_fc[a]  * arc_act[a]   for a in A_fixed if a in arc_act)
    obj += xp.Sum(total_cost[(a, p)] * x[(a, p)] for (a, p) in x)
    prob.setObjective(obj, sense=xp.minimize)

    def inf_(n, p): return xp.Sum(x[(a,p)] for a in arcs_into.get(n,[]) if (a,p) in x)
    def out_(n, p): return xp.Sum(x[(a,p)] for a in arcs_from.get(n,[]) if (a,p) in x)

    for (c, p), d in Dem_act.items():
        prob.addConstraint(xp.constraint(inf_(c, p) == d, name=f'C1_{c}_{p}'))
    for p, ss in S_p_act.items():
        for s in ss:
            if (s, p) in Sup_act:
                prob.addConstraint(xp.constraint(out_(s, p) <= Sup_act[(s,p)],
                                                 name=f'C2_{s}_{p}'))
    for a in A_always:
        prob.addConstraint(xp.constraint(
            xp.Sum(x[(a,p)] for p in PRODUCTS if (a,p) in x) <= arc_cap[a],
            name=f'C3_{a}'))
    for a in A_fixed:
        prob.addConstraint(xp.constraint(
            xp.Sum(x[(a,p)] for p in PRODUCTS if (a,p) in x) <= arc_cap[a]*arc_act[a],
            name=f'C4_{a}'))
    for w in W_act:
        prob.addConstraint(xp.constraint(
            xp.Sum(x[(a,p)] for a in arcs_into[w] for p in PRODUCTS if (a,p) in x)
            <= wh_cap[w] * openWH[w], name=f'C5_{w}'))
    for w in W_act:
        for p in PRODUCTS:
            prob.addConstraint(xp.constraint(inf_(w,p)==out_(w,p), name=f'C6_{w}_{p}'))
    for h in H_act:
        for p in PRODUCTS:
            prob.addConstraint(xp.constraint(inf_(h,p)==out_(h,p), name=f'C7_{h}_{p}'))

    t0 = time.time()
    prob.solve()
    st = prob.attributes.solstatus
    elapsed = time.time() - t0

    if st not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        return st, None, elapsed

    ov      = prob.getObjVal()
    wh_open = {w for w in W_act if round(prob.getSolution(openWH[w])) == 1}
    arcs_on = {a for a in A_fixed if round(prob.getSolution(arc_act[a])) == 1}
    x_sol   = {(a,p): prob.getSolution(x[(a,p)]) for (a,p) in x}
    vtrans  = sum(total_cost[(a,p)] * x_sol[(a,p)] for (a,p) in x_sol)
    fwh     = sum(wh_cost[w] for w in wh_open)
    farc    = sum(arc_fc[a]  for a in arcs_on)

    return st, {'obj': ov, 'open_wh': wh_open, 'active_arcs': arcs_on,
                'x_sol': x_sol, 'var_trans': vtrans, 'fixed_wh': fwh,
                'fixed_arc': farc, 'logistics': fwh + farc + vtrans,
                'solve_time': elapsed}, elapsed

# =============================================================================
# 8. EXPERIMENT 1 — RESILIENT BASELINE
# =============================================================================
print(f"\n{'='*70}")
print("  EXPERIMENT 1: Resilient Baseline (Z*_res)")
print(f"{'='*70}")

net1  = build_network()
st1, sol1, t1 = solve_mip(net1, S, H, W, C, S_p, supplier_prods, Dem, Sup,
                           forced_arcs=set(insurance_fc.keys()))

if sol1 is None:
    sys.exit(f"ERROR: Experiment 1 infeasible (status={st1})")

Z_RES      = sol1['logistics']
Z_STAR_STD = 4_454_800.57
surcharge  = Z_RES - Z_STAR_STD

print(f"  Status   : {st1}  ({t1:.2f}s)")
print(f"  Z*       = ${Z_STAR_STD:>14,.2f}")
print(f"  Z*_res   = ${Z_RES:>14,.2f}")
print(f"  Surcharge= ${surcharge:>+14,.2f}  (+{surcharge/Z_STAR_STD*100:.4f}%)")
print(f"  Open WH  : {sorted(sol1['open_wh'])}")

print("\n  Synthetic arc usage at Z*_res:")
for eid in SYNTHETIC_ARCS:
    for p in PRODUCTS:
        if (eid, p) in sol1['x_sol']:
            fl = sol1['x_sol'][(eid, p)]
            print(f"    {eid} ({p.split('_')[0]}): "
                  f"{'flow='+str(round(fl,1)) if fl>0.01 else 'flow=0 (available, unused at baseline)'}")

# =============================================================================
# 9. EXPERIMENT 2 — S1 ON RESILIENT BASELINE (Strategy F greenfield)
# =============================================================================
print(f"\n{'='*70}")
print("  EXPERIMENT 2: S1 (H3 closure) — Strategy F on Resilient Baseline")
print(f"{'='*70}")

rem_nodes_s1 = set(pd.read_excel(EXCEL_FILE, sheet_name='NodesRemoved_S1')['node_id'])
rem_arcs_s1  = set(pd.read_excel(EXCEL_FILE, sheet_name='ArcsRemoved_S1')['arc_id'])
costs_s1_df  = pd.read_excel(EXCEL_FILE, sheet_name='ArcCosts_S1')

# Build S1 variable cost dict (scenarios sheet overrides baseline)
vc_s1 = dict(var_cost_baseline)
for _, row in costs_s1_df.iterrows():
    if row['arc_id'] not in rem_arcs_s1:
        vc_s1[(row['arc_id'], row['product'])] = row['variable_cost']

print(f"  Removed nodes: {sorted(rem_nodes_s1)}")
print(f"  Removed arcs : {len(rem_arcs_s1)}")
surv_syn = {eid for eid, spec in SYNTHETIC_ARCS.items()
            if spec['src'] not in rem_nodes_s1 and spec['tgt'] not in rem_nodes_s1}
print(f"  Surviving synthetic arcs: {sorted(surv_syn)}")

net2 = build_network(removed_arc_ids=rem_arcs_s1,
                     removed_node_ids=rem_nodes_s1,
                     override_var_cost=vc_s1)

(arc_src2, *_, arcs_from2, arcs_into2, A_fixed2, A_always2, tc2) = net2

S2   = S  - rem_nodes_s1
H2_  = H  - rem_nodes_s1
W2   = W  - rem_nodes_s1
C2   = C  - rem_nodes_s1
Dem2 = {k: v for k,v in Dem.items() if k[0] not in rem_nodes_s1}
Sup2 = {k: v for k,v in Sup.items() if k[0] not in rem_nodes_s1}
Sp2, sprod2 = {}, {}
for _, row in supply_df.iterrows():
    if row['supplier_id'] in rem_nodes_s1: continue
    Sp2.setdefault(row['product'], set()).add(row['supplier_id'])
    sprod2.setdefault(row['supplier_id'], set()).add(row['product'])

forced2 = {a for a in insurance_fc if a in A_fixed2}

st2, sol2, t2 = solve_mip(net2, S2, H2_, W2, C2, Sp2, sprod2, Dem2, Sup2,
                           forced_arcs=forced2)

Z_F_S1_STD = 8_088_281.0

print(f"\n  Status   : {st2}  ({t2:.2f}s)")
if sol2 is None:
    print("  ERROR: Experiment 2 infeasible.")
    print("  Likely cause: H2 outgoing capacity insufficient for all rerouted flows.")
    Z_S1_RES = delta_res = savings = ratio = None
else:
    Z_S1_RES  = sol2['logistics']
    delta_res = Z_S1_RES - Z_RES
    savings   = Z_F_S1_STD - Z_S1_RES
    inv       = Z_RES - Z_STAR_STD
    ratio     = savings / inv if inv > 0 else 0

    print(f"  ZF(S1) standard          = ${Z_F_S1_STD:>14,.2f}  (+81.6%)")
    print(f"  ZF(S1|resilient, pess.)  = ${Z_S1_RES:>14,.2f}  "
          f"(+{delta_res/Z_RES*100:.1f}% vs Z*_res)")
    print(f"  Savings vs standard      = ${savings:>+14,.2f}")
    print(f"  Resilience investment    = ${inv:>+14,.2f}")
    print(f"  Resilience ratio         = ${ratio:,.0f} saved / $1 invested  (pessimistic)")

    print("\n  Synthetic arc usage under S1:")
    for eid in surv_syn:
        for p in PRODUCTS:
            if (eid, p) in sol2['x_sol']:
                fl = sol2['x_sol'][(eid, p)]
                if fl > 0.01:
                    print(f"    {eid} ({p.split('_')[0]}): flow={fl:.1f}  "
                          f"vc={tc2.get((eid,p),0):.2f}")

    print("\n  Demand fulfilment under S1:")
    for p in PRODUCTS:
        del_ = sum(sol2['x_sol'].get((a,p),0)
                   for c in C2 for a in arcs_into2.get(c,[]) if (a,p) in sol2['x_sol'])
        dem_ = sum(v for (c,pp),v in Dem2.items() if pp==p)
        pct  = 100*del_/dem_ if dem_ else 0
        print(f"    {p:35s}: {del_:>7.1f} / {dem_:.0f}  ({pct:.1f}%)")

# =============================================================================
# 10. LATEX SUMMARY
# =============================================================================
print(f"\n{'='*70}")
print("  NUMBERS FOR LATEX REPORT")
print(f"{'='*70}")
print(f"  Pessimism factor               : ×{PESSIMISM_FACTOR} per supplier")
print(f"  Z*  (standard)                 : ${Z_STAR_STD:>14,.2f}")
print(f"  Z*_res (resilient)             : ${Z_RES:>14,.2f}")
print(f"  Resilience surcharge           : ${surcharge:>+14,.2f}"
      f"  (+{surcharge/Z_STAR_STD*100:.4f}%  ≈ +{surcharge/Z_STAR_STD*100:.2f}%)")
print(f"  Insurance arcs fc              : ${INSURANCE_COST:>14,.2f}")
print(f"  ZF(S1) standard                : ${Z_F_S1_STD:>14,.2f}")
if sol2:
    print(f"  ZF(S1|resilient, pessimistic)  : ${Z_S1_RES:>14,.2f}")
    print(f"  ΔZ(S1) standard                : ${Z_F_S1_STD-Z_STAR_STD:>+14,.2f}  (+81.6%)")
    print(f"  ΔZ(S1|resilient)               : ${delta_res:>+14,.2f}"
          f"  (+{delta_res/Z_RES*100:.1f}%)")
    print(f"  Savings under S1               : ${savings:>+14,.2f}")
    print(f"  Resilience ratio               : ${ratio:,.0f} / $1  (pessimistic costs, strategy F)")

# =============================================================================
# 11. EXPORT EXCEL
# =============================================================================
print(f"\nExporting → {OUTPUT_FILE}")

sum_rows = [
    ('METHODOLOGY', ''),
    ('Pessimism factor', PESSIMISM_FACTOR),
    ('Tariff Asia→MiddleEast', tariff_asia_me),
    ('Distance proxy', f'dist_S_H3 + {dist_H3_H2:.0f} km (H3→H2)'),
    ('', ''),
    ('EXPERIMENT 1 — Resilient Baseline', ''),
    ('Z* standard ($)', round(Z_STAR_STD, 2)),
    ('Z*_res ($)', round(Z_RES, 2)),
    ('Surcharge ($)', round(surcharge, 2)),
    ('Surcharge (%)', round(surcharge/Z_STAR_STD*100, 4)),
    ('Insurance arcs fc ($)', round(INSURANCE_COST, 2)),
    ('Warehouses open', str(sorted(sol1['open_wh']))),
    ('', ''),
    ('EXPERIMENT 2 — S1 on Resilient Baseline (F)', ''),
    ('ZF(S1) standard ($)', round(Z_F_S1_STD, 2)),
    ('ZF(S1|resilient) ($)', round(Z_S1_RES, 2) if sol2 else 'infeasible'),
    ('ΔZ(S1) standard ($)', round(Z_F_S1_STD - Z_STAR_STD, 2)),
    ('ΔZ(S1) standard (%)', 81.6),
    ('ΔZ(S1|resilient) ($)', round(delta_res, 2) if sol2 else 'N/A'),
    ('ΔZ(S1|resilient) (%)', round(delta_res/Z_RES*100, 1) if sol2 else 'N/A'),
    ('Savings ($)', round(savings, 2) if sol2 else 'N/A'),
    ('Resilience ratio ($/1$)', round(ratio, 0) if sol2 else 'N/A'),
]

syn_rows = []
for sup_id, (orig_arc, prod) in ORIG_ARCS.items():
    eid = f'SYN_{sup_id}_H2'
    orig_row  = arcs_df[arcs_df['arc_id']==orig_arc].iloc[0]
    dist_tot  = orig_row['distance_km'] + dist_H3_H2
    bv        = var_cost_baseline.get((orig_arc, prod), 0)
    cpkm      = bv / orig_row['distance_km']
    vc_est    = cpkm * dist_tot
    vc_pess   = SYN_VAR_COST.get((eid, prod), 0)
    vc_emg    = 2.5 * cpkm * dist_tot
    vol       = int(supply_df[supply_df['supplier_id']==sup_id]['supply'].values[0])
    syn_rows.append({
        'arc_id':             eid,
        'supplier':           sup_id,
        'product':            prod,
        'dist_km_proxy':      round(dist_tot, 1),
        'vc_estimated':       round(vc_est, 2),
        'vc_pessimistic':     round(vc_pess, 2),
        'vc_emergency_2.5':   round(vc_emg, 2),
        'saving_per_unit':    round(vc_emg - vc_pess, 2),
        'supply_volume':      vol,
        'saving_total_est':   round(max(0, vc_emg-vc_pess)*vol, 0),
    })

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    pd.DataFrame(sum_rows, columns=['Metric','Value']).to_excel(
        writer, sheet_name='Summary', index=False)
    pd.DataFrame(syn_rows).to_excel(
        writer, sheet_name='Synthetic Arcs', index=False)

print(f"Done → {OUTPUT_FILE}")
print("\nAll experiments complete.")