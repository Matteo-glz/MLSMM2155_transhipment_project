"""
GlobalFlow — Canal Capacity Analysis
=====================================
Generates an Excel report showing exactly what happens to arc capacities
after the x0.40 reduction in scenarios S4.2, S4.3, S5.2, S5.3.

Focus: identifying which supplier arcs become genuine bottlenecks
       (outgoing capacity after reduction < supplier supply volume).

Output: canal_capacity_analysis.xlsx
"""

import os
import pandas as pd

# ── Path resolution (same logic as main solver) ────────────────────────────
_EXCEL_CANDIDATES = [
    os.path.join(os.getcwd(), 'data', 'globalflow_instance.xlsx'),
    os.path.join(os.getcwd(), 'globalflow_instance.xlsx'),
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 '..', 'data', 'globalflow_instance.xlsx'),
]
EXCEL_FILE = next((p for p in _EXCEL_CANDIDATES if os.path.exists(p)), None)
if EXCEL_FILE is None:
    raise FileNotFoundError(
        "globalflow_instance.xlsx not found. Tried:\n  "
        + "\n  ".join(_EXCEL_CANDIDATES))

OUTPUT_FILE = os.path.join(os.getcwd(), 'phase3', 'results',
                           'canal_capacity_analysis.xlsx')
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

CAP_FACTOR = 0.40   # reduction applied in S_2 and S_3 variants

# ── Load data ───────────────────────────────────────────────────────────────
arcs   = pd.read_excel(EXCEL_FILE, sheet_name='Arcs')
supply = pd.read_excel(EXCEL_FILE, sheet_name='Supply')
demand = pd.read_excel(EXCEL_FILE, sheet_name='Demand')
nodes  = pd.read_excel(EXCEL_FILE, sheet_name='Nodes')

# ── Canal arc sets (same definition as phase2_canal_scenarios.py) ────────────
SUEZ_ZONE_PAIRS = {
    ('Europe',      'Asia'),          ('Asia',         'Europe'),
    ('Europe',      'MiddleEast'),    ('MiddleEast',   'Europe'),
    ('MiddleEast',  'Asia'),          ('Asia',         'MiddleEast'),
    ('Africa',      'Europe'),        ('Africa',       'MiddleEast'),
    ('EastEurope',  'MiddleEast'),    ('EastEurope',   'Europe'),
    ('Europe',      'SouthAmerica'),  ('SouthAmerica', 'Europe'),
    ('SouthAmerica','MiddleEast'),    ('MiddleEast',   'SouthAmerica'),
}
PANAMA_ZONE_PAIRS = {
    ('Americas',     'Asia'),          ('Asia',         'Americas'),
    ('SouthAmerica', 'Asia'),          ('Asia',         'SouthAmerica'),
    ('Americas',     'SouthAmerica'),  ('SouthAmerica', 'Americas'),
    ('Americas',     'Europe'),        ('Europe',       'Americas'),
    ('Americas',     'MiddleEast'),    ('MiddleEast',   'Americas'),
    ('Caribbean',    'Americas'),      ('Caribbean',    'SouthAmerica'),
    ('Americas',     'Americas'),
}

sea = arcs[arcs['transport_mode'] == 'sea']
suez_ids   = set(sea[sea.apply(
    lambda r: (r['zone_from'], r['zone_to']) in SUEZ_ZONE_PAIRS, axis=1)
]['arc_id'])
panama_ids = set(sea[sea.apply(
    lambda r: (r['zone_from'], r['zone_to']) in PANAMA_ZONE_PAIRS, axis=1)
]['arc_id'])

print(f"Suez arcs: {len(suez_ids)} | Panama arcs: {len(panama_ids)}")
assert suez_ids.isdisjoint(panama_ids), "ERROR: Suez and Panama sets overlap!"

# ── Sheet 1 — Full arc list with capacities before/after reduction ───────────
def build_arc_sheet(canal_ids, canal_label):
    rows = []
    for _, row in arcs.iterrows():
        a = row['arc_id']
        bl_cap   = row['shared_capacity']
        new_cap  = bl_cap * CAP_FACTOR if a in canal_ids else bl_cap
        affected = a in canal_ids

        rows.append({
            'arc_id':           a,
            'from_id':          row['from_id'],
            'from_name':        row['from_name'],
            'to_id':            row['to_id'],
            'to_name':          row['to_name'],
            'transport_mode':   row['transport_mode'],
            'zone_from':        row['zone_from'],
            'zone_to':          row['zone_to'],
            'distance_km':      row['distance_km'],
            'canal_affected':   'YES' if affected else 'no',
            'cap_baseline':     bl_cap,
            f'cap_x{CAP_FACTOR:.2f}': round(new_cap, 1),
            'cap_reduction':    round(bl_cap - new_cap, 1),
            'cap_reduction_%':  round((1 - CAP_FACTOR) * 100, 0) if affected else 0,
        })
    df = pd.DataFrame(rows)
    # Sort: affected arcs first, then by from_id
    df = df.sort_values(['canal_affected', 'from_id', 'arc_id'],
                        ascending=[False, True, True])
    return df

# ── Sheet 2 — Supplier bottleneck analysis ──────────────────────────────────
def build_supplier_sheet(canal_ids, canal_label):
    rows = []
    for supplier_id in sorted(supply['supplier_id'].unique()):
        s_arcs = arcs[arcs['from_id'] == supplier_id]
        s_supply = supply[supply['supplier_id'] == supplier_id]

        # Total supply volume across all products
        total_supply = s_supply['supply'].sum()
        products     = ', '.join(sorted(s_supply['product'].unique()))

        # Outgoing arcs breakdown
        affected_arcs    = s_arcs[s_arcs['arc_id'].isin(canal_ids)]
        non_affected     = s_arcs[~s_arcs['arc_id'].isin(canal_ids)]

        cap_affected_bl  = affected_arcs['shared_capacity'].sum()
        cap_affected_new = cap_affected_bl * CAP_FACTOR
        cap_non_affected = non_affected['shared_capacity'].sum()
        cap_total_bl     = s_arcs['shared_capacity'].sum()
        cap_total_new    = cap_affected_new + cap_non_affected

        is_bottleneck    = cap_total_new < total_supply
        deficit          = max(0, total_supply - cap_total_new)

        rows.append({
            'supplier_id':             supplier_id,
            'products':                products,
            'total_supply':            total_supply,
            'total_outgoing_arcs':     len(s_arcs),
            f'{canal_label}_arcs':     len(affected_arcs),
            'non_canal_arcs':          len(non_affected),
            'cap_total_baseline':      cap_total_bl,
            f'cap_{canal_label}_baseline': cap_affected_bl,
            f'cap_{canal_label}_x{CAP_FACTOR:.2f}': round(cap_affected_new, 1),
            'cap_non_canal':           cap_non_affected,
            f'cap_total_x{CAP_FACTOR:.2f}': round(cap_total_new, 1),
            'supply_covered_%':        round(cap_total_new / total_supply * 100, 1)
                                       if total_supply > 0 else 100.0,
            'bottleneck':              'YES — INFEASIBLE' if is_bottleneck else 'no',
            'deficit_units':           round(deficit, 1),
            'remedy_needed':           (f'Raise arc cap by ≥{deficit:.0f} units, '
                                        f'or add alternative arc')
                                        if is_bottleneck else '',
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(['bottleneck', 'supplier_id'],
                        ascending=[False, True])
    return df

# ── Sheet 3 — Network-level capacity summary ────────────────────────────────
def build_network_summary(canal_ids, canal_label):
    total_demand = demand['demand'].sum()

    all_cap_bl   = arcs['shared_capacity'].sum()
    canal_cap_bl = sum(arcs.loc[arcs['arc_id'].isin(canal_ids),
                                'shared_capacity'])
    canal_cap_new = canal_cap_bl * CAP_FACTOR
    non_canal_cap = all_cap_bl - canal_cap_bl
    total_cap_new = canal_cap_new + non_canal_cap

    rows = [
        ('Canal',                                canal_label.capitalize()),
        ('Capacity factor applied',              CAP_FACTOR),
        ('', ''),
        ('=== Demand ===',                       ''),
        ('Total demand (all products)',           total_demand),
        ('  A_Fertilizers',
         demand[demand['product']=='A_Fertilizers']['demand'].sum()),
        ('  B_Semiconductors',
         demand[demand['product']=='B_Semiconductors']['demand'].sum()),
        ('  C_BatteryComponents',
         demand[demand['product']=='C_BatteryComponents']['demand'].sum()),
        ('', ''),
        ('=== Network capacity (all arcs) ===',  ''),
        ('Total baseline capacity',              round(all_cap_bl, 0)),
        (f'Canal arcs baseline capacity ({len(canal_ids)} arcs)',
         round(canal_cap_bl, 0)),
        (f'Canal arcs after x{CAP_FACTOR} reduction',
         round(canal_cap_new, 0)),
        ('Non-canal arc capacity (unchanged)',   round(non_canal_cap, 0)),
        (f'Total network capacity after reduction',
         round(total_cap_new, 0)),
        ('Network feasible (total cap ≥ demand)',
         'YES' if total_cap_new >= total_demand else 'NO'),
        ('', ''),
        ('=== Verdict ===',                      ''),
        ('Global capacity >> demand?',
         f'YES ({total_cap_new/total_demand:.0f}x demand)'),
        ('Root cause of infeasibility',
         'Supplier-level arc capacity bottlenecks (see Supplier sheet)'),
        ('Explanation',
         'Some suppliers only connect to the network via canal arcs. '
         'After x0.40 reduction, their total outgoing capacity falls '
         'below their supply volume, making it impossible to route '
         'all their production to hubs regardless of network structure.'),
    ]
    return pd.DataFrame(rows, columns=['Metric', 'Value'])

# ── Sheet 4 — Proposed capacity fix ─────────────────────────────────────────
def build_fix_sheet(canal_ids, canal_label):
    """
    For each bottleneck arc (supplier outgoing, canal-affected),
    show the minimum capacity needed to restore feasibility.
    Two options:
      Option A — raise the arc capacity so that total outgoing cap = supply
      Option B — restore to baseline capacity (x1.0) for supplier arcs only
    """
    rows = []
    for supplier_id in sorted(supply['supplier_id'].unique()):
        s_arcs        = arcs[arcs['from_id'] == supplier_id]
        total_supply  = supply[supply['supplier_id'] == supplier_id]['supply'].sum()
        affected      = s_arcs[s_arcs['arc_id'].isin(canal_ids)]
        non_affected  = s_arcs[~s_arcs['arc_id'].isin(canal_ids)]

        cap_aff_new   = affected['shared_capacity'].sum() * CAP_FACTOR
        cap_non_aff   = non_affected['shared_capacity'].sum()
        cap_total_new = cap_aff_new + cap_non_aff

        if cap_total_new >= total_supply:
            continue   # not a bottleneck, skip

        deficit = total_supply - cap_total_new

        for _, arc_row in affected.iterrows():
            bl_cap   = arc_row['shared_capacity']
            new_cap  = bl_cap * CAP_FACTOR

            # Option A: scale up this arc proportionally to cover deficit
            # (distribute deficit across all affected arcs of this supplier
            #  proportionally to their baseline capacity)
            prop_share = (bl_cap / affected['shared_capacity'].sum()
                          if affected['shared_capacity'].sum() > 0 else 0)
            option_a_cap = new_cap + deficit * prop_share

            rows.append({
                'arc_id':              arc_row['arc_id'],
                'supplier_id':         supplier_id,
                'from_name':           arc_row['from_name'],
                'to_name':             arc_row['to_name'],
                'zone_from':           arc_row['zone_from'],
                'zone_to':             arc_row['zone_to'],
                'supplier_supply':     total_supply,
                'cap_baseline':        bl_cap,
                f'cap_x{CAP_FACTOR:.2f} (scenario)': round(new_cap, 1),
                'total_outgoing_new':  round(cap_total_new, 1),
                'deficit_vs_supply':   round(deficit, 1),
                'option_A_cap (proportional fix)': round(option_a_cap, 1),
                'option_B_cap (restore baseline)': bl_cap,
                'note': (
                    'Option A: distribute deficit proportionally across '
                    'affected arcs. '
                    'Option B: restore this arc to baseline capacity '
                    '(only supplier arcs, not transit arcs).'
                ),
            })

    return pd.DataFrame(rows)

# ── Build all sheets ─────────────────────────────────────────────────────────
print("Building Suez sheets...")
suez_arcs_df    = build_arc_sheet(suez_ids,   'suez')
suez_supply_df  = build_supplier_sheet(suez_ids,   'suez')
suez_summary_df = build_network_summary(suez_ids,  'suez')
suez_fix_df     = build_fix_sheet(suez_ids,   'suez')

print("Building Panama sheets...")
pan_arcs_df    = build_arc_sheet(panama_ids,   'panama')
pan_supply_df  = build_supplier_sheet(panama_ids,   'panama')
pan_summary_df = build_network_summary(panama_ids,  'panama')
pan_fix_df     = build_fix_sheet(panama_ids,   'panama')

# ── Write workbook ───────────────────────────────────────────────────────────
print(f"Writing -> {OUTPUT_FILE}")

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    suez_summary_df.to_excel(writer,  sheet_name='Suez — Network Summary',   index=False)
    suez_supply_df.to_excel(writer,   sheet_name='Suez — Supplier Bottlenecks', index=False)
    suez_fix_df.to_excel(writer,      sheet_name='Suez — Capacity Fix',      index=False)
    suez_arcs_df.to_excel(writer,     sheet_name='Suez — All Arcs',          index=False)
    pan_summary_df.to_excel(writer,   sheet_name='Panama — Network Summary',  index=False)
    pan_supply_df.to_excel(writer,    sheet_name='Panama — Supplier Bottlenecks', index=False)
    pan_fix_df.to_excel(writer,       sheet_name='Panama — Capacity Fix',    index=False)
    pan_arcs_df.to_excel(writer,      sheet_name='Panama — All Arcs',        index=False)

print("Done.")

# ── Console summary ───────────────────────────────────────────────────────────
print()
print("=" * 60)
print("SUEZ x0.40 — SUPPLIER BOTTLENECKS")
print("=" * 60)
bottlenecks_suez = suez_supply_df[suez_supply_df['bottleneck'].str.startswith('YES')]
print(bottlenecks_suez[['supplier_id','products','total_supply',
                         'cap_total_baseline',
                         f'cap_total_x{CAP_FACTOR:.2f}',
                         'deficit_units','bottleneck']].to_string(index=False))

print()
print("=" * 60)
print("PANAMA x0.40 — SUPPLIER BOTTLENECKS")
print("=" * 60)
bottlenecks_pan = pan_supply_df[pan_supply_df['bottleneck'].str.startswith('YES')]
print(bottlenecks_pan[['supplier_id','products','total_supply',
                        'cap_total_baseline',
                        f'cap_total_x{CAP_FACTOR:.2f}',
                        'deficit_units','bottleneck']].to_string(index=False))