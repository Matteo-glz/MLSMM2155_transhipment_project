"""
analyze_scenarios.py
Reads phase2 Excel outputs and generates LaTeX tables for 05_Robustness_analysis.tex
Covers T1, T2, T3, S2 (feasible scenarios).
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path("/mnt/project")
SCENARIOS = ["T1", "T2", "T3", "S2"]
STRATEGIES = ["R", "A", "F"]
PRODUCTS = {
    "A Fertilizers":        "A (Fertilizers)",
    "B Semiconductors":     "B (Semiconductors)",
    "C BatteryComponents":  "C (Battery Comp.)",
}

# ── helpers ────────────────────────────────────────────────────────────────────

def fmt(v, sign=False):
    """Format a number as $X,XXX"""
    if v is None:
        return "---"
    prefix = "+" if sign and v > 0 else ""
    return f"\\${prefix}{v:,.0f}"

def fmt_pct(v, sign=False):
    prefix = "+" if sign and v > 0 else ""
    return f"{prefix}{v:.1f}\\%"

def get_metric(df_summary, label):
    """Extract a single value from the Summary sheet by matching label substring."""
    mask = df_summary["Metric"].astype(str).str.contains(label, na=False)
    rows = df_summary[mask]
    if rows.empty:
        return None
    val = rows.iloc[0]["Value"]
    try:
        return float(val)
    except (ValueError, TypeError):
        return val

def load_summary(scenario, strategy):
    path = DATA_DIR / f"{scenario}_strategy_{strategy}.xlsx"
    df = pd.read_excel(path, sheet_name="Summary")
    df.columns = ["Metric", "Value"]
    return df

def load_product_flows(scenario, strategy):
    path = DATA_DIR / f"{scenario}_strategy_{strategy}.xlsx"
    costs = {}
    for sheet, label in PRODUCTS.items():
        df = pd.read_excel(path, sheet_name=sheet)
        costs[label] = df["flow_cost"].sum()
    return costs

def load_arc_activations(scenario, strategy):
    path = DATA_DIR / f"{scenario}_strategy_{strategy}.xlsx"
    df = pd.read_excel(path, sheet_name="Arc Activations")
    return df

def load_baseline():
    path = DATA_DIR / "globalflow_solution.xlsx"
    summary = pd.read_excel(path, sheet_name="Summary")
    summary.columns = ["Metric", "Value"]
    costs = {}
    for sheet, label in PRODUCTS.items():
        df = pd.read_excel(path, sheet_name=sheet)
        costs[label] = df["flow_cost"].sum()
    return summary, costs

# ── build cost breakdown table ──────────────────────────────────────────────────

def build_cost_table(scenario):
    bl_sum, bl_prod = load_baseline()

    bl_wh  = 22046.49
    bl_arc = 19638.46
    bl_var = sum(bl_prod.values())
    bl_tot = 4454800.57  # Z*

    rows = {}  # strategy -> dict of components
    for strat in STRATEGIES:
        s = load_summary(scenario, strat)
        pc = load_product_flows(scenario, strat)

        wh_sunk  = get_metric(s, "Sunk baseline WH cost")   or 0
        wh_new   = get_metric(s, "New WH opening cost")      or 0
        arc_sunk = get_metric(s, "Sunk baseline arc cost")   or 0
        arc_new  = get_metric(s, "New arc activation cost")  or 0
        var      = get_metric(s, "Variable transport cost")  or 0
        total    = get_metric(s, "Logistics Cost")

        rows[strat] = {
            "wh_sunk": wh_sunk,
            "wh_new":  wh_new,
            "arc_sunk": arc_sunk,
            "arc_new":  arc_new,
            "var": var,
            "total": total,
            "prod": pc,
        }

    # LaTeX table
    lines = []
    lines.append(r"\begin{table}[H]")
    lines.append(r"\centering")
    lines.append(f"\\caption{{Cost decomposition --- scenario {scenario} (\\$).}}")
    lines.append(f"\\label{{tab:cost_decomp_{scenario.lower()}}}")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{lrrrr}")
    lines.append(r"\toprule")
    lines.append(r"\textbf{Component} & \textbf{Baseline} & \textbf{R} & \textbf{A} & \textbf{F} \\")
    lines.append(r"\midrule")

    def row(label, bl_val, get_val):
        r_val = get_val(rows["R"])
        a_val = get_val(rows["A"])
        f_val = get_val(rows["F"])
        return f"{label} & {fmt(bl_val)} & {fmt(r_val)} & {fmt(a_val)} & {fmt(f_val)} \\\\"

    lines.append(r"\multicolumn{5}{l}{\textit{Fixed costs}} \\")
    lines.append(row("~~WH opening (sunk)",   bl_wh,  lambda r: r["wh_sunk"]))
    lines.append(row("~~WH opening (new)",    0,      lambda r: r["wh_new"]))
    lines.append(row("~~Arc activation (sunk)", bl_arc, lambda r: r["arc_sunk"]))
    lines.append(row("~~Arc activation (new)",  0,      lambda r: r["arc_new"]))
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{5}{l}{\textit{Variable transport costs by product}} \\")
    for label in PRODUCTS.values():
        bl_v = bl_prod[label]
        lines.append(row(f"~~{label}", bl_v, lambda r, l=label: r["prod"][l]))
    lines.append(r"\midrule")

    # totals
    r_tot = rows["R"]["total"]; a_tot = rows["A"]["total"]; f_tot = rows["F"]["total"]
    lines.append(f"\\textbf{{Total $Z$}} & {fmt(bl_tot)} & {fmt(r_tot)} & {fmt(a_tot)} & {fmt(f_tot)} \\\\")
    lines.append(r"\midrule")
    lines.append(f"$\\Delta Z$ vs $Z^*$ & --- & {fmt(r_tot-bl_tot, sign=True)} & {fmt(a_tot-bl_tot, sign=True)} & {fmt(f_tot-bl_tot, sign=True)} \\\\")
    lines.append(f"$\\Delta Z$ (\\%) & --- & {fmt_pct((r_tot-bl_tot)/bl_tot*100, sign=True)} & {fmt_pct((a_tot-bl_tot)/bl_tot*100, sign=True)} & {fmt_pct((f_tot-bl_tot)/bl_tot*100, sign=True)} \\\\")
    lines.append(f"Flex.\ value $Z_R - Z_A$ & --- & \\multicolumn{{2}}{{c}}{{{fmt(r_tot-a_tot)}}} & --- \\\\")
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


# ── build network changes table ─────────────────────────────────────────────────

def build_network_table(scenario):
    bl_sum, _ = load_baseline()

    lines = []
    lines.append(r"\begin{table}[H]")
    lines.append(r"\centering")
    lines.append(f"\\caption{{Network changes vs.\ baseline --- scenario {scenario}.}}")
    lines.append(f"\\label{{tab:network_{scenario.lower()}}}")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{llllr}")
    lines.append(r"\toprule")
    lines.append(r"\textbf{Strategy} & \textbf{Arc} & \textbf{Route} & \textbf{Mode} & \textbf{Fixed cost (\$)} \\")
    lines.append(r"\midrule")

    for strat in STRATEGIES:
        arcs = load_arc_activations(scenario, strat)
        new_arcs = arcs[arcs["status_vs_baseline"] == "newly_activated"]
        deact    = arcs[arcs["status_vs_baseline"] == "deactivated"]

        if new_arcs.empty and deact.empty:
            lines.append(f"{strat} & \\multicolumn{{4}}{{l}}{{No change vs.\ baseline}} \\\\")
        else:
            first = True
            for _, row in new_arcs.iterrows():
                label = strat if first else ""
                lines.append(f"{label} & {row['arc_id']} & {row['source']}$\\to${row['target']} & {row['transport_mode']} & {fmt(row['cost_charged'])} \\\\")
                first = False
            for _, row in deact.iterrows():
                label = strat if first else ""
                lines.append(f"{label} & {row['arc_id']} & {row['source']}$\\to${row['target']} & {row['transport_mode']} & (deactivated) \\\\")
                first = False
        lines.append(r"\midrule")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


# ── build warehouse changes table ───────────────────────────────────────────────

def build_warehouse_table(scenario):
    lines = []
    lines.append(r"\begin{table}[H]")
    lines.append(r"\centering")
    lines.append(f"\\caption{{Warehouse status --- scenario {scenario}.}}")
    lines.append(f"\\label{{tab:wh_{scenario.lower()}}}")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{llrr}")
    lines.append(r"\toprule")
    lines.append(r"\textbf{Strategy} & \textbf{Change} & \textbf{Warehouse(s)} & \textbf{Cost (\$)} \\")
    lines.append(r"\midrule")

    for strat in STRATEGIES:
        path = DATA_DIR / f"{scenario}_strategy_{strat}.xlsx"
        df = pd.read_excel(path, sheet_name="Warehouses")
        opened = df[df["status_vs_baseline"] == "newly_opened"]
        closed = df[df["status_vs_baseline"] == "closed"]

        if opened.empty and closed.empty:
            lines.append(f"{strat} & No change & --- & --- \\\\")
        else:
            for _, row in opened.iterrows():
                lines.append(f"{strat} & Opened & {row['warehouse_id']} & {fmt(row['cost_charged'])} \\\\")
            for _, row in closed.iterrows():
                lines.append(f"{strat} & Closed & {row['warehouse_id']} & --- \\\\")
        lines.append(r"\midrule")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


# ── build R flow-shift narrative ────────────────────────────────────────────────

def build_r_flow_narrative(scenario):
    """Compare flow volumes per arc between baseline and R to find significant shifts."""
    bl_path = DATA_DIR / "globalflow_solution.xlsx"
    sc_path = DATA_DIR / f"{scenario}_strategy_R.xlsx"

    shifts = []
    for sheet, label in PRODUCTS.items():
        bl_df = pd.read_excel(bl_path, sheet_name=sheet)[["arc_id","source","target","flow","flow_cost"]]
        sc_df = pd.read_excel(sc_path, sheet_name=sheet)[["arc_id","source","target","flow","flow_cost"]]
        merged = bl_df.merge(sc_df, on=["arc_id","source","target"], suffixes=("_bl","_sc"))
        merged["delta_flow"] = merged["flow_sc"] - merged["flow_bl"]
        merged["delta_cost"] = merged["flow_cost_sc"] - merged["flow_cost_bl"]
        significant = merged[merged["delta_flow"].abs() > 10].copy()
        significant["product"] = label
        shifts.append(significant)

    if not shifts:
        return "% No significant flow shifts for R\n"

    all_shifts = pd.concat(shifts).sort_values("delta_cost", ascending=False)

    lines = []
    lines.append(r"\begin{table}[H]")
    lines.append(r"\centering")
    lines.append(f"\\caption{{Significant flow shifts under R --- scenario {scenario} (arcs with $|\\Delta \\text{{flow}}| > 10$ units).}}")
    lines.append(f"\\label{{tab:r_shifts_{scenario.lower()}}}")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{llrrrr}")
    lines.append(r"\toprule")
    lines.append(r"\textbf{Arc} & \textbf{Product} & \textbf{Flow (BL)} & \textbf{Flow (R)} & $\Delta$\textbf{Flow} & $\Delta$\textbf{Cost (\$)} \\")
    lines.append(r"\midrule")
    for _, row in all_shifts.iterrows():
        sign = "+" if row["delta_flow"] > 0 else ""
        csign = "+" if row["delta_cost"] > 0 else ""
        lines.append(
            f"{row['arc_id']} ({row['source']}$\\to${row['target']}) & "
            f"{row['product']} & "
            f"{row['flow_bl']:.0f} & {row['flow_sc']:.0f} & "
            f"{sign}{row['delta_flow']:.0f} & "
            f"{csign}{row['delta_cost']:,.0f} \\\\"
        )
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


# ── main ────────────────────────────────────────────────────────────────────────

output = []
output.append("% ============================================================")
output.append("% AUTO-GENERATED by analyze_scenarios.py — do not edit by hand")
output.append("% ============================================================\n")

for sc in SCENARIOS:
    output.append(f"\n% ─── Scenario {sc} ───────────────────────────────────────────\n")
    output.append(f"\\subsubsection*{{Detailed breakdown --- {sc}}}\n")
    output.append(build_cost_table(sc))
    output.append("\n")
    output.append(build_network_table(sc))
    output.append("\n")
    output.append(build_warehouse_table(sc))
    output.append("\n")
    output.append(build_r_flow_narrative(sc))
    output.append("\n")

result = "\n".join(output)
out_path = Path("/home/claude/scenario_tables.tex")
out_path.write_text(result)
print(f"Written {len(result)} chars to {out_path}")
print("Done.")