"""
strategy_A.py — Adaptation
"""
import xpress as xp
import pandas as pd


def solve_adaptation(scenario_name):
    """
    Strategy A: Re-optimize warehouses and arcs given new scenario costs.
    All decision variables are free.
    
    This is nearly identical to Phase 1, but with scenario costs instead of baseline.
    """
    
    # Load scenario costs
    costs_scenario = pd.read_excel(
        'data/globalflow_instance.xlsx',
        sheet_name=scenario_name
    )
    
    # Build the FULL model exactly as Phase 1
    # (all variables free, all constraints present)
    
    # Objective: FULL cost with scenario parameters
    obj = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W) \
        + xp.Sum(arc_fc[a] * arc_act[a] for a in A_fixed) \
        + xp.Sum(total_cost_scenario[(a, p)] * x[(a, p)] for (a, p) in x)
    
    prob.setObjective(obj, sense=xp.minimize)
    prob.solve()
    
    total_cost = prob.getObjVal()
    solution = extract_solution(prob, x, openWarehouse, arc_act)
    
    return total_cost, solution