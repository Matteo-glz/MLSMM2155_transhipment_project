"""
strategy_R.py — Rerouting only
"""

import xpress as xp
import pandas as pd

def solve_rerouting(scenario_name, open_wh_baseline, active_arcs_baseline):
    """
    Strategy R: Fix warehouse and arc decisions from baseline.
    Re-optimize flows only.
    
    Parameters:
    -----------
    scenario_name : str
        Sheet name in globalflow_instance.xlsx (e.g., 'ArcCosts_T1')
    open_wh_baseline : set
        Set of warehouse IDs that were open in baseline
    active_arcs_baseline : set
        Set of arc IDs that were active in baseline
    
    Returns:
    --------
    (total_cost, solution_dict)
    """
    
    # Load scenario costs
    costs_scenario = pd.read_excel(
        'data/globalflow_instance.xlsx',
        sheet_name=scenario_name
    )
    
    # ... (Load all other data: arcs, nodes, demand, supply, etc.)
    # This is identical to Phase 1
    
    # Build model (identical to Phase 1)
    prob = xp.problem()
    
    # Decision variables
    x = {}  # flow (free to optimize)
    openWarehouse = {}
    arc_act = {}
    
    # ... (Create variables as in Phase 1)
    
    # Constraints
    # ... (C1–C7 as in Phase 1)
    
    # NEW: Fix warehouse and arc activation variables to baseline values
    for w in W:
        if w in open_wh_baseline:
            prob.addConstraint(openWarehouse[w] == 1)
        else:
            prob.addConstraint(openWarehouse[w] == 0)
    
    for a in A_fixed:
        if a in active_arcs_baseline:
            prob.addConstraint(arc_act[a] == 1)
        else:
            prob.addConstraint(arc_act[a] == 0)
    
    # Objective: ONLY variable transportation costs (fixed costs are sunk)
    obj = xp.Sum(total_cost_scenario[(a, p)] * x[(a, p)] for (a, p) in x)
    prob.setObjective(obj, sense=xp.minimize)
    
    # Solve
    prob.solve()
    
    # Extract and return
    total_cost = prob.getObjVal()
    solution = extract_solution(prob, x, openWarehouse, arc_act)
    
    return total_cost, solution