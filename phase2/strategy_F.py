"""
strategy_F.py — Full redesign
"""

from phase1.phase1_solver import Phase1Solver

def solve_full_redesign(scenario_name):
    """
    Strategy F: Clean-slate redesign using scenario costs.
    This is Phase 1, but with scenario sheet instead of baseline.
    """
    
    solver = Phase1Solver(
        excel_file='data/globalflow_instance.xlsx',
        cost_scenario=scenario_name  # Pass scenario instead of 'ArcCosts_Baseline'
    )
    
    solver.build_and_solve()
    
    total_cost = solver.prob.getObjVal()
    solution = solver.extract_solution()
    
    return total_cost, solution