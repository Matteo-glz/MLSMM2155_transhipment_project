"""
strategy_helpers.py
===================
Clean, focused implementations of the three Phase 2 strategies.

This file can be imported or run standalone. Each function has minimal setup
and returns (total_cost, model_dict) for export.
"""

import xpress as xp
import pandas as pd
import time

# =============================================================================
# STRATEGY R: Rerouting Only
# =============================================================================

def strategy_R(model_builder, data, open_wh_baseline, active_arcs_baseline, verbose=True):
    """
    Strategy R: Rerouting only.
    
    Fix warehouse and arc decisions to baseline values.
    Re-optimize flows only.
    Sunk fixed costs are NOT included in objective.
    
    Parameters:
    -----------
    model_builder : callable
        Function that builds and returns model dict with all necessary components.
        Signature: model_builder(data, fixed_wh=None, fixed_arcs=None) -> dict
    data : dict
        Scenario data loaded from Excel
    open_wh_baseline : set
        Warehouses that were open in baseline
    active_arcs_baseline : set
        Arcs that were active in baseline
    verbose : bool
        Print timing info
    
    Returns:
    --------
    (total_cost, model_dict)
        total_cost: float, objective value (variable costs only)
        model_dict: dict with extracted solution
    """
    
    if verbose:
        print("    Strategy R (Rerouting only)...", end=' ', flush=True)
    
    # Build model with fixed warehouse and arc decisions
    model = model_builder(data, fixed_wh=open_wh_baseline, fixed_arcs=active_arcs_baseline)
    
    prob = model['prob']
    x = model['x']
    total_cost_param = model['total_cost']
    
    # Objective: ONLY variable transportation costs (fixed costs are sunk)
    obj = xp.Sum(total_cost_param[(a, p)] * x[(a, p)] for (a, p) in x)
    prob.setObjective(obj, sense=xp.minimize)
    
    t0 = time.time()
    prob.solve()
    elapsed = time.time() - t0
    
    if prob.attributes.solstatus not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        raise RuntimeError(f"Strategy R infeasible (status: {prob.attributes.solstatus})")
    
    cost = prob.getObjVal()
    
    if verbose:
        print(f"${cost:.2f} ({elapsed:.1f}s)")
    
    return cost, model, prob


# =============================================================================
# STRATEGY A: Adaptation
# =============================================================================

def strategy_A(model_builder, data, verbose=True):
    """
    Strategy A: Adaptation.
    
    Allow full network re-optimization:
    - Warehouses can open/close
    - Arcs can activate/deactivate
    - Flows can be re-routed
    
    Include full fixed + variable costs in objective.
    This is a mid-term operational adjustment.
    
    Parameters:
    -----------
    model_builder : callable
        Function that builds and returns model dict.
        Signature: model_builder(data, fixed_wh=None, fixed_arcs=None) -> dict
    data : dict
        Scenario data
    verbose : bool
        Print timing info
    
    Returns:
    --------
    (total_cost, model_dict)
    """
    
    if verbose:
        print("    Strategy A (Adaptation)...", end=' ', flush=True)
    
    # Build model with no constraints (all variables free)
    model = model_builder(data, fixed_wh=None, fixed_arcs=None)
    
    prob = model['prob']
    x = model['x']
    openWarehouse = model['openWarehouse']
    arc_act = model['arc_act']
    wh_cost = model['wh_cost']
    arc_fc = model['arc_fc']
    total_cost_param = model['total_cost']
    W = model['W']
    A_fixed = model['A_fixed']
    
    # Objective: FULL cost (warehouse opening + arc activation + variable transport)
    obj = xp.Sum(wh_cost[w] * openWarehouse[w] for w in W)
    obj += xp.Sum(arc_fc[a] * arc_act[a] for a in A_fixed)
    obj += xp.Sum(total_cost_param[(a, p)] * x[(a, p)] for (a, p) in x)
    
    prob.setObjective(obj, sense=xp.minimize)
    
    t0 = time.time()
    prob.solve()
    elapsed = time.time() - t0
    
    if prob.attributes.solstatus not in (xp.SolStatus.OPTIMAL, xp.SolStatus.FEASIBLE):
        raise RuntimeError(f"Strategy A infeasible (status: {prob.attributes.solstatus})")
    
    cost = prob.getObjVal()
    
    if verbose:
        print(f"${cost:.2f} ({elapsed:.1f}s)")
    
    return cost, model, prob


# =============================================================================
# STRATEGY F: Full Redesign
# =============================================================================

def strategy_F(model_builder, data, verbose=True):
    """
    Strategy F: Full redesign (greenfield).
    
    Identical to Strategy A in terms of decision variables and objective.
    This is a clean-slate re-solve with scenario costs, ignoring sunk costs.
    
    Provides a lower bound on achievable cost under the scenario,
    but the practical cost difference from A is the impact of sunk costs.
    
    Parameters:
    -----------
    model_builder : callable
        Function that builds and returns model dict.
    data : dict
        Scenario data
    verbose : bool
        Print timing info
    
    Returns:
    --------
    (total_cost, model_dict)
    """
    
    if verbose:
        print("    Strategy F (Full redesign)...", end=' ', flush=True)
    
    # Strategy F is mathematically identical to A
    # (both allow all decisions to vary, both include full cost)
    # The conceptual difference is one of framing:
    # - A: "We already paid for some baseline infra; what's the best move now?"
    # - F: "If we had to design from scratch with these costs, what would we do?"
    
    cost, model, prob = strategy_A(model_builder, data, verbose=False)
    
    if verbose:
        elapsed = 0  # Already timed in strategy_A
        print(f"${cost:.2f} (same as A)")
    
    return cost, model, prob


# =============================================================================
# SUMMARY: Cost Comparison
# =============================================================================

def compare_strategies(cost_R, cost_A, cost_F, baseline_cost):
    """
    Compute comparison metrics between the three strategies.
    
    Returns:
    --------
    dict with keys:
      - 'disruption_R', 'disruption_A', 'disruption_F': cost increase vs baseline
      - 'flex_value': cost savings from A vs R (benefit of adaptation)
      - 'sunk_impact': cost increase from F vs A (cost of baseline sunk costs)
    """
    
    return {
        'disruption_R': cost_R - baseline_cost,
        'disruption_A': cost_A - baseline_cost,
        'disruption_F': cost_F - baseline_cost,
        'flex_value': cost_R - cost_A,  # How much adaptation helps
        'sunk_impact': cost_A - cost_F,  # How much sunk costs hurt
    }