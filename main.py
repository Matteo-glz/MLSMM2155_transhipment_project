"""
GlobalFlow Network Design Optimization - Python + Xpress Template
==================================================================

This template implements the corrected arc-centric network design model
using FICO Xpress as the solver.

Author: Claude
Date: 2026
"""

import pandas as pd
import xpress as xp
xp.init('/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr')
from collections import defaultdict
import time


# ============================================================================
# CONFIGURATION
# ============================================================================

EXCEL_FILE = 'globalflow_instance.xlsx'
SCENARIO = 'ArcCosts_Baseline'  # Change to T1, T2, T3, S1, S2, S3 for shock scenarios
PRODUCTS = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']
MAX_SOLVE_TIME = 300  # seconds


# ============================================================================
# CLASS: GlobalFlowModel
# ============================================================================

class GlobalFlowModel:
    """
    Encapsulates the GlobalFlow network design optimization problem.
    """

    def __init__(self, excel_file, scenario='ArcCosts_Baseline'):
        """Initialize the model by loading data and building data structures."""
        self.excel_file = excel_file
        self.scenario = scenario
        self.prob = None

        # Data containers
        self.nodes = {}
        self.arcs = {}
        self.var_costs = {}
        self.demands = {}
        self.supplies = {}
        self.warehouses = {}
        self.suppliers = set()
        self.hubs = set()
        self.warehouse_set = set()
        self.customers = set()

        # Decision variables
        self.x = {}  # Flow variables
        self.y = {}  # Arc activation variables
        self.w = {}  # Warehouse opening variables

        print("Loading data...")
        self._load_data()
        print("Building model...")
        self._build_model()

    def _load_data(self):
        """Load all data from Excel file into dictionaries."""

        # Load sheets
        nodes_df = pd.read_excel(self.excel_file, sheet_name='Nodes')
        warehouses_df = pd.read_excel(self.excel_file, sheet_name='Warehouses')
        suppliers_df = pd.read_excel(self.excel_file, sheet_name='Suppliers')
        arcs_df = pd.read_excel(self.excel_file, sheet_name='Arcs')
        demand_df = pd.read_excel(self.excel_file, sheet_name='Demand')
        supply_df = pd.read_excel(self.excel_file, sheet_name='Supply')
        costs_df = pd.read_excel(self.excel_file, sheet_name=self.scenario)

        # Build node dictionary
        self.nodes = {row['node_id']: row for _, row in nodes_df.iterrows()}

        # Identify sets
        self.suppliers = set(suppliers_df['supplier_id'].values)
        self.hubs = set(nodes_df[nodes_df['type'] == 'HUB']['node_id'].values)
        self.warehouse_set = set(warehouses_df['warehouse_id'].values)
        self.customers = set(demand_df['customer_id'].unique())

        # Build arc dictionary: key = (source, target)
        for _, row in arcs_df.iterrows():
            source = row['from_id']
            target = row['to_id']
            key = (source, target)

            self.arcs[key] = {
                'arc_id': row['arc_id'],
                'capacity': row['shared_capacity'],
                'fixed_cost': row['fixed_activation_cost'],
                'mode': row['transport_mode'],
                'distance': row['distance_km']
            }

        # Build variable cost dictionary: key = (arc_id, product)
        for _, row in costs_df.iterrows():
            arc_id = row['arc_id']
            product = row['product']
            cost = row['variable_cost']

            self.var_costs[(arc_id, product)] = cost

        # Build demand dictionary: key = (customer_id, product)
        for _, row in demand_df.iterrows():
            customer = row['customer_id']
            product = row['product']
            demand = row['demand']

            self.demands[(customer, product)] = demand

        # Build supply dictionary: key = (supplier_id, product)
        for _, row in supply_df.iterrows():
            supplier = row['supplier_id']
            product = row['product']
            sup = row['supply']

            self.supplies[(supplier, product)] = sup

        # Build warehouse parameters dictionary: key = warehouse_id
        for _, row in warehouses_df.iterrows():
            wh_id = row['warehouse_id']
            self.warehouses[wh_id] = {
                'capacity': row['capacity'],
                'fixed_cost': row['opening_cost']
            }

        # Print summary
        print(f"  Nodes: {len(self.nodes)}")
        print(f"    - Suppliers: {len(self.suppliers)}")
        print(f"    - Hubs: {len(self.hubs)}")
        print(f"    - Warehouses: {len(self.warehouse_set)}")
        print(f"    - Customers: {len(self.customers)}")
        print(f"  Arcs: {len(self.arcs)} (sparse network)")
        print(f"  Arc-Product pairs: {len(self.var_costs)}")
        print(f"  Demands: {len(self.demands)}")
        print(f"  Supplies: {len(self.supplies)}")

    def _build_model(self):
        """Build the Xpress optimization model."""

        self.prob = xp.problem()

        # ====================================================================
        # CREATE DECISION VARIABLES
        # ====================================================================

        # Flow variables: x[arc_id][product]
        for arc_id, product in self.var_costs.keys():
            var_name = f'x_{arc_id}_{product}'
            self.x[(arc_id, product)] = self.prob.addVariable(name=var_name, lb=0, vartype=xp.continuous)

        # Warehouse opening: w[warehouse_id]
        for wh in self.warehouse_set:
            self.w[wh] = self.prob.addVariable(name=f'w_{wh}', vartype=xp.binary)

        # Arc activation: y[arc_id] (ONLY for arcs with fixed cost > 0)
        for (source, target), arc_info in self.arcs.items():
            arc_id = arc_info['arc_id']
            if arc_info['fixed_cost'] > 0:
                self.y[arc_id] = self.prob.addVariable(name=f'y_{arc_id}', vartype=xp.binary)

        print(f"Variables created:")
        print(f"  Flow (x): {len(self.x)}")
        print(f"  Warehouse opening (w): {len(self.w)}")
        print(f"  Arc activation (y): {len(self.y)}")

        # ====================================================================
        # BUILD OBJECTIVE FUNCTION
        # ====================================================================

        obj = 0

        # Warehouse opening costs
        for wh in self.warehouse_set:
            obj += self.warehouses[wh]['fixed_cost'] * self.w[wh]

        # Arc activation costs
        for arc_id in self.y.keys():
            # Find the fixed cost for this arc
            for (source, target), arc_info in self.arcs.items():
                if arc_info['arc_id'] == arc_id:
                    obj += arc_info['fixed_cost'] * self.y[arc_id]
                    break

        # Variable transportation costs
        for (arc_id, product), var in self.x.items():
            cost = self.var_costs[(arc_id, product)]
            obj += cost * var

        self.prob.setObjective(obj, sense=xp.minimize)
        print(f"\nObjective function created")

        # ====================================================================
        # ADD CONSTRAINTS
        # ====================================================================

        constraint_count = 0

        # (C1) Demand satisfaction at customers
        for customer, product in self.demands.keys():
            # Find all arcs that end at this customer with this product
            inflow = xp.Sum(
                self.x[(arc_id, product)]
                for (arc_id, prod) in self.x.keys()
                if prod == product
                for (source, target), arc_info in self.arcs.items()
                if arc_info['arc_id'] == arc_id and target == customer
            )

            self.prob.addConstraint(
                xp.constraint(inflow == self.demands[(customer, product)], name=f'demand_{customer}_{product}')
            )
            constraint_count += 1

        # (C2) Supply availability at suppliers
        for supplier, product in self.supplies.keys():
            # Find all arcs that start at this supplier with this product
            outflow = xp.Sum(
                self.x[(arc_id, product)]
                for (arc_id, prod) in self.x.keys()
                if prod == product
                for (source, target), arc_info in self.arcs.items()
                if arc_info['arc_id'] == arc_id and source == supplier
            )

            self.prob.addConstraint(
                xp.constraint(outflow == self.supplies[(supplier, product)], name=f'supply_{supplier}_{product}')
            )
            constraint_count += 1

        # (C3) Flow conservation at hubs
        for hub in self.hubs:
            for product in PRODUCTS:
                inflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (source, target), arc_info in self.arcs.items()
                    if arc_info['arc_id'] == arc_id and target == hub
                )

                outflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (source, target), arc_info in self.arcs.items()
                    if arc_info['arc_id'] == arc_id and source == hub
                )

                self.prob.addConstraint(
                    xp.constraint(inflow == outflow, name=f'flow_conserv_hub_{hub}_{product}')
                )
                constraint_count += 1

        # (C4) Flow conservation at warehouses (only if open)
        for warehouse in self.warehouse_set:
            for product in PRODUCTS:
                inflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (source, target), arc_info in self.arcs.items()
                    if arc_info['arc_id'] == arc_id and target == warehouse
                )

                outflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (source, target), arc_info in self.arcs.items()
                    if arc_info['arc_id'] == arc_id and source == warehouse
                )

                self.prob.addConstraint(
                    xp.constraint(inflow == outflow, name=f'flow_conserv_wh_{warehouse}_{product}')
                )
                constraint_count += 1

        # (C5) Warehouse capacity constraint
        for warehouse in self.warehouse_set:
            inflow = xp.Sum(
                self.x[(arc_id, product)]
                for (arc_id, product) in self.x.keys()
                for (source, target), arc_info in self.arcs.items()
                if arc_info['arc_id'] == arc_id and target == warehouse
            )

            capacity = self.warehouses[warehouse]['capacity']

            self.prob.addConstraint(
                xp.constraint(inflow <= capacity * self.w[warehouse], name=f'wh_capacity_{warehouse}')
            )
            constraint_count += 1

        # (C6/C7) Arc capacity constraints
        for (source, target), arc_info in self.arcs.items():
            arc_id = arc_info['arc_id']
            capacity = arc_info['capacity']
            fixed_cost = arc_info['fixed_cost']

            # Sum flow across all products on this arc
            flow_sum = xp.Sum(
                self.x[(arc_id, product)]
                for product in PRODUCTS
                if (arc_id, product) in self.x
            )

            if fixed_cost > 0:  # Optional arc: must activate with y
                self.prob.addConstraint(
                    xp.constraint(flow_sum <= capacity * self.y[arc_id], name=f'arc_cap_{arc_id}')
                )
            else:  # Always-active arc
                self.prob.addConstraint(
                    xp.constraint(flow_sum <= capacity, name=f'arc_cap_{arc_id}')
                )

            constraint_count += 1

        print(f"Constraints added: {constraint_count}")

        # Set solver options
        self.prob.setControl('MAXTIME', MAX_SOLVE_TIME)
        self.prob.setControl('OUTPUTLOG', 1)  # Detailed output

    def solve(self):
        """Solve the optimization problem."""
        print(f"\n{'='*70}")
        print(f"SOLVING (Scenario: {self.scenario}, Time limit: {MAX_SOLVE_TIME}s)")
        print(f"{'='*70}\n")

        start_time = time.time()
        self.prob.solve()
        solve_time = time.time() - start_time

        return solve_time

    def report_solution(self):
        """Print a detailed solution report."""

        status = self.prob.getProbStatus()
        print(f"\n{'='*70}")
        print(f"SOLUTION REPORT")
        print(f"{'='*70}")
        print(f"Status: {status}")

        if status == xp.SolStatus.Optimal or status == xp.SolStatus.Feasible:
            obj_val = self.prob.getObjective()
            print(f"Objective Value: {obj_val:,.2f}\n")

            # Decompose objective
            wh_cost = sum(
                self.warehouses[wh]['fixed_cost'] * self.w[wh].getSolution()
                for wh in self.warehouse_set
            )

            arc_cost = sum(
                self.y[arc_id].getSolution() *
                [arc_info['fixed_cost'] for (s, t), arc_info in self.arcs.items() if arc_info['arc_id'] == arc_id][0]
                for arc_id in self.y.keys()
            )

            var_cost = obj_val - wh_cost - arc_cost

            print(f"Cost Breakdown:")
            print(f"  Fixed Warehouse Costs:    {wh_cost:>12,.2f}")
            print(f"  Fixed Arc Activation:     {arc_cost:>12,.2f}")
            print(f"  Variable Transport Costs: {var_cost:>12,.2f}")
            print(f"  {'─'*40}")
            print(f"  Total:                    {obj_val:>12,.2f}\n")

            # Report open warehouses
            print(f"Open Warehouses:")
            open_count = 0
            for wh in self.warehouse_set:
                if self.w[wh].getSolution() > 0.99:
                    open_count += 1
                    cost = self.warehouses[wh]['fixed_cost']
                    print(f"  {wh}: opening_cost={cost:,.2f}")
            if open_count == 0:
                print("  (None)")

            # Report activated arcs (with flow)
            print(f"\nActive Arcs with Flow:")
            arc_count = 0
            for (source, target), arc_info in self.arcs.items():
                arc_id = arc_info['arc_id']
                total_flow = sum(
                    self.x[(arc_id, product)].getSolution()
                    for product in PRODUCTS
                    if (arc_id, product) in self.x
                )

                if total_flow > 0.01:
                    arc_count += 1
                    if arc_count <= 20:  # Print first 20
                        print(f"  {source:>6} -> {target:>6} ({arc_id:>4}): "
                              f"flow={total_flow:>8.1f}, capacity={arc_info['capacity']:>6}, "
                              f"util={(total_flow/arc_info['capacity']*100):>5.1f}%")

            if arc_count > 20:
                print(f"  ... and {arc_count - 20} more arcs")

            # Get LP relaxation info
            try:
                lp_bound = self.prob.getMIPgap()
                print(f"\nLP-IP Gap Information:")
                print(f"  MIP Gap: {lp_bound*100:.2f}%")
            except:
                pass
        else:
            print(f"No feasible solution found.")

    def export_solution(self, filename='globalflow_solution.csv'):
        """Export the solution to CSV."""

        rows = []

        # Export flows
        for (arc_id, product), var in self.x.items():
            flow = var.getSolution()
            if flow > 0.01:
                rows.append({
                    'type': 'flow',
                    'arc_id': arc_id,
                    'product': product,
                    'value': flow
                })

        # Export warehouse decisions
        for wh, var in self.w.items():
            rows.append({
                'type': 'warehouse',
                'warehouse_id': wh,
                'product': None,
                'value': var.getSolution()
            })

        # Export arc activations
        for arc_id, var in self.y.items():
            rows.append({
                'type': 'arc_activation',
                'arc_id': arc_id,
                'product': None,
                'value': var.getSolution()
            })

        df = pd.DataFrame(rows)
        df.to_csv(filename, index=False)
        print(f"\nSolution exported to {filename}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":

    # Create and solve model
    model = GlobalFlowModel(EXCEL_FILE, scenario=SCENARIO)
    solve_time = model.solve()
    model.report_solution()
    model.export_solution('globalflow_solution.csv')

    print(f"\nSolve time: {solve_time:.2f} seconds")