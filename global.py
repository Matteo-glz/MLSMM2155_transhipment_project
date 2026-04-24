"""
GlobalFlow Network Design Optimization Solver
==============================================

Solves the GlobalFlow logistics network design problem using FICO Xpress.
Implements the corrected model from GLOBALFLOW_MODEL_CORRECTED.tex

Author: Claude
Date: April 2026
"""

import pandas as pd
import xpress as xp
from collections import defaultdict
import time
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

EXCEL_FILE = 'globalflow_instance.xlsx'
SCENARIO = 'ArcCosts_Baseline'  # Change to T1, T2, T3, S1, S2, S3 for other scenarios
MAX_SOLVE_TIME = 300  # seconds
PRODUCTS = ['A_Fertilizers', 'B_Semiconductors', 'C_BatteryComponents']


# ============================================================================
# CLASS: GlobalFlowSolver
# ============================================================================

class GlobalFlowSolver:
    """
    Encapsulates the complete GlobalFlow network design optimization.
    Follows the model in GLOBALFLOW_MODEL_CORRECTED.tex
    """

    def __init__(self, excel_file, scenario='ArcCosts_Baseline'):
        """Initialize solver by loading data."""
        self.excel_file = excel_file
        self.scenario = scenario
        self.prob = None
        self.solve_time = None
        self.objective_value = None

        # Data containers
        self.nodes = {}
        self.arcs = {}
        self.var_costs = {}
        self.demands = {}
        self.supplies = {}
        self.warehouses = {}
        self.tariff_rates = {}

        # Node sets
        self.suppliers = set()
        self.hubs = set()
        self.warehouse_set = set()
        self.customers = set()

        # Product-specific supplier subsets
        self.suppliers_by_product = {}  # product -> set of suppliers
        self.supplier_products = {}     # supplier -> set of products they produce

        # Arc subsets
        self.arc_ids_fixed = set()  # Arcs with activation cost
        self.arc_ids_always = set()  # Always-active arcs
        self.arc_dict = {}  # (source, target) → arc_info

        # Decision variables
        self.x = {}  # Flow variables: x[(arc_id, product)]
        self.y = {}  # Arc activation variables: y[arc_id] (only for fixed arcs)
        self.w = {}  # Warehouse opening variables: w[warehouse_id]

        print("=" * 80)
        print("GlobalFlow Network Design Optimization Solver")
        print("=" * 80)
        print(f"\nLoading data from: {excel_file}")
        print(f"Scenario: {scenario}")
        print()

        self._load_data()
        self._build_model()

    def _load_data(self):
        """Load all data from Excel file."""
        print("Loading data...")

        # Load sheets
        nodes_df = pd.read_excel(self.excel_file, sheet_name='Nodes')
        warehouses_df = pd.read_excel(self.excel_file, sheet_name='Warehouses')
        suppliers_df = pd.read_excel(self.excel_file, sheet_name='Suppliers')
        arcs_df = pd.read_excel(self.excel_file, sheet_name='Arcs')
        demand_df = pd.read_excel(self.excel_file, sheet_name='Demand')
        supply_df = pd.read_excel(self.excel_file, sheet_name='Supply')
        costs_df = pd.read_excel(self.excel_file, sheet_name=self.scenario)
        tariffs_df = pd.read_excel(self.excel_file, sheet_name='TariffZones')

        # Build node dictionary
        self.nodes = {row['node_id']: row for _, row in nodes_df.iterrows()}

        # Identify node sets
        self.suppliers = set(suppliers_df['supplier_id'].values)
        self.hubs = set(nodes_df[nodes_df['type'] == 'HUB']['node_id'].values)
        self.warehouse_set = set(warehouses_df['warehouse_id'].values)
        self.customers = set(demand_df['customer_id'].unique())

        # Build arc dictionary: key = (source, target)
        for _, row in arcs_df.iterrows():
            source = row['from_id']
            target = row['to_id']
            key = (source, target)
            arc_id = row['arc_id']

            self.arc_dict[key] = {
                'arc_id': arc_id,
                'capacity': row['shared_capacity'],
                'fixed_cost': row['fixed_activation_cost'],
                'mode': row['transport_mode'],
                'distance': row['distance_km'],
                'zone_from': row['zone_from'],
                'zone_to': row['zone_to']
            }

            # Classify arc as fixed or always-active
            if row['fixed_activation_cost'] > 0:
                self.arc_ids_fixed.add(arc_id)
            else:
                self.arc_ids_always.add(arc_id)

        # Build variable cost dictionary: key = (arc_id, product)
        for _, row in costs_df.iterrows():
            arc_id = row['arc_id']
            product = row['product']
            cost = row['variable_cost']
            self.var_costs[(arc_id, product)] = cost

        # Build tariff lookup: (zone_from, zone_to) → rate
        for _, row in tariffs_df.iterrows():
            key = (row['zone_pair_from'], row['zone_pair_to'])
            self.tariff_rates[key] = row['interzonal_tariff_rate']

        # Calculate total costs with tariffs: (arc_id, product) → total_cost
        self.total_costs = {}
        for arc_id, product in self.var_costs.keys():
            base_cost = self.var_costs[(arc_id, product)]

            # Find zone pair for this arc
            zone_from = None
            zone_to = None
            for (src, tgt), arc_info in self.arc_dict.items():
                if arc_info['arc_id'] == arc_id:
                    zone_from = arc_info['zone_from']
                    zone_to = arc_info['zone_to']
                    break

            # Look up tariff rate (default 0 if not found)
            tariff_rate = self.tariff_rates.get((zone_from, zone_to), 0.0)

            # Calculate total cost: (1 + tariff) × base_cost
            total_cost = (1 + tariff_rate) * base_cost
            self.total_costs[(arc_id, product)] = total_cost

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
            self.suppliers_by_product.setdefault(product, set()).add(supplier)
            self.supplier_products.setdefault(supplier, set()).add(product)

        # Build warehouse parameters dictionary: key = warehouse_id
        for _, row in warehouses_df.iterrows():
            wh_id = row['warehouse_id']
            self.warehouses[wh_id] = {
                'capacity': row['capacity'],
                'fixed_cost': row['opening_cost']
            }

        # Print data summary
        print(f"  ✓ Nodes: {len(self.nodes)}")
        print(f"    - Suppliers: {len(self.suppliers)}")
        print(f"    - Hubs: {len(self.hubs)}")
        print(f"    - Warehouses: {len(self.warehouse_set)}")
        print(f"    - Customers: {len(self.customers)}")
        print(f"  ✓ Arcs: {len(self.arc_dict)} total")
        print(f"    - Always-active (A_always): {len(self.arc_ids_always)}")
        print(f"    - Optional (A_fixed): {len(self.arc_ids_fixed)}")
        print(f"  ✓ Arc-Product pairs: {len(self.total_costs)}")
        print(f"  ✓ Demands: {len(self.demands)}")
        print(f"  ✓ Supplies: {len(self.supplies)}")
        print(f"  ✓ Tariff zone pairs: {len(self.tariff_rates)}")

    def _build_model(self):
        """Build the Xpress optimization model."""
        print("\nBuilding optimization model...")

        self.prob = xp.problem()

        # ====================================================================
        # CREATE DECISION VARIABLES
        # ====================================================================

        # Build arc_id -> source node lookup
        arc_source = {info['arc_id']: src for (src, tgt), info in self.arc_dict.items()}

        # (C8) Flow variables: x[(arc_id, product)]
        # For supplier arcs, only create variables for products that supplier produces
        for (arc_id, product) in self.total_costs.keys():
            src = arc_source.get(arc_id)
            if src in self.suppliers and product not in self.supplier_products.get(src, set()):
                continue
            var_name = f'x_{arc_id}_{product}'
            self.x[(arc_id, product)] = self.prob.addVariable(name=var_name, lb=0, vartype=xp.continuous)

        # Warehouse opening variables: w[warehouse_id]
        for wh in self.warehouse_set:
            self.w[wh] = self.prob.addVariable(name=f'w_{wh}', vartype=xp.binary)

        # (C9) Arc activation variables: y[arc_id] (ONLY for A_fixed)
        for arc_id in self.arc_ids_fixed:
            self.y[arc_id] = self.prob.addVariable(name=f'y_{arc_id}', vartype=xp.binary)

        print(f"  ✓ Variables created:")
        print(f"    - Flow variables (x): {len(self.x)}")
        print(f"    - Warehouse opening (w): {len(self.w)}")
        print(f"    - Arc activation (y): {len(self.y)}")

        # ====================================================================
        # BUILD OBJECTIVE FUNCTION
        # ====================================================================

        print("\nBuilding objective function...")

        obj = 0

        # Warehouse opening costs
        for wh in self.warehouse_set:
            obj += self.warehouses[wh]['fixed_cost'] * self.w[wh]

        # Arc activation costs (only for A_fixed)
        for arc_id in self.y.keys():
            # Find fixed cost for this arc
            for (src, tgt), arc_info in self.arc_dict.items():
                if arc_info['arc_id'] == arc_id:
                    obj += arc_info['fixed_cost'] * self.y[arc_id]
                    break

        # Variable transportation costs (includes tariffs)
        for (arc_id, product), var in self.x.items():
            cost = self.total_costs[(arc_id, product)]
            obj += cost * var

        self.prob.setObjective(obj, sense=xp.minimize)
        print(f"  ✓ Objective function set (minimize)")

        # ====================================================================
        # ADD CONSTRAINTS
        # ====================================================================

        print("\nAdding constraints...")

        constraint_count = 0

        # (C1) Demand satisfaction at customers
        for customer in self.customers:
            for product in PRODUCTS:
                if (customer, product) not in self.demands:
                    continue

                inflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (src, tgt), arc_info in self.arc_dict.items()
                    if arc_info['arc_id'] == arc_id and tgt == customer
                )

                self.prob.addConstraint(
                    xp.constraint(inflow == self.demands[(customer, product)], name=f'C1_demand_{customer}_{product}')
                )
                constraint_count += 1

        # (C2) Supply availability — one constraint per (product, supplier) pair
        # Only suppliers in suppliers_by_product[p] can ship product p
        for product, sup_set in self.suppliers_by_product.items():
            for supplier in sup_set:
                outflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (src, tgt), arc_info in self.arc_dict.items()
                    if arc_info['arc_id'] == arc_id and src == supplier
                )
                self.prob.addConstraint(
                    xp.constraint(outflow <= self.supplies[(supplier, product)], name=f'C2_supply_{supplier}_{product}')
                )
                constraint_count += 1

        # (C3) Arc capacity for always-active arcs (A_always)
        for arc_id in self.arc_ids_always:
            flow_sum = xp.Sum(
                self.x[(arc_id, product)]
                for product in PRODUCTS
                if (arc_id, product) in self.x
            )

            # Find capacity
            capacity = None
            for (src, tgt), arc_info in self.arc_dict.items():
                if arc_info['arc_id'] == arc_id:
                    capacity = arc_info['capacity']
                    break

            self.prob.addConstraint(
                xp.constraint(flow_sum <= capacity, name=f'C3_arc_cap_always_{arc_id}')
            )
            constraint_count += 1

        # (C4) Arc capacity for optional arcs (A_fixed) linked to activation
        for arc_id in self.arc_ids_fixed:
            flow_sum = xp.Sum(
                self.x[(arc_id, product)]
                for product in PRODUCTS
                if (arc_id, product) in self.x
            )

            # Find capacity
            capacity = None
            for (src, tgt), arc_info in self.arc_dict.items():
                if arc_info['arc_id'] == arc_id:
                    capacity = arc_info['capacity']
                    break

            self.prob.addConstraint(
                xp.constraint(flow_sum <= capacity * self.y[arc_id], name=f'C4_arc_cap_fixed_{arc_id}')
            )
            constraint_count += 1

        # (C5) Warehouse capacity linked to opening
        for warehouse in self.warehouse_set:
            inflow = xp.Sum(
                self.x[(arc_id, product)]
                for (arc_id, product) in self.x.keys()
                for (src, tgt), arc_info in self.arc_dict.items()
                if arc_info['arc_id'] == arc_id and tgt == warehouse
            )

            capacity = self.warehouses[warehouse]['capacity']

            self.prob.addConstraint(
                xp.constraint(inflow <= capacity * self.w[warehouse], name=f'C5_wh_cap_{warehouse}')
            )
            constraint_count += 1

        # (C6) Flow conservation at warehouses
        for warehouse in self.warehouse_set:
            for product in PRODUCTS:
                inflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (src, tgt), arc_info in self.arc_dict.items()
                    if arc_info['arc_id'] == arc_id and tgt == warehouse
                )

                outflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (src, tgt), arc_info in self.arc_dict.items()
                    if arc_info['arc_id'] == arc_id and src == warehouse
                )

                self.prob.addConstraint(
                    xp.constraint(inflow == outflow, name=f'C6_flow_conserv_wh_{warehouse}_{product}')
                )
                constraint_count += 1

        # (C7) Flow conservation at hubs
        for hub in self.hubs:
            for product in PRODUCTS:
                inflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (src, tgt), arc_info in self.arc_dict.items()
                    if arc_info['arc_id'] == arc_id and tgt == hub
                )

                outflow = xp.Sum(
                    self.x[(arc_id, product)]
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (src, tgt), arc_info in self.arc_dict.items()
                    if arc_info['arc_id'] == arc_id and src == hub
                )

                self.prob.addConstraint(
                    xp.constraint(inflow == outflow, name=f'C7_flow_conserv_hub_{hub}_{product}')
                )
                constraint_count += 1

        print(f"  ✓ Constraints added: {constraint_count}")

        # Set solver options
        self.prob.setControl('MAXTIME', MAX_SOLVE_TIME)
        self.prob.setControl('OUTPUTLOG', 1)  # Detailed output

        print(f"\n  Model Summary:")
        print(f"    - Variables: {len(self.x) + len(self.w) + len(self.y)}")
        print(f"    - Constraints: {constraint_count}")
        print(f"    - Max solve time: {MAX_SOLVE_TIME}s")

    def solve(self):
        """Solve the optimization problem."""
        print(f"\n{'=' * 80}")
        print(f"SOLVING")
        print(f"{'=' * 80}\n")

        start_time = time.time()
        self.prob.solve()
        self.solve_time = time.time() - start_time

        return self.solve_time

    def report_solution(self):
        """Print detailed solution report."""
        status = self.prob.attributes.solstatus

        print(f"\n{'=' * 80}")
        print(f"SOLUTION REPORT")
        print(f"{'=' * 80}\n")

        print(f"Status: {status}")

        if status == xp.SolStatus.OPTIMAL or status == xp.SolStatus.FEASIBLE:
            self.objective_value = self.prob.getObjVal()
            print(f"Objective Value: ${self.objective_value:,.2f}\n")

            # Decompose objective value
            wh_cost = sum(
                self.warehouses[wh]['fixed_cost'] * self.prob.getSolution(self.w[wh])
                for wh in self.warehouse_set
            )

            arc_cost = 0
            for arc_id in self.arc_ids_fixed:
                if arc_id in self.y:
                    # Find fixed cost
                    for (src, tgt), arc_info in self.arc_dict.items():
                        if arc_info['arc_id'] == arc_id:
                            arc_cost += arc_info['fixed_cost'] * self.prob.getSolution(self.y[arc_id])
                            break

            var_cost = self.objective_value - wh_cost - arc_cost

            print(f"Cost Breakdown:")
            print(f"  Warehouse Opening Costs:    ${wh_cost:>15,.2f}")
            print(f"  Arc Activation Costs:       ${arc_cost:>15,.2f}")
            print(f"  Variable Transport Costs:   ${var_cost:>15,.2f}")
            print(f"  {'-' * 50}")
            print(f"  Total Cost:                 ${self.objective_value:>15,.2f}\n")

            # Report open warehouses
            print(f"Open Warehouses:")
            open_warehouses = []
            for wh in self.warehouse_set:
                if self.prob.getSolution(self.w[wh]) > 0.99:
                    open_warehouses.append(wh)
                    cost = self.warehouses[wh]['fixed_cost']
                    print(f"  {wh}: ${cost:,.2f}")

            if len(open_warehouses) == 0:
                print(f"  (None)")
            print()

            # Report activated arcs with flow
            print(f"Activated Arcs with Flow (showing top 30):")
            arc_flows = []
            for (src, tgt), arc_info in self.arc_dict.items():
                arc_id = arc_info['arc_id']
                total_flow = sum(
                    self.prob.getSolution(self.x[(arc_id, product)])
                    for product in PRODUCTS
                    if (arc_id, product) in self.x
                )

                if total_flow > 0.01:
                    utilization = (total_flow / arc_info['capacity']) * 100
                    arc_flows.append({
                        'arc_id': arc_id,
                        'source': src,
                        'target': tgt,
                        'flow': total_flow,
                        'capacity': arc_info['capacity'],
                        'utilization': utilization
                    })

            # Sort by flow descending
            arc_flows.sort(key=lambda x: x['flow'], reverse=True)

            for i, arc in enumerate(arc_flows[:30]):
                print(f"  {arc['arc_id']:>4s}: {arc['source']:>6s} → {arc['target']:>6s} | "
                      f"Flow: {arc['flow']:>7.1f} / {arc['capacity']:>6.0f} ({arc['utilization']:>5.1f}%)")

            if len(arc_flows) > 30:
                print(f"  ... and {len(arc_flows) - 30} more arcs with flow")
            print()

            # Report demand delivered to customers (flows on arcs ending at a customer)
            print(f"Demand Delivered by Product:")
            for product in PRODUCTS:
                delivered = sum(
                    self.prob.getSolution(self.x[(arc_id, product)])
                    for (arc_id, prod) in self.x.keys()
                    if prod == product
                    for (_, tgt), arc_info in self.arc_dict.items()
                    if arc_info['arc_id'] == arc_id and tgt in self.customers
                )
                demand_total = sum(v for (_, p), v in self.demands.items() if p == product)
                print(f"  {product:30s}: {delivered:>7.1f} / {demand_total:>7.1f} units")
            print()

            # Get LP relaxation info
            try:
                lp_gap = self.prob.getMIPgap()
                print(f"MIP Gap (LP-IP): {lp_gap*100:.2f}%\n")
            except:
                print(f"LP gap information not available\n")

            print(f"Solve Time: {self.solve_time:.2f} seconds\n")

        else:
            print(f"No feasible solution found.\n")

    def export_solution(self, filename='globalflow_solution.csv'):
        """Export solution to CSV."""
        rows = []

        # Export flows > 0
        for (arc_id, product), var in self.x.items():
            flow = round(self.prob.getSolution(var), 4)
            if flow > 0.01:
                rows.append({
                    'type': 'flow',
                    'arc_id': arc_id,
                    'product': product,
                    'value': flow
                })

        # Export warehouse decisions (round to clean 0/1)
        for wh, var in self.w.items():
            rows.append({
                'type': 'warehouse',
                'warehouse_id': wh,
                'product': None,
                'value': round(self.prob.getSolution(var))
            })

        # Export arc activations
        for arc_id, var in self.y.items():
            rows.append({
                'type': 'arc_activation',
                'arc_id': arc_id,
                'product': None,
                'value': round(self.prob.getSolution(var))
            })

        df = pd.DataFrame(rows)
        df.to_csv(filename, index=False)
        print(f"\nSolution exported to {filename}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    try:
        # Create solver
        solver = GlobalFlowSolver(EXCEL_FILE, scenario=SCENARIO)

        # Solve
        solve_time = solver.solve()

        # Report
        solver.report_solution()

        # Export
        solver.export_solution('globalflow_solution.csv')

    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"ERROR")
        print(f"{'=' * 80}\n")
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)