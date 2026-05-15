# GlobalFlow Under Pressure
### Network Design, Disruption, and Resilience in a Multi-Commodity Logistics Network

> Graduate course project — Quantitative Decision Making (MLSMM2155) — UCLouvain, Q2 2026

---

## Overview

**GlobalFlow** is an international third-party logistics operator (3PL) managing the end-to-end distribution of goods across a global network of suppliers, international hubs, regional warehouses, and final customers. This project addresses a central strategic question:

> *Is a network that is optimal under normal operating conditions also robust to the geopolitical and economic shocks that characterise today's global trade environment? And if not, at what cost can resilience be achieved?*

The work is structured in three phases:

| Phase | Objective | Scripts |
|---|---|---|
| **Phase 1** | Optimal network design under baseline conditions (MIP) | `phase1/phase1_solver.py`, `phase1/min_cut.py`, `phase1/warehouse_analysis.py` |
| **Phase 2** | Stress-testing under 6 adversarial scenarios × 3 response strategies | `phase2/phase2_solver.py`, `phase2/resilient_baseline.py` |
| **Phase 3** | Free scenario — Suez Canal / Red Sea crisis (S4) | `phase3/S4_Suez_combined.py` |

A unified interactive HTML visualizer (`visualizer.py`) renders all solutions on a world map.

---

## Network Structure

The GlobalFlow network is a directed graph **G = (N, A)** with **59 nodes** across four tiers:

| Tier | Symbol | Count | Role |
|---|---|---|---|
| Suppliers | S | 9 | Origin nodes; always-active arcs to hubs |
| International Hubs | H | 5 | Transit-only transshipment nodes (Frankfurt, Dubai, Singapore, Chicago, São Paulo) |
| Regional Warehouses | W | 15 | Strategic binary open/close decision; capacity-constrained |
| Customers | C | 30 | Final delivery destinations (USA/Canada ×8, Europe ×8, Brazil ×5, Japan ×5, India ×4) |

### Products

Three product families share the same network infrastructure:

| ID | Name | Transport mode | Key exposure |
|---|---|---|---|
| **A** | Fertilizers & agricultural chemicals | Sea / Road | Energy costs, port disruptions |
| **B** | Semiconductors & electronic components | Air | East Asian hub closures, export restrictions |
| **C** | Battery components & energy transition materials | Sea / Air | Tariff shocks, energy cost increases |

---

## Mathematical Model (Phase 1)

The Phase 1 model is a **Mixed-Integer Program (MIP)** that simultaneously optimises:

1. Which regional warehouses to open — binary variables `open_w ∈ {0,1}`
2. Which optional arcs to activate — binary variables `arc_a ∈ {0,1}`
3. How to route each product's flow — continuous variables `x[a,p] ≥ 0`

**Objective:** minimise total cost = warehouse opening costs + arc activation costs + variable transportation costs (including inter-zonal tariffs).

**Constraints:**
- **C1** — Demand satisfaction at each customer node (hard equality)
- **C2** — Supply availability at each supplier node
- **C3** — Arc capacity for always-active arcs
- **C4** — Arc capacity for optional arcs, gated by activation variable (aggregated formulation)
- **C5** — Warehouse capacity, gated by opening variable
- **C6** — Flow conservation at warehouses (per product)
- **C7** — Flow conservation at hubs (per product)

An **LP relaxation analysis** compares the aggregated (C4) vs. disaggregated (C4') formulation to assess bound quality and LP-IP gap.

---

## Phase 2 — Stress Testing

The baseline network G\* is subjected to **6 adversarial scenarios**, each evaluated under **3 response strategies**:

### Scenarios

| ID | Type | Description | Products affected |
|---|---|---|---|
| **T1** | Tariff shock | Trump-era tariff +40% on transatlantic / transpacific corridors | B, C |
| **T2** | Energy crisis | Ukraine/Iran conflict: air +60%, road +20%, sea +10% | All |
| **T3** | Seasonal surcharge | Hub handling fees +30% during Q4 peak season | A, B |
| **S1** | Node disruption | Singapore hub closed (Taiwan Strait crisis) | B, C |
| **S2** | Arc disruption | Korea-to-Europe corridor blocked (South Korea export restrictions) | B |
| **S3** | Combined shock | T2 + S1 simultaneously | All |

### Response Strategies

| Strategy | Description | Fixed cost accounting |
|---|---|---|
| **R — Rerouting** | Baseline infrastructure W\*, A\* held fixed; only flows re-optimised | Sunk (added back at reporting time) |
| **A — Adaptation** | Warehouse/arc decisions freed; only *new* openings/activations charged | Sunk baseline + new fixed costs |
| **F — Full redesign** | Greenfield re-solve from scratch; lower bound on achievable cost | All fixed costs paid |

Disruption cost: **ΔZ = Z(scenario, strategy) − Z\***

---

## Phase 3 — Free Scenario: Suez Crisis (S4)

**S4** models the 2024 Red Sea / Houthi crisis: all sea-mode arcs on Suez-corridor zone pairs receive a **cost shock ×1.80** and a **capacity reduction ×0.40** simultaneously. The same R/A/F framework is applied.

Additionally, `phase2/resilient_baseline.py` implements a **resilient baseline** experiment: the Phase 1 model is re-solved with 10 insurance arcs forced open and 4 synthetic bypass arcs injected for suppliers stranded by an S1-type closure, priced at a pessimistic ×1.5 cost premium. This quantifies the price of proactive resilience investment.

---

## Repository Structure

```
.
├── main.py                          # Pipeline runner — orchestrates all 7 steps
│
├── data/
│   └── globalflow_instance.xlsx     # Master input file (nodes, arcs, costs, scenarios)
│
├── phase1/
│   ├── phase1_solver.py             # MIP solver — baseline optimal design + LP relaxation
│   ├── min_cut.py                   # Max-flow / min-cut vulnerability analysis
│   ├── warehouse_analysis.py        # Combinatorial warehouse failure analysis (k=1..8)
│   └── results/
│       ├── baseline_solution.xlsx
│       ├── mincut_analysis.xlsx
│       └── warehouse_vulnerability.xlsx
│
├── phase2/
│   ├── phase2_solver.py             # Multi-scenario × multi-strategy solver (18 runs)
│   ├── resilient_baseline.py        # Resilient baseline + S1 experiment
│   └── results/
│       ├── scenario_ArcCosts_{KEY}/
│       │   ├── strategy_R.xlsx
│       │   ├── strategy_A.xlsx
│       │   └── strategy_F.xlsx
│       ├── summary_all_scenarios.xlsx
│       └── resilient_baseline.xlsx
│
├── phase3/
│   ├── S4_Suez_combined.py          # S4 Suez crisis scenario (R/A/F)
│   └── results/
│       ├── S4_strategy_R.xlsx
│       ├── S4_strategy_A.xlsx
│       └── S4_strategy_F.xlsx
│
├── visualizer.py                    # Interactive HTML world-map visualizer
└── globalflow_map.html              # Generated output map
```

---

## Running the Pipeline

### Prerequisites

- Python ≥ 3.10
- [FICO Xpress](https://www.fico.com/en/products/fico-xpress-optimization) with a valid licence (`xpauth.xpr`)

Install all Python dependencies at once:

```bash
pip install -r requirements.txt
```

> **Note:** the `xpress` package is bundled with the FICO Xpress suite and requires a valid licence. It cannot be installed independently via `pip`. Make sure Xpress is set up on your system before running the pipeline.

### Run all steps

```bash
python main.py
```

### Selective execution

```bash
# Run only specific steps
python main.py --only phase1 mincut

# Skip a step
python main.py --skip warehouse

# Resume from a given step
python main.py --from phase2
```

### Available step aliases

| Alias | Script |
|---|---|
| `phase1` | `phase1/phase1_solver.py` |
| `mincut` | `phase1/min_cut.py` |
| `warehouse` | `phase1/warehouse_analysis.py` |
| `phase2` | `phase2/phase2_solver.py` |
| `resilient` | `phase2/resilient_baseline.py` |
| `s4` / `suez` | `phase3/S4_Suez_combined.py` |
| `visualizer` | `visualizer.py` |

Each script is also independently executable from the project root:

```bash
python phase1/phase1_solver.py
python phase2/phase2_solver.py
python visualizer.py
```

### Environment variable overrides

| Variable | Description |
|---|---|
| `XPRESS_LICENSE` | Path to `xpauth.xpr` (default: `/Applications/FICO Xpress/xpressmp/bin/xpauth.xpr`) |
| `EXCEL_FILE` | Path to `globalflow_instance.xlsx` |
| `BASELINE_FILE` | Path to `baseline_solution.xlsx` (Phase 2 / Phase 3) |
| `OUTPUT_DIR` | Output directory for result workbooks |

---

## Key Outputs

| File | Content |
|---|---|
| `phase1/results/baseline_solution.xlsx` | Optimal baseline: open warehouses, active arcs, per-product flows, LP relaxation analysis |
| `phase1/results/mincut_analysis.xlsx` | Global and per-product min-cuts; node criticality scores |
| `phase1/results/warehouse_vulnerability.xlsx` | Combinatorial removal analysis (k=1..8): worst subsets, degradation thresholds |
| `phase2/results/scenario_*/strategy_*.xlsx` | Per-run workbook: cost breakdown, ΔZ, warehouse and arc changes, demand fulfilment |
| `phase2/results/summary_all_scenarios.xlsx` | Master cross-scenario comparison table (long + wide format) |
| `phase2/results/resilient_baseline.xlsx` | Resilient baseline cost, synthetic arc specs, S1 experiment results |
| `phase3/results/S4_strategy_*.xlsx` | Suez crisis results for each strategy |
| `globalflow_map.html` | Self-contained interactive Plotly map of the network and all solutions |

---

## Solver Configuration

| Parameter | Value |
|---|---|
| Solver | FICO Xpress (`xpress` Python API) |
| MIP time limit | 300 s per run |
| MIP relative stop | 1 × 10⁻⁷ |
| Output log | Suppressed in Phase 2 / Phase 3 |

---

## Dependencies Summary

| Package | Purpose |
|---|---|
| `xpress` | FICO Xpress MIP solver |
| `pandas` | Data loading (Excel), result export |
| `openpyxl` | Excel read/write engine |
| `networkx` | Max-flow / min-cut analysis (Phase 1) |
| `json`, `os`, `sys`, `time` | Standard library |
