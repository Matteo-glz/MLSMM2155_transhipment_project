"""
GlobalFlow — Pipeline Runner
============================
Runs all project scripts in the correct dependency order.
Each script remains independently executable — this file
only orchestrates them via subprocess.

Usage
-----
  python main.py                        # run all 7 steps
  python main.py --only phase1 phase2   # run specific steps
  python main.py --skip warehouse       # skip one step
  python main.py --from resilient       # start from a given step
  python main.py --help                 # list available aliases

Step aliases (case-insensitive)
--------------------------------
  phase1      phase1/phase1_solver.py
  mincut      phase1/min_cut.py
  warehouse   phase1/warehouse_analysis.py
  phase2      phase2/phase2_solver.py
  resilient   phase2/resilient_baseline.py
  s4 / suez   phase3/S4_Suez_combined.py
  visualizer  visualizer.py
"""

import argparse
import subprocess
import sys
import time
import os

# =============================================================================
# 1. PIPELINE DEFINITION
# =============================================================================

STEPS = [
    ("phase1",     "phase1/phase1_solver.py"),
    ("mincut",     "phase1/min_cut.py"),
    ("warehouse",  "phase1/warehouse_analysis.py"),
    ("phase2",     "phase2/phase2_solver.py"),
    ("resilient",  "phase2/resilient_baseline.py"),
    ("s4",         "phase3/S4_Suez_combined.py"),
    ("visualizer", "visualizer.py"),
]

# Extra aliases pointing to the same step
ALIASES = {
    "suez": "s4",
}

def resolve_alias(name: str) -> str:
    """Return canonical step name for a given alias."""
    n = name.lower()
    return ALIASES.get(n, n)

def step_index(name: str) -> int:
    canonical = resolve_alias(name)
    for i, (alias, _) in enumerate(STEPS):
        if alias == canonical:
            return i
    raise ValueError(f"Unknown step alias: '{name}'. Run --help for the list.")

# =============================================================================
# 2. CLI
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="GlobalFlow pipeline runner.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    group = p.add_mutually_exclusive_group()
    group.add_argument(
        "--only", nargs="+", metavar="STEP",
        help="Run only the listed steps (space-separated aliases).",
    )
    group.add_argument(
        "--skip", nargs="+", metavar="STEP",
        help="Run all steps EXCEPT the listed ones.",
    )
    group.add_argument(
        "--from", dest="from_step", metavar="STEP",
        help="Start pipeline from this step (inclusive).",
    )
    return p

# =============================================================================
# 3. RUNNER
# =============================================================================

BANNER_WIDTH = 54

def banner(n: int, total: int, script: str) -> None:
    line = "═" * BANNER_WIDTH
    print(f"\n{line}")
    print(f"  [{n}/{total}]  {script}")
    print(f"{line}")

def run_step(script: str) -> tuple[bool, float]:
    """Run a single script. Returns (success, elapsed_seconds)."""
    root = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(root, script)
    t0 = time.time()
    try:
        subprocess.run(
            [sys.executable, path],
            check=True,
            cwd=root,
        )
        return True, time.time() - t0
    except subprocess.CalledProcessError as e:
        return False, time.time() - t0
    except FileNotFoundError:
        print(f"  ERROR: Script not found — {path}")
        return False, time.time() - t0

def ask_continue(script: str) -> bool:
    """Ask user whether to continue after a failure."""
    try:
        ans = input(f"\n  ⚠  '{script}' failed. Continue anyway? [y/N] ").strip().lower()
        return ans in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        return False

# =============================================================================
# 4. MAIN
# =============================================================================

def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    # ── Determine which steps to run ──────────────────────────────────────────
    all_steps = list(STEPS)

    if args.only:
        indices = sorted({step_index(n) for n in args.only})
        selected = [all_steps[i] for i in indices]
    elif args.skip:
        skip_idx = {step_index(n) for n in args.skip}
        selected = [(a, s) for i, (a, s) in enumerate(all_steps) if i not in skip_idx]
    elif args.from_step:
        start = step_index(args.from_step)
        selected = all_steps[start:]
    else:
        selected = all_steps

    if not selected:
        print("No steps to run.")
        return

    total   = len(selected)
    passed  = 0
    failed  = []

    print(f"\n  GlobalFlow Pipeline — {total} step(s) queued")
    print(f"  Python: {sys.executable}\n")

    for n, (alias, script) in enumerate(selected, start=1):
        banner(n, total, script)
        ok, elapsed = run_step(script)

        if ok:
            passed += 1
            print(f"\n  ✓  Done in {elapsed:.1f}s")
        else:
            failed.append(script)
            print(f"\n  ✗  FAILED after {elapsed:.1f}s  (return code ≠ 0)")
            if not ask_continue(script):
                print("\n  Pipeline aborted.")
                break

    # ── Summary ───────────────────────────────────────────────────────────────
    line = "═" * BANNER_WIDTH
    print(f"\n{line}")
    print(f"  PIPELINE COMPLETE")
    print(f"  Ran: {passed + len(failed)}/{total} scripts   "
          f"Failed: {len(failed)}")
    if failed:
        print(f"  Failed scripts:")
        for s in failed:
            print(f"    • {s}")
    else:
        print(f"  All steps completed successfully ✓")
    print(f"{line}\n")


if __name__ == "__main__":
    main()
