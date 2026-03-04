import subprocess
import sys

SCRIPTS = [
    "src/visualization/01_trend_plot.py",
    "src/visualization/02_cost_decomposition_plot.py",
    "src/visualization/03_top_providers_excess_plot.py",
    "src/visualization/04_cpt_group_contributions.py",
    "src/visualization/05_residual_distribution_raw_vs_eb.py",
]

def main() -> None:
    for path in SCRIPTS:
        print(f"\nRunning: {path}")
        result = subprocess.run([sys.executable, path], check=False)
        if result.returncode != 0:
            raise SystemExit(f"Failed: {path}")

if __name__ == "__main__":
    main()
