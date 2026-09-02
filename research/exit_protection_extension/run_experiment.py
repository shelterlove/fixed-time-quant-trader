from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.exit_protection_extension.experiment import load_settings, run_experiment  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the isolated recent-protection extension study.")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.toml"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    settings = load_settings(args.config.resolve())
    output = args.output.resolve() if args.output else root / "research" / "exit_protection_extension" / "results" / settings.experiment_id
    print(json.dumps(run_experiment(root, settings, output), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
