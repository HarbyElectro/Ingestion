from __future__ import annotations

import argparse
import json
from pathlib import Path

from smartingest.orchestrator import build_orchestrator, load_sources


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SmartIngest orchestration workflow.")
    parser.add_argument("--config", default="configs/smartingest_sources.json")
    parser.add_argument("--registry", default="metadata/schema_registry.json")
    parser.add_argument("--catalog", default="metadata/catalog.json")
    parser.add_argument("--staging", default="lakehouse/staging")
    parser.add_argument("--lakehouse", default="lakehouse/tables")
    parser.add_argument("--quarantine", default="lakehouse/quarantine")
    parser.add_argument("--output", default="metadata/latest_run.json")
    args = parser.parse_args()

    sources = load_sources(args.config)
    orchestrator = build_orchestrator(args)
    results = orchestrator.run(sources)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"\n[SmartIngest] Run summary saved to {output_path}")


if __name__ == "__main__":
    main()
