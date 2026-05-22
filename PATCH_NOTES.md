# Patch notes

Add these files to the root of the existing repository:

```text
run_smartingest.py
requirements_smartingest.txt
README_SMARTINGEST.md
configs/smartingest_sources.json
smartingest/__init__.py
smartingest/core.py
smartingest/workers.py
smartingest/orchestrator.py
```

Then run:

```bash
python run_smartingest.py --config configs/smartingest_sources.json
```

