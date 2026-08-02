import sys
from pathlib import Path

# Make `main` (ml_service/main.py) importable regardless of the pytest launch dir.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
