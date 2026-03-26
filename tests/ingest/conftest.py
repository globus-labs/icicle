import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
INGEST_ROOT = REPO_ROOT / "src" / "ingest"

for path in (REPO_ROOT, INGEST_ROOT):
    str_path = str(path)
    if str_path not in sys.path:
        sys.path.insert(0, str_path)
