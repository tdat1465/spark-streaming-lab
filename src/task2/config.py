import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

SRC_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ROOT = SRC_ROOT.parent
TASK2_DIR = Path(__file__).resolve().parent
PARSER_SCRIPT = TASK2_DIR / "cpg_parser.py"
DISCOVERED_CSV = PROJECT_ROOT / "output" / "discovered_files.csv"
REPO_ROOT = PROJECT_ROOT / "peft"
OUTPUT_DIR = PROJECT_ROOT / "output"

SCHEMA_VERSION = "1.0.0"
DEMO_LIMIT = 5
