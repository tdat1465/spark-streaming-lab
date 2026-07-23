import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

SRC_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ROOT = SRC_ROOT.parent
TASK4_DIR = SRC_ROOT / "task4"
PARSER_SCRIPT = SRC_ROOT / "task2" / "cpg_parser.py"
DISCOVERED_CSV = PROJECT_ROOT / "output" / "discovered_files.csv"
REPO_ROOT = PROJECT_ROOT / "peft"
OUTPUT_DIR = PROJECT_ROOT / "output"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_CONTAINER = "cpg-kafka"
SCHEMA_VERSION = "1.0.0"
DEMO_LIMIT = 5

TOPICS = ["cpg-nodes", "cpg-edges", "cpg-metadata", "cpg-errors"]
