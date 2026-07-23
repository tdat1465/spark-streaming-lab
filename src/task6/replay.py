"""
src/task6/replay.py

Bước 2 của Task 6 — Idempotent Replay Verification.

Mục đích:
  Đọc file đã được mutate (ghi trong output/mutated_file.txt) rồi gọi
  lại cpg_parser.py chỉ cho file đó, để:
    - Emit lại CPG nodes/edges lên topic cpg-nodes, cpg-edges
    - Emit lại metadata lên topic cpg-metadata

  Các message mang đúng stable UUID nên Neo4j sẽ MERGE (không duplicate),
  còn Spark/MongoDB sẽ upsert metadata mới nhất.

Chạy:
    python -m src.task6.replay
"""
import subprocess
import sys
from pathlib import Path

# ── Đường dẫn cơ sở ─────────────────────────────────────────────────────────────────
SRC_ROOT        = Path(__file__).resolve().parent.parent
PROJECT_ROOT    = SRC_ROOT.parent                       # spark-streaming-lab/
PARSER_SCRIPT   = SRC_ROOT / "task2" / "cpg_parser.py"
REPO_ROOT       = PROJECT_ROOT / "peft"                  # spark-streaming-lab/peft/
RECORD_FILE     = PROJECT_ROOT / "output" / "mutated_file.txt"
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_VERSION  = "1.0.0"


def find_python() -> str:
    return sys.executable  # Dùng Python đang chạy để đảm bảo đúng env/packages


if __name__ == "__main__":

    # Kiểm tra file record tồn tại
    if not RECORD_FILE.exists():
        print(f"[LỖI] Không tìm thấy {RECORD_FILE}.")
        print("      Hãy chạy   python -m src.task6.mutate   trước.")
        sys.exit(1)

    mutated_rel = RECORD_FILE.read_text(encoding="utf-8").strip()
    print(f"[INFO] File sẽ reprocess: {mutated_rel}")

    # Kiểm tra file thực sự tồn tại
    if not (REPO_ROOT / mutated_rel).exists():
        print(f"[LỖI] Không tìm thấy file trong repo: {REPO_ROOT / mutated_rel}")
        sys.exit(1)

    python_exe = find_python()

    print(f"[INFO] Gọi cpg_parser.py cho đúng file đã mutate...")
    cmd = [
        python_exe,
        str(PARSER_SCRIPT),
        "--file",              mutated_rel,        # chỉ xử lý 1 file
        "--repo-root",         str(REPO_ROOT),
        "--bootstrap-servers", KAFKA_BOOTSTRAP,
        "--schema-version",    SCHEMA_VERSION,
    ]

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), encoding="utf-8", errors="replace")

    if result.returncode != 0:
        print(f"[LỖI] cpg_parser.py thất bại (exit code: {result.returncode})")
        sys.exit(1)

    print("[OK] Đã emit lại events lên Kafka.")
    print("[INFO] Tiếp theo hãy chạy:")
    print("       python -m src.task6.verify")
