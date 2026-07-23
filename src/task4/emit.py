import subprocess
import sys
import os
from pathlib import Path

# Đường dẫn đến các tệp và thư mục cần thiết
SRC_ROOT        = Path(__file__).resolve().parent.parent
PROJECT_ROOT    = SRC_ROOT.parent
PARSER_SCRIPT   = SRC_ROOT / "task2" / "cpg_parser.py"
# peft/ và output/ đã được di chuyển lên spark-streaming-lab/ (ra ngoài src/)
DISCOVERED_CSV  = PROJECT_ROOT / "output" / "discovered_files.csv"
REPO_ROOT       = PROJECT_ROOT / "peft"
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_VERSION  = "1.0.0"
LIMIT           = 5


# Luôn dùng Python đang chạy hiện tại (đảm bảo đúng environment + đúng phiên bản kafka-python)
PYTHON_EXE = sys.executable


if __name__ == "__main__":

    # Kiểm tra sự tồn tại của các tệp cần thiết
    for p in [PARSER_SCRIPT, DISCOVERED_CSV, REPO_ROOT]:
        if not p.exists():
            print(f"[LỖI] Không tìm thấy: {p}")
            sys.exit(1)

    print(f"[INFO] Bắt đầu phát {LIMIT} tệp lên Kafka ({KAFKA_BOOTSTRAP})...")

    cmd = [
        PYTHON_EXE,
        str(PARSER_SCRIPT),
        "--limit", str(LIMIT),
        "--repo-root", str(REPO_ROOT),
        "--discovered-csv", str(DISCOVERED_CSV),
        "--bootstrap-servers", KAFKA_BOOTSTRAP,
        "--schema-version", SCHEMA_VERSION,
    ]

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), encoding="utf-8", errors="replace")

    if result.returncode != 0:
        print(f"[LỖI] Trình phân tích gặp lỗi (mã thoát: {result.returncode})")
        sys.exit(1)

    print("[OK] Hoàn thành việc phát dữ liệu lên Kafka.")
    print("[INFO] Tiếp theo hãy chạy verify.py để kiểm tra dữ liệu trong Neo4j.")