import subprocess
import sys
import os
from pathlib import Path

# Đường dẫn đến các tệp và thư mục cần thiết
PROJECT_ROOT    = Path(__file__).resolve().parent.parent
PARSER_SCRIPT   = PROJECT_ROOT / "cpg_parser.py"
DISCOVERED_CSV  = PROJECT_ROOT / "output" / "discovered_files.csv"
REPO_ROOT       = PROJECT_ROOT / "peft"
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_VERSION  = "1.0.0"
LIMIT           = 5


# Tìm Python theo thứ tự ưu tiên
def find_python():
    candidates = [
        os.path.join(os.path.expanduser("~"), "miniconda3", "bin", "python3"),
        os.path.join(os.path.expanduser("~"), "anaconda3",  "bin", "python3"),
        os.path.join(os.path.expanduser("~"), "miniforge3", "bin", "python3"),
    ]

    for c in candidates:
        if os.path.exists(c):
            return c

    # Nếu không tìm thấy thì sử dụng Python hiện tại
    return sys.executable


PYTHON_EXE = find_python()


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

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))

    if result.returncode != 0:
        print(f"[LỖI] Trình phân tích gặp lỗi (mã thoát: {result.returncode})")
        sys.exit(1)

    print("[OK] Hoàn thành việc phát dữ liệu lên Kafka.")
    print("[INFO] Tiếp theo hãy chạy verify.py để kiểm tra dữ liệu trong Neo4j.")