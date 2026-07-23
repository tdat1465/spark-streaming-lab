import subprocess
import sys

from src.task3.config import (
    DEMO_LIMIT,
    DISCOVERED_CSV,
    KAFKA_BOOTSTRAP,
    OUTPUT_DIR,
    PARSER_SCRIPT,
    PROJECT_ROOT,
    REPO_ROOT,
    SCHEMA_VERSION,
)
from src.task3.setup_kafka import is_broker_reachable


def emit():
    if not PARSER_SCRIPT.exists():
        print(f"[LOI] Khong tim thay: {PARSER_SCRIPT}")
        return False
    if not DISCOVERED_CSV.exists():
        print(f"[LOI] Khong tim thay: {DISCOVERED_CSV}")
        return False

    if not is_broker_reachable():
        print(
            "[SKIP] Kafka broker chua san sang tai",
            KAFKA_BOOTSTRAP,
            "— hay chay setup_kafka.py truoc.",
        )
        return False

    cmd_emit = [
        sys.executable,
        str(PARSER_SCRIPT),
        "--limit",
        str(DEMO_LIMIT),
        "--repo-root",
        str(REPO_ROOT),
        "--discovered-csv",
        str(DISCOVERED_CSV),
        "--bootstrap-servers",
        KAFKA_BOOTSTRAP,
        "--schema-version",
        SCHEMA_VERSION,
    ]
    print("CMD:", " ".join(cmd_emit))
    print("-" * 72)

    proc_emit = subprocess.run(
        cmd_emit,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(proc_emit.stdout)
    if proc_emit.stderr:
        print(proc_emit.stderr)
    if proc_emit.returncode != 0:
        print(f"[LOI] Emit Kafka that bai, exit={proc_emit.returncode}")
        return False

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_emit = OUTPUT_DIR / "task3_emit.log"
    log_emit.write_text(proc_emit.stdout, encoding="utf-8")
    print(f"[OK] Da emit va luu log -> {log_emit}")
    return True


if __name__ == "__main__":
    ok = emit()
    sys.exit(0 if ok else 1)
