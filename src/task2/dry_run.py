import subprocess
import sys

from src.task2.config import (
    DEMO_LIMIT,
    DISCOVERED_CSV,
    OUTPUT_DIR,
    PARSER_SCRIPT,
    PROJECT_ROOT,
    REPO_ROOT,
    SCHEMA_VERSION,
)


def dry_run():
    if not PARSER_SCRIPT.exists():
        print(f"[LOI] Khong tim thay: {PARSER_SCRIPT}")
        return False
    if not DISCOVERED_CSV.exists():
        print(f"[LOI] Khong tim thay: {DISCOVERED_CSV}")
        print("[INFO] Hay chay Task 1 truoc de tao discovered_files.csv")
        return False

    print(f"[INFO] Bat dau dry-run Parser Service tren {DEMO_LIMIT} files...")
    cmd_dry = [
        sys.executable,
        str(PARSER_SCRIPT),
        "--dry-run",
        "--limit",
        str(DEMO_LIMIT),
        "--repo-root",
        str(REPO_ROOT),
        "--discovered-csv",
        str(DISCOVERED_CSV),
        "--schema-version",
        SCHEMA_VERSION,
    ]

    proc = subprocess.run(
        cmd_dry,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    if proc.returncode != 0:
        print(f"[LOI] dry-run that bai, exit={proc.returncode}")
        return False

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_dry = OUTPUT_DIR / "task2_dryrun.log"
    log_dry.write_text(proc.stdout, encoding="utf-8")
    print(f"[OK] Da luu log -> {log_dry}")
    return True


if __name__ == "__main__":
    ok = dry_run()
    sys.exit(0 if ok else 1)
