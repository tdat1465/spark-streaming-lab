"""
src/task6/mutate.py

Bước 1 của Task 6 — Idempotent Replay Verification.

Mục đích:
  Sửa đúng MỘT file Python trong repository peft để tạo ra sự thay đổi
  có thể đo lường được (thêm một comment + hằng số mới), rồi ghi lại
  đường dẫn tương đối của file đó vào output/mutated_file.txt để các
  bước sau biết file nào cần reprocess.

Chạy:
    python -m src.task6.mutate
"""
import sys
import datetime
from pathlib import Path

# ── Đường dẫn cơ sở ─────────────────────────────────────────────────────────────────
SRC_ROOT     = Path(__file__).resolve().parent.parent   # src/
PROJECT_ROOT = SRC_ROOT.parent                          # spark-streaming-lab/
REPO_ROOT    = PROJECT_ROOT / "peft"
OUTPUT_DIR   = PROJECT_ROOT / "output"
RECORD_FILE  = OUTPUT_DIR / "mutated_file.txt"

# File cố định sẽ bị mutate — đủ nhỏ, không ảnh hưởng logic thật
TARGET_REL   = "src/peft/__init__.py"


def mutate(repo_root: Path, rel_path: str) -> Path:
    """
    Thêm một comment + hằng số vào cuối file để thay đổi SHA-256 và num_lines.
    Trả về absolute path của file đã sửa.
    """
    abs_path = repo_root / rel_path

    if not abs_path.exists():
        print(f"[LỖI] Không tìm thấy file: {abs_path}")
        sys.exit(1)

    timestamp = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    patch = (
        f"\n"
        f"# --- Task6 idempotent-replay patch ({timestamp}) ---\n"
        f"_TASK6_REPLAY_TS = \"{timestamp}\"\n"
    )

    content = abs_path.read_text(encoding="utf-8", errors="ignore")

    # Xóa patch cũ (nếu đã patch lần trước) để file chỉ có đúng 1 patch
    marker = "# --- Task6 idempotent-replay patch"
    if marker in content:
        content = content[: content.index(marker)]

    abs_path.write_text(content + patch, encoding="utf-8")
    print(f"[OK] Đã mutate file: {abs_path}")
    print(f"     Timestamp patch : {timestamp}")
    return abs_path


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    mutated_abs = mutate(REPO_ROOT, TARGET_REL)

    # Lưu lại đường dẫn tương đối để replay.py dùng
    RECORD_FILE.write_text(TARGET_REL, encoding="utf-8")
    print(f"[OK] Đã ghi đường dẫn vào: {RECORD_FILE}")
    print(f"\n[INFO] Tiếp theo hãy chạy:")
    print(f"       python -m src.task6.replay")
