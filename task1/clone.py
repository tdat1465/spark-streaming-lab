"""
Task 1 - Bước 1: Clone repository peft vào thư mục ./peft
"""
import subprocess
import sys
from pathlib import Path

REPO_URL  = "https://github.com/huggingface/peft.git"
REPO_ROOT = Path(__file__).resolve().parent.parent / "peft"


def clone_repo_if_needed(repo_root: Path, clone_url: str) -> None:
    git_dir = repo_root / ".git"

    if git_dir.exists():
        print(f"[INFO] Repository đã tồn tại tại: {repo_root}")
        print("[INFO] Bỏ qua bước clone.")
        return

    repo_root.parent.mkdir(parents=True, exist_ok=True)

    print("[INFO] Repository chưa tồn tại. Bắt đầu clone (shallow --depth 1)...")
    print(f"[INFO] URL      : {clone_url}")
    print(f"[INFO] Thư mục đích: {repo_root}")

    cmd = ["git", "clone", "--depth", "1", clone_url, str(repo_root)]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        print("[git stdout]\n" + result.stdout)
    if result.stderr:
        print("[git stderr]\n" + result.stderr)

    if result.returncode != 0:
        print(f"[LỖI] Git clone thất bại (mã thoát: {result.returncode})")
        sys.exit(1)

    print(f"[OK] Clone thành công -> {repo_root}")


if __name__ == "__main__":
    clone_repo_if_needed(REPO_ROOT, REPO_URL)
    assert REPO_ROOT.exists(), f"[LỖI] Không tìm thấy repository tại {REPO_ROOT}"
    print(f"\n[OK] Repository hợp lệ: {REPO_ROOT}")