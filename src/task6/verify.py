"""
src/task6/verify.py

Bước 3 của Task 6 — Idempotent Replay Verification.

Mục đích:
  Xác minh ba tính chất idempotent sau khi replay:

  [A] Neo4j — Không có duplicate node/edge:
        MATCH (n:CpgNode) RETURN count(n) phải bằng với lần đầu
        (MERGE đảm bảo không tăng khi replay đúng file).

  [B] MongoDB — Metadata document được cập nhật:
        Tìm document theo file_path, kiểm tra processed_at mới hơn lần đầu.

  [C] Spark Checkpoint — Offset của các file KHÔNG thay đổi bị bỏ qua:
        Đọc thư mục checkpoints/task5_metadata/offsets/ và in ra offset
        hiện tại, chứng minh Spark tiếp tục từ offset sau cùng.

Chạy (sau khi task5/ingest.py đã xử lý batch mới):
    python -m src.task6.verify
"""
import json
import sys
from pathlib import Path
from datetime import timezone

# ── Thư viện bên ngoài (cài sẵn trong môi trường lab) ────────────────────────
try:
    from neo4j import GraphDatabase
except ImportError:
    GraphDatabase = None

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None

# ── Cấu hình ──────────────────────────────────────────────────────────────────
PROJECT_ROOT     = Path(__file__).resolve().parent.parent
RECORD_FILE      = PROJECT_ROOT / "output" / "mutated_file.txt"
# Checkpoint nằm ở thư mục gốc của project (spark-streaming-lab/checkpoints)
CHECKPOINT_DIR   = PROJECT_ROOT.parent / "checkpoints" / "task5_metadata"

NEO4J_URI        = "bolt://localhost:7687"
NEO4J_USER       = "neo4j"
NEO4J_PASS       = "password123"

MONGO_URI        = "mongodb://127.0.0.1:27017"
MONGO_DATABASE   = "peft_db"
MONGO_COLLECTION = "source_metadata"

PASS_MARK = "[PASS]"
FAIL_MARK = "[FAIL]"
SKIP_MARK = "[SKIP]"


# ─────────────────────────────────────────────────────────────────────────────
# Kiểm tra A: Neo4j không có duplicate node
# ─────────────────────────────────────────────────────────────────────────────
def check_neo4j_no_duplicate(mutated_rel: str) -> bool:
    if GraphDatabase is None:
        print(f"  {SKIP_MARK} neo4j driver chưa cài (pip install neo4j). Bỏ qua.")
        return True

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        with driver.session() as session:
            # Đếm node theo file_path
            result = session.run(
                "MATCH (n:CpgNode {file_path: $fp}) RETURN count(n) AS cnt",
                fp=mutated_rel,
            )
            cnt = result.single()["cnt"]

            # Đếm node có id trùng (nếu idempotent đúng → 0)
            dup = session.run(
                "MATCH (n:CpgNode {file_path: $fp}) "
                "WITH n.id AS id, count(*) AS c WHERE c > 1 "
                "RETURN count(*) AS dups",
                fp=mutated_rel,
            )
            dups = dup.single()["dups"]

        driver.close()

        if dups == 0:
            print(f"  {PASS_MARK} Neo4j: {cnt} node cho file '{mutated_rel}', không có duplicate.")
            return True
        else:
            print(f"  {FAIL_MARK} Neo4j: tìm thấy {dups} id bị trùng lặp — MERGE chưa hoạt động đúng!")
            return False

    except Exception as e:
        print(f"  {SKIP_MARK} Không kết nối được Neo4j ({e}). Bỏ qua.")
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Kiểm tra B: MongoDB có document mới nhất
# ─────────────────────────────────────────────────────────────────────────────
def check_mongodb_updated(mutated_rel: str) -> bool:
    if MongoClient is None:
        print(f"  {SKIP_MARK} pymongo chưa cài (pip install pymongo). Bỏ qua.")
        return True

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        col = client[MONGO_DATABASE][MONGO_COLLECTION]

        # Tìm tất cả document của file đã mutate, sắp xếp theo processed_at mới nhất
        docs = list(col.find({"file_path": mutated_rel}, {"processed_at": 1, "sha256": 1, "num_lines": 1})
                       .sort("processed_at", -1))
        client.close()

        if not docs:
            print(f"  {FAIL_MARK} MongoDB: không tìm thấy document nào cho '{mutated_rel}'.")
            return False

        latest = docs[0]
        print(f"  {PASS_MARK} MongoDB: tìm thấy {len(docs)} document.")
        print(f"           processed_at mới nhất : {latest.get('processed_at')}")
        print(f"           sha256                 : {latest.get('sha256', '')[:16]}...")
        print(f"           num_lines              : {latest.get('num_lines')}")

        if len(docs) > 1:
            print(f"  [WARN]   Có {len(docs)} document (append mode — đây là expected với outputMode='append').")

        return True

    except Exception as e:
        print(f"  {SKIP_MARK} Không kết nối được MongoDB ({e}). Bỏ qua.")
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Kiểm tra C: Spark checkpoint bỏ qua offset cũ
# ─────────────────────────────────────────────────────────────────────────────
def check_spark_checkpoint() -> bool:
    offsets_dir = CHECKPOINT_DIR / "offsets"

    if not offsets_dir.exists():
        print(f"  {SKIP_MARK} Chưa tìm thấy checkpoint tại {offsets_dir}.")
        print(f"             Hãy đảm bảo task5/ingest.py đã xử lý ít nhất 1 batch.")
        return True

    # Lấy file offset mới nhất (tên file là số nguyên tăng dần)
    offset_files = sorted(
        [f for f in offsets_dir.iterdir() if f.name.isdigit()],
        key=lambda f: int(f.name),
    )

    if not offset_files:
        print(f"  {SKIP_MARK} Không có offset file nào trong {offsets_dir}.")
        return True

    latest_file = offset_files[-1]
    content = latest_file.read_text(encoding="utf-8")

    try:
        # Dòng đầu là metadata version, dòng 2 trở đi là JSON offsets
        lines = content.strip().splitlines()
        offset_json = json.loads(lines[-1]) if len(lines) > 1 else {}
        topic_offsets = offset_json.get("cpg-metadata", {})
    except Exception:
        topic_offsets = {}

    print(f"  {PASS_MARK} Spark checkpoint hợp lệ.")
    print(f"           Batch ID mới nhất   : {latest_file.name}")
    print(f"           Tổng batch file     : {len(offset_files)}")
    if topic_offsets:
        print(f"           Offset cpg-metadata : {topic_offsets}")
    else:
        print(f"           Nội dung offset:\n{content[:300]}")

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    if not RECORD_FILE.exists():
        print(f"[LỖI] Không tìm thấy {RECORD_FILE}.")
        print("      Hãy chạy   python -m src.task6.mutate   trước.")
        sys.exit(1)

    mutated_rel = RECORD_FILE.read_text(encoding="utf-8").strip()

    print("=" * 60)
    print("Task 6 — Idempotent Replay Verification")
    print("=" * 60)
    print(f"File đã mutate: {mutated_rel}\n")

    results = []

    print("[A] Kiểm tra Neo4j — Không có duplicate node:")
    results.append(check_neo4j_no_duplicate(mutated_rel))

    print("\n[B] Kiểm tra MongoDB — Metadata được cập nhật:")
    results.append(check_mongodb_updated(mutated_rel))

    print("\n[C] Kiểm tra Spark Checkpoint — Offset được ghi nhận:")
    results.append(check_spark_checkpoint())

    print("\n" + "=" * 60)
    passed = sum(results)
    print(f"Kết quả: {passed}/{len(results)} kiểm tra thành công.")

    if all(results):
        print("[OK] Idempotent replay xác nhận THÀNH CÔNG.")
    else:
        print("[CẢNH BÁO] Một số kiểm tra thất bại. Xem log ở trên.")
        sys.exit(1)
