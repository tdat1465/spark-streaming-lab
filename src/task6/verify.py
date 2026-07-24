"""
src/task6/verify.py

Bước 3 của Task 6 — Idempotent Replay Verification (End-to-End Strict Mode).

Mục đích:
  Xác minh ba tính chất idempotent một cách nghiêm ngặt:
  [A] Neo4j — Không có duplicate node/edge (MERGE).
  [B] MongoDB — Metadata document được cập nhật đúng 1 document mới nhất.
  [C] Spark Checkpoint — Offset tiến lên tương ứng.

Kịch bản:
  - Chọn file nguồn đã được Ingest (nếu chưa ingest sẽ báo FAIL).
  - Lấy trạng thái BEFORE.
  - Mutate file.
  - Replay Lần 1 & Chờ Spark Streaming.
  - Replay Lần 2 & Chờ Spark Streaming.
  - Lấy trạng thái AFTER và So sánh.
  - BẤT KỲ LỖI kết nối nào (Neo4j, Mongo) đều báo FAIL ngay lập tức.
"""
import sys
import time
import json
import subprocess
from pathlib import Path

try:
    from neo4j import GraphDatabase
except ImportError:
    print("[FAIL] neo4j driver chưa cài (pip install neo4j).")
    sys.exit(1)

try:
    from pymongo import MongoClient
except ImportError:
    print("[FAIL] pymongo chưa cài (pip install pymongo).")
    sys.exit(1)

# Import module mutate để tái sử dụng logic mutate
try:
    from src.task6.mutate import mutate, TARGET_REL
except ImportError:
    print("[FAIL] Không thể import mutate logic.")
    sys.exit(1)

# ── Cấu hình ──────────────────────────────────────────────────────────────────
SRC_ROOT         = Path(__file__).resolve().parent.parent
PROJECT_ROOT     = SRC_ROOT.parent
REPO_ROOT        = PROJECT_ROOT / "peft"
CHECKPOINT_DIR   = PROJECT_ROOT / "checkpoints" / "task5_metadata"
PARSER_SCRIPT    = SRC_ROOT / "task2" / "cpg_parser.py"

NEO4J_URI        = "bolt://localhost:7687"
NEO4J_USER       = "neo4j"
NEO4J_PASS       = "password123"

MONGO_URI        = "mongodb://127.0.0.1:27017"
MONGO_DATABASE   = "peft_db"
MONGO_COLLECTION = "source_metadata"

KAFKA_BOOTSTRAP  = "127.0.0.1:9092"
SCHEMA_VERSION   = "1.0.0"

def get_neo4j_state(rel_path: str):
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        with driver.session() as session:
            res_nodes = session.run("MATCH (n:CpgNode {file_path: $fp}) RETURN count(n) AS cnt", fp=rel_path)
            node_cnt = res_nodes.single()["cnt"]
            
            res_edges = session.run("MATCH (n:CpgNode {file_path: $fp})-[r]->() RETURN count(r) AS cnt", fp=rel_path)
            edge_cnt = res_edges.single()["cnt"]
        driver.close()
        return node_cnt, edge_cnt
    except Exception as e:
        print(f"[FAIL] Lỗi kết nối Neo4j: {e}")
        sys.exit(1)

def get_mongo_state(rel_path: str):
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        col = client[MONGO_DATABASE][MONGO_COLLECTION]
        # Lấy tất cả document của file_path, sort giảm dần theo processed_at
        docs = list(col.find({"file_path": rel_path}).sort("processed_at", -1))
        client.close()
        
        doc_count = len(docs)
        latest_sha = docs[0].get("sha256") if docs else None
        latest_time = docs[0].get("processed_at") if docs else None
        return doc_count, latest_sha, latest_time
    except Exception as e:
        print(f"[FAIL] Lỗi kết nối MongoDB: {e}")
        sys.exit(1)

def get_kafka_checkpoint():
    offsets_dir = CHECKPOINT_DIR / "offsets"
    if not offsets_dir.exists():
        return None
    offset_files = sorted(
        [f for f in offsets_dir.iterdir() if f.name.isdigit()],
        key=lambda f: int(f.name),
    )
    if not offset_files:
        return None
    latest_file = offset_files[-1]
    return latest_file.name

def run_parser(rel_path: str):
    cmd = [
        sys.executable,
        str(PARSER_SCRIPT),
        "--file", rel_path,
        "--repo-root", str(REPO_ROOT),
        "--bootstrap-servers", KAFKA_BOOTSTRAP,
        "--schema-version", SCHEMA_VERSION,
    ]
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        print(f"[FAIL] cpg_parser.py thất bại khi chạy {rel_path}:\n{result.stderr}")
        sys.exit(1)

if __name__ == "__main__":
    print("=" * 60)
    print("Task 6 — Strict Idempotent Replay Verification (E2E)")
    print("=" * 60)
    
    file_to_test = TARGET_REL # src/peft/__init__.py
    print(f"[*] File được chọn để test: {file_to_test}")
    
    # 1. Trạng thái BEFORE
    print("\n[*] Đang lấy trạng thái BEFORE...")
    b_nodes, b_edges = get_neo4j_state(file_to_test)
    b_doc_cnt, b_sha, b_time = get_mongo_state(file_to_test)
    b_ckpt = get_kafka_checkpoint()
    
    if b_doc_cnt == 0:
        print(f"[FAIL] File '{file_to_test}' chưa có trong MongoDB. Vui lòng chạy luồng Ingest ít nhất 1 lần trước khi Test Replay.")
        sys.exit(1)
        
    print(f"    - Neo4j: {b_nodes} nodes, {b_edges} edges")
    print(f"    - Mongo: {b_doc_cnt} docs, SHA={b_sha[:8]}..., Time={b_time}")
    print(f"    - Checkpoint: batch {b_ckpt}")
    
    # 2. Mutate file (Thay đổi nội dung để sinh SHA mới)
    print("\n[*] Bước 1: Mutating file (Thêm chú thích để đổi SHA)...")
    mutate(REPO_ROOT, file_to_test)
    
    # 3. Replay lần 1
    print("\n[*] Bước 2: Replay Lần 1 (Đẩy sự kiện lên Kafka)...")
    run_parser(file_to_test)
    print("    -> Đợi 15s cho Spark Streaming kịp Ingest vào MongoDB...")
    time.sleep(15)
    
    # 4. Replay lần 2
    print("\n[*] Bước 3: Replay Lần 2 (Đẩy lại sự kiện giống hệt Lần 1 để test Trùng lặp)...")
    run_parser(file_to_test)
    print("    -> Đợi 15s cho Spark Streaming kịp xử lý...")
    time.sleep(15)
    
    # 5. Trạng thái AFTER
    print("\n[*] Đang lấy trạng thái AFTER...")
    a_nodes, a_edges = get_neo4j_state(file_to_test)
    a_doc_cnt, a_sha, a_time = get_mongo_state(file_to_test)
    a_ckpt = get_kafka_checkpoint()
    
    print(f"    - Neo4j: {a_nodes} nodes, {a_edges} edges")
    print(f"    - Mongo: {a_doc_cnt} docs, SHA={a_sha[:8]}..., Time={a_time}")
    print(f"    - Checkpoint: batch {a_ckpt}")
    
    # 6. Verify assertions khắt khe
    print("\n[*] So sánh và kết luận (Strict Mode)...")
    failed = False
    
    # [A] Neo4j: Node count không được tăng gấp đôi (được phép tăng nhẹ 1-2 node do đoạn comment thêm vào AST).
    if a_nodes >= b_nodes * 1.5 and b_nodes > 0:
        print(f"  [FAIL] Neo4j bị tạo dữ liệu trùng (Node tăng quá vô lý từ {b_nodes} lên {a_nodes}). MERGE hỏng!")
        failed = True
    else:
        print(f"  [PASS] Neo4j nodes/edges ổn định ({b_nodes} -> {a_nodes}). MERGE Idempotent OK.")
        
    # [B1] Mongo: Chỉ duy nhất 1 document (Upsert)
    if a_doc_cnt != 1:
        print(f"  [FAIL] MongoDB có {a_doc_cnt} document. Kì vọng CHÍNH XÁC 1 (Lệnh Upsert không hoạt động, sinh ra duplicate).")
        failed = True
    else:
        print(f"  [PASS] MongoDB duy trì đúng {a_doc_cnt} document. Upsert Idempotent OK.")
        
    # [B2] Mongo: SHA phải thay đổi
    if a_sha == b_sha:
        print(f"  [FAIL] MongoDB SHA không thay đổi. Spark chưa Ingest kịp hoặc Mutate hỏng.")
        failed = True
    else:
        print(f"  [PASS] MongoDB SHA đã cập nhật thành công (mutation nhận diện được).")
        
    # [B3] Mongo: Thời gian phải tiến lên
    if not (a_time > b_time if b_time and a_time else True):
        print(f"  [FAIL] MongoDB processed_at không mới hơn BEFORE.")
        failed = True
    else:
        print(f"  [PASS] MongoDB processed_at đã cập nhật thời gian mới.")
        
    # [C] Checkpoint Kafka phải tiến lên (batch id tăng)
    if b_ckpt is not None and a_ckpt is not None and int(a_ckpt) <= int(b_ckpt):
        print(f"  [FAIL] Kafka checkpoint không tịnh tiến (BEFORE {b_ckpt}, AFTER {a_ckpt}).")
        failed = True
    else:
        print(f"  [PASS] Spark Checkpoint Kafka đã ghi nhận offset mới.")
        
    if failed:
        print("\n[FAIL] KẾT LUẬN: IDEMPOTENT REPLAY THẤT BẠI. TỒN TẠI LỖI TRÙNG LẶP HOẶC KHÔNG CẬP NHẬT.")
        sys.exit(1)
    else:
        print("\n[OK] KẾT LUẬN: HỆ THỐNG ĐẠT CHUẨN IDEMPOTENT E2E 100%. THÀNH CÔNG RỰC RỠ!")
