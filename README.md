# Lab 04 – Spark Streaming: CPG Pipeline cho repo `peft`

## Group: CDHD

### Team Members

| STT | Full Name           | Student ID |
| :-: | -------------------- | :--------: |
|  1  | Phan Thị Phương Chi  |  23120025  |
|  2  | Trần Thanh Đạt        |  23120030  |
|  3  | Phạm Ngọc Duy         |  23120035  |
|  4  | Lê Hoàng Mỹ Hạ        |  23120038  |

---

## 1. Giới thiệu

Lab xây dựng một pipeline end-to-end trích xuất **Code Property Graph (CPG)** từ mã nguồn Python của repo [`huggingface/peft`](https://github.com/huggingface/peft.git), truyền qua **Kafka**, ghi cấu trúc đồ thị vào **Neo4j** (qua Kafka Connect) và metadata vào **MongoDB** (qua Spark Structured Streaming), sau đó kiểm chứng tính **idempotent** khi replay dữ liệu.

Sơ đồ luồng tổng quát:

```
peft repo -> File Discovery -> cpg_parser.py (AST/CFG/DFG/CALL) -> Kafka
   -> [cpg-nodes, cpg-edges] -> Kafka Connect Neo4j Sink -> Neo4j
   -> [cpg-metadata]         -> Spark Structured Streaming   -> MongoDB
   -> Task 6: mutate -> replay -> verify (kiểm chứng idempotent)
```

## 2. Cấu trúc thư mục

```
spark-streaming-lab/
├── notebook.ipynb                      # Notebook chính, chạy tuần tự Task 1 -> Task 6
├── diagram.gv                          # Graphviz sơ đồ pipeline CPG
├── requirements.txt                    # Các package Python cần cài
├── [BigData] Lab04 - StreamingV0.pdf   # Đề bài Lab 04
├── peft/                               # Repo huggingface/peft được clone (Task 1)
├── output/                             # Các file output của pipeline
│   ├── discovered_files.csv            # Danh sách file .py được discover (Task 1)
│   ├── mutated_file.txt                # Ghi lại file bị mutate (Task 6)
│   ├── task2_dryrun.log                # Log dry-run parser (Task 2)
│   ├── task2_parse_stats.csv           # Thống kê parse CPG (Task 2)
│   ├── task3_emit.log                  # Log emit lên Kafka (Task 3)
│   ├── task3_kafka_samples.json        # Mẫu message Kafka (Task 3)
│   ├── task3_offsets.csv               # Offset các topic Kafka (Task 3)
│   ├── task3_topics_describe.log       # Mô tả topic Kafka (Task 3)
│   └── task3_topics_list.log           # Danh sách topic Kafka (Task 3)
├── result/
│   └── task4/                          # Ảnh kết quả kiểm chứng Neo4j (Task 4)
│       ├── 1_v2.png
│       ├── 2_v2.png
│       ├── 3.1_v2.png
│       └── 3.2_v2.png
├── src/
│   ├── task1/                          # Clone repo & file discovery
│   │   ├── __init__.py
│   │   ├── clone.py                    # Clone repo peft từ GitHub
│   │   └── discover.py                 # Duyệt & phân loại file .py
│   ├── task2/                          # Incremental CPG Parser Service
│   │   ├── __init__.py
│   │   ├── config.py                   # Cấu hình đường dẫn & tham số
│   │   ├── cpg_parser.py               # Parser AST/CFG/DFG/CALL -> Kafka
│   │   ├── dry_run.py                  # Chạy parser không gửi Kafka
│   │   ├── test_cpg_ids.py             # Unit test UUIDv5 ổn định
│   │   └── verify_stable_id.py         # Xác nhận ID ổn định khi reprocess
│   ├── task3/                          # Kafka Topic Design & emit
│   │   ├── __init__.py
│   │   ├── config.py                   # Cấu hình Kafka broker & topic
│   │   ├── setup_kafka.py              # Tạo 4 Kafka topic
│   │   ├── emit.py                     # Phát sự kiện CPG lên Kafka
│   │   └── verify.py                   # Kiểm tra offset & mẫu message
│   ├── task4/                          # Graph Topology Ingestion -> Neo4j
│   │   ├── __init__.py
│   │   ├── docker-compose.yml          # Stack Kafka/ZooKeeper/Kafka Connect/Neo4j
│   │   ├── start.py                    # Khởi động Docker Compose (qua WSL)
│   │   ├── setup.py                    # Đăng ký Kafka Connect sink connector
│   │   ├── emit.py                     # Emit sự kiện lên cpg-nodes/cpg-edges
│   │   └── verify.py                   # Query Neo4j kiểm tra CpgNode & CPG_EDGE
│   ├── task5/                          # Spark Structured Streaming -> MongoDB
│   │   ├── docker-compose.yml          # Stack MongoDB
│   │   └── ingest.py                   # Spark job đọc cpg-metadata -> MongoDB
│   └── task6/                          # Idempotent Replay Verification
│       ├── mutate.py                   # Sửa 1 file peft (đổi SHA-256)
│       ├── replay.py                   # Replay emit lại sự kiện lên Kafka
│       └── verify.py                   # Kiểm tra 3 invariant [A][B][C]
└── checkpoints/
    └── task5_metadata/                 # Spark Structured Streaming checkpoint
```

## 3. Yêu cầu môi trường

- **Python** (Anaconda/Miniconda), môi trường conda mẫu dùng trong lab: `min_ds-env`
- **Docker Desktop** (chạy container Kafka/ZooKeeper/Kafka Connect/Neo4j/MongoDB)
- **WSL2** (một số dịch vụ Task 3/Task 4 chạy Kafka trong WSL, notebook tự chuyển đổi đường dẫn Windows ↔ WSL)
- **Apache Spark** cài sẵn (có `spark-submit`), **Hadoop** (Windows cần `HADOOP_HOME`/`winutils`)
- Các package Python: `pandas`, `kafka-python`, `pyspark`, `pymongo`, `neo4j`, `jupyter`, ...

> Cài nhanh: `pip install -r requirements.txt` (file đã có sẵn trong repo). Package chính: `pandas`, `kafka-python`, `pyspark==3.5.0`, `pymongo`, `neo4j`, `requests`, `jupyterlab`, `pytest`.

## 4. Chạy Lab từng bước

Mở `notebook.ipynb` và chạy tuần tự các cell theo từng Task bên dưới.

### Task 1 – Repository Cloning & File Discovery
1. Chạy cell cấu hình đường dẫn (`PROJECT_ROOT`, `REPO_ROOT`, `REPO_CLONE_URL = https://github.com/huggingface/peft.git`).
2. Chạy `clone_repo_if_needed(...)` để clone repo `peft` (`git clone --depth 1`) nếu chưa tồn tại.
3. Chạy `scan_repo` để duyệt đệ quy 431 file `.py`, phân loại theo thứ tự `auto-generated -> setup -> test -> source`.
4. Kết quả xuất ra `output/discovered_files.csv` (309 source / 63 test / 59 setup / 0 auto-generated).

### Task 2 – Incremental CPG Parser Service
1. Import `src/task2/cpg_parser.py`, `dry_run.py`, `verify_stable_id.py`.
2. Chạy **dry-run** (`--dry-run`, không gửi Kafka) trên 5 file source đầu tiên để kiểm tra parser.
3. Chạy `verify_stable_id.py` để xác nhận UUIDv5 sinh ra ổn định khi reprocess cùng nội dung file.

### Task 3 – Kafka Topic Design
1. Đảm bảo Kafka broker đang chạy (`127.0.0.1:9092`, qua Docker Compose Task 4 hoặc Kafka trong WSL).
2. Chạy `src/task3/setup_kafka.py` — tự khởi động ZooKeeper + Kafka (Docker, dùng `docker-compose.yml` của Task 4) nếu cần, tạo 4 topic: `cpg-nodes`, `cpg-edges`, `cpg-metadata`, `cpg-errors`.
3. Chạy `src/task3/emit.py` để phát sự kiện từ Parser Service lên Kafka.
4. Chạy `src/task3/verify.py` để kiểm tra offset và mẫu message JSON.

### Task 4 – Graph Topology Ingestion vào Neo4j
1. Khởi động toàn bộ stack bằng Docker Compose (ZooKeeper `cpg-zookeeper`, Kafka `cpg-kafka`, Kafka Connect `cpg-kafka-connect`, Neo4j `cpg-neo4j`) qua `src/task4/start.py` (chạy thông qua WSL, sử dụng `run_wsl()` để gọi các script trong WSL).
2. Đăng ký 2 Kafka Connect sink connector qua REST API (`src/task4/setup.py`, Kafka Connect tại `http://localhost:8083`, sử dụng **Neo4j Connector v5.1.10**):
   - `neo4j-cpg-nodes-sink` — đọc topic `cpg-nodes`, ghi node bằng `MERGE` (Neo4j URI: `bolt://neo4j:7687`), cấu hình key `neo4j.cypher.topic.cpg-nodes`.
   - `neo4j-cpg-edges-sink` — đọc topic `cpg-edges`, ghi relationship bằng `MERGE`, cấu hình key `neo4j.cypher.topic.cpg-edges`.
3. Chạy `src/task2/cpg_parser.py` hoặc `src/task4/emit.py` trên 5 file đầu để phát sự kiện lên `cpg-nodes`, `cpg-edges`, `cpg-metadata`.
4. Đợi ~90 giây để Kafka Connect xử lý theo batch, sau đó query Neo4j kiểm tra số lượng `CpgNode` (**15,148**) và `CPG_EDGE` (**26,780**) > 0.

### Task 5 – Spark Structured Streaming -> MongoDB
1. Khởi động MongoDB bằng Docker Compose: `docker compose up -d` (trong `src/task5/`).
2. **Chạy `spark-submit` trong một terminal riêng** (đây là tiến trình long-running, chạy song song với notebook). Notebook tự động phát hiện `HADOOP_HOME` và `spark-submit`; nếu không tìm thấy, set thủ công theo ví dụ dưới đây (Windows):
   ```cmd
   set HADOOP_HOME=D:\hadoop
   set PATH=D:\hadoop\bin;%PATH%
   set PYSPARK_PYTHON=D:\Anaconda3\envs\min_ds-env\python.exe
   set PYSPARK_DRIVER_PYTHON=D:\Anaconda3\envs\min_ds-env\python.exe
   D:\Anaconda3\envs\min_ds-env\Scripts\spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0" src\task5\ingest.py
   ```
   Job này đọc topic `cpg-metadata` từ `127.0.0.1:9092`, parse JSON, ghi vào `peft_db.source_metadata` (MongoDB tại `mongodb://127.0.0.1:27017`) với `processingTime=10 seconds`, `outputMode=append`, `operationType=update` (upsert theo `file_path`) và checkpoint tại `checkpoints/task5_metadata`.
3. Quay lại notebook, kiểm chứng dữ liệu đã ghi vào MongoDB.

### Task 6 – Idempotent Replay Verification
1. **Mutate**: chạy `python -m src.task6.mutate` — thêm comment + hằng số vào `peft/src/peft/__init__.py` (thay đổi SHA-256 và `num_lines`), ghi đường dẫn file đã mutate vào `output/mutated_file.txt`.
2. **Replay**: chạy `python -m src.task6.replay` — gọi lại `src/task2/cpg_parser.py --file <mutated>` chỉ cho file đó, emit lại toàn bộ sự kiện lên Kafka (`localhost:9092`), đợi ~15 giây cho Spark xử lý micro-batch.
3. **Verify**: chạy `python -m src.task6.verify` — kiểm tra 3 invariant:
   - **[A]** Neo4j không có `CpgNode` id trùng lặp cho file đã mutate (MERGE hoạt động đúng).
   - **[B]** MongoDB có document mới nhất cho file đó với `processed_at` mới hơn (append mode + upsert theo `file_path`).
   - **[C]** Spark Checkpoint (`checkpoints/task5_metadata/offsets/`) có file offset mới nhất, chứng minh Spark tiếp tục từ offset sau cùng.

## 5. Kết quả mong đợi

| Task | Output chính |
|:-----|:--------------|
| 1 | `output/discovered_files.csv` (431 file: 309 source / 63 test / 59 setup) |
| 2 | Node/Edge có UUIDv5 ổn định, xác nhận qua `verify_stable_id.py` |
| 3 | 4 Kafka topic hoạt động, message JSON đúng schema; offsets: `cpg-nodes`=15,896 / `cpg-edges`=27,900 / `cpg-metadata`=10 / `cpg-errors`=0 |
| 4 | Neo4j: **15,148** `CpgNode`, **26,780** `CPG_EDGE` (AST: 15,138 · CFG: 9,791 · DFG: 1,106 · CALL: 745) |
| 5 | MongoDB `peft_db.source_metadata` được cập nhật liên tục, checkpoint hoạt động (latest batch ≥ 1) |
| 6 | **2/3** invariant PASS — [A] Neo4j idempotent ✅ · [B] MongoDB update ⚠️ (cần Spark job đang chạy liên tục) · [C] Checkpoint offset ✅ |

## 6. Một số lưu ý khi troubleshoot

- Nếu Task 3 báo chưa tạo được topic: kiểm tra Kafka đã chạy trong Docker/WSL chưa rồi chạy lại cell `setup_kafka()`.
- `advertised.listeners=PLAINTEXT://localhost:9092` cần cấu hình đúng để producer chạy trên Windows kết nối được broker trong WSL.
- Thứ tự phân loại file ở Task 1 phải là `auto-generated -> setup -> test -> source`; nếu đặt `test` trước `setup` thì `conftest.py` sẽ bị nhận nhầm thành `test` (do `"test"` là substring của `"conftest"`).
- MongoDB Spark Connector cần bản `v10.3.0` tương thích Spark `3.5.0` để tránh lỗi `RowEncoder`.
- **Task 6 [B] FAILED**: `verify.py` báo không tìm thấy document MongoDB nếu Spark job chưa chạy hoặc chưa xử lý xong micro-batch. Đảm bảo `spark-submit ingest.py` đang chạy trong terminal riêng *trước* khi chạy `replay` và `verify`.
- **Neo4j Connector v5.x**: dùng key cấu hình `neo4j.cypher.topic.<name>` (thay vì `neo4j.topic.cypher.<name>` của phiên bản cũ). Kiểm tra version plugin qua `GET http://localhost:8083/connector-plugins`.
- **Task 5 – HADOOP_HOME**: notebook tự động tìm `HADOOP_HOME` và `spark-submit`; nếu auto-detect thất bại, set thủ công biến môi trường trước khi chạy cell spark-submit.