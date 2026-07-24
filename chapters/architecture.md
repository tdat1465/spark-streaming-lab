# Architecture Diagram

## End-to-End CPG Streaming Pipeline

Pipeline xử lý mã nguồn Python từ repo [`huggingface/peft`](https://github.com/huggingface/peft) theo mô hình streaming incremental:

```
peft repo
  └─> File Discovery (Task 1)
        └─> CPG Parser Service (Task 2) — AST / CFG / DFG / CALL
              └─> Apache Kafka (Task 3)
                    ├─> cpg-nodes  ──┐
                    ├─> cpg-edges  ──┼─> Neo4j Kafka Connect Sink (Task 4) ──> Neo4j
                    ├─> cpg-metadata ──> Spark Structured Streaming (Task 5) ──> MongoDB
                    └─> cpg-errors
  Task 6: mutate file → replay → verify idempotent (Neo4j MERGE, MongoDB upsert, Spark checkpoint)
```

```{raw} html
<img src="../architecture.png" alt="Architecture Diagram" width="900"/>
```

## Thành phần chính

| Thành phần | Vai trò | Công nghệ |
|:-----------|:--------|:----------|
| Parser Service | Parse từng file `.py`, emit events | Python `ast`, UUIDv5 |
| Message Broker | Phân luồng node/edge/metadata/error | Apache Kafka (4 topics) |
| Graph Store | Lưu topology CPG | Neo4j + Kafka Connect Sink |
| Metadata Store | Lưu metadata file nguồn | MongoDB + Spark Structured Streaming |
| Orchestration | Docker Compose cho Kafka/Neo4j/MongoDB | Docker Desktop + WSL2 |

## Kafka Topic Layout

| Topic | Nội dung | Consumer |
|:------|:---------|:---------|
| `cpg-nodes` | AST node events | Neo4j Connect Sink |
| `cpg-edges` | AST/CFG/DFG/CALL edge events | Neo4j Connect Sink |
| `cpg-metadata` | File metadata (sha256, num_lines, …) | Spark Structured Streaming |
| `cpg-errors` | Parser error events | Monitoring / debug |

Mỗi message đều có `schema_version` và `event_time` (UTC ISO-8601).

## Idempotent Design

- **Parser:** UUIDv5 stable ID cho node/edge/metadata — cùng nội dung file → cùng ID.
- **Neo4j:** Cypher `MERGE` trên `id` — replay không tạo duplicate node/edge.
- **MongoDB:** Upsert theo `file_path` — cập nhật document khi metadata thay đổi.
- **Spark:** Checkpoint tại `checkpoints/task5_metadata` — resume từ offset cuối, không reprocess message cũ.

## Reflection

**What worked**
- Kiến trúc tách biệt rõ ràng: graph topology (Neo4j) và metadata (MongoDB) qua Kafka topics riêng.
- Neo4j Kafka Connect Sink loại bỏ tầng Spark trung gian cho graph ingestion, đơn giản hóa pipeline.
- Docker Compose gom Kafka, ZooKeeper, Kafka Connect và Neo4j trong một stack có thể khởi động lại.

**What failed**
- Kết nối Kafka từ Windows host tới broker trong Docker/WSL gặp timeout do `advertised.listeners` và API version handshake.
- Spark checkpoint path relative gây lỗi native IO trên Windows.

**How resolved**
- Cấu hình `advertised.listeners=PLAINTEXT://localhost:9092` và `api_version=(2, 6, 0)` cho Kafka producer.
- Dùng absolute path cho Spark checkpoint directory.
