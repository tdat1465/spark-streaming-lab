# Lab 04: Spark Streaming – Code Property Graph (CPG) Pipeline

## Nhóm Thực Hiện: CDHD

| STT | Họ và Tên | MSSV |
| :-: | -------------------- | :--------: |
| 1 | Phan Thị Phương Chi | 23120025 |
| 2 | Trần Thanh Đạt | 23120030 |
| 3 | Phạm Ngọc Duy | 23120035 |
| 4 | Lê Hoàng Mỹ Hạ | 23120038 |

---

## 1. Giới thiệu Đồ Án

Báo cáo này trình bày kết quả xây dựng hệ thống **Incremental Code Property Graph (CPG) Construction and Real-Time Streaming Ingestion Pipeline** thuộc đồ án môn học Big Data (Lab 04 - Spark Streaming).

Repository được lựa chọn làm đối tượng phân tích static code: **[`huggingface/peft`](https://github.com/huggingface/peft.git)** (Parameter-Efficient Fine-Tuning).

### Các mục tiêu chính

1. **Repository Cloning and Discovery (Task 1):** Shallow clone và quét phân loại toàn bộ 431 file `.py` trong kho mã nguồn.
2. **Incremental CPG Parser Service (Task 2):** Parser service bounded memory, trích xuất AST/CFG/DFG/CALL với UUIDv5 stable ID.
3. **Kafka Topic Design (Task 3):** 4 Kafka topic (`cpg-nodes`, `cpg-edges`, `cpg-metadata`, `cpg-errors`) kèm schema version và UTC event timestamp.
4. **Graph Topology Ingestion into Neo4j (Task 4):** Neo4j Kafka Connector Sink ghi graph bằng `MERGE` idempotent.
5. **Source Metadata Ingestion into MongoDB (Task 5):** Spark Structured Streaming ghi metadata vào MongoDB với checkpoint.
6. **Idempotent Replay Verification (Task 6):** Mutate 1 file, replay và verify không trùng lặp trên Neo4j/MongoDB/Spark.

---

## 2. Danh Mục Các Chương

- **[Task 1: Repository Cloning and File Discovery](chapters/task1)**
- **[Task 2: Incremental CPG Parser Service](chapters/task2)**
- **[Task 3: Kafka Topic Design](chapters/task3)**
- **[Task 4: Graph Topology Ingestion into Neo4j](chapters/task4)**
- **[Task 5: Source Metadata Ingestion into MongoDB](chapters/task5)**
- **[Task 6: Idempotent Replay Verification](chapters/task6)**
- **[Architecture & End-to-End Pipeline Overview](chapters/architecture)**

---

## 3. Repository & Source Code

Toàn bộ source code nằm trong thư mục [`src/`](https://github.com/tdat1465/spark-streaming-lab/tree/master/src) trên GitHub.
Notebook gốc chạy tuần tự Task 1–6: [`notebook.ipynb`](https://github.com/tdat1465/spark-streaming-lab/blob/master/notebook.ipynb).
