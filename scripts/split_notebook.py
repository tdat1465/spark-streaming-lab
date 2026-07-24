"""Split notebook.ipynb into per-task chapter notebooks for Jupyter Book."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NB_PATH = ROOT / "notebook.ipynb"
CHAPTERS_DIR = ROOT / "chapters"

TASK_BOUNDARIES = {
    1: (1, 18),
    2: (18, 26),
    3: (26, 36),
    4: (36, 50),
    5: (50, 58),
    6: (58, 66),
}

METADATA = {
    "kernelspec": {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    },
    "language_info": {
        "name": "python",
        "pygments_lexer": "ipython3",
    },
}


def cell_text(cell: dict) -> str:
    src = cell.get("source", [])
    return "".join(src) if isinstance(src, list) else src


def fix_paths(text: str) -> str:
    """Adjust relative asset paths for files living under chapters/."""
    # html_extra_path copies images/* to site root (_build/html/)
    text = text.replace('src="result/task4/', 'src="../task4/')
    text = text.replace("src='result/task4/", "src='../task4/")
    return text


def fix_cell(cell: dict) -> dict:
    cell = deepcopy(cell)
    if cell.get("cell_type") == "markdown":
        src = cell.get("source", [])
        if isinstance(src, list):
            cell["source"] = [fix_paths(line) for line in src]
        else:
            cell["source"] = fix_paths(src)
    return cell


def make_chapter_notebook(cells: list[dict]) -> dict:
    return {
        "cells": [fix_cell(c) for c in cells],
        "metadata": deepcopy(METADATA),
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    nb = json.loads(NB_PATH.read_text(encoding="utf-8"))
    all_cells = nb["cells"]
    CHAPTERS_DIR.mkdir(parents=True, exist_ok=True)

    for task_num, (start, end) in TASK_BOUNDARIES.items():
        chapter_cells = all_cells[start:end]
        if chapter_cells and chapter_cells[0]["cell_type"] == "markdown":
            first = cell_text(chapter_cells[0])
            first = re.sub(
                r"^## \*\*(Task \d+:[^*]+)\*\*",
                r"# \1",
                first.strip(),
                count=1,
            )
            chapter_cells[0] = {
                **deepcopy(chapter_cells[0]),
                "source": [first + "\n"],
            }
        chapter_nb = make_chapter_notebook(chapter_cells)
        out_path = CHAPTERS_DIR / f"task{task_num}.ipynb"
        out_path.write_text(
            json.dumps(chapter_nb, ensure_ascii=False, indent=1),
            encoding="utf-8",
        )
        print(f"Wrote {out_path} ({len(chapter_cells)} cells)")

    # intro.md from cell 0
    intro_cell = all_cells[0]
    intro_text = cell_text(intro_cell).strip()
    intro_text = re.sub(r"^#\s+", "# ", intro_text, count=1)
    intro_body = """# Lab 04: Spark Streaming – Code Property Graph (CPG) Pipeline

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
"""
    (ROOT / "intro.md").write_text(intro_body, encoding="utf-8")
    print("Wrote intro.md")


if __name__ == "__main__":
    main()
