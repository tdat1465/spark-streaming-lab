import json
import sys
from collections import Counter

import pandas as pd

from src.task2.cpg_parser import process_file, validate_cpg
from src.task2.config import DISCOVERED_CSV, REPO_ROOT, SCHEMA_VERSION


def test_stable_id():
    if not DISCOVERED_CSV.exists():
        print(f"[LOI] Khong tim thay: {DISCOVERED_CSV}")
        print("[INFO] Hay chay Task 1 truoc de tao discovered_files.csv")
        return False

    df_all = pd.read_csv(DISCOVERED_CSV)
    df_source = df_all[df_all["category"] == "source"].copy()
    if df_source.empty:
        print("[LOI] Khong co file source trong discovered_files.csv")
        return False

    sample_rel = df_source.iloc[0]["relative_path"]
    print("Sample file:", sample_rel)

    nodes1, edges1, meta1 = process_file(sample_rel, REPO_ROOT, SCHEMA_VERSION)
    nodes2, edges2, meta2 = process_file(sample_rel, REPO_ROOT, SCHEMA_VERSION)

    ids_n1 = {n["id"] for n in nodes1}
    ids_n2 = {n["id"] for n in nodes2}
    ids_e1 = {e["id"] for e in edges1}
    ids_e2 = {e["id"] for e in edges2}

    print(
        f"Nodes  run1={len(nodes1)} run2={len(nodes2)} | "
        f"ID overlap = {len(ids_n1 & ids_n2)}/{len(ids_n1)}"
    )
    print(
        f"Edges  run1={len(edges1)} run2={len(edges2)} | "
        f"ID overlap = {len(ids_e1 & ids_e2)}/{len(ids_e1)}"
    )
    print(f"Metadata id run1={meta1['id']}")
    print(f"Metadata id run2={meta2['id']}")
    print(f"Metadata ID identical? {meta1['id'] == meta2['id']}")

    assert ids_n1 == ids_n2, "Node IDs khong on dinh giua 2 lan parse!"
    assert ids_e1 == ids_e2, "Edge IDs khong on dinh giua 2 lan parse!"
    assert meta1["id"] == meta2["id"], "Metadata ID khong on dinh!"

    validation = validate_cpg(nodes1, edges1)
    assert validation["valid"], f"CPG validation failed: {validation}"
    print("\n[OK] Stable ID verified — reprocess cung noi dung khong tao ID moi.")
    print(
        f"[OK] ID uniqueness: nodes={len(nodes1)} edges={len(edges1)} "
        f"dangling=0"
    )

    edge_types = Counter(e["type"] for e in edges1)
    print("\nEdge type counts:", dict(edge_types))

    print("\n--- SAMPLE NODE ---")
    print(json.dumps(nodes1[0], indent=2, ensure_ascii=False))
    print("\n--- SAMPLE EDGE ---")
    print(json.dumps(edges1[0], indent=2, ensure_ascii=False))
    print("\n--- SAMPLE METADATA ---")
    print(json.dumps(meta1, indent=2, ensure_ascii=False))

    for label, obj in [("node", nodes1[0]), ("edge", edges1[0]), ("metadata", meta1)]:
        assert "schema_version" in obj, f"{label} thieu schema_version"
        assert "event_time" in obj, f"{label} thieu event_time"
    print("\n[OK] schema_version + event_time co tren moi loai event.")
    return True


if __name__ == "__main__":
    ok = test_stable_id()
    sys.exit(0 if ok else 1)
