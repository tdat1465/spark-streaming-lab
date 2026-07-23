"""Validation tests for stable and unique CPG identifiers."""

import sys
from pathlib import Path

import pandas as pd

from src.task2.cpg_parser import process_file, validate_cpg

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REPO_ROOT = PROJECT_ROOT / "peft"
DISCOVERED_CSV = PROJECT_ROOT / "output" / "discovered_files.csv"
SCHEMA_VERSION = "1.0.0"
SAMPLE_SIZE = 15


def test_unique_ids_across_sample_files():
    df = pd.read_csv(DISCOVERED_CSV)
    source_files = df[df["category"] == "source"]["relative_path"].tolist()[:SAMPLE_SIZE]

    for rel_path in source_files:
        nodes, edges, _ = process_file(rel_path, REPO_ROOT, SCHEMA_VERSION)
        result = validate_cpg(nodes, edges)
        assert result["valid"], (
            f"{rel_path}: duplicate_nodes={result['duplicate_nodes']}, "
            f"duplicate_edges={result['duplicate_edges']}, "
            f"dangling_edges={result['dangling_edges']}"
        )


def test_stable_ids_on_reprocess():
    df = pd.read_csv(DISCOVERED_CSV)
    rel_path = df[df["category"] == "source"].iloc[0]["relative_path"]

    nodes1, edges1, meta1 = process_file(rel_path, REPO_ROOT, SCHEMA_VERSION)
    nodes2, edges2, meta2 = process_file(rel_path, REPO_ROOT, SCHEMA_VERSION)

    assert {n["id"] for n in nodes1} == {n["id"] for n in nodes2}
    assert {e["id"] for e in edges1} == {e["id"] for e in edges2}
    assert meta1["id"] == meta2["id"]


def test_edges_reference_existing_nodes():
    code = '''\
def add(a, b):
    x, y = a, b
    return x + y

result = add(1, 2)
'''
    sample = REPO_ROOT / "_cpg_test_sample.py"
    sample.write_text(code, encoding="utf-8")
    try:
        nodes, edges, _ = process_file("_cpg_test_sample.py", REPO_ROOT, SCHEMA_VERSION)
        node_ids = {n["id"] for n in nodes}
        for edge in edges:
            assert edge["source_id"] in node_ids, edge
            assert edge["target_id"] in node_ids, edge
        assert len(node_ids) == len(nodes)
    finally:
        if sample.exists():
            sample.unlink()


def run_all_tests():
    tests = [
        test_unique_ids_across_sample_files,
        test_stable_ids_on_reprocess,
        test_edges_reference_existing_nodes,
    ]
    passed = 0
    for test in tests:
        name = test.__name__
        try:
            test()
            print(f"[PASS] {name}")
            passed += 1
        except Exception as exc:
            print(f"[FAIL] {name}: {exc}")
    print(f"\nResult: {passed}/{len(tests)} tests passed.")
    return passed == len(tests)


if __name__ == "__main__":
    ok = run_all_tests()
    sys.exit(0 if ok else 1)
