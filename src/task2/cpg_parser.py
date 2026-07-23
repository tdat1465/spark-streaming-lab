import argparse
import ast
import datetime
import hashlib
import json
import sys
import traceback
import uuid
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

CPG_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


class CPGExtractor(ast.NodeVisitor):
    def __init__(self, file_path, source_code):
        self.file_path = file_path
        self.source_code = source_code
        self.lines = source_code.splitlines()
        self.nodes = []
        self.edges = []
        self.path_stack = []
        self.parent_stack = []
        self.last_cfg_node_id = None
        self.last_definitions = {}
        self.current_function_id = None
        self._segment_counts = {}

    def _segment_for_node(self, node):
        node_class = node.__class__.__name__
        name = (
            getattr(node, "name", None)
            or getattr(node, "id", None)
            or getattr(node, "attr", None)
            or (node.arg if isinstance(node, ast.arg) else None)
        )
        lineno = getattr(node, "lineno", 0)
        col = getattr(node, "col_offset", 0)
        end_lineno = getattr(node, "end_lineno", lineno)
        end_col = getattr(node, "end_col_offset", col)
        pos = f"{lineno}:{col}-{end_lineno}:{end_col}"
        base = f"{node_class}[{name}]@{pos}" if name is not None else f"{node_class}@{pos}"

        parent_path = "/".join(self.path_stack)
        key = f"{parent_path}|{base}"
        count = self._segment_counts.get(key, 0)
        self._segment_counts[key] = count + 1
        return base if count == 0 else f"{base}#{count}"

    def get_node_id_by_path(self, ast_path):
        unique_str = f"{self.file_path}:{ast_path}"
        return str(uuid.uuid5(CPG_NAMESPACE, unique_str))

    def get_edge_id(self, source_id, target_id, edge_type, edge_index=None):
        unique_str = f"{self.file_path}:{source_id}->{target_id}:{edge_type}"
        if edge_index is not None:
            unique_str += f":{edge_index}"
        return str(uuid.uuid5(CPG_NAMESPACE, unique_str))

    def get_code_snippet(self, node):
        try:
            start_line = getattr(node, "lineno", None)
            end_line = getattr(node, "end_lineno", None)
            if start_line is not None:
                start_idx = start_line - 1
                end_idx = end_line if end_line is not None else start_line
                snippet_lines = self.lines[start_idx:end_idx]

                if len(snippet_lines) == 1:
                    start_col = getattr(node, "col_offset", 0)
                    end_col = getattr(node, "end_col_offset", len(snippet_lines[0]))
                    return snippet_lines[0][start_col:end_col]
                return "\n".join(snippet_lines)
        except Exception:
            pass
        return ""

    def visit(self, node):
        self.path_stack.append(self._segment_for_node(node))
        ast_path = "/".join(self.path_stack)
        node_id = self.get_node_id_by_path(ast_path)
        node_class = node.__class__.__name__

        properties = {}
        if hasattr(node, "name"):
            properties["name"] = node.name
        elif hasattr(node, "id"):
            properties["name"] = node.id
        elif hasattr(node, "attr"):
            properties["name"] = node.attr
        elif isinstance(node, ast.Constant):
            properties["value"] = str(node.value)
        elif isinstance(node, ast.arg):
            properties["name"] = node.arg

        node_entry = {
            "id": node_id,
            "type": node_class,
            "label": node_class,
            "file_path": self.file_path,
            "ast_path": ast_path,
            "start_line": getattr(node, "lineno", None),
            "start_column": getattr(node, "col_offset", None),
            "end_line": getattr(node, "end_lineno", None),
            "end_column": getattr(node, "end_col_offset", None),
            "code": self.get_code_snippet(node),
            "properties": properties,
        }
        self.nodes.append(node_entry)

        if self.parent_stack:
            parent_id = self.parent_stack[-1]
            self.edges.append(
                {
                    "id": self.get_edge_id(parent_id, node_id, "AST"),
                    "source_id": parent_id,
                    "target_id": node_id,
                    "type": "AST",
                }
            )

        self.parent_stack.append(node_id)

        prev_function = self.current_function_id
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.current_function_id = node_id

        is_statement = isinstance(node, (ast.stmt, ast.expr))
        if is_statement:
            if self.last_cfg_node_id:
                self.edges.append(
                    {
                        "id": self.get_edge_id(self.last_cfg_node_id, node_id, "CFG"),
                        "source_id": self.last_cfg_node_id,
                        "target_id": node_id,
                        "type": "CFG",
                    }
                )
            self.last_cfg_node_id = node_id

        if isinstance(node, ast.Assign):
            for target in node.targets:
                self._register_definition(target, node_id)
        elif isinstance(node, ast.AnnAssign):
            self._register_definition(node.target, node_id)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            last_def_id = self.last_definitions.get(node.id)
            if last_def_id:
                self.edges.append(
                    {
                        "id": self.get_edge_id(last_def_id, node_id, "DFG"),
                        "source_id": last_def_id,
                        "target_id": node_id,
                        "type": "DFG",
                    }
                )

        if isinstance(node, ast.Call):
            callee_name = ""
            if isinstance(node.func, ast.Name):
                callee_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                callee_name = node.func.attr

            if self.current_function_id:
                edge = {
                    "id": self.get_edge_id(self.current_function_id, node_id, "CALL"),
                    "source_id": self.current_function_id,
                    "target_id": node_id,
                    "type": "CALL",
                    "properties": {"callee": callee_name},
                }
                self.edges.append(edge)

        self.generic_visit(node)

        self.current_function_id = prev_function
        self.parent_stack.pop()
        self.path_stack.pop()

    def _register_definition(self, target, node_id):
        if isinstance(target, ast.Name):
            self.last_definitions[target.id] = node_id
        elif isinstance(target, (ast.Tuple, ast.List)):
            for elt in target.elts:
                self._register_definition(elt, node_id)


def normalize_rel_path(file_path):
    return Path(file_path).as_posix()


def validate_cpg(nodes, edges):
    node_ids = [n["id"] for n in nodes]
    edge_ids = [e["id"] for e in edges]
    node_set = set(node_ids)

    duplicate_nodes = len(node_ids) - len(node_set)
    duplicate_edges = len(edge_ids) - len(edge_set := set(edge_ids))
    dangling = [
        e
        for e in edges
        if e["source_id"] not in node_set or e["target_id"] not in node_set
    ]

    return {
        "duplicate_nodes": duplicate_nodes,
        "duplicate_edges": duplicate_edges,
        "dangling_edges": len(dangling),
        "valid": duplicate_nodes == 0 and duplicate_edges == 0 and not dangling,
    }


def process_file(file_path, repo_root, schema_version):
    rel_path = Path(file_path)
    abs_path = repo_root / rel_path
    stable_path = normalize_rel_path(rel_path)

    with open(abs_path, "r", encoding="utf-8", errors="ignore") as f:
        source_code = f.read()

    sha256 = hashlib.sha256(source_code.encode("utf-8")).hexdigest()
    file_size = abs_path.stat().st_size
    num_lines = len(source_code.splitlines())

    tree = ast.parse(source_code, filename=stable_path)
    extractor = CPGExtractor(stable_path, source_code)
    extractor.visit(tree)

    validation = validate_cpg(extractor.nodes, extractor.edges)
    if not validation["valid"]:
        raise ValueError(
            "CPG validation failed for "
            f"{stable_path}: duplicate_nodes={validation['duplicate_nodes']}, "
            f"duplicate_edges={validation['duplicate_edges']}, "
            f"dangling_edges={validation['dangling_edges']}"
        )

    event_time = datetime.datetime.utcnow().isoformat() + "Z"

    nodes = []
    for node in extractor.nodes:
        node = dict(node)
        node.update({"schema_version": schema_version, "event_time": event_time})
        nodes.append(node)

    edges = []
    for edge in extractor.edges:
        edge = dict(edge)
        edge.update({"schema_version": schema_version, "event_time": event_time})
        edges.append(edge)

    metadata = {
        "id": str(uuid.uuid5(CPG_NAMESPACE, stable_path)),
        "file_path": stable_path,
        "size_bytes": file_size,
        "sha256": sha256,
        "num_lines": num_lines,
        "processed_at": event_time,
        "status": "SUCCESS",
        "schema_version": schema_version,
        "event_time": event_time,
    }

    return nodes, edges, metadata


def main():
    parser = argparse.ArgumentParser(description="Incremental CPG Parser Service for Python Files")
    parser.add_argument("--discovered-csv", default="output/discovered_files.csv")
    parser.add_argument("--repo-root", default="peft")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--schema-version", default="1.0.0")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--file", default=None)
    args = parser.parse_args()

    discovered_csv_path = Path(args.discovered_csv)
    repo_root_path = Path(args.repo_root)

    if not discovered_csv_path.exists() and args.file is None:
        print(f"Error: Discovered CSV file not found at {discovered_csv_path}")
        sys.exit(1)

    if not repo_root_path.exists():
        print(f"Error: Repository root not found at {repo_root_path}")
        sys.exit(1)

    print("=== Starting CPG Parser Service ===")
    print(f"Discovered CSV : {discovered_csv_path}")
    print(f"Repo Root      : {repo_root_path}")
    print(f"Kafka Brokers  : {args.bootstrap_servers}")
    print(f"Schema Version : {args.schema_version}")
    print(f"Dry Run Mode   : {args.dry_run}\n")

    if args.file:
        source_files = [args.file]
    else:
        df = pd.read_csv(discovered_csv_path)
        source_files = df[df["category"] == "source"]["relative_path"].tolist()
        if args.limit and args.limit > 0:
            source_files = source_files[: args.limit]

    total_files = len(source_files)
    print(f"Found {total_files} source files to parse.")

    producer = None
    if not args.dry_run:
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                request_timeout_ms=30000,
                api_version=(2, 6, 0),
            )
            print("[OK] Connected to Kafka Producer successfully.\n")
        except Exception as exc:
            print(f"[ERROR] Failed to connect to Kafka: {exc}")
            sys.exit(1)

    success_count = 0
    error_count = 0
    total_nodes = 0
    total_edges = 0

    for idx, rel_path_str in enumerate(source_files, 1):
        rel_path = Path(rel_path_str)
        print(f"[{idx}/{total_files}] Processing: {rel_path} ... ", end="")

        try:
            nodes, edges, metadata = process_file(rel_path, repo_root_path, args.schema_version)
            total_nodes += len(nodes)
            total_edges += len(edges)

            if args.dry_run:
                print(f"Parsed successfully (Nodes: {len(nodes)}, Edges: {len(edges)}) [DRY RUN]")
                if idx == 1:
                    print("\n--- SAMPLE NODE EVENT ---")
                    print(json.dumps(nodes[0] if nodes else {}, indent=2))
                    print("\n--- SAMPLE EDGE EVENT ---")
                    print(json.dumps(edges[0] if edges else {}, indent=2))
                    print("\n--- SAMPLE METADATA EVENT ---")
                    print(json.dumps(metadata, indent=2))
                    print("-------------------------\n")
            else:
                for node_event in nodes:
                    producer.send("cpg-nodes", key=node_event["id"], value=node_event)
                for edge_event in edges:
                    producer.send("cpg-edges", key=edge_event["id"], value=edge_event)
                producer.send("cpg-metadata", key=metadata["id"], value=metadata)
                producer.flush()
                print(f"Parsed and Sent (Nodes: {len(nodes)}, Edges: {len(edges)})")

            success_count += 1

        except Exception as exc:
            error_msg = str(exc)
            trace = traceback.format_exc()
            event_time = datetime.datetime.utcnow().isoformat() + "Z"
            stable_path = normalize_rel_path(rel_path_str)

            error_event = {
                "id": str(uuid.uuid5(CPG_NAMESPACE, f"{stable_path}:error")),
                "file_path": stable_path,
                "error_message": error_msg,
                "error_type": exc.__class__.__name__,
                "stack_trace": trace,
                "occurred_at": event_time,
                "schema_version": args.schema_version,
                "event_time": event_time,
            }

            print(f"FAILED: {error_msg}")

            if args.dry_run:
                print(f"[DRY RUN ERROR EVENT] {json.dumps(error_event, indent=2)}")
            else:
                try:
                    producer.send("cpg-errors", key=error_event["id"], value=error_event)
                    producer.flush()
                except Exception as kafka_err:
                    print(f"  [CRITICAL] Failed to send error event to Kafka: {kafka_err}")

            error_count += 1

    if producer:
        producer.close()

    print("\n=== Parser Run Summary ===")
    print(f"Total Source Files processed : {total_files}")
    print(f"Successfully Parsed          : {success_count}")
    print(f"Failed                       : {error_count}")
    print(f"Total Nodes emitted          : {total_nodes}")
    print(f"Total Edges emitted          : {total_edges}")
    print("===========================")


if __name__ == "__main__":
    main()
