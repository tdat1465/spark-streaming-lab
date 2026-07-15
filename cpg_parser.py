import os
import sys
import json
import hashlib
import uuid
import datetime
import argparse
import ast
import traceback
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

class CPGExtractor(ast.NodeVisitor):
    def __init__(self, file_path, source_code):
        self.file_path = file_path
        self.source_code = source_code
        self.lines = source_code.splitlines()
        self.nodes = []
        self.edges = []
        self.path_stack = []
        
        # Track parent-child relationships for AST edges
        self.parent_stack = []
        
        # Track control flow (CFG)
        self.last_cfg_node_id = None
        
        # Track data flow (DFG) - Map variable name to the node ID of its last assignment
        self.last_definitions = {}
        
        # Track surrounding function for Call graph edges
        self.current_function_id = None

    def get_node_id(self, node):
        """Generates a stable, deterministic UUIDv5 for an AST node."""
        # Use location information to ensure uniqueness within the file
        lineno = getattr(node, 'lineno', 0)
        col_offset = getattr(node, 'col_offset', 0)
        node_class = node.__class__.__name__
        unique_str = f"{self.file_path}:{node_class}:{lineno}:{col_offset}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))

    def get_edge_id(self, source_id, target_id, edge_type):
        """Generates a stable, deterministic UUIDv5 for a CPG edge."""
        unique_str = f"{source_id}->{target_id}:{edge_type}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))

    def get_code_snippet(self, node):
        """Extracts the exact code snippet for the node if line numbers are present."""
        try:
            start_line = getattr(node, 'lineno', None)
            end_line = getattr(node, 'end_lineno', None)
            if start_line is not None:
                start_idx = start_line - 1
                end_idx = end_line if end_line is not None else start_line
                lines = self.lines[start_idx:end_idx]
                
                # Adjust column offsets if it's a single line
                if len(lines) == 1:
                    start_col = getattr(node, 'col_offset', 0)
                    end_col = getattr(node, 'end_col_offset', len(lines[0]))
                    return lines[0][start_col:end_col]
                return "\n".join(lines)
        except Exception:
            pass
        return ""

    def visit(self, node):
        """Overrides visit to collect node properties and build the CPG edges."""
        node_class = node.__class__.__name__
        name = getattr(node, "name", None) or getattr(node, "id", None) or getattr(node, "attr", None)
        seg = f"{node_class}[{name}]" if name else f"{node_class}[{self._sibling_index(node)}]"
        self.path_stack.append(seg)
        ast_path = "/".join(self.path_stack)
        node_id = self.get_node_id_by_path(ast_path)
        
        # Extract properties
        properties = {}
        if hasattr(node, 'name'):
            properties['name'] = node.name
        elif hasattr(node, 'id'):
            properties['name'] = node.id
        elif hasattr(node, 'attr'):
            properties['name'] = node.attr
        elif isinstance(node, ast.Constant):
            properties['value'] = str(node.value)
        elif isinstance(node, ast.arg):
            properties['name'] = node.arg
            
        # Collect Node
        node_entry = {
            "id": node_id,
            "type": node_class,
            "label": node_class,
            "file_path": self.file_path,
            "start_line": getattr(node, 'lineno', None),
            "start_column": getattr(node, 'col_offset', None),
            "end_line": getattr(node, 'end_lineno', None),
            "end_column": getattr(node, 'end_col_offset', None),
            "code": self.get_code_snippet(node),
            "properties": properties
        }
        self.nodes.append(node_entry)

        # 1. Construct AST Edge (Parent -> Child)
        if self.parent_stack:
            parent_id = self.parent_stack[-1]
            self.edges.append({
                "id": self.get_edge_id(parent_id, node_id, "AST"),
                "source_id": parent_id,
                "target_id": node_id,
                "type": "AST"
            })

        # Push to parent stack for children visit
        self.parent_stack.append(node_id)
        
        # Save previous context for specific analyses
        prev_function = self.current_function_id
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.current_function_id = node_id
            
        # Handle CFG (Control Flow Graph)
        # Simply link statement nodes in execution sequence (top-level or within functions)
        is_statement = isinstance(node, (ast.stmt, ast.expr))
        old_cfg_last = self.last_cfg_node_id
        if is_statement:
            if self.last_cfg_node_id:
                self.edges.append({
                    "id": self.get_edge_id(self.last_cfg_node_id, node_id, "CFG"),
                    "source_id": self.last_cfg_node_id,
                    "target_id": node_id,
                    "type": "CFG"
                })
            self.last_cfg_node_id = node_id

        # Handle DFG (Data Flow Graph)
        # Track variable definitions and reference flows
        if isinstance(node, ast.Assign):
            # Target (Store) variables get definitions
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.last_definitions[target.id] = node_id
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            # If variable is used (Loaded), create DFG edge from its last definition to this use
            last_def_id = self.last_definitions.get(node.id)
            if last_def_id:
                self.edges.append({
                    "id": self.get_edge_id(last_def_id, node_id, "DFG"),
                    "source_id": last_def_id,
                    "target_id": node_id,
                    "type": "DFG"
                })

        # Handle Call Edges
        if isinstance(node, ast.Call):
            callee_name = ""
            if isinstance(node.func, ast.Name):
                callee_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                callee_name = node.func.attr
            
            # Connect the Call node to the surrounding function if inside one
            if self.current_function_id:
                self.edges.append({
                    "id": self.get_edge_id(self.current_function_id, node_id, "CALL"),
                    "source_id": self.current_function_id,
                    "target_id": node_id,
                    "type": "CALL",
                    "properties": {"callee": callee_name}
                })

        # Recursively visit children
        self.generic_visit(node)

        # Restore contexts
        self.current_function_id = prev_function
        self.parent_stack.pop()
        self.path_stack.pop()

    def _sibling_index(self, node):
        return getattr(node, 'lineno', 0)
    
    def get_node_id_by_path(self, ast_path):
        unique_str = f"{self.file_path}:{ast_path}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))



def normalize_rel_path(file_path):
    """Normalize relative paths to forward slashes for stable IDs across OS."""
    return Path(file_path).as_posix()


def process_file(file_path, repo_root, schema_version):
    """Parses a single Python source file into CPG elements."""
    # CSV may store Windows backslashes; Path handles both on Windows/Linux
    rel_path = Path(file_path)
    abs_path = repo_root / rel_path
    stable_path = normalize_rel_path(rel_path)

    with open(abs_path, "r", encoding="utf-8", errors="ignore") as f:
        source_code = f.read()

    # Calculate sha256 and details for metadata
    sha256 = hashlib.sha256(source_code.encode("utf-8")).hexdigest()
    file_size = abs_path.stat().st_size
    num_lines = len(source_code.splitlines())

    # Parse AST and generate CPG (stable_path => stable UUIDs across OS)
    tree = ast.parse(source_code, filename=stable_path)
    extractor = CPGExtractor(stable_path, source_code)
    extractor.visit(tree)

    event_time = datetime.datetime.utcnow().isoformat() + "Z"

    # Format nodes
    nodes = []
    for n in extractor.nodes:
        n.update({
            "schema_version": schema_version,
            "event_time": event_time
        })
        nodes.append(n)

    # Format edges
    edges = []
    for e in extractor.edges:
        e.update({
            "schema_version": schema_version,
            "event_time": event_time
        })
        edges.append(e)

    # Format metadata
    metadata = {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, stable_path)),
        "file_path": stable_path,
        "size_bytes": file_size,
        "sha256": sha256,
        "num_lines": num_lines,
        "processed_at": event_time,
        "status": "SUCCESS",
        "schema_version": schema_version,
        "event_time": event_time
    }

    return nodes, edges, metadata


def main():
    parser = argparse.ArgumentParser(description="Incremental CPG Parser Service for Python Files")
    parser.add_argument("--discovered-csv", default="output/discovered_files.csv", help="Path to discovered files CSV")
    parser.add_argument("--repo-root", default="peft", help="Path to repository root directory")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka broker servers")
    parser.add_argument("--schema-version", default="1.0.0", help="Schema version for messages")
    parser.add_argument("--dry-run", action="store_true", help="Print stats and events without sending to Kafka")
    parser.add_argument("--limit", type=int, default=0, help="Process only first N source files (0 = all)")
    parser.add_argument("--file", default=None, help="Process a single relative file path (overrides CSV list)")
    args = parser.parse_args()

    discovered_csv_path = Path(args.discovered_csv)
    repo_root_path = Path(args.repo_root)

    if not discovered_csv_path.exists() and args.file is None:
        print(f"Error: Discovered CSV file not found at {discovered_csv_path}")
        sys.exit(1)

    if not repo_root_path.exists():
        print(f"Error: Repository root not found at {repo_root_path}")
        sys.exit(1)

    print(f"=== Starting CPG Parser Service ===")
    print(f"Discovered CSV : {discovered_csv_path}")
    print(f"Repo Root      : {repo_root_path}")
    print(f"Kafka Brokers  : {args.bootstrap_servers}")
    print(f"Schema Version : {args.schema_version}")
    print(f"Dry Run Mode   : {args.dry_run}\n")

    # Build file list: single file OR discovered CSV (category == source)
    if args.file:
        source_files = [args.file]
    else:
        df = pd.read_csv(discovered_csv_path)
        source_files = df[df["category"] == "source"]["relative_path"].tolist()
        if args.limit and args.limit > 0:
            source_files = source_files[:args.limit]

    total_files = len(source_files)
    print(f"Found {total_files} source files to parse.")

    # Initialize Kafka Producer if not in dry-run mode
    producer = None
    if not args.dry_run:
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                # Kafka message key = stable id for partitioning / idempotent sinks
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                request_timeout_ms=30000
            )
            print("[OK] Connected to Kafka Producer successfully.\n")
        except Exception as e:
            print(f"[ERROR] Failed to connect to Kafka: {e}")
            sys.exit(1)

    success_count = 0
    error_count = 0
    total_nodes = 0
    total_edges = 0

    # Process files sequentially to maintain bounded memory usage
    for idx, rel_path_str in enumerate(source_files, 1):
        rel_path = Path(rel_path_str)
        print(f"[{idx}/{total_files}] Processing: {rel_path} ... ", end="")

        try:
            nodes, edges, metadata = process_file(rel_path, repo_root_path, args.schema_version)
            total_nodes += len(nodes)
            total_edges += len(edges)

            # Emit messages
            if args.dry_run:
                print(f"Parsed successfully (Nodes: {len(nodes)}, Edges: {len(edges)}) [DRY RUN]")
                if idx == 1:
                    # Print first file sample to show format
                    print("\n--- SAMPLE NODE EVENT ---")
                    print(json.dumps(nodes[0] if nodes else {}, indent=2))
                    print("\n--- SAMPLE EDGE EVENT ---")
                    print(json.dumps(edges[0] if edges else {}, indent=2))
                    print("\n--- SAMPLE METADATA EVENT ---")
                    print(json.dumps(metadata, indent=2))
                    print("-------------------------\n")
            else:
                # 1. Send Nodes (key = stable node id)
                for node_event in nodes:
                    producer.send("cpg-nodes", key=node_event["id"], value=node_event)
                # 2. Send Edges (key = stable edge id)
                for edge_event in edges:
                    producer.send("cpg-edges", key=edge_event["id"], value=edge_event)
                # 3. Send Metadata (key = stable file id)
                producer.send("cpg-metadata", key=metadata["id"], value=metadata)

                # Flush to ensure delivery and bounded memory
                producer.flush()
                print(f"Parsed and Sent (Nodes: {len(nodes)}, Edges: {len(edges)})")

            success_count += 1

        except Exception as e:
            error_msg = str(e)
            trace = traceback.format_exc()
            event_time = datetime.datetime.utcnow().isoformat() + "Z"
            stable_path = normalize_rel_path(rel_path_str)

            error_event = {
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{stable_path}:error")),
                "file_path": stable_path,
                "error_message": error_msg,
                "error_type": e.__class__.__name__,
                "stack_trace": trace,
                "occurred_at": event_time,
                "schema_version": args.schema_version,
                "event_time": event_time
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
    print(f"===========================")

if __name__ == "__main__":
    main()
