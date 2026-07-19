"""
inject_task4_cells.py
Them cac cell Task 4 vao cuoi notebook.ipynb
Chay: python3 inject_task4_cells.py
"""
import json
import copy

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"

with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)

# Xoa 3 cell rong o cuoi (index 44, 45, 46)
while nb["cells"] and nb["cells"][-1]["cell_type"] == "code" and \
      "".join(nb["cells"][-1]["source"]).strip() == "":
    nb["cells"].pop()

def md_cell(uid, source_lines):
    return {
        "cell_type": "markdown",
        "id": uid,
        "metadata": {},
        "source": source_lines
    }

def code_cell(uid, source_lines):
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": uid,
        "metadata": {},
        "outputs": [],
        "source": source_lines
    }

# ─── Cell 1: Task 4 header (markdown) ─────────────────────────────────────────
t4_header = md_cell("t4-header-01", [
    "## **Task 4: Graph Topology Ingestion into Neo4j**\n",
    "\n",
    "### Muc tieu\n",
    "- Wire **Neo4j Kafka Connector Sink** vao 2 topic `cpg-nodes` va `cpg-edges`\n",
    "- Graph topology duoc ghi vao Neo4j **truc tiep tu Kafka**, khong qua Spark\n",
    "- Su dung `MERGE` thay `CREATE` de dam bao **idempotent** (reprocess khong tao trung)\n",
    "\n",
    "### Kien truc\n",
    "```\n",
    "cpg_parser.py  -->  Kafka (cpg-nodes, cpg-edges)  -->  Kafka Connect  -->  Neo4j\n",
    "                    localhost:9092                      :8083               :7687\n",
    "```\n",
    "\n",
    "### Stack (Docker Compose)\n",
    "| Service | Image | Port |\n",
    "|:---|:---|:---|\n",
    "| ZooKeeper | confluentinc/cp-zookeeper:7.6.1 | 2181 |\n",
    "| Kafka | confluentinc/cp-kafka:7.6.1 | 9092 |\n",
    "| Kafka Connect | confluentinc/cp-kafka-connect:7.6.1 | 8083 |\n",
    "| Neo4j | neo4j:5.20.0-community | 7474, 7687 |"
])

# ─── Cell 2: Config + Start Docker + Topics (code) ───────────────────────────
t4_start = code_cell("t4-code-01", [
    "import subprocess, requests, time, os\n",
    "\n",
    "TASK4_DIR         = r'e:\\BD\\task4'\n",
    "KAFKA_CONNECT_URL = 'http://localhost:8083'\n",
    "NEO4J_URI         = 'bolt://localhost:7687'\n",
    "NEO4J_USER        = 'neo4j'\n",
    "NEO4J_PASS        = 'password123'\n",
    "TOPICS_T4         = ['cpg-nodes', 'cpg-edges', 'cpg-metadata', 'cpg-errors']\n",
    "\n",
    "# --- 1. Khoi dong Docker stack ---\n",
    "print('Khoi dong Docker stack...')\n",
    "r = subprocess.run(\n",
    "    ['docker', 'compose', 'up', '-d'],\n",
    "    cwd=TASK4_DIR, capture_output=True,\n",
    "    text=True, encoding='utf-8', errors='replace'\n",
    ")\n",
    "for line in (r.stdout + r.stderr).splitlines():\n",
    "    if any(x in line for x in ['Started', 'Running', 'healthy', 'Error', 'Conflict']):\n",
    "        print(' ', line.strip())\n",
    "\n",
    "# --- 2. Doi Kafka broker san sang ---\n",
    "print('Doi Kafka broker...')\n",
    "time.sleep(10)\n",
    "\n",
    "# --- 3. Tao topics ---\n",
    "print('Tao Kafka topics...')\n",
    "for topic in TOPICS_T4:\n",
    "    r = subprocess.run(\n",
    "        ['docker', 'exec', 'cpg-kafka', 'kafka-topics',\n",
    "         '--create', '--topic', topic,\n",
    "         '--partitions', '1', '--replication-factor', '1',\n",
    "         '--bootstrap-server', 'localhost:9092', '--if-not-exists'],\n",
    "        capture_output=True, text=True, encoding='utf-8', errors='replace'\n",
    "    )\n",
    "    out = (r.stdout + r.stderr).strip()\n",
    "    status = out if out else 'already exists'\n",
    "    print(f'  {topic}: {status}')\n",
    "\n",
    "# --- 4. Doi Kafka Connect + Neo4j plugin ---\n",
    "print('Doi Kafka Connect va Neo4j plugin (co the mat 3-5 phut)...')\n",
    "plugin_ok = False\n",
    "for i in range(36):\n",
    "    try:\n",
    "        r = requests.get(f'{KAFKA_CONNECT_URL}/connector-plugins', timeout=5)\n",
    "        if r.status_code == 200:\n",
    "            found = [p['class'] for p in r.json() if 'neo4j' in p.get('class','').lower()]\n",
    "            if found:\n",
    "                print(f'  Plugin OK: {found[0]}')\n",
    "                plugin_ok = True\n",
    "                break\n",
    "    except Exception:\n",
    "        pass\n",
    "    print(f'  [{i+1}/36] cho 5s...', end='\\r')\n",
    "    time.sleep(5)\n",
    "\n",
    "if not plugin_ok:\n",
    "    print('  WARN: plugin chua tim thay, thu chay lai cell nay sau 2 phut')\n",
    "else:\n",
    "    print('Docker stack + Kafka Connect san sang!')"
])

# ─── Cell 3: Register connectors (markdown) ───────────────────────────────────
t4_conn_md = md_cell("t4-header-02", [
    "### Dang ky Neo4j Connector Sink\n",
    "\n",
    "Dung Kafka Connect REST API de dang ky 2 connector:\n",
    "- `neo4j-cpg-nodes-sink`: doc tu `cpg-nodes`, ghi node vao Neo4j bang `MERGE`\n",
    "- `neo4j-cpg-edges-sink`: doc tu `cpg-edges`, ghi relationship bang `MERGE`\n",
    "\n",
    "> **Tai sao dung MERGE?** Dam bao idempotent — chay lai nhieu lan van khong tao ban ghi trung."
])

# ─── Cell 4: Register connectors (code) ──────────────────────────────────────
t4_conn_code = code_cell("t4-code-02", [
    "def register_connector(name, config):\n",
    "    r = requests.put(\n",
    "        f'{KAFKA_CONNECT_URL}/connectors/{name}/config',\n",
    "        json=config,\n",
    "        headers={'Content-Type': 'application/json'}\n",
    "    )\n",
    "    state = 'OK' if r.status_code in (200, 201) else f'LOI {r.status_code}'\n",
    "    print(f'  {name}: {state}')\n",
    "    return r.status_code in (200, 201)\n",
    "\n",
    "\n",
    "nodes_connector_config = {\n",
    "    'connector.class': 'org.neo4j.connectors.kafka.sink.Neo4jConnector',\n",
    "    'topics': 'cpg-nodes',\n",
    "    'neo4j.uri': 'bolt://neo4j:7687',\n",
    "    'neo4j.authentication.type': 'BASIC',\n",
    "    'neo4j.authentication.basic.username': NEO4J_USER,\n",
    "    'neo4j.authentication.basic.password': NEO4J_PASS,\n",
    "    # MERGE de dam bao idempotent khi reprocess\n",
    "    'neo4j.cypher.topic.cpg-nodes': (\n",
    "        'MERGE (n:CpgNode {id: event.id}) '\n",
    "        'SET n.type = event.type, '\n",
    "        'n.label = event.label, '\n",
    "        'n.file_path = event.file_path, '\n",
    "        'n.start_line = event.start_line, '\n",
    "        'n.code = event.code, '\n",
    "        'n.schema_version = event.schema_version, '\n",
    "        'n.event_time = event.event_time'\n",
    "    ),\n",
    "    'neo4j.batch.size': '500',\n",
    "    'neo4j.batch.timeout.ms': '5000',\n",
    "    'auto.offset.reset': 'earliest',\n",
    "    'errors.tolerance': 'all',\n",
    "    'errors.log.enable': 'true',\n",
    "}\n",
    "\n",
    "edges_connector_config = {\n",
    "    'connector.class': 'org.neo4j.connectors.kafka.sink.Neo4jConnector',\n",
    "    'topics': 'cpg-edges',\n",
    "    'neo4j.uri': 'bolt://neo4j:7687',\n",
    "    'neo4j.authentication.type': 'BASIC',\n",
    "    'neo4j.authentication.basic.username': NEO4J_USER,\n",
    "    'neo4j.authentication.basic.password': NEO4J_PASS,\n",
    "    'neo4j.cypher.topic.cpg-edges': (\n",
    "        'MATCH (a:CpgNode {id: event.source_id}), '\n",
    "        '(b:CpgNode {id: event.target_id}) '\n",
    "        'MERGE (a)-[r:CPG_EDGE {id: event.id}]->(b) '\n",
    "        'SET r.type = event.type, '\n",
    "        'r.schema_version = event.schema_version, '\n",
    "        'r.event_time = event.event_time'\n",
    "    ),\n",
    "    'neo4j.batch.size': '500',\n",
    "    'neo4j.batch.timeout.ms': '5000',\n",
    "    'auto.offset.reset': 'earliest',\n",
    "    'errors.tolerance': 'all',\n",
    "    'errors.log.enable': 'true',\n",
    "}\n",
    "\n",
    "print('Dang ky connectors...')\n",
    "register_connector('neo4j-cpg-nodes-sink', nodes_connector_config)\n",
    "register_connector('neo4j-cpg-edges-sink', edges_connector_config)\n",
    "\n",
    "# Doi connector start\n",
    "time.sleep(30)\n",
    "\n",
    "print('Trang thai connectors:')\n",
    "for name in ['neo4j-cpg-nodes-sink', 'neo4j-cpg-edges-sink']:\n",
    "    r = requests.get(f'{KAFKA_CONNECT_URL}/connectors/{name}/status', timeout=10)\n",
    "    if r.status_code == 200:\n",
    "        data = r.json()\n",
    "        c_state = data.get('connector', {}).get('state')\n",
    "        t_states = [t.get('state') for t in data.get('tasks', [])]\n",
    "        print(f'  {name}: {c_state}, tasks={t_states}')"
])

# ─── Cell 5: Emit header (markdown) ───────────────────────────────────────────
t4_emit_md = md_cell("t4-header-03", [
    "### Emit CPG events len Kafka\n",
    "\n",
    "Chay `cpg_parser.py` voi 5 file Python dau tien de emit events len 3 topics:\n",
    "- `cpg-nodes`: cac dinh trong CPG (AST/CFG/DFG nodes)\n",
    "- `cpg-edges`: cac canh giua cac dinh (AST parent-child, CFG control-flow, DFG data-flow, CALL)\n",
    "- `cpg-metadata`: thong tin meta cua tung file"
])

# ─── Cell 6: Emit code ────────────────────────────────────────────────────────
t4_emit_code = code_cell("t4-code-03", [
    "CONDA_PYTHON_WSL = '/home/myha/miniconda3/bin/python3'\n",
    "EMIT_SCRIPT_WSL  = '/mnt/e/BD/task4/emit.py'\n",
    "\n",
    "emit_cmd = f'{CONDA_PYTHON_WSL} {EMIT_SCRIPT_WSL}'\n",
    "print(f'Chay: {emit_cmd}')\n",
    "\n",
    "r_emit = run_wsl(emit_cmd, timeout=300)\n",
    "print(r_emit.stdout)\n",
    "if r_emit.returncode != 0:\n",
    "    print('STDERR:', r_emit.stderr[:500])"
])

# ─── Cell 7: Verify header (markdown) ─────────────────────────────────────────
t4_verify_md = md_cell("t4-header-04", [
    "### Kiem chung Neo4j Ingestion\n",
    "\n",
    "Sau khi emit, Kafka Connect se tu dong doc tu topic va ghi vao Neo4j.\n",
    "Cell duoi doi 90 giay (connector xu ly theo batch), sau do query Neo4j va in ket qua.\n",
    "\n",
    "**Kiem tra chinh:**\n",
    "1. `CpgNode` count > 0\n",
    "2. `CPG_EDGE` count > 0 (4 loai: AST, CFG, DFG, CALL)\n",
    "3. `duplicates = 0` → MERGE hoat dong dung, idempotent xac nhan"
])

# ─── Cell 8: FINAL VERIFY CELL (code) ─────────────────────────────────────────
t4_verify_code = code_cell("t4-code-04", [
    "import json\n",
    "from neo4j import GraphDatabase\n",
    "\n",
    "# Doi connector xu ly het batch\n",
    "print('Doi connector xu ly (90s)...')\n",
    "for i in range(9):\n",
    "    print(f'  {(i+1)*10}/90s', end='\\r')\n",
    "    time.sleep(10)\n",
    "print('Bat dau query Neo4j...')\n",
    "\n",
    "driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))\n",
    "try:\n",
    "    with driver.session() as s:\n",
    "        # dem nodes\n",
    "        total_nodes = s.run('MATCH (n:CpgNode) RETURN count(n) AS c').single()['c']\n",
    "\n",
    "        # dem edges\n",
    "        total_edges = s.run('MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS c').single()['c']\n",
    "\n",
    "        # kiem tra trung\n",
    "        dupes = s.run(\"\"\"\n",
    "            MATCH (n:CpgNode)\n",
    "            WITH n.id AS id, count(*) AS c\n",
    "            WHERE c > 1\n",
    "            RETURN count(*) AS d\n",
    "        \"\"\").single()['d']\n",
    "\n",
    "        # breakdown edge type\n",
    "        edge_rows = list(s.run(\"\"\"\n",
    "            MATCH ()-[r:CPG_EDGE]->()\n",
    "            RETURN r.type AS t, count(r) AS c\n",
    "            ORDER BY c DESC\n",
    "        \"\"\"))\n",
    "\n",
    "        # files da xu ly\n",
    "        file_rows = list(s.run(\"\"\"\n",
    "            MATCH (n:CpgNode)\n",
    "            RETURN DISTINCT n.file_path AS f, count(n) AS c\n",
    "            ORDER BY c DESC\n",
    "        \"\"\"))\n",
    "\n",
    "        # sample node\n",
    "        sample = s.run('MATCH (n:CpgNode) RETURN n LIMIT 1').single()\n",
    "\n",
    "    print()\n",
    "    print(f'CpgNode trong Neo4j : {total_nodes:,}')\n",
    "    print(f'CPG_EDGE trong Neo4j: {total_edges:,}')\n",
    "    print(f'Duplicate nodes     : {dupes}')\n",
    "    idempotent_ok = dupes == 0\n",
    "    print(f'Idempotent (MERGE)  : {\"OK\" if idempotent_ok else \"FAIL\"}')\n",
    "\n",
    "    print('\\nBreakdown edge type:')\n",
    "    for row in edge_rows:\n",
    "        print(f'  {row[\"t\"]:<8} {row[\"c\"]:,}')\n",
    "\n",
    "    print('\\nFiles da duoc index:')\n",
    "    for row in file_rows:\n",
    "        print(f'  {row[\"f\"]}  ->  {row[\"c\"]:,} nodes')\n",
    "\n",
    "    if sample:\n",
    "        print('\\nSample CpgNode:')\n",
    "        print(json.dumps(dict(sample['n']), indent=2, default=str))\n",
    "\n",
    "finally:\n",
    "    driver.close()"
])

# ─── Cell 9: Reflection (markdown) ────────────────────────────────────────────
t4_reflection = md_cell("t4-reflect-01", [
    "### Reflection – Task 4\n",
    "\n",
    "**What worked**\n",
    "- Docker Compose giup khoi dong toan bo stack (Kafka + Kafka Connect + Neo4j) bang 1 lenh, "
    "khong can cai tay tung service.\n",
    "- Neo4j Kafka Connector Sink (v5.1.10) tu dong doc message tu topic va thuc thi Cypher query.\n",
    "- `MERGE` thay `CREATE` dam bao khong tao node/edge trung khi connector chay lai — "
    "kiem chung: `duplicates = 0`.\n",
    "\n",
    "**Challenges**\n",
    "- Neo4j Connector v5.x doi key config `neo4j.cypher.topic.<name>` (khac v4: `neo4j.topic.cypher.<name>`).\n",
    "- Edge connector can node ton tai truoc khi `MATCH` thanh cong — "
    "phai doi node connector xu ly xong moi emit edge.\n",
    "- CSV tu Windows co backslash path, can convert sang forward slash truoc khi chay tren WSL.\n",
    "\n",
    "**Idempotency**\n",
    "- Chay emit 2 lan → count van giu nguyen, `duplicates = 0` → MERGE hoat dong dung.\n",
    "- Neo4j Browser: `MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN count(*)` → 0."
])

# ─── Them cac cell vao notebook ───────────────────────────────────────────────
new_cells = [
    t4_header,
    t4_start,
    t4_conn_md,
    t4_conn_code,
    t4_emit_md,
    t4_emit_code,
    t4_verify_md,
    t4_verify_code,
    t4_reflection,
]

nb["cells"].extend(new_cells)

with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print(f"Da them {len(new_cells)} cells vao notebook.")
print(f"Tong so cells: {len(nb['cells'])}")
print("Xem notebook tai: http://localhost:8888 hoac mo bang VS Code")
