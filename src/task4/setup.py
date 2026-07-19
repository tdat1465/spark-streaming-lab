import requests
import time

# Cấu hình kết nối
CONNECT_URL = "http://localhost:8083"
NEO4J_URI   = "bolt://neo4j:7687"
NEO4J_USER  = "neo4j"
NEO4J_PASS  = "password123"


def wait_for_connect(timeout=120):
    """Chờ Kafka Connect sẵn sàng."""
    print("Đang chờ Kafka Connect sẵn sàng...", end="", flush=True)

    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            r = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if r.status_code == 200:
                print(" OK")
                return True
        except Exception:
            pass

        print(".", end="", flush=True)
        time.sleep(5)

    print(" Hết thời gian chờ")
    return False


def register_connector(name, config):
    """Đăng ký hoặc cập nhật Kafka Connector."""
    r = requests.put(
        f"{CONNECT_URL}/connectors/{name}/config",
        json=config,
        headers={"Content-Type": "application/json"},
    )

    if r.status_code in (200, 201):
        print(f"  [{name}] Đăng ký thành công")
        return True

    print(f"  [{name}] Lỗi {r.status_code}: {r.text[:200]}")
    return False


def check_status(name):
    """Lấy trạng thái của connector."""
    r = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10)

    if r.status_code != 200:
        return "UNKNOWN", []

    data = r.json()
    state = data.get("connector", {}).get("state", "UNKNOWN")
    tasks = [t.get("state") for t in data.get("tasks", [])]

    return state, tasks


# Cấu hình connector: cpg-nodes -> Neo4j Node
# Sử dụng MERGE để đảm bảo idempotent
nodes_config = {
    "connector.class": "org.neo4j.connectors.kafka.sink.Neo4jConnector",
    "topics": "cpg-nodes",
    "neo4j.uri": NEO4J_URI,
    "neo4j.authentication.type": "BASIC",
    "neo4j.authentication.basic.username": NEO4J_USER,
    "neo4j.authentication.basic.password": NEO4J_PASS,
    "neo4j.cypher.topic.cpg-nodes": (
        "MERGE (n:CpgNode {id: event.id}) "
        "SET n.type = event.type, "
        "n.label = event.label, "
        "n.file_path = event.file_path, "
        "n.start_line = event.start_line, "
        "n.start_column = event.start_column, "
        "n.end_line = event.end_line, "
        "n.end_column = event.end_column, "
        "n.code = event.code, "
        "n.schema_version = event.schema_version, "
        "n.event_time = event.event_time"
    ),
    "neo4j.batch.size": "500",
    "neo4j.batch.timeout.ms": "5000",
    "auto.offset.reset": "earliest",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
}

# Cấu hình connector: cpg-edges -> Neo4j Relationship
edges_config = {
    "connector.class": "org.neo4j.connectors.kafka.sink.Neo4jConnector",
    "topics": "cpg-edges",
    "neo4j.uri": NEO4J_URI,
    "neo4j.authentication.type": "BASIC",
    "neo4j.authentication.basic.username": NEO4J_USER,
    "neo4j.authentication.basic.password": NEO4J_PASS,
    "neo4j.cypher.topic.cpg-edges": (
        "MATCH (a:CpgNode {id: event.source_id}), "
        "(b:CpgNode {id: event.target_id}) "
        "MERGE (a)-[r:CPG_EDGE {id: event.id}]->(b) "
        "SET r.type = event.type, "
        "r.schema_version = event.schema_version, "
        "r.event_time = event.event_time"
    ),
    "neo4j.batch.size": "500",
    "neo4j.batch.timeout.ms": "5000",
    "auto.offset.reset": "earliest",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
}


if __name__ == "__main__":

    if not wait_for_connect():
        print("Kafka Connect chưa sẵn sàng, vui lòng thử lại sau.")
        raise SystemExit(1)

    # Kiểm tra plugin Neo4j đã được cài đặt hay chưa
    print("\nKiểm tra plugin Neo4j:")

    r = requests.get(f"{CONNECT_URL}/connector-plugins", timeout=10)
    plugins = r.json() if r.status_code == 200 else []

    for p in plugins:
        if "neo4j" in p.get("class", "").lower():
            print(f"  Tìm thấy: {p['class']} (phiên bản {p.get('version', '?')})")

    # Đăng ký hai connector
    print("\nĐăng ký các connector...")

    register_connector("neo4j-cpg-nodes-sink", nodes_config)
    register_connector("neo4j-cpg-edges-sink", edges_config)

    print("\nChờ connector khởi động (30 giây)...")
    time.sleep(30)

    # Kiểm tra trạng thái connector
    print("\nTrạng thái connector:")

    for name in ["neo4j-cpg-nodes-sink", "neo4j-cpg-edges-sink"]:
        state, tasks = check_status(name)
        print(f"  {name}: {state}, tasks={tasks}")