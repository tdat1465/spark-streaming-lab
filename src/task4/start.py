import subprocess
from pathlib import Path
import time
import sys
import requests

TASK4_DIR         = str(Path(__file__).resolve().parent)
KAFKA_CONNECT_URL = "http://localhost:8083"
TOPICS            = ["cpg-nodes", "cpg-edges", "cpg-metadata", "cpg-errors"]
CONTAINERS        = ["cpg-zookeeper", "cpg-kafka", "cpg-kafka-connect", "cpg-neo4j"]


def run(cmd, cwd=None, timeout=60):
    return subprocess.run(
        cmd, cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout
    )


def cleanup_old_containers():
    # Xóa các container cũ nếu còn tồn tại
    print("Kiểm tra và xóa container cũ (nếu có)...")

    for name in CONTAINERS:
        r = run(["docker", "inspect", "--format", "{{.State.Status}}", name])

        if r.returncode == 0:
            status = r.stdout.strip()
            run(["docker", "rm", "-f", name])
            print(f"  Đã xóa container: {name} (trạng thái trước đó: {status})")


if __name__ == "__main__":

    # Xóa các container cũ
    cleanup_old_containers()

    # Khởi động Docker Compose
    print("Khởi động Docker Stack...")

    r = run(["docker", "compose", "up", "-d"], cwd=TASK4_DIR, timeout=120)

    for line in (r.stdout + r.stderr).splitlines():
        if any(x in line for x in ["Started", "Running", "healthy", "Error", "Conflict", "created"]):
            print(f"  {line.strip()}")

    if r.returncode not in (0, 1):
        print(f"[CẢNH BÁO] Docker Compose trả về mã {r.returncode}")

    # Chờ Kafka Broker khởi động
    print("Chờ Kafka Broker sẵn sàng (15 giây)...")
    time.sleep(15)

    # Tạo các Kafka topic
    print("Tạo các Kafka topic...")

    for topic in TOPICS:
        r = run(
            [
                "docker", "exec", "cpg-kafka", "kafka-topics",
                "--create",
                "--topic", topic,
                "--partitions", "1",
                "--replication-factor", "1",
                "--bootstrap-server", "localhost:9092",
                "--if-not-exists"
            ],
            timeout=15
        )

        out = (r.stdout + r.stderr).strip()
        print(f"  {topic}: {out if out else 'Đã tồn tại'}")

    # Chờ Kafka Connect và plugin Neo4j
    print("Chờ Kafka Connect và plugin Neo4j khởi động (có thể mất 3–5 phút)...")

    plugin_ok = False

    for i in range(36):
        try:
            r = requests.get(f"{KAFKA_CONNECT_URL}/connector-plugins", timeout=5)

            if r.status_code == 200:
                found = [
                    p["class"]
                    for p in r.json()
                    if "neo4j" in p.get("class", "").lower()
                ]

                if found:
                    print(f"  Đã tìm thấy plugin Neo4j: {found[0]}")
                    plugin_ok = True
                    break

        except Exception:
            pass

        print(f"  [{i + 1}/36] Chờ 5 giây...", end="\r")
        time.sleep(5)

    if not plugin_ok:
        print("  [CẢNH BÁO] Plugin Neo4j chưa sẵn sàng, vui lòng thử lại sau.")
        sys.exit(1)

    print("Docker Stack và Kafka Connect đã sẵn sàng.")