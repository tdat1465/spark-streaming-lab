import shutil
import subprocess
import time

from src.task3.config import KAFKA_BOOTSTRAP, KAFKA_CONTAINER, OUTPUT_DIR, TASK4_DIR, TOPICS


def run(cmd, **kwargs):
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        **kwargs,
    )


def is_docker_available():
    return shutil.which("docker") is not None


def is_kafka_container_running():
    if not is_docker_available():
        return False
    r = run(["docker", "inspect", "--format", "{{.State.Running}}", KAFKA_CONTAINER])
    return r.returncode == 0 and r.stdout.strip() == "true"


def is_broker_reachable():
    try:
        from kafka import KafkaAdminClient

        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
            api_version=(2, 6, 0),
        )
        admin.close()
        return True
    except Exception:
        return False


def start_kafka_stack():
    print("[INFO] Kafka chua san sang — khoi dong ZooKeeper + Kafka (Docker Compose)...")
    r = run(
        ["docker", "compose", "up", "-d", "zookeeper", "kafka"],
        cwd=str(TASK4_DIR),
        timeout=180,
    )
    if r.stdout:
        print(r.stdout.strip())
    if r.stderr:
        print(r.stderr.strip())
    if r.returncode != 0:
        print(f"[CANH BAO] Docker Compose tra ve ma {r.returncode}")
        return False

    print("[INFO] Cho Kafka Broker san sang (15 giay)...")
    time.sleep(15)
    return True


def create_topics_docker():
    for topic in TOPICS:
        r = run(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "kafka-topics",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--bootstrap-server",
                "localhost:9092",
                "--if-not-exists",
            ],
            timeout=30,
        )
        out = (r.stdout + r.stderr).strip()
        print(f"  {topic}: {out if out else 'da ton tai'}")


def list_topics_docker():
    r = run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "kafka-topics",
            "--list",
            "--bootstrap-server",
            "localhost:9092",
        ],
        timeout=30,
    )
    return [t.strip() for t in r.stdout.splitlines() if t.strip()]


def setup_kafka():
    if not is_docker_available():
        print("[CANH BAO] Docker khong co trong PATH.")
        if is_broker_reachable():
            print("[INFO] Broker Kafka da san sang tai", KAFKA_BOOTSTRAP)
        else:
            print(
                "[SKIP] Khong the tao topic — hay khoi dong Kafka truoc "
                "(Docker Compose Task 4 hoac broker WSL)."
            )
            return False

    if not is_broker_reachable():
        if not is_kafka_container_running():
            if not start_kafka_stack():
                return False
        else:
            print("[INFO] Container Kafka dang chay, cho broker...")
            time.sleep(10)

    if not is_broker_reachable():
        print("[SKIP] Broker Kafka chua san sang tai", KAFKA_BOOTSTRAP)
        return False

    print("[INFO] Tao 4 topic bat buoc tren broker Kafka...")
    create_topics_docker()

    print("\n[INFO] kafka-topics --list")
    topics_found = list_topics_docker()
    print("\n".join(f"  - {t}" for t in topics_found))

    missing = set(TOPICS) - set(topics_found)
    if missing:
        print(f"[LOI] Thieu topic: {sorted(missing)}")
        return False

    print("[OK] Du 4 topic Task 3 tren broker.")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "task3_topics_list.log").write_text("\n".join(topics_found), encoding="utf-8")
    return True


if __name__ == "__main__":
    import sys

    ok = setup_kafka()
    sys.exit(0 if ok else 1)
