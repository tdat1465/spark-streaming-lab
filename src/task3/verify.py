import json
import sys

import pandas as pd
from kafka import KafkaConsumer, TopicPartition

from src.task3.config import KAFKA_BOOTSTRAP, OUTPUT_DIR, TOPICS
from src.task3.setup_kafka import is_broker_reachable


def verify_topics():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        request_timeout_ms=15000,
        api_version=(2, 6, 0),
    )

    print("=== Kafka topic offsets (bang chung ghi thanh cong) ===")
    offset_rows = []
    for topic in TOPICS:
        tp = TopicPartition(topic, 0)
        begin = consumer.beginning_offsets([tp])[tp]
        end = consumer.end_offsets([tp])[tp]
        count = end - begin
        offset_rows.append({"topic": topic, "begin": begin, "end": end, "messages": count})
        print(f"  {topic:14s}  begin={begin:<8d} end={end:<8d} messages={count}")

    consumer.close()

    offset_df = pd.DataFrame(offset_rows)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    offset_df.to_csv(OUTPUT_DIR / "task3_offsets.csv", index=False)
    print(f"[OK] Saved -> {OUTPUT_DIR / 'task3_offsets.csv'}")
    return offset_rows


def read_one(topic_name, timeout_ms=8000):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        api_version=(2, 6, 0),
    )
    for msg in consumer:
        consumer.close()
        return msg.key, msg.value
    consumer.close()
    return None, None


def verify_samples():
    samples = {}
    for topic in ["cpg-nodes", "cpg-edges", "cpg-metadata"]:
        key, value = read_one(topic)
        samples[topic] = {"key": key, "value": value}
        print(f"\n===== SAMPLE from {topic} =====")
        print("Kafka key:", key)
        print(json.dumps(value, indent=2, ensure_ascii=False) if value else "(no message)")
        if value:
            assert "schema_version" in value, f"{topic} thieu schema_version"
            assert "event_time" in value, f"{topic} thieu event_time"

    sample_path = OUTPUT_DIR / "task3_kafka_samples.json"
    sample_path.write_text(json.dumps(samples, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n[OK] Saved samples -> {sample_path}")


def verify():
    if not is_broker_reachable():
        print(
            "[SKIP] Kafka broker chua san sang tai",
            KAFKA_BOOTSTRAP,
            "— hay chay setup_kafka.py va emit.py truoc.",
        )
        return False

    verify_topics()
    verify_samples()
    return True


if __name__ == "__main__":
    ok = verify()
    sys.exit(0 if ok else 1)
