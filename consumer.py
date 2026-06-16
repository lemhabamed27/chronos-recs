from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
from datetime import datetime

KAFKA_TOPIC = "chronos.public.users"
KAFKA_BOOTSTRAP = "localhost:9093"
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042

def connect_cassandra():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect("chronos")
    return session

def insert_click_event(session, user_id):
    query = """
        INSERT INTO click_events (user_id, event_time, product_id, event_type, session_id)
        VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (
        user_id,
        datetime.utcnow(),
        0,
        "user_created",
        "cdc_stream"
    ))
    print(f"Saved to Cassandra: user_id={user_id}")

def main():
    print("Connecting to Cassandra...")
    session = connect_cassandra()
    print("Cassandra connected!")

    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("Kafka connected! Waiting for messages...")

    for message in consumer:
        data = message.value
        payload = data.get("payload", {})
        after = payload.get("after")
        if after:
            user_id = after.get("id")
            print(f"New event: {after}")
            insert_click_event(session, user_id)

if __name__ == "__main__":
    main()