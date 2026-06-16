import json
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

KAFKA_TOPIC = "chronos.public.users"
KAFKA_BOOTSTRAP = "localhost:9093"
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "admin123"

system_vector_clocks = {}
event_buffer = {}

def connect_cassandra():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    return cluster.connect("chronos")

def insert_cassandra_log(session, user_id, event_type="user_action"):
    query = """
        INSERT INTO click_events (user_id, event_time, product_id, event_type, session_id)
        VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (
        user_id,
        datetime.utcnow(),
        0,
        event_type,
        "cdc_stream_pipeline"
    ))
    print(f"[Cassandra] Persistent log saved for User ID: {user_id}")

def update_neo4j_graph(driver, user_id, name):
    query = """
        MERGE (u:User {id: $user_id})
        SET u.name = $name, u.last_updated = timestamp()
        RETURN u
    """
    with driver.session() as session:
        session.execute_write(lambda tx: tx.run(query, user_id=user_id, name=name))
    print(f"[Neo4j Graph] Node updated/merged for User: {name} (ID: {user_id})")

def validate_vector_clock(user_id, incoming_clock):
    if user_id not in system_vector_clocks:
        system_vector_clocks[user_id] = incoming_clock.copy()
        return True

    current_clock = system_vector_clocks[user_id]
    incoming_counter = incoming_clock.get("postgres_node", 0)
    current_counter = current_clock.get("postgres_node", 0)

    if incoming_counter == current_counter + 1:
        system_vector_clocks[user_id] = incoming_clock.copy()
        return True
    elif incoming_counter <= current_counter:
        print(f"[Vector Clock Reject] Stale event dropped for User {user_id}. Local: {current_counter}, Incoming: {incoming_counter}")
        return False
    else:
        print(f"[Vector Clock Buffer] Gap detected for User {user_id}. Local: {current_counter}, Incoming: {incoming_counter}")
        return False

def main():
    print("Initializing Database Connections...")
    cass_session = connect_cassandra()
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    print("Database connections established.")

    print("Subscribing to Kafka Topic...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("Pipeline active. Awaiting CDC events...")

    for message in consumer:
        data = message.value
        payload = data.get("payload", {})
        after = payload.get("after")
        
        if after:
            user_id = after.get("id")
            user_name = after.get("name", "Unknown")
            incoming_clock = payload.get("vector_clock", {"postgres_node": after.get("version", 1)})
            
            print(f"\n--- Processing Event for User ID: {user_id} ---")
            
            if validate_vector_clock(user_id, incoming_clock):
                insert_cassandra_log(cass_session, user_id, event_type="user_sync")
                update_neo4j_graph(neo4j_driver, user_id, user_name)
                print(f"[Status] Pipeline execution successfully completed under target threshold.")
            else:
                event_buffer[user_id] = {"after": after, "clock": incoming_clock}

if __name__ == "__main__":
    main()