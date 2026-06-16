import time
import random
import psycopg2
from cassandra.cluster import Cluster

# Database Connection Configurations
PG_CONN = psycopg2.connect(
    host="localhost",
    port=5432,
    database="chronos_db",
    user="admin",
    password="admin123"
)
PG_CURSOR = PG_CONN.cursor()

CASS_CLUSTER = Cluster(["localhost"], port=9042)
CASS_SESSION = CASS_CLUSTER.connect("chronos")

NUM_OPERATIONS = 1000

def benchmark_postgres_write():
    start = time.time()
    for i in range(NUM_OPERATIONS):
        PG_CURSOR.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            (f"user_{i}", f"user_{i}@test.com")
        )
        PG_CONN.commit() 
    elapsed = (time.time() - start) * 1000
    return elapsed

def benchmark_postgres_read():
    start = time.time()
    for i in range(NUM_OPERATIONS):
        PG_CURSOR.execute("SELECT * FROM users WHERE id = %s", (random.randint(1, 10),))
        PG_CURSOR.fetchone()
    elapsed = (time.time() - start) * 1000
    return elapsed

def benchmark_cassandra_write():
    start = time.time()
    futures = []
    for i in range(NUM_OPERATIONS):
        future = CASS_SESSION.execute_async(
            "INSERT INTO click_events (user_id, event_time, product_id, event_type, session_id) VALUES (%s, toTimestamp(now()), %s, %s, %s)",
            (random.randint(1, 100), random.randint(1, 10), "click", f"session_{i}")
        )
        futures.append(future)
    
    for future in futures:
        future.result()
        
    elapsed = (time.time() - start) * 1000
    return elapsed

def benchmark_cassandra_read():
    start = time.time()
    for i in range(NUM_OPERATIONS):
        CASS_SESSION.execute(
            "SELECT * FROM click_events WHERE user_id = %s LIMIT 10",
            (random.randint(1, 100),)
        )
    elapsed = (time.time() - start) * 1000
    return elapsed

# Execution Flow
print("Cleaning up old benchmark data...")
# FIX: Added CASCADE to bypass foreign key constraints with linked tables
PG_CURSOR.execute("TRUNCATE TABLE users CASCADE;")
PG_CONN.commit()
CASS_SESSION.execute("TRUNCATE click_events;")

print("Starting Benchmark...")
print(f"Operations: {NUM_OPERATIONS}\n")

print("Running PostgreSQL Write Benchmark...")
pg_write = benchmark_postgres_write()
print(f"PostgreSQL Write: {pg_write:.2f}ms total | {pg_write/NUM_OPERATIONS:.2f}ms per op\n")

print("Running PostgreSQL Read Benchmark...")
pg_read = benchmark_postgres_read()
print(f"PostgreSQL Read:  {pg_read:.2f}ms total | {pg_read/NUM_OPERATIONS:.2f}ms per op\n")

print("Running Cassandra Write Benchmark...")
cass_write = benchmark_cassandra_write()
print(f"Cassandra Write: {cass_write:.2f}ms total | {cass_write/NUM_OPERATIONS:.2f}ms per op\n")

print("Running Cassandra Read Benchmark...")
cass_read = benchmark_cassandra_read()
print(f"Cassandra Read:  {cass_read:.2f}ms total | {cass_read/NUM_OPERATIONS:.2f}ms per op\n")

print("===== FINAL BENCHMARK RESULTS =====")
print(f"Write Winner: {'Cassandra (LSM-Tree)' if cass_write < pg_write else 'PostgreSQL (B-Tree)'}")
print(f"Read Winner:  {'Cassandra (LSM-Tree)' if cass_read < pg_read else 'PostgreSQL (B-Tree)'}")

PG_CURSOR.close()
PG_CONN.close()
CASS_CLUSTER.shutdown()