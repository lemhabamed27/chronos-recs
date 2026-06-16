import subprocess
import time
import requests

API_URL = "http://localhost:8000/recommendations/1"

def check_api():
    try:
        start = time.time()
        response = requests.get(API_URL, timeout=5)
        elapsed = (time.time() - start) * 1000
        return True, round(elapsed, 2)
    except Exception as e:
        return False, str(e)

def stop_container(container_name):
    print(f"Stopping container: {container_name}...")
    subprocess.run(["docker", "stop", container_name])

def start_container(container_name):
    print(f"Starting container: {container_name}...")
    subprocess.run(["docker", "start", container_name])

print("===== CHAOS ENGINEERING TEST =====\n")

print("1. Testing API baseline behavior...")
ok, latency = check_api()
print(f"   Status: {'OK' if ok else 'FAIL'} | Latency: {latency}ms\n")

print("2. Injecting failure: Stopping Neo4j graph database...")
stop_container("neo4j")
time.sleep(3)

print("3. Testing API resilience during Neo4j outage...")
ok, latency = check_api()
print(f"   Status: {'OK' if ok else 'FAIL'} | Feedback: {latency}\n")

print("4. Recovering Neo4j infrastructure...")
start_container("neo4j")
print("   Waiting for cluster stabilization...")
time.sleep(12)

print("5. Verifying API performance post-recovery...")
ok, latency = check_api()
print(f"   Status: {'OK' if ok else 'FAIL'} | Latency: {latency}ms\n")

print("6. Injecting network partition: Stopping Apache Kafka message broker...")
stop_container("kafka")
time.sleep(3)

print("7. Testing API availability during message pipeline interruption...")
ok, latency = check_api()
print(f"   Status: {'OK' if ok else 'FAIL'} | Latency: {latency}ms\n")

print("8. Re-establishing network pipeline: Starting Apache Kafka...")
start_container("kafka")
print("   Waiting for consumer group rebalance...")
time.sleep(12)

print("9. Executing final verification under normal conditions...")
ok, latency = check_api()
print(f"   Status: {'OK' if ok else 'FAIL'} | Latency: {latency}ms\n")

print("===== CHAOS TEST COMPLETE =====")