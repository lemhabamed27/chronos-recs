import time
import requests

# Direct IPv4 connection to bypass Windows DNS lookup delay
API_URL = "http://127.0.0.1:8000/recommendations/2000"

print("Starting Real-Time API Latency Verification (IPv4 Optimized)...")
print("------------------------------------------------")

total_time = 0
tests = 5

for i in range(tests):
    start = time.time()
    try:
        response = requests.get(API_URL)
        elapsed = (time.time() - start) * 1000
        total_time += elapsed
        print(f"Request {i+1}: Status {response.status_code} | Real Latency: {elapsed:.2f}ms")
    except Exception as e:
        print(f"Request {i+1} Failed: {e}")
    time.sleep(0.2)

print("------------------------------------------------")
print(f"Average System Latency: {total_time/tests:.2f}ms")