import socket
import sys
import time

def test_socket_connection(host="localhost", port=9092):
    print(f"🔍 Testing socket connection to {host}:{port}...")
    try:
        with socket.create_connection((host, port), timeout=2.0) as s:
            print(f"✅ Success: Able to open a raw socket connection to {host}:{port}!")
            return True
    except Exception as e:
        print(f"❌ Error: Could not connect to {host}:{port}. Reason: {e}")
        print("\n💡 Trouble-shooting Tips:")
        print("  1. Make sure your docker container is running (run 'docker ps' and look for 'sre-kafka-kraft').")
        print("  2. If it's running but unreachable, make sure port 9092 is exposed in docker-compose.")
        return False

def test_kafka_library_connection():
    try:
        from kafka import KafkaProducer
        import json
        print("\n🔍 Testing KafkaProducer connection using 'kafka-python-ng'...")
        
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=2000,
            api_version_auto_timeout_ms=2000
        )
        print("✅ Success: KafkaProducer successfully initialized and handshake completed!")
        
        test_msg = {"test": "hello_world", "timestamp": int(time.time() * 1000)}
        print(f"📤 Attempting to send a test message: {test_msg}")
        
        # Send to topic
        future = producer.send("sre-pod-telemetry", value=test_msg)
        result = future.get(timeout=2.0)
        print(f"✅ Success: Message successfully acknowledged by broker! (Topic: {result.topic}, Partition: {result.partition}, Offset: {result.offset})")
        producer.close()
        return True
    except ImportError:
        print("❌ Error: 'kafka-python-ng' library is not installed in this Python environment.")
        print("👉 Run: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"❌ Error: KafkaProducer failed to handshake or send message. Reason: {e}")
        return False

if __name__ == "__main__":
    print("====================================================")
    print("🔒 SRE LOCAL KAFKA DIAGNOSTIC TEST SUITE")
    print("====================================================")
    
    socket_ok = test_socket_connection()
    if socket_ok:
        test_kafka_library_connection()
        
    print("====================================================")
