"""
Test script to verify all services are running and accessible
"""

import sys

def test_postgres():
    """Test PostgreSQL connection"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="traffic_db",
            user="trafficuser",
            password="trafficpass"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM traffic_raw;")
        count = cursor.fetchone()[0]
        conn.close()
        print("✅ PostgreSQL: Connected successfully")
        print(f"   - Records in traffic_raw: {count}")
        return True
    except Exception as e:
        print(f"❌ PostgreSQL: Failed - {e}")
        return False

def test_kafka():
    """Test Kafka connection"""
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.admin import KafkaAdminClient
        
        # Test connection
        admin = KafkaAdminClient(
            bootstrap_servers='localhost:9092',
            request_timeout_ms=5000
        )
        
        # List topics
        topics = admin.list_topics()
        admin.close()
        
        print("✅ Kafka: Connected successfully")
        print(f"   - Available topics: {topics}")
        
        # Check required topics
        required = ['traffic-data', 'critical-traffic']
        missing = [t for t in required if t not in topics]
        
        if missing:
            print(f"   ⚠️  Missing topics: {missing}")
            print(f"   Run: docker exec -it kafka kafka-topics --create --topic {missing[0]} --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1")
            return False
        else:
            print(f"   - All required topics exist")
        
        return True
    except Exception as e:
        print(f"❌ Kafka: Failed - {e}")
        return False

def test_airflow():
    """Test Airflow connection"""
    try:
        import requests
        response = requests.get('http://localhost:8081/health', timeout=5)
        if response.status_code == 200:
            print("✅ Airflow: Webserver accessible")
            print("   - URL: http://localhost:8081")
            print("   - Login: admin / admin")
            return True
        else:
            print(f"❌ Airflow: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Airflow: Failed - {e}")
        return False

def main():
    print("\n" + "="*60)
    print("🔍 TESTING ALL SERVICES")
    print("="*60 + "\n")
    
    results = {
        'PostgreSQL': test_postgres(),
        'Kafka': test_kafka(),
        'Airflow': test_airflow()
    }
    
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    for service, status in results.items():
        symbol = "✅" if status else "❌"
        print(f"{symbol} {service}: {'PASS' if status else 'FAIL'}")
    
    print("="*60 + "\n")
    
    if all(results.values()):
        print("🎉 All services are running correctly!\n")
        return 0
    else:
        print("⚠️  Some services failed. Check the errors above.\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())