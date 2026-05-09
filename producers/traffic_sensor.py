"""
Traffic Sensor Simulator - Generates mock traffic data for 4 junctions
This script simulates traffic sensors sending data every second
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'traffic-data'

# Sensor locations (4 junctions in Colombo)
SENSORS = {
    'A': 'Galle Road Junction',
    'B': 'Baseline Road Junction', 
    'C': 'Duplication Road Junction',
    'D': 'Marine Drive Junction'
}

class TrafficSensor:
    """Simulates a traffic sensor at a junction"""
    
    def __init__(self, sensor_id, location):
        self.sensor_id = sensor_id
        self.location = location
        self.congestion_mode = False  # Flag to simulate congestion
        
    def generate_data(self):
        """Generate realistic traffic data"""
        
        current_hour = datetime.now().hour
        
        # Normal traffic patterns based on time of day
        if 7 <= current_hour <= 9 or 17 <= current_hour <= 19:
            # Peak hours - more vehicles, slower speeds
            base_vehicle_count = random.randint(40, 80)
            base_speed = random.randint(15, 35)
        elif 10 <= current_hour <= 16:
            # Mid-day - moderate traffic
            base_vehicle_count = random.randint(20, 40)
            base_speed = random.randint(30, 50)
        else:
            # Off-peak - light traffic
            base_vehicle_count = random.randint(5, 20)
            base_speed = random.randint(40, 60)
        
        # Randomly trigger congestion (5% chance per reading)
        if random.random() < 0.05:
            self.congestion_mode = True
            print(f"⚠️  CONGESTION TRIGGERED at {self.location}!")
        
        # If in congestion mode, generate critical data
        if self.congestion_mode:
            vehicle_count = random.randint(60, 100)
            avg_speed = random.randint(3, 9)  # Below 10 km/h threshold
            
            # Exit congestion mode after a few readings
            if random.random() < 0.3:
                self.congestion_mode = False
                print(f"✅ Congestion cleared at {self.location}")
        else:
            vehicle_count = base_vehicle_count
            avg_speed = base_speed
        
        # Create data packet
        data = {
            'sensor_id': self.sensor_id,
            'location': self.location,
            'timestamp': datetime.now().isoformat(),
            'vehicle_count': vehicle_count,
            'avg_speed': avg_speed
        }
        
        return data


class TrafficProducer:
    """Kafka Producer that sends sensor data"""
    
    def __init__(self):
        self.producer = None
        self.sensors = []
        self.connect_kafka()
        self.initialize_sensors()
        
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3
            )
            print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
        except KafkaError as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def initialize_sensors(self):
        """Create sensor objects for each junction"""
        for sensor_id, location in SENSORS.items():
            sensor = TrafficSensor(sensor_id, location)
            self.sensors.append(sensor)
        print(f"✅ Initialized {len(self.sensors)} traffic sensors")
    
    def send_data(self, data):
        """Send data to Kafka topic"""
        try:
            future = self.producer.send(KAFKA_TOPIC, value=data)
            future.get(timeout=10)  # Wait for confirmation
            return True
        except Exception as e:
            print(f"❌ Error sending data: {e}")
            return False
    
    def run(self, duration_seconds=None):
        """
        Run the sensor simulation
        Args:
            duration_seconds: How long to run (None = forever)
        """
        print("\n" + "="*60)
        print("🚦 TRAFFIC SENSOR SIMULATION STARTED")
        print("="*60)
        print(f"Topic: {KAFKA_TOPIC}")
        print(f"Sensors: {len(self.sensors)}")
        print(f"Frequency: Every 1 second per sensor")
        print("="*60 + "\n")
        
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                # Check if duration exceeded
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break
                
                # Each sensor generates and sends data
                for sensor in self.sensors:
                    data = sensor.generate_data()
                    
                    if self.send_data(data):
                        message_count += 1
                        
                        # Print clean log
                        status = "🔴 CRITICAL" if data['avg_speed'] < 10 else "🟢 Normal"
                        print(f"{status} | Sensor {data['sensor_id']} | "
                              f"Vehicles: {data['vehicle_count']:3d} | "
                              f"Speed: {data['avg_speed']:2d} km/h | "
                              f"{datetime.now().strftime('%H:%M:%S')}")
                
                # Wait 1 second before next reading
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Stopping sensor simulation...")
        finally:
            self.cleanup()
            
        # Summary
        elapsed = time.time() - start_time
        print("\n" + "="*60)
        print(f"📊 SIMULATION SUMMARY")
        print("="*60)
        print(f"Duration: {elapsed:.1f} seconds")
        print(f"Messages sent: {message_count}")
        print(f"Avg rate: {message_count/elapsed:.1f} messages/second")
        print("="*60 + "\n")
    
    def cleanup(self):
        """Close Kafka producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            print("✅ Kafka producer closed")


if __name__ == "__main__":
    # Run the simulation
    producer = TrafficProducer()
    
    # Run forever (press Ctrl+C to stop)
    # Or specify duration: producer.run(duration_seconds=300)  # 5 minutes
    producer.run()