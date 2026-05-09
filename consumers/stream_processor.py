"""
Real-time Traffic Stream Processor (Simplified)
Replaces Spark Streaming with lightweight Python consumer
- Processes traffic data from Kafka in real-time
- Detects congestion (speed < 10 km/h)
- Sends alerts to Kafka critical-traffic topic
- Saves all data to PostgreSQL
"""

import json
import time
from datetime import datetime
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import psycopg2
from psycopg2.extras import execute_batch
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_INPUT_TOPIC = 'traffic-data'
KAFKA_ALERT_TOPIC = 'critical-traffic'
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'traffic_db',
    'user': 'trafficuser',
    'password': 'trafficpass'
}
CONGESTION_THRESHOLD = 10  # km/h
WINDOW_SIZE_SECONDS = 300  # 5 minutes
BATCH_SIZE = 10  # Insert to DB every N messages


class TrafficStreamProcessor:
    """Real-time traffic data processor"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.db_conn = None
        self.db_cursor = None
        
        # Windowing data structures (5-minute sliding window)
        self.window_data = defaultdict(lambda: deque(maxlen=300))  # Store last 5 min per sensor
        
        # Batch insert buffers
        self.raw_buffer = []
        self.alert_buffer = []
        
        self.initialize_connections()
    
    def initialize_connections(self):
        """Initialize Kafka and PostgreSQL connections"""
        try:
            # Kafka Consumer
            self.consumer = KafkaConsumer(
                KAFKA_INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='traffic-processor-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"✅ Connected to Kafka consumer: {KAFKA_INPUT_TOPIC}")
            
            # Kafka Producer (for alerts)
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            logger.info(f"✅ Connected to Kafka producer: {KAFKA_ALERT_TOPIC}")
            
            # PostgreSQL
            self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.db_cursor = self.db_conn.cursor()
            logger.info("✅ Connected to PostgreSQL")
            
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise
    
    def process_message(self, data):
        """Process a single traffic message"""
        try:
            sensor_id = data['sensor_id']
            location = data['location']
            timestamp = datetime.fromisoformat(data['timestamp'])
            vehicle_count = data['vehicle_count']
            avg_speed = data['avg_speed']
            
            # Add to raw data buffer
            self.raw_buffer.append((
                sensor_id, location, timestamp, vehicle_count, avg_speed
            ))
            
            # Check for congestion
            if avg_speed < CONGESTION_THRESHOLD:
                self.handle_congestion(data)
            
            # Batch insert to database
            if len(self.raw_buffer) >= BATCH_SIZE:
                self.flush_to_database()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            return False
    
    def handle_congestion(self, data):
        """Handle congestion detection"""
        logger.warning(
            f"🚨 CONGESTION DETECTED | "
            f"Sensor {data['sensor_id']} ({data['location']}) | "
            f"Speed: {data['avg_speed']} km/h | "
            f"Vehicles: {data['vehicle_count']}"
        )
        
        # Add to alert buffer
        self.alert_buffer.append((
            data['sensor_id'],
            data['location'],
            datetime.fromisoformat(data['timestamp']),
            data['vehicle_count'],
            data['avg_speed'],
            'HIGH',
            f"Critical congestion: Speed {data['avg_speed']} km/h (threshold: {CONGESTION_THRESHOLD} km/h)"
        ))
        
        # Send alert to Kafka
        alert_message = {
            'sensor_id': data['sensor_id'],
            'location': data['location'],
            'timestamp': data['timestamp'],
            'vehicle_count': data['vehicle_count'],
            'avg_speed': data['avg_speed'],
            'status': 'CRITICAL',
            'action': 'Deploy Traffic Police',
            'severity': 'HIGH'
        }
        
        try:
            self.producer.send(KAFKA_ALERT_TOPIC, value=alert_message)
            self.producer.flush()
        except KafkaError as e:
            logger.error(f"❌ Failed to send alert to Kafka: {e}")
    
    def flush_to_database(self):
        """Batch insert accumulated data to PostgreSQL"""
        try:
            # Insert raw data
            if self.raw_buffer:
                execute_batch(
                    self.db_cursor,
                    """INSERT INTO traffic_raw 
                       (sensor_id, location, timestamp, vehicle_count, avg_speed)
                       VALUES (%s, %s, %s, %s, %s)""",
                    self.raw_buffer
                )
                logger.info(f"💾 Inserted {len(self.raw_buffer)} raw records")
                self.raw_buffer.clear()
            
            # Insert alerts
            if self.alert_buffer:
                execute_batch(
                    self.db_cursor,
                    """INSERT INTO traffic_alerts 
                       (sensor_id, location, timestamp, vehicle_count, avg_speed, 
                        congestion_level, alert_message)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    self.alert_buffer
                )
                logger.info(f"🚨 Inserted {len(self.alert_buffer)} alert records")
                self.alert_buffer.clear()
            
            self.db_conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Database insert failed: {e}")
            self.db_conn.rollback()
    
    def calculate_window_stats(self, sensor_id):
        """Calculate statistics for 5-minute window"""
        window_data = list(self.window_data[sensor_id])
        
        if not window_data:
            return None
        
        avg_speed = sum(d['avg_speed'] for d in window_data) / len(window_data)
        avg_vehicles = sum(d['vehicle_count'] for d in window_data) / len(window_data)
        
        return {
            'sensor_id': sensor_id,
            'window_avg_speed': avg_speed,
            'window_avg_vehicles': avg_vehicles,
            'reading_count': len(window_data)
        }
    
    def run(self):
        """Main processing loop"""
        logger.info("\n" + "="*60)
        logger.info("🚀 TRAFFIC STREAM PROCESSOR STARTED")
        logger.info("="*60)
        logger.info(f"📥 Consuming from: {KAFKA_INPUT_TOPIC}")
        logger.info(f"📤 Publishing alerts to: {KAFKA_ALERT_TOPIC}")
        logger.info(f"💾 Saving to PostgreSQL: traffic_db")
        logger.info(f"🚨 Congestion threshold: {CONGESTION_THRESHOLD} km/h")
        logger.info("="*60 + "\n")
        
        message_count = 0
        start_time = time.time()
        
        try:
            for message in self.consumer:
                data = message.value
                
                if self.process_message(data):
                    message_count += 1
                    
                    # Log progress every 20 messages
                    if message_count % 20 == 0:
                        elapsed = time.time() - start_time
                        rate = message_count / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"📊 Processed {message_count} messages "
                            f"({rate:.1f} msg/sec)"
                        )
        
        except KeyboardInterrupt:
            logger.info("\n⏹️  Shutting down processor...")
            
        finally:
            self.shutdown()
            
            # Final statistics
            elapsed = time.time() - start_time
            logger.info("\n" + "="*60)
            logger.info("📊 PROCESSING SUMMARY")
            logger.info("="*60)
            logger.info(f"Total messages: {message_count}")
            logger.info(f"Duration: {elapsed:.1f} seconds")
            logger.info(f"Avg rate: {message_count/elapsed:.1f} messages/sec")
            logger.info("="*60 + "\n")
    
    def shutdown(self):
        """Clean shutdown"""
        logger.info("🔄 Flushing remaining data...")
        
        # Flush any remaining data
        self.flush_to_database()
        
        # Close connections
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Kafka consumer closed")
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Kafka producer closed")
        
        if self.db_cursor:
            self.db_cursor.close()
        
        if self.db_conn:
            self.db_conn.close()
            logger.info("✅ PostgreSQL connection closed")


def main():
    """Entry point"""
    processor = TrafficStreamProcessor()
    processor.run()


if __name__ == "__main__":
    main()