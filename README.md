smart-city-traffic-pipeline/
│
├── README.md                          # Project overview and setup instructions
├── requirements.txt                   # Python dependencies
├── docker-compose.yml                 # Docker setup for all services
├── .env                               # Environment variables
│
├── docs/                              # Documentation folder
│   ├── architecture_diagram.png       # Your architecture diagram
│   ├── project_report.pdf             # 1500-word report
│   └── screenshots/                   # Screenshots of running system
│       ├── kafka_console.png
│       ├── spark_output.png
│       └── airflow_dag.png
│
├── data/                              # Data storage
│   ├── raw/                           # Raw sensor data (if saving to files)
│   ├── processed/                     # Processed data
│   └── reports/                       # Generated reports
│       └── daily_traffic_report.csv
│
├── producers/                         # Data generators (Kafka producers)
│   ├── __init__.py
│   ├── traffic_sensor.py             # Main sensor simulator
│   ├── config.py                     # Configuration for sensors
│   └── utils.py                      # Helper functions
│
├── consumers/                         # Spark streaming jobs
│   ├── __init__.py
│   ├── stream_processor.py           # Main Spark streaming application
│   ├── congestion_detector.py        # Logic for detecting congestion
│   └── alert_handler.py              # Handle critical traffic alerts
│
├── airflow/                          # Airflow orchestration
│   ├── dags/                         # DAG definitions
│   │   ├── __init__.py
│   │   └── daily_traffic_report.py   # Nightly batch job DAG
│   ├── plugins/                      # Custom Airflow plugins
│   │   └── __init__.py
│   └── config/
│       └── airflow.cfg               # Airflow configuration
│
├── database/                         # Database scripts
│   ├── init_db.sql                   # PostgreSQL table creation scripts
│   ├── schema.sql                    # Database schema
│   └── queries.sql                   # Useful queries for testing
│
├── scripts/                          # Utility scripts
│   ├── setup_kafka.sh                # Script to create Kafka topics
│   ├── start_all.sh                  # Start all services
│   ├── stop_all.sh                   # Stop all services
│   └── test_connection.py            # Test if all services are running
│
├── notebooks/                        # Jupyter notebooks (optional)
│   └── data_analysis.ipynb           # For testing and visualization
│
├── tests/                            # Unit tests
│   ├── __init__.py
│   ├── test_producer.py
│   ├── test_consumer.py
│   └── test_airflow_dag.py
│
└── config/                           # Configuration files
    ├── kafka_config.yml
    ├── spark_config.yml
    └── postgres_config.yml

#####
# Smart City Traffic Management Pipeline

**Applied Big Data Engineering - Mini Project**  
**Lambda Architecture for Real-time Traffic Analysis**

---

##  Project Overview

A production-grade data pipeline implementing **Lambda Architecture** to process real-time traffic data from IoT sensors across Colombo city. The system detects traffic congestion in real-time while generating comprehensive daily reports for traffic management decisions.

### Key Features

-  **Real-time Stream Processing**: Detects congestion within seconds
-  **Batch Processing**: Generates nightly analytical reports  
-  **Dual-Layer Storage**: Hot path (alerts) + Cold path (historical)
-  **Scalable Architecture**: Handles 4+ sensors, expandable to hundreds
-  **Automated Alerts**: Critical traffic notifications via Kafka

---

##  Architecture

**Lambda Architecture Components:**

```
┌─────────────┐
│   Sensors   │ (4 Traffic Junctions)
│  (Producer) │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Kafka     │ (Message Broker)
│   Topics    │ • traffic-data
└──────┬──────┘ • critical-traffic
       │
       ├──────────────────┬─────────────────┐
       ▼                  ▼                 ▼
┌─────────────┐    ┌─────────────┐  ┌─────────────┐
│   Stream    │    │ PostgreSQL  │  │   Airflow   │
│  Processor  │───▶│  Database   │◀─│  (Batch)    │
└─────────────┘    └─────────────┘  └─────────────┘
  (Real-time)        (Storage)        (Reports)
```

### Speed Layer (Real-time)
- **Apache Kafka**: Event streaming (4 partitions)
- **Python Consumer**: Real-time congestion detection
- **PostgreSQL**: Immediate alert storage

### Batch Layer (Historical)
- **Daily ETL**: Aggregate traffic patterns
- **Peak Hour Analysis**: Identify busiest times
- **Report Generation**: CSV reports with recommendations

---

##  Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Message Broker** | Apache Kafka 7.4.0 | Real-time data streaming |
| **Stream Processing** | Python + kafka-python | Congestion detection |
| **Orchestration** | Apache Airflow 2.7.0 | Batch job scheduling |
| **Database** | PostgreSQL 15 | Data persistence |
| **Containerization** | Docker + Docker Compose | Service orchestration |
| **Language** | Python 3.9+ | Application logic |

### Why These Tools?

**Kafka**: Handles high-throughput data (1000+ msg/sec), partitioned for scalability, fault-tolerant replication.

**PostgreSQL**: ACID compliance for critical traffic data, robust indexing for time-series queries, proven reliability.

**Airflow**: Industry-standard workflow orchestration, visual DAG monitoring, built-in retry mechanisms.

---

##  Project Structure

```
smart-city-traffic-pipeline/
│
├── producers/
│   └── traffic_sensor.py         # Simulates 4 traffic sensors
│
├── consumers/
│   └── stream_processor_simple.py # Real-time congestion detector
│
├── airflow/dags/
│   └── daily_traffic_report.py   # Nightly batch job
│
├── database/
│   └── init_db.sql               # PostgreSQL schema
│
├── scripts/
│   ├── test_connection.py        # System health check
│   └── generate_report_manual.py # Standalone report generator
│
├── data/reports/                 # Generated CSV reports
├── docker-compose.yml            # Service definitions
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

---

##  Quick Start

### Prerequisites

- Docker Desktop 4.0+
- Python 3.9+

### 1. Start Infrastructure

```bash
# Clone repository
git clone https://github.com/ashanwijebandara/big-data-mini-project.git
cd big-data-mini-project

# Start all services
docker-compose up -d

# Wait 2 minutes for initialization
docker ps  # Verify 6 containers running
```

### 2. Create Kafka Topics

```bash
# Traffic data topic
docker exec kafka kafka-topics --create \
  --topic traffic-data \
  --bootstrap-server localhost:9092 \
  --partitions 4 \
  --replication-factor 1

# Alert topic
docker exec kafka kafka-topics --create \
  --topic critical-traffic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 3. Install Python Dependencies

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Run the System

**Terminal 1 - Producer:**
```bash
python producers/traffic_sensor.py
```

**Terminal 2 - Stream Processor:**
```bash
python consumers/stream_processor_simple.py
```

**Terminal 3 - Generate Report:**
```bash
python scripts/generate_report_manual.py
```

---

##  Monitoring & Verification

### Check Database

```bash
docker exec -it postgres psql -U trafficuser -d traffic_db
```

```sql
-- View recent alerts
SELECT sensor_id, location, avg_speed, vehicle_count, timestamp 
FROM traffic_alerts 
ORDER BY timestamp DESC LIMIT 10;

-- Check hourly summary
SELECT * FROM v_hourly_traffic 
ORDER BY hour DESC LIMIT 5;
```

### View Kafka Messages

```bash
# See traffic data
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic traffic-data \
  --from-beginning \
  --max-messages 10

# See alerts
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic critical-traffic \
  --from-beginning \
  --max-messages 5
```

### Access Airflow UI

- URL: http://localhost:8081
- Username: `admin`
- Password: `admin`

---

##  Configuration

### Congestion Threshold

Edit `consumers/stream_processor_simple.py`:
```python
CONGESTION_THRESHOLD = 10  # km/h (default)
```

### Sensor Locations

Edit `producers/traffic_sensor.py`:
```python
SENSORS = {
    'A': 'Galle Road Junction',
    'B': 'Baseline Road Junction',
    'C': 'Duplication Road Junction',
    'D': 'Marine Drive Junction'
}
```

---

##  Performance Metrics

**Observed System Performance:**

- **Throughput**: ~4 messages/second per sensor
- **Latency**: < 2 seconds from sensor to alert
- **Database Inserts**: Batch of 10 records every 2.5 seconds
- **Alert Detection**: Real-time (< 1 second)
- **Report Generation**: < 5 seconds for daily data

**Scalability:**

- Current: 4 sensors, 4 Kafka partitions
- Tested: Up to 10 sensors without degradation
- Potential: 100+ sensors with partition increase

---

##  Testing

```bash
# Test all connections
python scripts/test_connection.py

# Run producer for 1 minute
python producers/traffic_sensor.py

# Verify data flow
docker exec -it postgres psql -U trafficuser -d traffic_db
```

Expected results:
-  240+ raw records in `traffic_raw` table
-  10-30 alerts in `traffic_alerts` table
-  Console showing congestion detections

---

##  Shutdown

```bash
# Stop all services
docker-compose down

# Remove volumes (clean slate)
docker-compose down -v
```

---

##  Event Time vs Processing Time

**Implementation:**

- **Event Time**: Timestamp from sensor reading (`data['timestamp']`)
- **Processing Time**: System time when processed (`current_timestamp()`)
- **Watermarking**: Not implemented (low latency requirements)
- **Late Arrival**: Accepted (traffic data tolerates minor delays)

**Trade-offs:**

- Event time preserves data accuracy
- Processing time adds metadata for debugging
- Both stored in PostgreSQL for audit trail

---

##  Ethics & Data Governance

### Privacy Considerations

**Data Collected:**
- Aggregate vehicle counts (no individual vehicles)
- Average speeds (no license plates)
- Junction locations (public infrastructure)

**Not Collected:**
- Personal identification
- Vehicle registration numbers
- Driver information

### Compliance

- **Anonymization**: No PII collected
- **Retention**: 7-day rolling window (configurable)
- **Access Control**: Database authentication required
- **Audit Trail**: All queries logged

### Recommendations

1. **Data Minimization**: Only collect what's necessary
2. **Purpose Limitation**: Use only for traffic management
3. **Transparency**: Public dashboard showing anonymized stats
4. **Security**: Encrypt data in transit (TLS) and at rest

---

## 🐛 Troubleshooting

### Kafka Not Starting

```bash
docker-compose down -v
docker-compose up -d
```

### Database Connection Error

```bash
# Check if PostgreSQL is running
docker ps | grep postgres

# Test connection
docker exec -it postgres psql -U trafficuser -d traffic_db
```

### Producer Not Sending Data

```bash
# Verify Kafka topics exist
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check producer logs
python producers/traffic_sensor.py  # Look for connection errors
```

---

##  References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Airflow](https://airflow.apache.org/)
- [Lambda Architecture](http://lambda-architecture.net/)
- [PostgreSQL Time-Series Best Practices](https://www.postgresql.org/docs/)

---

##  Contributors

- **[EG/2020/4003 - Kadanage K.D.P.H]** 
- **[EG/2020/4078 - Morawaliyadda M.G.H.S.M]** 
- **[EG/2020/4289 - Wijebandara P.A.I]** 
- **Course**: Applied Big Data Engineering
- **Institution**: University of Ruhuna, Faculty of Engineering
- **Date**: May 2026

---

##  License

This project is for educational purposes as part of the Applied Big Data Engineering course.

---
