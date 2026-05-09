"""
Airflow DAG: Daily Traffic Report
Runs nightly to analyze traffic patterns and generate reports
- Aggregates the day's data
- Identifies peak traffic hours per junction
- Generates recommendations for traffic police deployment
- Creates CSV report
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import logging

# Default arguments
default_args = {
    'owner': 'traffic-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Database configuration
POSTGRES_CONFIG = {
    'host': 'postgres',  # Docker service name
    'port': 5432,
    'database': 'traffic_db',
    'user': 'trafficuser',
    'password': 'trafficpass'
}

def get_db_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(**POSTGRES_CONFIG)


def extract_daily_data(**context):
    """
    Task 1: Extract yesterday's traffic data
    """
    execution_date = context['execution_date']
    report_date = execution_date.date()
    
    logging.info(f"📅 Extracting data for date: {report_date}")
    
    conn = get_db_connection()
    
    # Query to get yesterday's data
    query = f"""
        SELECT 
            sensor_id,
            location,
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) as reading_count,
            AVG(vehicle_count) as avg_vehicles,
            AVG(avg_speed) as avg_speed,
            MIN(avg_speed) as min_speed,
            MAX(avg_speed) as max_speed,
            COUNT(CASE WHEN avg_speed < 10 THEN 1 END) as congestion_events
        FROM traffic_raw
        WHERE DATE(timestamp) = '{report_date}'
        GROUP BY sensor_id, location, DATE_TRUNC('hour', timestamp)
        ORDER BY sensor_id, hour
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    logging.info(f"✅ Extracted {len(df)} hourly records")
    
    # Save to XCom for next task
    context['ti'].xcom_push(key='daily_data', value=df.to_json())
    
    return len(df)


def analyze_peak_hours(**context):
    """
    Task 2: Analyze peak traffic hours per junction
    """
    # Get data from previous task
    daily_data_json = context['ti'].xcom_pull(key='daily_data', task_ids='extract_data')
    df = pd.read_json(daily_data_json)
    
    logging.info("📊 Analyzing peak hours...")
    
    # Find peak hour for each sensor (highest avg vehicles)
    peak_hours = df.loc[df.groupby('sensor_id')['avg_vehicles'].idxmax()]
    
    # Prepare summary
    summary = []
    for _, row in peak_hours.iterrows():
        hour = pd.to_datetime(row['hour']).hour
        
        # Determine recommendation
        if row['congestion_events'] > 10:
            recommendation = "URGENT: Deploy 2 traffic officers"
            priority = "HIGH"
        elif row['congestion_events'] > 5:
            recommendation = "Deploy 1 traffic officer during peak"
            priority = "MEDIUM"
        else:
            recommendation = "Monitor only"
            priority = "LOW"
        
        summary.append({
            'sensor_id': row['sensor_id'],
            'location': row['location'],
            'peak_hour': f"{hour:02d}:00",
            'avg_vehicles': round(row['avg_vehicles'], 1),
            'avg_speed': round(row['avg_speed'], 1),
            'congestion_events': int(row['congestion_events']),
            'recommendation': recommendation,
            'priority': priority
        })
    
    summary_df = pd.DataFrame(summary)
    
    logging.info(f"✅ Identified peak hours for {len(summary_df)} junctions")
    
    # Save to XCom
    context['ti'].xcom_push(key='summary', value=summary_df.to_json())
    
    return len(summary_df)


def save_to_database(**context):
    """
    Task 3: Save summary to daily_traffic_summary table
    """
    execution_date = context['execution_date']
    report_date = execution_date.date()
    
    # Get summary from previous task
    summary_json = context['ti'].xcom_pull(key='summary', task_ids='analyze_peak_hours')
    df = pd.read_json(summary_json)
    
    logging.info("💾 Saving summary to database...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Insert into database
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO daily_traffic_summary 
            (report_date, sensor_id, location, peak_hour, total_vehicles, 
             avg_daily_speed, congestion_count, recommendation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date, sensor_id) 
            DO UPDATE SET
                peak_hour = EXCLUDED.peak_hour,
                total_vehicles = EXCLUDED.total_vehicles,
                avg_daily_speed = EXCLUDED.avg_daily_speed,
                congestion_count = EXCLUDED.congestion_count,
                recommendation = EXCLUDED.recommendation
            """,
            (
                report_date,
                row['sensor_id'],
                row['location'],
                int(row['peak_hour'].split(':')[0]),  # Extract hour
                int(row['avg_vehicles']),
                float(row['avg_speed']),
                int(row['congestion_events']),
                row['recommendation']
            )
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info(f"✅ Saved {len(df)} summary records to database")
    
    return len(df)


def generate_csv_report(**context):
    """
    Task 4: Generate CSV report file
    """
    execution_date = context['execution_date']
    report_date = execution_date.date()
    
    # Get summary from XCom
    summary_json = context['ti'].xcom_pull(key='summary', task_ids='analyze_peak_hours')
    df = pd.read_json(summary_json)
    
    # Sort by priority
    priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    df['priority_rank'] = df['priority'].map(priority_order)
    df = df.sort_values('priority_rank').drop('priority_rank', axis=1)
    
    # Generate report
    report_path = f'/opt/airflow/data/reports/traffic_report_{report_date}.csv'
    
    df.to_csv(report_path, index=False)
    
    logging.info(f"📄 Report generated: {report_path}")
    logging.info("\n" + "="*60)
    logging.info("DAILY TRAFFIC REPORT SUMMARY")
    logging.info("="*60)
    logging.info(df.to_string(index=False))
    logging.info("="*60)
    
    return report_path


def send_notification(**context):
    """
    Task 5: Send notification (simulated)
    In production, this would send email/SMS
    """
    execution_date = context['execution_date']
    report_date = execution_date.date()
    
    # Get summary
    summary_json = context['ti'].xcom_pull(key='summary', task_ids='analyze_peak_hours')
    df = pd.read_json(summary_json)
    
    # Count high priority junctions
    high_priority = len(df[df['priority'] == 'HIGH'])
    
    message = f"""
    ====================================
    📊 DAILY TRAFFIC REPORT - {report_date}
    ====================================
    
    Junctions Analyzed: {len(df)}
    High Priority Alerts: {high_priority}
    
    Action Required:
    {df[df['priority'] == 'HIGH'][['location', 'peak_hour', 'recommendation']].to_string(index=False) if high_priority > 0 else 'None'}
    
    Full report available at: /opt/airflow/data/reports/traffic_report_{report_date}.csv
    ====================================
    """
    
    logging.info(message)
    
    return "Notification sent"


# Define the DAG
dag = DAG(
    'daily_traffic_report',
    default_args=default_args,
    description='Generate daily traffic analysis and recommendations',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    tags=['traffic', 'reporting', 'batch'],
)

# Define tasks
task_extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_daily_data,
    dag=dag,
)

task_analyze = PythonOperator(
    task_id='analyze_peak_hours',
    python_callable=analyze_peak_hours,
    dag=dag,
)

task_save = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database,
    dag=dag,
)

task_report = PythonOperator(
    task_id='generate_csv_report',
    python_callable=generate_csv_report,
    dag=dag,
)

task_notify = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    dag=dag,
)

# Define task dependencies (pipeline)
task_extract >> task_analyze >> [task_save, task_report] >> task_notify