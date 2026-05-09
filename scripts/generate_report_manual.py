"""
Manual report generator - Test the reporting logic without Airflow
Run this to quickly generate a report from existing data
"""

import psycopg2
import pandas as pd
from datetime import datetime, date

# Database configuration
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'traffic_db',
    'user': 'trafficuser',
    'password': 'trafficpass'
}

def generate_traffic_report(report_date=None):
    """Generate traffic report for a specific date"""
    
    if report_date is None:
        report_date = date.today()
    
    print(f"\n{'='*60}")
    print(f"📊 GENERATING TRAFFIC REPORT FOR {report_date}")
    print('='*60)
    
    # Connect to database
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    
    # Extract hourly data
    print("\n📥 Extracting data from database...")
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
    
    if len(df) == 0:
        print(f"❌ No data found for {report_date}")
        print("💡 Using TODAY's data instead...")
        query = query.replace(f"DATE(timestamp) = '{report_date}'", "DATE(timestamp) = CURRENT_DATE")
        df = pd.read_sql(query, conn)
    
    print(f"✅ Found {len(df)} hourly records")
    
    # Analyze peak hours
    print("\n📊 Analyzing peak hours...")
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
            'Sensor': row['sensor_id'],
            'Location': row['location'],
            'Peak Hour': f"{hour:02d}:00",
            'Avg Vehicles': round(row['avg_vehicles'], 1),
            'Avg Speed (km/h)': round(row['avg_speed'], 1),
            'Congestion Events': int(row['congestion_events']),
            'Priority': priority,
            'Recommendation': recommendation
        })
    
    summary_df = pd.DataFrame(summary)
    
    # Sort by priority
    priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    summary_df['priority_rank'] = summary_df['Priority'].map(priority_order)
    summary_df = summary_df.sort_values('priority_rank').drop('priority_rank', axis=1)
    
    # Display report
    print("\n" + "="*60)
    print("📄 DAILY TRAFFIC REPORT")
    print("="*60)
    print(summary_df.to_string(index=False))
    print("="*60)
    
    # Save to CSV
    report_filename = f'data/reports/traffic_report_{report_date}.csv'
    summary_df.to_csv(report_filename, index=False)
    print(f"\n✅ Report saved to: {report_filename}")
    
    # Save to database
    print("\n💾 Saving summary to database...")
    cursor = conn.cursor()
    
    for _, row in summary_df.iterrows():
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
                row['Sensor'],
                row['Location'],
                int(row['Peak Hour'].split(':')[0]),
                int(row['Avg Vehicles']),
                float(row['Avg Speed (km/h)']),
                int(row['Congestion Events']),
                row['Recommendation']
            )
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Saved {len(summary_df)} records to database")
    
    # Summary statistics
    print("\n📈 SUMMARY STATISTICS")
    print("="*60)
    print(f"Total Junctions Analyzed: {len(summary_df)}")
    print(f"High Priority Junctions: {len(summary_df[summary_df['Priority'] == 'HIGH'])}")
    print(f"Medium Priority: {len(summary_df[summary_df['Priority'] == 'MEDIUM'])}")
    print(f"Low Priority: {len(summary_df[summary_df['Priority'] == 'LOW'])}")
    print("="*60 + "\n")

if __name__ == "__main__":
    generate_traffic_report()