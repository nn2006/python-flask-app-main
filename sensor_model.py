# sensor_model.py

import sqlite3

# Function to create a SQLite database and table if they don't exist
def create_db():
    conn = sqlite3.connect('2/db/sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            latitude REAL,
            longitude REAL,
            temperature REAL,
            pressure REAL,
            steam_injection REAL
        )
    ''')
    conn.commit()
    conn.close()

def fetch_sensor_data():
    conn = sqlite3.connect('2/db/sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM sensor_readings')
    sensor_data = cursor.fetchall()
    conn.close()

    columns = ['id', 'latitude', 'longitude', 'temperature', 'pressure', 'steam_injection']
    sensor_dicts = [dict(zip(columns, row)) for row in sensor_data]

    return sensor_dicts

def add_sensor_data(data):
    conn = sqlite3.connect('2/db/sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO sensor_readings (latitude, longitude, temperature, pressure, steam_injection)
        VALUES (?, ?, ?, ?, ?)
    ''', (data['latitude'], data['longitude'], data['temperature'], data['pressure'], data['steam_injection']))
    conn.commit()
    conn.close()
