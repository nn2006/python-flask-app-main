from flask import Flask, render_template, jsonify, request,redirect, session
from ldap3 import Server, Connection, ALL, SIMPLE
import sqlite3

app = Flask(__name__)
app.secret_key = 'your_secret_key' 
# Function to create a SQLite database and table if they don't exist
def create_db():
    conn = sqlite3.connect('app/db/sensor_data.db')
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
@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if authenticate_active_directory(username, password):
            session['logged_in'] = True
             #return render_template('index.html')
            return redirect('/dashboard')
        else:
            return render_template('login.html', error='Invalid credentials')
    return render_template('login.html', error='')
def authenticate_active_directory(username, password):
    server = Server('dc1-arpatechonline.net', get_info=ALL)  # Replace with your domain controller address
    try:
        conn = Connection(server, user=f"{username}@dc1-arpatechonline.net", password=password, authentication=SIMPLE, auto_bind=True)
        if conn.bind():
            conn.unbind()
            return True
    except Exception as e:
        print(f"Authentication failed: {str(e)}")
    return False
@app.route('/dashboard')
def dashboard():
    if session.get('logged_in'):
        return render_template('index.html')
    return redirect('/')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect('/')
@app.route('/get-sensor-data')
def get_sensor_data():
    conn = sqlite3.connect('app/db/sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM sensor_readings')
    sensor_data = cursor.fetchall()
    conn.close()

    columns = ['id', 'latitude', 'longitude', 'temperature', 'pressure', 'steam_injection']
    sensor_dicts = [dict(zip(columns, row)) for row in sensor_data]

    return jsonify(sensor_dicts)



@app.route('/sensor-data', methods=['POST'])
def add_sensor_data():
    data = request.get_json()
    conn = sqlite3.connect('app/db/sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO sensor_readings (latitude, longitude, temperature, pressure, steam_injection)
        VALUES (?, ?, ?, ?, ?)
    ''', (data['latitude'], data['longitude'], data['temperature'], data['pressure'], data['steam_injection']))
    conn.commit()
    conn.close()
    return jsonify({'message': 'Sensor data added successfully'})

if __name__ == '__main__':
    create_db()
    app.run(debug=True)
