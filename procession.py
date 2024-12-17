import paho.mqtt.client as mqtt
import json
import sqlite3
from datetime import datetime

DB_FILE = 'database.db'

def create_table(conn):
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sensordata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            base_station_id INTEGER,
            timestamp DATETIME,
            event_type TEXT,
            data_type TEXT,
            value FLOAT,
            mc INTEGER
        );
    ''')
    conn.commit()

def insert_into_db(conn, parsed_data):
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO sensordata (device_id, base_station_id, timestamp, event_type, data_type, value, mc)
        VALUES (:device_id, :base_station_id, :timestamp, :event_type, :data_type, :value, :mc);
    ''', parsed_data)
    conn.commit()

def on_connect(client, userdata, flags, rc):
    client.subscribe('sensor/802241/data')

def on_message(client, userdata, msg):
    message = str(msg.payload.decode("utf-8"))
    try:
        data = parse_message(message)
        save_to_db(data)
    except Exception as e:
        print(f"Error parsing or saving message: {e}")

def parse_message(message):
    try:
        data = json.loads(message)
        return {
            'device_id': data['id'],
            'base_station_id': data['bs_id'],
            'timestamp': datetime.utcfromtimestamp(data['time']).strftime('%Y-%m-%d %H:%M:%S'),
            'event_type': data['event'],
            'data_type': data['type'],
            'value': data['value'],
            'mc': data['mc']
        }
    except KeyError as e:
        raise ValueError(f"Invalid message format: missing key {e}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid message format: {e}")

def save_to_db(parsed_data):
    conn = sqlite3.connect(DB_FILE)
    insert_into_db(conn, parsed_data)
    conn.close()

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    conn = sqlite3.connect(DB_FILE)
    create_table(conn)
    conn.close()

    client.connect("dev.rvts.ru", 1883, 60)
    client.loop_forever()

if __name__ == '__main__':
    main()