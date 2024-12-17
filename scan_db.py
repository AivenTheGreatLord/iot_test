import sqlite3

def get_temperature_data(start_time: int, end_time: int):

    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    

    query = """
        SELECT *
        FROM events
        WHERE event = 'period'
          AND type = 'temperature'
          AND time >= ?
          AND time <= ?;
    """
    

    cursor.execute(query, (start_time, end_time))
    rows = cursor.fetchall()

    conn.close()
    
    return rows