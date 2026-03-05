import mysql.connector

def make_connection():
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='actowiz',
        database='grabfood'
    )
    return conn
def create_table(table_name:str):
    create_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name}(
        restaurant_name  varchar(100),
        restaurant_id  varchar(50) PRIMARY KEY,
        restaurant_cuisine  varchar(100),
        restaurant_IMG  varchar(255),
        restaurant_timeZone  varchar(100),
        restaurant_time  TEXT,
        restaurant_menu  LONGTEXT 
    )
    '''
    conn = make_connection()
    cursor = conn.cursor()
    cursor.execute(create_query)
    conn.commit()
    conn.close()
    
def insert_into_db(table_name: str, data):

    if not data:
        return

    conn = make_connection()
    cursor = conn.cursor()

    cols = ",".join(data[0].keys())
    vals = ",".join(["%s"] * len(data[0]))

    query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"

    rows = [tuple(i.values()) for i in data]

    cursor.executemany(query, rows)

    conn.commit()
    conn.close()