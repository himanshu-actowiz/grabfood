import mysql.connector
import logging

query_logger = logging.getLogger("query_logger")
query_logger.setLevel(logging.INFO)

handler = logging.FileHandler("query.log", encoding="utf-8")
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)

query_logger.addHandler(handler)
query_logger.propagate = False


def make_connection():
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='actowiz',
        database='grabfood'
    )
    return conn


def create_table(table_name: str):

    create_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name}(
        restaurant_name varchar(100),
        restaurant_id varchar(50) PRIMARY KEY,
        restaurant_cuisine varchar(100),
        restaurant_IMG varchar(255),
        restaurant_timeZone varchar(100),
        restaurant_time TEXT,
        restaurant_menu LONGTEXT
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

    # query logging 
    for r in rows:
        safe_row = [str(v).replace("'", "''") for v in r]
        formatted_query = query.replace("%s", "'{}'").format(*safe_row)
        query_logger.info(formatted_query + ";")

    cursor.executemany(query, rows)

    conn.commit()
    conn.close()