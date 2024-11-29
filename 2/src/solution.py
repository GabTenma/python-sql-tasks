import psycopg2

conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


# BEGIN (write your solution here)
def make_cars_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id SERIAL PRIMARY KEY,
            brand VARCHAR(255) NOT NULL,
            model VARCHAR(255) NOT NULL
        );
        """)
    conn.commit()

def populate_cars_table(conn, cars):
    with conn.cursor() as cursor:
        cursor.executemany("""
        INSERT INTO cars (brand, model)
        VALUES (%s, %s);
        """, cars)
    conn.commit()

def get_all_cars(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, brand, model FROM cars ORDER BY brand;")
        return cursor.fetchall()
# END
