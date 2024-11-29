import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


# BEGIN (write your solution here)
def batch_insert(conn, products):
    with conn.cursor() as cursor:
        query = "INSERT INTO products (name, price, quantity) VALUES %s"
        values = [(product['name'], product['price'], product['quantity']) for product in products]
        execute_values(cursor, query, values)
    conn.commit()

def get_all_products(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, name, price, quantity FROM products ORDER BY price DESC;")
        return cursor.fetchall()
# END
