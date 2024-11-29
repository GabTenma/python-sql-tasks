import psycopg2
from psycopg2.extras import DictCursor


conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


# BEGIN (write your solution here)
def get_order_sum(conn, month):
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute("""
        SELECT c.customer_name, SUM(o.total_amount) AS total_sum
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE EXTRACT(MONTH FROM o.order_date) = %s
        GROUP BY c.customer_name
        ORDER BY c.customer_name;
        """, (month,))
        results = cursor.fetchall()
        return '\n'.join(
            f"Покупатель {row['customer_name']} совершил покупок на сумму {row['total_sum']}"
            for row in results
        )
# END
