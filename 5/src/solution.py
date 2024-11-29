import psycopg2
from psycopg2.extras import DictCursor


conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


# BEGIN
def create_post(conn, post):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO posts (title, content, author_id)
        VALUES (%s, %s, %s)
        RETURNING id;
        """, (post['title'], post['content'], post['author_id']))
        return cursor.fetchone()[0]

def add_comment(conn, comment):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO comments (post_id, author_id, content)
        VALUES (%s, %s, %s)
        RETURNING id;
        """, (comment['post_id'], comment['author_id'], comment['content']))
        return cursor.fetchone()[0]

def get_latest_posts(conn, n):
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute("""
        WITH latest_posts AS (
            SELECT p.id, p.title, p.content, p.author_id, p.created_at
            FROM posts p
            ORDER BY p.created_at DESC
            LIMIT %s
        )
        SELECT 
            lp.id AS post_id, lp.title, lp.content, lp.author_id, lp.created_at,
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', c.id,
                        'author_id', c.author_id,
                        'content', c.content,
                        'created_at', c.created_at
                    )
                ) FILTER (WHERE c.id IS NOT NULL), '[]'
            ) AS comments
        FROM latest_posts lp
        LEFT JOIN comments c ON lp.id = c.post_id
        GROUP BY lp.id, lp.title, lp.content, lp.author_id, lp.created_at
        ORDER BY lp.created_at DESC;
        """, (n,))
        return [dict(row) for row in cursor.fetchall()]
# END

