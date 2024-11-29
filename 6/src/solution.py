from models import Course, Lesson
import psycopg2
from psycopg2.extras import NamedTupleCursor

conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


def commit(conn):
    conn.commit()


def save_course(conn, course):
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        if course.id is None:
            cur.execute(
                "INSERT INTO courses (name, description) VALUES (%s, %s) RETURNING id;",
                (course.name, course.description)
            )
            course.id = cur.fetchone().id
        else:
            cur.execute(
                "UPDATE courses SET name = %s, description = %s WHERE id = %s;",
                (course.name, course.description, course.id)
            )
    return course.id


def find_course(conn, course_id):
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        cur.execute("SELECT * FROM courses WHERE id = %s;", (course_id,))
        result = cur.fetchone()
        if result:
            return Course(
                id=result.id,
                name=result.name,
                description=result.description
                )
    return None


def get_all_courses(conn):
    courses = []
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        cur.execute("SELECT * FROM courses;")
        for row in cur.fetchall():
            courses.append(Course(
                    id=row.id,
                    name=row.name,
                    description=row.description
                    ))
        return courses


# BEGIN (write your solution here)
def save_lesson(conn, lesson):
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        if lesson.id is None:
            cur.execute(
                "INSERT INTO lessons (course_id, name, text) VALUES (%s, %s, %s) RETURNING id;",
                (lesson.course_id, lesson.name, lesson.text)
            )
            lesson.id = cur.fetchone().id
        else:
            cur.execute(
                "UPDATE lessons SET course_id = %s, name = %s, text = %s WHERE id = %s;",
                (lesson.course_id, lesson.name, lesson.text, lesson.id)
            )
    return lesson.id

def find_lesson(conn, lesson_id):
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        cur.execute("SELECT * FROM lessons WHERE id = %s;", (lesson_id,))
        result = cur.fetchone()
        if result:
            return Lesson(
                id=result.id,
                course_id=result.course_id,
                name=result.name,
                text=result.text
            )
    return None

def get_course_lessons(conn, course_id):
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        cur.execute("SELECT * FROM lessons WHERE course_id = %s;", (course_id,))
        return [
            Lesson(
                id=row.id,
                course_id=row.course_id,
                name=row.name,
                text=row.text
            )
            for row in cur.fetchall()
        ]
# END
