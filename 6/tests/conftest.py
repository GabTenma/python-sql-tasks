import pytest
import psycopg2
import psycopg2.extras


@pytest.fixture(scope="session")
def db_connection():
    conn = psycopg2.connect("postgresql://postgres:@localhost:5432/test_db")
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    yield conn
    conn.close()


@pytest.fixture(scope="session", autouse=True)
def setup_database(db_connection):
    with db_connection.cursor() as cur:
        with open('init.sql', 'r') as f:
            sql_script = f.read()
        cur.execute(sql_script)
        db_connection.commit()

@pytest.fixture(scope="function")
def db_transaction(db_connection):
    with db_connection:
        with db_connection.cursor() as cur:
            cur.execute("BEGIN")
            yield db_connection
            cur.execute("ROLLBACK")
