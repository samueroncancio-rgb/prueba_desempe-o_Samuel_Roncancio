import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

from airflow.include.config import config

def get_db_connection(role: str = "api_reader"):
    """
    Returns a database connection.
    By default, uses the read-only 'role_api_reader' role, strictly limited
    to the 'gold' (and 'curated' mirror) schema.
    """
    conn_str = config.get_postgres_uri(role=role)
    conn = psycopg2.connect(conn_str)
    return conn

@contextmanager
def get_db_cursor(role: str = "api_reader"):
    conn = get_db_connection(role=role)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
