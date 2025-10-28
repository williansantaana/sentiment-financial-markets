import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        return connection
    except Exception as e:
        print("Error:", e)
        return None


def execute_query(query, params=None):
    connection = get_connection()
    if connection is None:
        return None

    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)

        # Se for uma consulta SELECT, busca os resultados
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
        else:
            connection.commit()
            result = None

        cursor.close()
        connection.close()
        return result
    except Exception as e:
        print("Error:", e)
        if connection:
            connection.rollback()
            connection.close()
        return None
