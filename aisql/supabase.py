import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

connection_string = os.getenv("DATABASE_URL")

def openConnection() -> tuple[any, any]:
  try:
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    return (conn, cursor)
  except Exception as e:
    print(f"Error opening connection: {e}")

def executeStatement(statement: str):
  conn, cursor = openConnection()
  cursor.execute(statement)

  res = cursor.fetchone()

  cursor.close()
  conn.close()
  return res
