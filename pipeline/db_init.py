import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# Use 'alanwang' user which we confirmed works for connection (to default DB 'postgres' or 'template1')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'alanwang')
DB_PASS = os.getenv('DB_PASS', '')

def create_database():
    try:
        # Connect to default 'template1' database to create new DB
        con = psycopg2.connect(dbname='template1', user=DB_USER, host=DB_HOST, password=DB_PASS)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        
        # Check if DB exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'property_db'")
        exists = cur.fetchone()
        
        if not exists:
            print("Creating database 'property_db'...")
            cur.execute('CREATE DATABASE property_db')
            print("Database created successfully.")
        else:
            print("Database 'property_db' already exists.")
            
        cur.close()
        con.close()
    except Exception as e:
        print(f"Error creating database: {e}")

if __name__ == '__main__':
    create_database()
