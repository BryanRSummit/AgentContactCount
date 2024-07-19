import psycopg2
from psycopg2 import sql
import os
from cryptography.fernet import Fernet
import json

def connect_to_db():
    cwd = os.getcwd()
    cred_path = os.path.join(cwd, "encrypted_db_creds.json")
    key_path = os.path.join(cwd, "db.key")

    # Load the key
    with open(key_path, 'rb') as key_file:
        key = key_file.read()

    cipher_suite = Fernet(key)

    # Load credentials from JSON file
    with open(cred_path, 'r') as f:
        creds = json.load(f)

    # Decrypt the password and token
    encrypted_password = creds['encrypted_password'].encode('utf-8')
    encrypted_host = creds["encrypted_host"].encode('utf-8')
    decrypted_password = cipher_suite.decrypt(encrypted_password).decode('utf-8')
    decrypted_host = cipher_suite.decrypt(encrypted_host).decode('utf-8')

    try:
        conn = psycopg2.connect(
            host=decrypted_host,
            database=creds["database"],
            user=creds["user"],
            password=decrypted_password
        )
        return conn
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None

def create_table(conn):
    try:
        cursor = conn.cursor()
        
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS agent_contacts (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                agent VARCHAR(255) NOT NULL,
                team_total INTEGER,
                team_customer_count INTEGER,
                team_non_customer_count INTEGER,
                agent_total_count INTEGER,
                agent_count_cust INTEGER,
                agent_count_non INTEGER,
                ams_total_count INTEGER,
                am_cust_count INTEGER,
                am_non_count INTEGER
            )
        """)
        
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'agent_contacts' created successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating table", error)
    finally:
        if cursor:
            cursor.close()

def main():
    conn = connect_to_db()
    if conn:
        create_table(conn)
        conn.close()

if __name__ == "__main__":
    main()