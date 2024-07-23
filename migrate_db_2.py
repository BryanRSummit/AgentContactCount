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

def update_agent_contacts(conn):
    try:
        cursor = conn.cursor()
        
        alter_table_query = sql.SQL("""
            ALTER TABLE agent_contacts
            ADD COLUMN team_delta INTEGER,
            ADD COLUMN team_cust_delta INTEGER,
            ADD COLUMN team_non_delta INTEGER,
            ADD COLUMN agent_total_delta INTEGER,
            ADD COLUMN agent_cust_delta INTEGER,
            ADD COLUMN agent_non_delta INTEGER,
            ADD COLUMN am_total_delta INTEGER,
            ADD COLUMN am_cust_delta INTEGER,
            ADD COLUMN am_non_delta INTEGER;
        """)
        
        cursor.execute(alter_table_query)
        conn.commit()
        print("Table 'agent_contacts' updat successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating table", error)
    finally:
        if cursor:
            cursor.close()

def main():
    conn = connect_to_db()
    if conn:
        update_agent_contacts(conn)
        conn.close()

if __name__ == "__main__":
    main()