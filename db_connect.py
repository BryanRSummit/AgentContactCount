import psycopg2
from psycopg2 import sql
import os
import json
from cryptography.fernet import Fernet

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
    

def insert_data_to_db(conn, data):
    try:
        cursor = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO agent_contacts (
                date, agent, team_total, team_customer_count, team_non_customer_count,
                agent_total_count, agent_count_cust, agent_count_non,
                ams_total_count, am_cust_count, am_non_count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """)
        cursor.executemany(insert_query, data)
        conn.commit()
        print("Data inserted successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into PostgreSQL", error)
    finally:
        if cursor:
            cursor.close()