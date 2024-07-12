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
    encrypted_host = creds["encrypted_sec_token"].encode('utf-8')
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