import json
from cryptography.fernet import Fernet
import os


if __name__ == "__main__":
    # Generate a key for encryption and decryption
    key = Fernet.generate_key()
    cipher_suite = Fernet(key)
    
    cwd = os.getcwd()
    cred_path = os.path.join(cwd, "db_creds.json")
    with open(cred_path) as f:
        db_creds = json.load(f)

    # Encrypt the password
    encrypted_password = cipher_suite.encrypt(db_creds['password'].encode('utf-8'))
    encrypted_host = cipher_suite.encrypt(db_creds['host'].encode('utf-8'))

    # Store the encrypted password and key (in a secure location)
    encrypted_credentials = {
        "encrypted_host": encrypted_password.decode('utf-8'),
        "database": "postgres",
        "user": "postgres",
        "encrypted_password": encrypted_password.decode('utf-8')
    }

    # Save to JSON file
    with open('encrypted_db_creds.json', 'w') as f:
        json.dump(encrypted_credentials, f)

    # Save the key securely
    with open('db.key', 'wb') as key_file:
        key_file.write(key)