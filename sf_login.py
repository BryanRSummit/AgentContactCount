from simple_salesforce import Salesforce
import os
import json
from cryptography.fernet import Fernet


def sf_login():
    cwd = os.getcwd()
    cred_path = os.path.join(cwd, "encrypted_credentials.json")
    key_path = os.path.join(cwd, "key.key")

    # Load the key
    with open(key_path, 'rb') as key_file:
        key = key_file.read()

    cipher_suite = Fernet(key)

    # Load credentials from JSON file
    with open(cred_path, 'r') as f:
        creds = json.load(f)

    # Decrypt the password and token
    encrypted_password = creds['encrypted_password'].encode('utf-8')
    encrypted_sec_token = creds["encrypted_sec_token"].encode('utf-8')
    decrypted_password = cipher_suite.decrypt(encrypted_password).decode('utf-8')
    decrypted_sec_token = cipher_suite.decrypt(encrypted_sec_token).decode('utf-8')

    sf = Salesforce(username=creds["username"], password=decrypted_password, security_token=decrypted_sec_token)
    return sf