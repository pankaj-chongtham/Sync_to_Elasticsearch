import os
from cryptography.fernet import Fernet
import configparser

def main():
    config = configparser.ConfigParser()
    config_file = 'config.ini'
    config.read(config_file)

    password = input("Enter your Password: ")
    key = Fernet.generate_key()
    fernet = Fernet(key)

    with open('key.key', 'wb') as f:
        f.write(key)
    encrypted_password = fernet.encrypt(password.encode())
    encrypted_password_str = encrypted_password.decode('utf-8')
    config.set('MSSQL', 'password', encrypted_password_str)
    with open(config_file, 'w') as config_update:
        config.write(config_update)
    print('Password Encrypted Successfully!')


if __name__ == "__main__":
    main()
