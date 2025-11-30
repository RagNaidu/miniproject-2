import bcrypt
import getpass


password = input('Enter your password: ')
password = password.encode('utf-8')
hashed = bcrypt.hashpw(password, bcrypt.gensalt())
print(hashed.decode())