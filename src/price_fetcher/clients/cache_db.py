from pymongo import MongoClient
from config import config

url = 'mongodb://{username}:{password}@{host}:{port}/{database}'.format(
    username=config['mongodb.username'],
    password=config['mongodb.password'],
    host=config['mongodb.host'],
    port=config['mongodb.port'],
    database=config['mongodb.database']
)

client = MongoClient(url)
db = client.get_default_database()
