from mongoengine import connect
import configparser
import redis
import pika


def connect_rabbit(username:str = "guest", 
            pasword:str = "guest", 
            host:str = "localhost", 
            port:str|int = 5672):
    
    credentials = pika.PlainCredentials(username=username, password=pasword)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials
            )
        )
    return connection

def connect_mongo():
    config = configparser.ConfigParser()
    config.read('tools/config.ini')

    mongo_user = config.get('DB', 'user')
    mongodb_pass = config.get('DB', 'pass')
    db_name = config.get('DB', 'db_name')
    domain = config.get('DB', 'domain')

    connect(host=f"""mongodb+srv://{mongo_user}:{mongodb_pass}@{domain}/{db_name}?retryWrites=true&w=majority""", ssl=True)

def connect_redis(host:str = "localhost",
                  port:str|int = 6379,
                  password:str = None):
    client = redis.StrictRedis(host=host, port=port, password=password)
    return client
