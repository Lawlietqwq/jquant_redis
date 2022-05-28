
import redis
from consumer import Consumer


def comservice(frequency):
    pool = redis.ConnectionPool(host='122.207.108.56', port=12479)
    client1=Consumer("client1",pool)
    client1.subscribe(frequency,client1.process_message)

comservice('1m')