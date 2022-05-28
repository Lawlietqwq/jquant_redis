import redis

from util.mysingleton import singleton

@singleton
class redis_pooling:
    
    def __init__(self):
        self.pool = redis.ConnectionPool(host="122.207.108.56", port=12479, decode_responses=True)
        self.conn = [redis.Redis(connection_pool=self.pool) for _ in range(3)]
        
    def get_conn(self, index):
        return self.conn[index]


