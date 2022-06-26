from mq.consumer import get_latest_data

class Strategy(object):
    def __init__(self, strategy_id, strategy_name):
        self.strategy_id : int = strategy_id
        self.strategy_name : str = strategy_name
        self.latest_data = None
        self.code : str = None
        
    def refresh_data(self):
        self.latest_data = get_latest_data(self.code)