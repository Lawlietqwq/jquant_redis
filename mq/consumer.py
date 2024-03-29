import threading
from util.mysingleton import singleton
from util.redis_util import redis_pooling
import json

@singleton
class Consumer(object):
    def __init__(self , channel = '1m'):
        self.__conn = redis_pooling().get_conn(1)
        self.client_id = 'futures'
        self.pub = None
        self.__active = False
        self.latest_data = None
        self.channel = channel
        self.strategy_list = []
        
    def set_client_id(self, client_id):
        self.client_id = client_id

    def process_message(self, message):
        self.latest_data = json.loads(message)
        self.notifyAllStrategy()
        
    def subscribe(self, channel):
        """
        订阅操作步骤:
           1. 判断 clientID是否在persists_sub 队列中
           2. 如果在队列中说明已经订阅， 或者将clientId 添加到队列中
        :param channel:
        :return:
        """
        # 将channel 注册到redis中
        self.pub = self.__conn.pubsub()
        self.pub.psubscribe(channel)

        channel_key = "%s/%s" % (self.client_id, channel)
        is_exists = self.__conn.sismember("PERSITS_SUB", channel_key)

        if not is_exists:
            self.__conn.sadd("PERSITS_SUB", channel_key)

        self.__active = True  # 将监听开关打开

        # 先处理完历史消息
        self.clear_msg(channel)

        def listen():
            print(f"开启 {self.client_id} {channel} 监听线程")

            msg = self.pub.listen()
            for item in msg:
                if not self.__active:
                    break
                # print("client：{} recieve a message: {}".format(self.client_id,item))
                #
                # print("item : {}".format(item[u'type']))
                if item["type"] == "pmessage":
                    print(f"{self.client_id} receive a message from {channel}")
                    self.handle(channel, item["data"])
            print(f"{self.client_id} {channel} 监听线程结束，退出")

        # 启动一个线程来对消息进行监听
        listen_thread = threading.Thread(target=listen)
        # 将监听线程设置为 守护线程， 主线程结束时，监听线程会一起结束
        listen_thread.daemon = 1
        listen_thread.start()
        listen_thread.join()

    def clear_msg(self, channel):
        """
        注册时，先检查一下 channel 对应的消息list 中是否有信息，如果有就先进行处理
        :return:
        """
        channel_key = f"{self.client_id}/{channel}"

        while True:
            # 获取第一个消息
            lm = self.__conn.lindex(channel_key, 0)

            if lm is None:
                break
            # else:
            #     lm = lm.decode()
            li = int(lm.index("/"))

            if li < 0:
                # 消息不合法
                result = self.__conn.lpop(channel_key)
                if result is None:
                    break
                print(f"接收到一个不合法的消息: {result}")
                #  callback(channel, message)
                continue

            # 消息序号
            lmid = int(lm[:li])
            # 取出消息内容
            lmessage = lm[(li + 1) :]

            # 判断是否退出消息,如果不是就执行回调函数
            if lmessage in ["EXIT", "exit", "Exit", "Quit", "quit", "QUIT"]:
                self.close(channel)
            else:
                self.process_message(lmessage)

            # 将处理过的消息丢弃
            self.__conn.lpop(channel_key)

    def handle(self, channel, message:str):
        """
        :param channel: - string 渠道名
        :param message: - string 消息 是从 真正的redis channel 中接收到的消息
        :return:
        """
        index = int(message.index("/"))

        if index < 0:
            # 消息不合法, 丢弃
            return

        txid = int(message[:index])
        channel_key = f"{self.client_id}/{channel}"

        # 下面是从redis client+channel 对应的 list 中获取消息
        while True:
            # 获取第一个消息
            lm = self.__conn.lindex(channel_key, 0)
                       
            if lm is None:
                break
            # else:
            #     lm=lm.decode()

            # print("Client {} 从 list:{} 中获取一个消息: {}".format(self.client_id, channel_key, lm))

            li = int(lm.index("/"))

            if li < 0:
                # 消息不合法
                result = self.__conn.lpop(channel_key)
                if result is None:
                    break
                print(f"接收到一个不合法的消息: {result}")
                #  callback(channel, message)
                continue

            # 消息序号
            lmid = int(lm[:li])

            # txid > lmid 表示队列中有没有处理的消息，
            # 调用回调函数进行消费，直到处理完txid个消息
            if txid >= lmid:
                self.__conn.lpop(channel_key)
                # 取出消息内容
                lmessage = lm[(li + 1) :]

                # 判断是否退出消息,如果不是就执行回调函数
                if lmessage in ["EXIT", "exit", "Exit", "Quit", "quit", "QUIT"]:
                    self.close(channel)
                else:
                    self.process_message(lmessage)
                continue
            else:
                break
    
    def attach(self, strategy):
        self.strategy_list.append(strategy)
    
    def notifyAllStrategy(self):
        for strategy in self.strategy_list:
            strategy.refresh_data()

                
    def close(self, channel):
        """
        关闭channel
        :param channel:
        :return:
        """
        self.__conn.publish(channel, "exit")
        # 删除channel对应的消息队列的list
        self.on_unsubscribe(channel)
        # 关闭监听开关，线程结束
        self.__active = False

    def on_unsubscribe(self, channel):
        """
        取消订阅
        :param client_id:
        :param channel:
        :return:
        """
        channel_key = f"{self.client_id}/{channel}"
        # 1. 从订阅者队列中删除
        self.__conn.srem("PERSITS_SUB", channel_key)
        # 2. 删除订阅者消息队列
        self.__conn.delete(channel_key)
        
    def get_latest_data(self, code:str=None):
        """
        获取每分钟新数据
        """
        if code:
            return self.latest_data.get(code)
        return self.latest_data


consumer = Consumer()
consumer.subscribe(consumer.channel)

def get_latest_data(code:str=None, consumer=consumer):
    """
    获取每分钟新数据
    :return 
        data(dict)
        code为None,返回全合约字典;code为str,返回code对应数据;
        
    格式为JSON字符串转dist:
    {
        'code':{
            'time':"YY:mm:dd HH:MM:SS",
            'open':float,
            'high':float,
            'low':float,
            'close':float,
            'volume':float,
        }.
        'code2':{
            ...
        }
    }
    
    """
    try:
        if code:
            return consumer.latest_data.get(code)
    except:
        print("合约数据不存在")
    return consumer.latest_data


