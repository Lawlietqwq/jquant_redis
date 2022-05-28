import imp
from pandas import Timestamp
import redis
import threading
import json
from time import time


class Producer(object):
    def __init__(self, conn_pool):
        self.__conn = redis.Redis(connection_pool=conn_pool)
        self.pub = None


    # def reset_msg_idx(self):
    #     """
    #     重置消息序号
    #     :return:
    #     """
    #     # 重置消息发送顺序号
    #     self.__conn.set("MESSAGE_TXID", 0)


    def publish(self, channel, message, date):
        """
        发布辅助函数
        :param channel:
        :param message:
        :return:
        """
        # txid = self.__conn.incr("MESSAGE_TXID")
        # 为每个消息设定id, 最终消息格式 1000/messageContent

        content = f"{date}/{message}"
        # 首先 将消息发布到 list 中
        # 获取 client 列表， 对每一个client channel 的list 发送当前消息  PERSITS_SUB
        clients_list = self.__conn.smembers(channel+'_clients')
        print(f"查询到的 channel 列表为: {clients_list}")

        # 将消息发送到每个client 注册的 channel 中  channel key的格式: ClientID/channelName
        for client_channel in clients_list:
            # Rpush 命令用于将一个或多个值插入到列表的尾部(最右边)
            # 如果列表不存在，一个空列表会被创建并执行 RPUSH 操作
            print(f"Publisher 向 list: f'{client_channel}' 发布一条消息: {content}")
            self.__conn.rpush(f'{client_channel}', content)

        self.__conn.sadd(date, message)
        self.__conn.expire(date, time=604800)
        # 再调用 redis 的 publish 方法 发送事件，触发 subscribe 接收消息
        self.__conn.publish(channel=channel, message=content)


if __name__ == "__main__":
    # val={"code":"A2201.XDCE","open":1,"close":2,"high":3,"low":4,"time":"2022-05-18 16:27"}
    # r = redis.Redis(host='localhost', port=6379, decode_responses=True)  
    # r.set('A2201.XDCE-202205181626',json.dumps(val))  # 设置 name 对应的值
    # print(r.get('A2201.XDCE-202205181626'))  # 取出键 name 对应的值
    pool = redis.ConnectionPool(host='122.207.108.56', port=12479, decode_responses=True)
    producer=Producer(pool)
    msg_num = 10
    for _ in range(msg_num):
        msg_obj = {"code":"A2201.XDCE","open":1,"close":2,"high":3,"low":4,"time":"2022-05-18 16:27"}
        msg = json.dumps(msg_obj)
        producer.publish("A2201.XDCE", msg)