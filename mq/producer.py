from util.redis_util import redis_pooling 
import threading
import json
import pandas as pd
from util.init_table import get_codes
from model.stock import Stock
from jqdatasdk import get_price, get_all_securities
from datetime import date, datetime, timedelta
from util.jquant_util import jquant_auth
from apscheduler.schedulers.blocking import BlockingScheduler


class Producer(object):
    def __init__(self):
        self.__conn = redis_pooling().get_conn(0)

    def reset_msg_idx(self):
        """
        重置消息序号
        :return:
        """
        # 重置消息发送顺序号
        self.__conn.set("MESSAGE_TXID", 0)

    def publish(self, channel, message):
        """
        发布辅助函数
        :param channel:
        :param message:
        :return:
        """
        txid = self.__conn.incr("MESSAGE_TXID")
        # 为每个消息设定id, 最终消息格式 1000/messageContent
        content = f"{txid}/{message}"
        # 首先 将消息发布到 list 中
        # 获取 client 列表， 对每一个client channel 的list 发送当前消息  PERSITS_SUB
        channel_keys = self.__conn.smembers("PERSITS_SUB")
        print(f"查询到的 channel 列表为: {channel_keys}")

        # 将消息发送到每个client 注册的 channel 中  channel key的格式: ClientID/channelName
        for channel_key in channel_keys:
            # Rpush 命令用于将一个或多个值插入到列表的尾部(最右边)
            # 如果列表不存在，一个空列表会被创建并执行 RPUSH 操作
            print(f"Publisher 向 list: {channel_key} 发布一条消息")
            self.__conn.rpush(channel_key, content)

        # 再调用 redis 的 publish 方法 发送事件，触发 subscribe 接收消息
        self.__conn.publish(channel=channel, message=content)


def test_redis(host="localhost", port=6379):
    r = redis_pooling().get_conn(2)
    r.set("message", "hello")  # 设置 name 对应的值
    print(r.get("message"))  # 取出键 name 对应的值


if __name__ == "__main__":
    # test_redis(host="122.207.108.56", port=12479)
    # pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True)
    producer = Producer()
    msg_num = 10
    for _ in range(msg_num):
        msg_obj = {
            "code": "A2201.XDCE",
            "open": 1,
            "close": 2,
            "high": 3,
            "low": 4,
            "time": "2022-05-18 16:27",
        }
        msg = json.dumps(msg_obj)
        producer.publish("1m", msg)
