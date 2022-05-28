import redis
import threading


class Consumer(object):

    def __init__(self, client_id,conn_pool):
        # pool = redis.ConnectionPool(host="localhost", port=6378, password="yourpassword", decode_responses=True)
        self.__conn = redis.Redis(connection_pool=conn_pool)
        self.clientId = client_id
        self.pub = None
        self.__active = False

    # def reset_msg_idx(self):
    #     """
    #     重置消息序号
    #     :return:
    #     """
    #     # 重置消息发送顺序号
    #     self.__conn.set("MESSAGE_TXID", 0)

    def set_clientID(self, clientId):
        self.clientId = clientId

    def process_message(self, channel, message):
        message = eval(message)
        print(f"callback function clinet: {self.clientId} recieve message: {channel} from channel: {message}")

    def subscribe(self, channel, callback):
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

        clients_list = channel + '_clients'

        client_channel = "%s/%s" % (self.clientId,channel)
        is_exists = self.__conn.sismember(clients_list, client_channel)

        if not is_exists:
            self.__conn.sadd(clients_list,client_channel)

        self.__active = True # 将监听开关打开

        # 先处理完历史消息
        self.clear_msg(channel, callback)


        def listen():
            print(f"开启{client_channel} 监听线程")


            msg = self.pub.listen()
            for item in msg:
                if not self.__active: break
                # print("client：{} recieve a message: {}".format(self.clientId,item))
                #
                # print("item : {}".format(item[u'type']))
                if item[u'type'] == "pmessage":
                    print(f"{client_channel} 收到一条channel message:{item}")
                    self.handle(channel, item[u'data'], callback)
            print(f"{client_channel} 监听线程结束，退出")

        # 启动一个线程来对消息进行监听
        listen_thread = threading.Thread(target=listen)
        # 将监听线程设置为 守护线程， 主线程结束时，监听线程会一起结束
        listen_thread.daemon = 1
        listen_thread.start()
        # listen_thread.join()


    def clear_msg(self, channel, callback):
        """
        注册时，先检查一下 channel 对应的消息list 中是否有信息，如果有就先进行处理
        :return:
        """
        # client_channel = f"{self.clientId}/{channel}"
        client_channel = f"{self.clientId}/{channel}"
        # channel_key = f"{channel}_{client_channel}"

        while True:
            # 获取第一个消息
            lm = self.__conn.lindex(client_channel, 0)

            if lm is None:
                break

            li = int(lm.index("/"))

            if li < 0:
                # 消息不合法
                result = self.__conn.lpop(client_channel)
                if result is None:
                    break
                print(f"接收到一个不合法的消息: {result}")
                #  callback(channel, message)
                continue

            # 消息序号
            lmid = lm[:li]
            # 取出消息内容
            lmessage = lm[(li+1):]

            # 判断是否退出消息,如果不是就执行回调函数
            if lmessage in ["EXIT", "exit", "Exit", "Quit", "quit", "QUIT"]:
                self.close(channel)
            else:
                callback(channel, lmessage)

            # 将处理过的消息丢弃
            self.__conn.lpop(client_channel)


    def handle(self, channel, message, callback):
        """
        :param channel: - string 渠道名
        :param message: - string 消息 是从 真正的redis channel 中接收到的消息
        :param callback: - function 回调函数
        :return:
        """
        index = int(message.index("/"))

        if index < 0:
            # 消息不合法, 丢弃
            return

        latest_date = message[:index],
        client_channel = f"{self.clientId}/{channel}"

        # 下面是从redis clinet+channel 对应的 list 中获取消息
        while True:
            # 获取第一个消息
            lm = self.__conn.lindex(client_channel, 0)

            if lm is None:
                break

            # print("Cleint {} 从 list:{} 中获取一个 消息: {}".format(self.clientId, channel_key, lm))

            li = int(lm.index("/"))

            if li < 0:
                # 消息不合法
                result = self.__conn.lpop(client_channel)
                if result is None:
                    break
                print(f"接收到一个不合法的消息: {result}")
                #  callback(channel, message)
                continue

            # 消息序号
            ldate = lm[:li]

            # txid > lmid 表示队列中有没有处理的消息，
            # 调用回调函数进行消费，直到处理完txid个消息
            if latest_date >= ldate:
                self.__conn.lpop(client_channel)
                # 取出消息内容
                lmessage = lm[(li+1):]

                # 判断是否退出消息,如果不是就执行回调函数
                if lmessage in ["EXIT" , "exit", "Exit", "Quit", "quit", "QUIT"]:
                    self.close(channel)
                else:
                    callback(channel, lmessage)
                continue
            else:
                break


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
        :param clientID:
        :param channel:
        :return:
        """
        client_channel = f"{self.clientId}/{channel}"
        # 1. 从订阅者队列中删除
        self.__conn.srem("1m_clients", client_channel)
        # 2. 删除订阅者消息队列
        self.__conn.delete(client_channel)

if __name__ == "__main__":
    pool = redis.ConnectionPool(host='122.207.108.56', port=12479, decode_responses=True)
    client1=Consumer("client1",pool)
    client1.subscribe("minute",client1.process_message)