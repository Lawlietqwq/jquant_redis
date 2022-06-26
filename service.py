from sqlite3 import connect
from util.db_util import get_connection
import jqdatasdk as jq
from datetime import datetime, timedelta
from util.init_table import future_mapping
from util.redis_util import redis_pooling
import pandas as pd
from requests import get
from mq.producer import Producer
from util import jquant_util, db_util
import logging
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
jquant_util.auth()


def get_codes_in_market():
    """
    检索当前时刻市场在售合约号
    Returns:
        codes (list)
    """
    data = jq.get_all_securities(["futures"])
    data = data[data["end_date"] >= datetime.now()]
    return data.index.tolist()


def get_dominate_codes_in_market():
    """
    检索当前时刻市场在售主力合约号
    Returns:
        codes (list)
    """
    today = datetime.now().strftime("%Y-%m-%d")
    db = db_util.get_connection()
    future_mapping(start_date=today)
    sql = (
        f"select distinct mapping_code from future_mapping where trade_date = '{today}'"
    )
    rows = db.get_all(sql)
    return list(map(lambda row: row[0].decode("utf-8"), rows))


def cache_codes_in_market(conn):
    """
    将市场中在售合约的代码缓存到redis中\n
    redis中缓存两类合约代码, 分别是主力合约和所有合约\n
    主力合约的key为"dominate_codes", value为list_dominate_codes的字符串表示\n
    所有合约的key为"codes", value为list_codes的字符串表示
    """
    
    dominate_codes = get_dominate_codes_in_market()
    conn.set("dominate_codes", str(dominate_codes))
    codes = get_codes_in_market()
    conn.set("codes", str(codes))

def get_latest_minute_data(
    conn, producer: Producer, expire_in_days: int = 7
):
    """
    从聚宽获取最新分钟级数据
    Args:
        pool: 数据库连接池
        producer: 消息生产者
        expire_in_days: 数据过期天数
    """
    codes = eval(conn.get("dominate_codes"))
    data = jq.get_price(
        security=codes, 
        end_date=datetime.now(),
        count=1,
        frequency="1m",
        fields=["open", "high", "low", "close", "volume", "money", "open_interest"],
    )  # 获取所有合约在当前分钟的一条最新数据
    data: pd.DataFrame = data[datetime.now() - data["time"] < timedelta(minutes=1)]
    if data is None or len(data) == 0:
        return None
    # print('====>',data)
    logging.info(f"number of data: {len(data)}")
    # logging.info(data.head())
    # print(mnt_date)
    data['time'] = data['time'].astype(str)
    mnt_date = data["time"].iloc[0]
    data.rename(columns={"time": "trade_date"}, inplace=True)
    data_copy = data.copy()
    data.set_index("code", drop=True, inplace=True)
    vjson = data.to_json(orient="index")
    # conn.set(
    #     mnt_date, vjson, ex=timedelta(days=expire_in_days)
    # )  # 将数据同步缓存在redis中, 保存`expire_in_days`天  
    conn.sadd(mnt_date, vjson)  
    conn.expire(mnt_date, time=timedelta(days=expire_in_days))  
    # 将数据同步缓存在redis中, 保存`expire_in_days`天
    # producer.publish("1m", vjson, mnt_date)
    producer.publish("1m", vjson)
    db = db_util.get_connection()
    fields = ",".join(data_copy.columns)
    values = ",".join(["%s"] * len(data_copy.columns))
    sql = f"""insert into future_m ({fields}) 
        values({values})"""
    data_copy = data_copy.values
    data_copy[pd.isna(data_copy)] = None
    db.executemany(sql, data_copy.tolist())
    
    return data_copy


# def simulate(
#     pool: redis.ConnectionPool,
#     start_date: datetime,
#     end_date: datetime,
#     sleep_seconds=60,
# ):
#     """
#     模拟从聚宽获取分钟级tick数据, 并推送到redis消息队列\n
#     Args:
#         pool: redis连接池对象
#         start_date: 开始日期datetime对象
#         end_date: 结束日期datetime对象
#         sleep_seconds: 每获取一跳数据暂停的秒数
#     """
#     producer = Producer(pool)
#     conn = redis.Redis(connection_pool=pool)
#     codes = eval(conn.get("dominate_codes"))
#     while start_date <= end_date:
#         data = jq.get_price(
#             security=codes, start_date=start_date, end_date=start_date, frequency="1m"
#         )
#         if data is None or len(data) == 0:
#             start_date = start_date + timedelta(minutes=1)
#             continue
#         logging.info(f"number of data: {len(data)}")
#         mnt_date = data["time"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
#         data.set_index("code", drop=True, inplace=True)
#         vjson = data.to_json(orient="index")
#         # conn.set(mnt_date, vjson, ex=timedelta(days=7))
#         conn.sadd(mnt_date, vjson)
#         conn.expire(mnt_date,time=timedelta(days=7))
#         producer.publish("1m", vjson)
#         start_date = start_date + timedelta(minutes=1)
#         time.sleep(sleep_seconds)

def init_scheduler(conn):
    """
    启动定时器
    定时任务1: 每天早上8点更新市场在售合约代码, 并缓存到redis
    定时任务2: 获取最新分钟级数据, 并推送到消息队列
    """
    # pool = redis.ConnectionPool(
    #     host="122.207.108.56", port=12479, decode_responses=True
    # )
    producer = Producer()
    scheduler = BlockingScheduler()
    cache_codes_in_market(conn)  # 测试时直接执行, 不设置为定时任务
    # scheduler.add_job(func=cache_codes_in_market, args=[pool], trigger="cron", hour=8)
    scheduler.add_job(
        func=cache_codes_in_market, args=[conn], trigger="cron", hour=8, minute=50
    )
    scheduler.add_job(
        func=get_latest_minute_data, args=[conn, producer], trigger="cron", second=1
    )
    scheduler.start()


if __name__ == "__main__":
    # init_scheduler()
    conn = redis_pooling().get_conn(2)

    # cache_codes_in_market(pool)
    init_scheduler(conn)
    # serviceOn(
    #     pool,
    #     datetime(2022, 5, 19, 13, 0),
    #     datetime(2022, 5, 19, 14, 0),
    #     sleep_seconds=3,
    # )
