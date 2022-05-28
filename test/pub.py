import redis
from producer import Producer
from util.init_table import get_minute_info
from datetime import datetime
from util import db_util


def get_dominate_codes_in_market():
    """
    检索当前时刻市场在售合约号
    Returns:
        codes (list)
    """
    today = datetime.now().strftime("%Y-%m-%d")
    db = db_util.get_connection()
    sql = (
        f"select distinct mapping_code from future_mapping where trade_date = '{today}'"
    )
    rows = db.get_all(sql)
    return list(map(lambda row: row[0].decode("utf-8"), rows))


def pub_service(frequency):
    pool = redis.ConnectionPool(
        host="122.207.108.56", port=12479, decode_responses=True
    )
    producer = Producer(pool)
    codes = get_dominate_codes_in_market()

    data = get_minute_info(
        codes,
        "2022-05-19 14:00:00",
        "2022-05-19 14:00:00",
    ).set_index("code")
    print(data.head())
    print(data["trade_date"].iloc[0])
    vjson = data.to_json(orient="index", force_ascii=False)
    producer.publish(frequency, vjson, data["trade_date"].iloc[0])
    # producer.publish(frequency, vjson)


pub_service("1m")
