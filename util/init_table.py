import logging
from datetime import datetime, timedelta
import jqdatasdk.api as jq
import pandas as pd
from jqdatasdk import auth
from jqdatasdk.utils import query
from tqdm import tqdm
from util.db_util import get_connection
from model.stop_loss import StopLossIndicator
import numpy as np


auth(username="18974988801", password="Bigdata12345678")


def get_underlyings():
    exchanges = {
        "XZCE": "郑州商品交易所",
        "XSGE": "上海期货交易所",
        "XDCE": "大连商品交易所",
        "CCFX": "中国金融期货交易所",
        "XINE": "上海国际能源交易中心",
    }
    today = datetime.now()
    data = jq.get_all_securities(types=["futures"], date=today)
    data["underlying"] = list(map(lambda x: x[:-9], data.index))
    data["underlying_name"] = list(map(lambda x: x[:-4], data["display_name"]))
    data["exchange"] = list(map(lambda x: x[-4:], data.index))
    data["exchange_name"] = list(map(lambda x: exchanges[x], data["exchange"]))
    data["dominate_code"] = list(map(lambda x: x[:-9] + "9999." + x[-4:], data.index))
    data.drop_duplicates(sub=["underlying"], inplace=True)
    data = data[
        ["underlying", "underlying_name", "exchange", "exchange_name", "dominate_code"]
    ]
    data.re_index(inplace=True, drop=True)
    return data


def future_underlying():
    """初始化future_underlying表"""
    data = get_underlyings()
    db = get_connection()
    db.truncate("future_underlying")
    sql = "insert into future_underlying (`underlying`,`underlying_name`,`exchange`,`exchange_name`,`dominate_code`) values(%s,%s,%s,%s,%s)"
    db.executemany(sql, data.values.tolist())


def get_mappings(underlying, start_date="2021-01-01", end_date=None):
    # end_date = datetime.today()
    # start_date = end_date - timedelta(days=500)
    data = jq.get_dominant_future(underlying, start_date, end_date)
    if isinstance(data, str):
        dominate_code = data[:-9] + "9999." + data[-4:]
        data = np.array([[underlying, dominate_code, start_date, data]])
        data = pd.DataFrame(data, columns=["underlying", "dominate_code", "trade_date", "mapping_code"])
        return data
    data = pd.DataFrame(data, columns=["mapping_code"])
    data.dropna(axis="columns", inplace=True)
    data = data[data["mapping_code"] != ""]
    data["trade_date"] = data.index
    data.reset_index(drop=True, inplace=True)
    data["dominate_code"] = list(
        map(lambda x: x[:-9] + "9999." + x[-4:], data["mapping_code"])
    )
    data["underlying"] = underlying
    data = data[["underlying", "dominate_code", "trade_date", "mapping_code"]]
    return data


def future_mapping(start_date="2021-01-01", end_date=None):
    """初始化future_mapping表"""
    df = get_underlyings()
    underlyings = df["underlying"].values.tolist()
    db = get_connection()
    db.truncate("future_mapping")
    sql = "insert into future_mapping (`underlying`,`dominate_code`,`trade_date`,`mapping_code`) values(%s,%s,%s,%s)"
    for underlying in tqdm(underlyings):
        mappings = get_mappings(underlying, start_date, end_date)
        db.executemany(sql, mappings.values.tolist())


def get_basic_info() -> pd.DataFrame:
    data: pd.DataFrame = jq.get_all_securities(["futures"])
    data.drop(columns=["type", "name"], inplace=True)
    data["underlying"] = list(map(lambda x: x[:-9], data.index))
    data["exchange"] = list(map(lambda x: x[-4:], data.index))
    data["code"] = data.index
    data.reset_index(drop=True, inplace=True)
    data.rename(columns={"display_name": "name"}, inplace=True)
    data["start_date"] = data["start_date"].dt.date
    data["end_date"] = data["end_date"].dt.date
    return data[["code", "underlying", "name", "exchange", "start_date", "end_date"]]


def future_basic():
    """初始化future_basic表"""
    data = get_basic_info()
    db = get_connection()
    db.truncate("future_basic")
    sql = "insert into future_basic (`code`,`underlying`,`name`,`exchange`,`start_date`,`end_date`) values(%s,%s,%s,%s,%s,%s)"
    db.executemany(sql, data.values.tolist())


def get_daily_info(
    codes,
    start_date,
    end_date,
    skip_paused=True,
    extra_fields=None,
) -> pd.DataFrame:
    """
    jquant的pre_close在获取期货数据时, 是前一个周期的结算价(pre_settle)
    本函数自动将pre_close重命名为pre_settle
    Args:
        skip_paused: True 跳过停盘日期 False 若当前周期停盘, 则用前一个周期的值替代
        cal_pre_close: True 按合约code分组计算pre_close False 不计算pre_close
        extra_fields: options = ["pre_close", "stop_loss"]
    """
    if type(codes) != list:
        codes = [codes]
    data: pd.DataFrame = jq.get_price(
        security=codes,
        start_date=start_date,
        end_date=end_date,
        frequency="daily",
        skip_paused=skip_paused,
        fields=[
            "open",
            "close",
            "low",
            "high",
            "volume",
            "money",
            "pre_close",
            "open_interest",
        ],
    )
    output_fields = [
        "code",
        "open",
        "close",
        "low",
        "high",
        "volume",
        "money",
        "pre_settle",
        "open_interest",
        "underlying",
        "trade_date",
    ]
    if data is None or len(data) == 0:
        return None
    data.rename(columns={"time": "trade_date", "pre_close": "pre_settle"}, inplace=True)
    data["trade_date"] = data["trade_date"].astype("str")
    data["underlying"] = list(map(lambda x: x[:-9], data["code"]))
    if extra_fields:
        df = pd.DataFrame(columns=output_fields.extend(extra_fields))
        groups = data.groupby("code")
        for code in tqdm(codes):
            group = groups.get_group(code).copy()
            if "pre_close" in extra_fields:
                group.loc[:, "pre_close"] = group.loc[:, "close"].shift(periods=1)
            if "stop_loss" in extra_fields:
                indicator = StopLossIndicator(data=data, code=code)
                stop_loss = indicator.predict_stop_loss(start=0)
                group.loc[:, "stop_loss"] = stop_loss
            df = df.append(group)
        return df
    return data[output_fields]


def get_codes(start_date, end_date):
    db = get_connection()
    sql = f"select distinct mapping_code from future_mapping where trade_date between '{start_date}' and '{end_date}'"
    rows = db.get_all(sql)
    return list(map(lambda row: row[0].decode("utf-8"), rows))


def future_daily(
    start_date="2021-01-01", end_date="2021-12-31", skip_paused=True, extra_fields=None
):
    """初始化future_daily表
    Args:
        start_date (str|datetime): 从聚宽获取日期的开始日期
        end_date (str|datetime):  从聚宽获取日期的结束日期
        skip_paused (bool): 是否跳过停盘日期
        extra_fields (list): ["pre_close","stop_loss"]
    """
    codes = get_codes(start_date, end_date)
    db = get_connection()
    db.truncate("future_daily")
    data = get_daily_info(codes, start_date, end_date, skip_paused, extra_fields)
    fields = ",".join(data.columns)
    values = ",".join(["%s"] * len(data.columns))
    sql = f"""insert into future_daily ({fields}) 
        values({values})"""
    # print(len(codes), len(data))
    # print(data)
    data = data.values
    data[pd.isna(data)] = None
    db.executemany(sql, data.tolist())


def get_minute_info(codes, start_date, end_date, skip_paused=True, extra_fields=None):
    if type(codes) != list:
        codes = [codes]
    data: pd.DataFrame = jq.get_price(
        security=codes,
        start_date=start_date,
        end_date=end_date,
        frequency="minute",
        skip_paused=skip_paused,
        fields=[
            "open",
            "close",
            "low",
            "high",
            "volume",
            "money",
            "open_interest",
        ],
    )
    output_fields = [
        "code",
        "open",
        "close",
        "low",
        "high",
        "volume",
        "money",
        "open_interest",
        "trade_date",
    ]
    if data is None or len(data) == 0:
        return None
    # data["underlying"] = list(map(lambda x: x[:-9], data["code"]))
    data.rename(columns={"time": "trade_date"}, inplace=True)
    data["trade_date"] = data["trade_date"].astype("str")
    # data["trade_date"] = pd.to_datetime(data["trade_date"], infer_datetime_format=True)
    # data["trade_date"] = data["trade_date"].dt.date
    if extra_fields:
        df = pd.DataFrame(columns=output_fields.extend(extra_fields))
        groups = data.groupby("code")
        for code in tqdm(codes):
            group = groups.get_group(code).copy()
            if "stop_loss" in extra_fields:
                indicator = StopLossIndicator(data=group, code=code)
                # start_index = group.iloc[0].name
                stop_loss = indicator.predict_stop_loss(start=0)
                group.loc[:, "stop_loss"] = stop_loss
            df = df.append(group)
        return df
    return data[output_fields]


def future_m(
    start_date="2021-01-01", end_date="2021-12-31", skip_paused=True, extra_fields=None
):
    """初始化future_m表
    Args:
     start_date (str|datetime): 从聚宽获取日期的开始日期
     end_date (str|datetime):  从聚宽获取日期的结束日期
     skip_paused (bool): 是否跳过停盘日期
     extra_fields (list): ["pre_close","stop_loss"]
    """
    codes = get_codes(start_date, end_date)
    db = get_connection()
    db.truncate("future_m")
    data: pd.DataFrame = get_minute_info(
        codes, start_date, end_date, skip_paused=skip_paused, extra_fields=extra_fields
    )
    # print(len(data))
    fields = ",".join(data.columns)
    values = ",".join(["%s"] * len(data.columns))
    sql = f"""insert into future_m ({fields}) 
        values({values})"""
    data = data.values
    data[pd.isna(data)] = None
    db.executemany(sql, data.tolist())


def future_warehouse_receipt(start_date, end_date):
    from jqdatasdk import finance
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    date = start_date
    db = get_connection()
    db.truncate("future_warehouse_receipt")
    sql = """insert into future_warehouse_receipt (`day`,`exchange`,`exchange_name`,`product_name`,
    `warehouse_name`,`warehouse_receipt_number`,`unit`,`warehouse_receipt_number_increase`) 
    values(%s,%s,%s,%s,%s,%s,%s,%s)"""
    while date <= end_date:
        data = finance.run_query(
            query(finance.FUT_WAREHOUSE_RECEIPT).filter(
                finance.FUT_WAREHOUSE_RECEIPT.day == date
            )
        )
        data.drop(columns=["id"], inplace=True)
        db.executemany(sql, data.values.tolist())
        # fields=[["day","exchange","exchange_name","product_name","warehouse_name","warehouse_receipt_number","unit","warehouse_receipt_number_increase"]]
        date = date + timedelta(days=1)


if __name__ == "__main__":

    # logging.info("dump data to table: future_underlying")
    # future_underlying()
    logging.info("dump data to table: future_mapping")
    future_mapping(start_date="2022-01-01", end_date="2022-12-31")
    # logging.info("dump data to table: future_basic")
    # future_basic()

    # logging.info("dump data to table: future_daily")
    # future_daily(
    #     start_date="2022-01-01", end_date="2022-12-31", extra_fields=["pre_close"]
    # )
    # logging.info("dump data to table: future_m")
    future_m(
        start_date="2022-06-21",
        end_date="2022-12-31",
        skip_paused=True,
        extra_fields=["pre_close"]
    )
