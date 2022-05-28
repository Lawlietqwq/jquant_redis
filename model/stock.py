import contextlib
import pandas as pd
from util.db_util import get_connection
from util.jquant_util import jquant_auth
from jqdatasdk.api import get_price, normalize_code


class Stock:
    """
    Args:
        code: 股票或期货的ts_code
        data_src: 数据源, mysql或jquant, 默认mysql
        start_date: 数据开始日期
        end_date: 数据结束日期
        table: data_src为mysql时的数据表
        frequency: 数据周期, daily: 日线; 1m: 1分钟
        skip_paused: 是否跳过停盘周期
        fields: jquant返回的字段 (https://www.joinquant.com/help/api/help#name:Future)
    """

    def __init__(
        self,
        code,
        start_date,
        end_date,
        frequency="daily",
        data_src="mysql",
        table="future_daily",
        skip_paused=True,
        fields: list = None,                                                                                                                                                                                                                                                                                                                                                                    
    ):
        if fields is None:
            fields = ["open", "close", "high", "low", "trade_date"]
        self.code = code
        self.start_date = start_date
        self.end_date = end_date
        assert data_src in ["mysql", "jquant"]
        if data_src == "mysql":
            self.db = get_connection()
            self.data = self.load_data_from_mysql(table, start_date, end_date, fields)
        elif data_src == "jquant":
            fields.remove("trade_date")
            self.data = self.load_data_from_jquant(
                start_date, end_date, frequency, skip_paused, fields
            )
        self.length = len(self.data)

    def __print__(self):
        print(self.data)

    def __len__(self):
        return len(self.data)

    def load_data_from_mysql(self, table, start_date, end_date, fields):
        fileds_str = ",".join(fields)
        sql = f"""select {fileds_str} from {table} where `code`='{self.code}' and `trade_date` between str_to_date('{start_date}','%Y-%m-%d') and str_to_date('{end_date}','%Y-%m-%d')"""
        data = pd.read_sql(
            sql,
            self.db.connect(),
        )
        for column in data.columns:
            if column == "trade_date":
                data["trade_date"] = pd.to_datetime(data["trade_date"], format="%Y%m%d")
            elif column in ["open", "close", "high", "low", "pre_close", "amount"]:
                data[column] = data[column].astype("float")
        data.sort_values("trade_date", inplace=True)
        data.reset_index(drop=True, inplace=True)
        # data = data[["trade_date", "open", "close", "high", "low", "stop_loss"]]
        return data

    @jquant_auth
    def load_data_from_jquant(
        self, start_date, end_date, frequency, skip_paused, fields
    ):
        """
        Args:
            start_date: 开始日期
            end_date: 结束日期
            frequency: daily(日线), 1m(分钟线)
        """
        stock_code = normalize_code(self.code)
        data = get_price(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            fields=fields,
            skip_paused=skip_paused,
        )
        data["trade_date"] = data.index
        data["trade_date"] = pd.to_datetime(data["trade_date"])
        data.sort_values("trade_date", inplace=True)
        data.reset_index(drop=True, inplace=True)
        # data = data[
        #     [
        #         "trade_date",
        #         "open",
        #         "close",
        #         "high",
        #         "low",
        #         "pre_close",
        #         "volume",
        #         "money",
        #     ]
        # ]
        return data

    def get_data(self, start_date, end_date):
        start_year, start_month, start_day = start_date.split("-")
        end_year, end_month, end_day = end_date.split("-")
        assert self.data is not None
        data = self.data
        data["trade_date"] = pd.to_datetime(data["trade_date"], format="%Y-%m-%d")
        data = data[data["trade_date"] >= pd.to_datetime(start_date, format="%Y-%m-%d")]
        data = data[data["trade_date"] <= pd.to_datetime(end_date, format="%Y-%m-%d")]
        return data

    def data_dropna(self):
        assert self.data is not None
        data = self.data
        reset_data = data.dropna().reset_index(drop=True)
        self.data = reset_data
        return reset_data

    def insert_to_mysql(self, **kwargs):
        self.data.to_sql(kwargs)

    def update_to_mysql(self, fields, table):
        """
        :parame fields: 需要更新的字段, 如open,close等, 必须包含trade_date
        """
        assert "trade_date" in fields
        data = self.data
        data["trade_date"] = data["trade_date"].dt.strftime("%Y%m%d")
        data = data[fields]
        data = data.values.tolist()
        db = get_connection()
        with contextlib.suppress(KeyError):
            fields.remove("trade_date")
        fields_str = ",".join(list(map(lambda x: x + "=%s", fields)))
        placeholder = "%s"
        if table == "fut_daily":
            sql = f"update fut_daily set {fields_str} where trade_date={placeholder}"
        elif table == "futures_m":
            sql = f"update futures_m set {fields_str} where trade_time={placeholder}"
        db.executemany(sql, data)


if __name__ == "__main__":
    jq_stock = Stock(
        code="A2201.DCE",
        start_date="2021-12-05",
        end_date="2021-12-09",
        data_src="jquant",
        frequency="daily",
        skip_paused=False,
        fields=["open", "close", "high", "low", "pre_close", "volume"],
    )
    # print(jq_stock.data)
    mysql_stock = Stock(
        code="A2201.DCE",
        start_date="2021-12-05",
        end_date="2021-12-09",
        data_src="mysql",
        table="fut_daily",
        fields=[
            "open",
            "close",
            "high",
            "low",
            "pre_close",
            "amount",
            "stop_loss",
            "trade_date",
        ],
    )
    print(mysql_stock.data)
    jq_stock.update_to_db(fields=["open", "close", "high", "low", "trade_date"])
    print(mysql_stock.data)
