import logging
import pandas as pd
from util.db_util import get_connection
from model.stock import Stock


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.WARNING
)


class StopLossIndicator:
    """
    止损线指标, 影响止损值变化的策略包括初始化,创新低, 创新高, 跌破, 涨破策略 \n
    Args:
        stock: stock对象
        period: 止损值变化策略(创新低,创新高等)考虑的交易周期数
        data: stock与data 2选1
        code: data不为None时需要指定code
    """

    def __init__(
        self,
        stock: Stock = None,
        period: int = 6,
        data: pd.DataFrame = None,
        code: str = None,
    ) -> None:
        if stock is not None:
            self.data = stock.data
            self.code = stock.code
            
        else:
            if data is not None:
                self.data = data
            else:
                raise ValueError("data is None")
            if code:
                self.code = code
            else:
                raise ValueError("code is None")
        self.length = len(self.data)
        self.open = self.data["open"].astype("float")
        self.close = self.data["close"].astype("float")
        self.high = self.data["high"].astype("float")
        self.low = self.data["low"].astype("float")
        self.data["trade_date"] = pd.to_datetime(self.data["trade_date"])
        self.data.set_index("trade_date", inplace=True, drop=False)
        self.data.sort_index(inplace=True)
        if "stop_loss" in self.data.columns:
            self.stop_loss = self.data["stop_loss"].astype("float")
        else:
            self.stop_loss = [0 for _ in range(len(self.data))]
        # print(self.stop_loss)
        self.period = period

    def __len__(self):
        return len(self.data)

    def to_sql(self, table):
        """
        将计算好的止损值更新到数据库中
        :params table 数据库表名
        """
        assert len(self.stop_loss) == len(self.data)
        data = self.data
        data["stop_loss"] = self.stop_loss
        data["code"] = self.code
        # data["trade_date"] = data["trade_date"].dt.strftime("%Y%m%d")
        data["trade_date"] = data["trade_date"].dt.date
        data = data[["stop_loss", "code", "trade_date"]].dropna().values
        data = list(
            map(
                lambda row: (row[0], row[1], row[2]),
                data,
            )
        )
        db = get_connection()
        if table == "future_daily":
            sql = "update future_daily set `stop_loss`=%s where `code`=%s and `trade_date`=%s"
        elif table == "future_m":
            sql = (
                "update future_m set `stop_loss`=%s where `code`=%s and `trade_date`=%s"
            )
        else:
            raise ("table is illegal")
        db.executemany(sql, data)

    def begin(self, cur):
        """
        TODO: 获取指标初始值, 与创新低类似
        """
        if self.stop_loss[-1] == None:
            return True
        # if cur > 0 and self.stop_loss[cur - 1] == self.high[cur - 1]:
        #     return True
        return False

    def begin_new_lowest(self, cur):
        if cur < self.period - 1:
            return False
        cur_low = self.low[cur]
        start = cur - self.period + 1
        if start < 0:
            start = 0
        lowest = self.low[start : cur + 1].min()
        cur_low = self.low[cur]
        if cur_low == lowest:
            return True
        return False

    def value_of_begin(self, cur):
        rank = 0
        cur_high = self.high[cur]
        pivot = cur_high
        for i in range(cur - 1, -1, -1):
            if pivot < self.high[i]:
                rank += 1
                pivot = self.high[i]
                if rank == 2:
                    return self.high[i]
        return pivot

    def below_stop_loss(self, cur: int) -> bool:
        """
        判断今日和昨日k线是否在止损线之下
        1. 当日收盘价低于昨日止损线
        2. 昨日收盘价低于昨日止损线
        3. 昨日开盘价和收盘价等于止损值时, 往前搜索
        """
        cur_open = self.open[cur]
        cur_close = self.close[cur]
        last_stop = self.stop_loss[cur - 1]
        if cur_close > last_stop:
            return False
        if cur_close < last_stop:
            return True
        if cur_close == last_stop:
            if cur_open < cur_close:
                return True
            elif cur_open > cur_close:
                return False
        for i in range(cur - 1, -1, -1):
            cur_close = self.close[i]
            cur_stop = self.stop_loss[i]
            if cur_stop is None:
                return False
            if cur_close == cur_open == cur_stop:
                continue
            if cur_close < cur_stop:
                return True
            if cur_close == cur_stop and cur_open < cur_close:
                return True
            else:
                return False
        return False

    def above_stop_loss(self, cur: int) -> bool:
        """
        判断今日和昨日k线是否在止损线之上
        1. 当日收盘价高于昨日止损线
        2. 昨日收盘价高于昨日止损线
        3. 昨日开盘价和收盘价等于止损值时, 往前搜索
        """
        cur_open = self.open[cur]
        cur_close = self.close[cur]
        last_stop = self.stop_loss[cur - 1]
        if cur_close < last_stop:
            return False
        if cur_close > last_stop:
            return True
        if cur_close == last_stop:
            if cur_open > cur_close:
                return True
            elif cur_open < cur_close:
                return False
        for i in range(cur - 1, -1, -1):
            cur_close = self.close[i]
            cur_stop = self.stop_loss[i]
            if cur_stop is None:
                return False
            if cur_close == cur_open == cur_stop:
                continue
            if cur_close > cur_stop:
                return True
            if cur_close == cur_stop and cur_open > cur_close:
                return True
            else:
                return False
        return False

    def new_lowest(self, cur):
        """判断是否出现创新低的情况,返回bool类型"""
        start = cur - self.period + 1
        if start < 0:
            start = 0
        lowest = self.low[start : cur + 1].min()
        cur_low = self.low[cur]
        if cur_low == lowest:
            return True
        else:
            return False

    def value_of_new_lowest(self, cur):
        """当出现创新低的情况时, 返回止损点的值"""
        rank = 0
        last_stop = self.stop_loss[cur - 1]
        cur_high = self.high[cur]
        pivot = cur_high
        # if cur_close != last_stop:
        end = -1
        if restrict:
            end = cur - 2 * (self.period) + 1
            if end < -1:
                end = -1
        for i in range(cur - 1, end, -1):
            # TODO: 往前查询的范围还需要确定
            # 止损值取比当前最高价高的第二连续最高价
            if pivot < self.high[i]:
                rank += 1
                pivot = self.high[i]
                if rank == 2:
                    return self.high[i]
        return last_stop

    def new_highest(self, cur):
        """判断是否出现创新高的情况, 返回bool类型"""
        start = cur - self.period + 1
        if start < 0:
            start = 0
        highest = self.high[start : cur + 1].max()
        cur_high = self.high[cur]
        if cur_high == highest:
            return True
        else:
            return False

    def value_of_new_highest(self, cur):
        """当出现创新高的情况时, 返回止损点的值"""
        rank = 0
        cur_low = self.low[cur]
        last_stop = self.stop_loss[cur - 1]
        pivot = cur_low
        # if cur_close != last_stop:
        end = -1
        if restrict:
            end = cur - 2 * (self.period) + 1
            if end < -1:
                end = -1
        for i in range(cur - 1, end, -1):
            # TODO: 止损值寻找范围还需确定
            # 止损值取比当前最低价低的第二连续最低价
            if pivot > self.low[i]:
                rank += 1
                pivot = self.low[i]
                if rank == 2:
                    return self.low[i]
        return last_stop

    def rise_above_stop_loss(self, cur):
        """
        判断是否涨破止损线, 返回bool类型
        """
        assert cur > 0
        cur_close = self.close[cur]
        last_stop = self.stop_loss[cur - 1]
        if last_stop is None:
            return False
        last_close = self.close[cur - 1]
        last_open = self.open[cur - 1]
        if cur_close > last_stop:
            if last_close < last_stop:
                return True
            elif last_close == last_stop:
                if last_open < last_close:
                    return True
                elif last_open == last_close:
                    for i in range(cur - 2, -1, -1):
                        pre_stop = self.stop_loss[i]
                        if pre_stop is None:
                            return False
                        pre_close = self.close[i]
                        pre_open = self.open[i]
                        if pre_close < pre_stop:
                            return True
                        elif pre_close == pre_stop:
                            if pre_open < pre_close:
                                return True
                            elif pre_open == pre_close:
                                continue
                            else:
                                return False
                        else:
                            return False
        return False

    def fall_below_stop_loss(self, cur):
        """
        判断是否跌破止损线,返回bool类型
        """
        assert cur > 0
        cur_close = self.close[cur]
        last_stop = self.stop_loss[cur - 1]
        if last_stop is None:
            return False
        last_open = self.open[cur - 1]
        last_close = self.close[cur - 1]
        if cur_close < last_stop:
            if last_close > last_stop:
                return True
            elif last_close == last_stop:
                if last_open > last_close:
                    return True
                elif last_open == last_close:
                    for i in range(cur - 2, -1, -1):
                        pre_stop = self.stop_loss[i]
                        if pre_stop is None:
                            return False
                        pre_close = self.close[i]
                        pre_open = self.open[i]
                        if pre_close > pre_stop:
                            return True
                        elif pre_close == pre_stop:
                            if pre_open == pre_close:
                                continue
                            elif pre_open > pre_close:
                                return True
                            else:
                                return False
                        else:
                            return False
        return False

    def index_of_last_rise_above(self, cur):
        for i in range(cur - 1, 1, -1):
            if not self.above_stop_loss(i):
                return i
        return -1

    def index_of_last_fall_below(self, cur):
        for i in range(cur - 1, 1, -1):
            if not self.below_stop_loss(i):
                return i
        return -1

    def value_of_fall_below(self, cur):
        rank = 0
        cur_high = self.high[cur]
        pivot = cur_high
        end = -1
        if restrict:
            end = cur - 2 * (self.period) + 1
            end = max(end, -1)
        for i in range(cur - 1, end, -1):
            if pivot < self.high[i]:
                rank += 1
                pivot = self.high[i]
                if rank == 2:
                    return self.high[i]
        return pivot

    def value_of_rise_above(self, cur):
        """
        返回涨破条件下的止损值
        """
        rank = 0
        cur_low = self.low[cur]
        pivot = cur_low
        end = -1
        if restrict:
            end = cur - 2 * (self.period) + 1
            end = max(end, -1)
        for i in range(cur - 1, end, -1):
            if pivot > self.low[i]:
                rank += 1
                pivot = self.low[i]
                if rank == 2:
                    return self.low[i]
        return pivot

    def predict_stop_loss(self, start) -> list:
        """
        Args:
            start(int): 计算止损值的初始位置
        Retures:
            stop_loss(list): 基于self.data计算出的止损值, len(self.data)==len(stop_loss)
        """
        try:
            for i in range(start, len(self.data)):
                if i < self.period - 1:
                    self.stop_loss[i] = None
                    continue
                if self.stop_loss[i - 1] is None:
                    value = self.value_of_begin(i) if self.new_lowest(i) else None
                elif self.fall_below_stop_loss(i) and not self.new_highest(i):
                    # 跌破且没有创新高
                    if volatile and i > 1 and self.rise_above_stop_loss(i - 1):
                        # 震荡: 昨日涨破, 今日跌破, 出现震荡, 止损值取昨日的前一天
                        value = self.stop_loss[i - 1 - 1]
                    elif backtrack and self.new_highest(i - 1):
                        # 回调: 昨日创新高, 取上一次涨破日的前一天的止损值
                        index = self.index_of_last_rise_above(i)
                        if index != -1:
                            value = self.stop_loss[index - 1]
                    else:
                        value = self.value_of_fall_below(i)
                elif self.rise_above_stop_loss(i) and not self.new_lowest(i):
                    # 涨破且没有创新低
                    if volatile and i > 1 and self.fall_below_stop_loss(i - 1):
                        # 震荡: 昨日跌破, 今日涨破, 出现震荡, 止损值取昨日的前一天
                        value = self.stop_loss[i - 1 - 1]
                    elif backtrack and self.new_lowest(i - 1):
                        # 回调: 昨日创新低, 取上一次跌破日的前一天的止损值
                        index = self.index_of_last_fall_below(i)
                        if index != -1:
                            value = self.stop_loss[index - 1]
                    else:
                        value = self.value_of_rise_above(i)
                elif self.fall_below_stop_loss(i) and self.new_highest(i):
                    # 跌破且创新高
                    value = self.value_of_new_highest(i)
                elif self.rise_above_stop_loss(i) and self.new_lowest(i):
                    # 涨破且创新低
                    value = self.value_of_new_lowest(i)
                elif self.above_stop_loss(i) and self.new_highest(i):
                    # 止损线之上且创新高
                    value = self.value_of_new_highest(i)
                elif self.below_stop_loss(i) and self.new_lowest(i):
                    # 止损线之下且创新低
                    value = self.value_of_new_lowest(i)
                else:
                    # 不符合上述条件取前一天的止损值
                    value = self.stop_loss[i - 1]
                self.stop_loss[i] = value
            return self.stop_loss
        except Exception as e:
            logging.exception(e)
            raise e


def filter_data_by_date(data, start, end):
    data = data[data["trade_date"] >= start]
    data = data[data["trade_date"] <= end]
    return data


# 全局变量 True: 限制策略往前查找的周期范围; False: 不限制
restrict = True

# 回测条件开关
backtrack = False

# 震荡条件开关
volatile = True
if __name__ == "__main__":
    code = "A2203.DCE"
    stock = Stock(
        code=code,
        start_date="2021-01-01",
        end_date="2021-12-31",
        data_src="mysql",
        table="future_daily",
    )
    # print(stock.data)
    indicator = StopLossIndicator(stock=stock, period=6)
    stop_loss = indicator.predict_stop_loss(start=len(stock) - 1)
    print(stop_loss[-1])
    # print(indicator.stop_loss,len(indicator.stop_loss))
