import logging
from time import process_time_ns
import numpy as np
import pytz

import redis
from mq.consumer import Consumer
from datetime import datetime, timedelta
from util.order import clientAPI
import tzlocal

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.WARNING
)

_local_zone = pytz.timezone('Asia/Shanghai')


class StopLossStrategy(Consumer):
    """
    止损线指标, 影响止损值变化的策略包括初始化,创新低, 创新高, 跌破, 涨破策略 \n
    Args:
        client_id: 客户端唯一标识id, 一个策略作为一个消费者客户端
        period: 止损值变化策略(创新低,创新高等)考虑的交易周期数
        code: 止损值策略针对的合约
        restrict: 是否限制止损值策略的某些规则往前搜索的天数
        volatile: 是否开启止损值震荡条件
        backtrack: 是否开启止损值回调条件
    """

    def __init__(
        self,
        client_id: str,
        code: str,
        period: int = 6,
        restrict: bool = False,
        volatile: bool = False,
        backtrack: bool = False,
    ) -> None:
        super().__init__(client_id)
        self.code = code
        self.stop_loss = []
        self.close_ = []
        self.open_ = []
        self.high = []
        self.low = []
        self.period = period
        self.restrict = restrict
        self.volatile = volatile
        self.backtrack = backtrack
        self.buy: bool = False
        self.sell: bool = False
        self.clientAPI = clientAPI('mt9025296', '15802644191')

    def begin(self):
        """
        TODO: 获取指标初始值, 与创新低类似
        """
        return self.stop_loss[-1] is None

    def begin_new_lowest(self, cur):
        if cur < self.period - 1:
            return False
        cur_low = self.low[cur]
        start = max(cur - self.period + 1, 0)
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
        cur_open = self.open_[cur]
        cur_close = self.close_[cur]
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
            cur_close = self.close_[i]
            cur_stop = self.stop_loss[i]
            if cur_stop is None:
                return False
            if cur_close == cur_open == cur_stop:
                continue
            if cur_close < cur_stop:
                return True
            return cur_close == cur_stop and cur_open < cur_close
        return False

    def above_stop_loss(self, cur: int) -> bool:
        """
        判断今日和昨日k线是否在止损线之上
        1. 当日收盘价高于昨日止损线
        2. 昨日收盘价高于昨日止损线
        3. 昨日开盘价和收盘价等于止损值时, 往前搜索
        """
        cur_open = self.open_[cur]
        cur_close = self.close_[cur]
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
            cur_close = self.close_[i]
            cur_stop = self.stop_loss[i]
            if cur_stop is None:
                return False
            if cur_close == cur_open == cur_stop:
                continue
            if cur_close > cur_stop:
                return True
            return cur_close == cur_stop and cur_open > cur_close
        return False

    def new_lowest(self, cur):
        """判断是否出现创新低的情况,返回bool类型"""
        start = max(cur - self.period + 1, 0)
        lowest = np.min(self.low[start : cur + 1])
        cur_low = self.low[cur]
        return cur_low == lowest

    def value_of_new_lowest(self, cur):
        """当出现创新低的情况时, 返回止损点的值"""
        rank = 0
        last_stop = self.stop_loss[cur - 1]
        cur_high = self.high[cur]
        pivot = cur_high
        end = max(cur - 2 * (self.period) + 1, -1) if self.restrict else -1
        for i in range(cur - 1, end, -1):
            # 止损值取比当前最高价高的第二连续最高价
            if pivot < self.high[i]:
                rank += 1
                pivot = self.high[i]
                if rank == 2:
                    return self.high[i]
        return last_stop

    def new_highest(self, cur):
        """判断是否出现创新高的情况, 返回bool类型"""
        start = max(cur - self.period + 1, 0)
        highest = np.max(self.high[start : cur + 1])
        cur_high = self.high[cur]
        return cur_high == highest

    def value_of_new_highest(self, cur):
        """当出现创新高的情况时, 返回止损点的值"""
        rank = 0
        cur_low = self.low[cur]
        last_stop = self.stop_loss[cur - 1]
        pivot = cur_low
        end = max(cur - 2 * (self.period) + 1, -1) if self.restrict else -1
        for i in range(cur - 1, end, -1):
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
        cur_close = self.close_[cur]
        last_stop = self.stop_loss[cur - 1]
        if last_stop is None:
            return False
        last_close = self.close_[cur - 1]
        last_open = self.open_[cur - 1]
        if cur_close > last_stop:
            if (
                last_close >= last_stop
                and last_close == last_stop
                and last_open < last_close
                or last_close < last_stop
            ):
                return True
            elif last_close == last_stop and last_open == last_close:
                for i in range(cur - 2, -1, -1):
                    pre_stop = self.stop_loss[i]
                    if pre_stop is None:
                        return False
                    pre_close = self.close_[i]
                    pre_open = self.open_[i]
                    if (
                        pre_close >= pre_stop
                        and pre_close == pre_stop
                        and pre_open < pre_close
                        or pre_close < pre_stop
                    ):
                        return True
                    elif pre_close == pre_stop and pre_open == pre_close:
                        continue
                    else:
                        return False
        return False

    def fall_below_stop_loss(self, cur):
        """
        判断是否跌破止损线,返回bool类型
        """
        assert cur > 0
        cur_close = self.close_[cur]
        last_stop = self.stop_loss[cur - 1]
        if last_stop is None:
            return False
        last_open = self.open_[cur - 1]
        last_close = self.close_[cur - 1]
        if cur_close < last_stop:
            if (
                last_close <= last_stop
                and last_close == last_stop
                and last_open > last_close
                or last_close > last_stop
            ):
                return True
            elif last_close == last_stop and last_open == last_close:
                for i in range(cur - 2, -1, -1):
                    pre_stop = self.stop_loss[i]
                    if pre_stop is None:
                        return False
                    pre_close = self.close_[i]
                    pre_open = self.open_[i]
                    if (
                        pre_close <= pre_stop
                        and pre_close == pre_stop
                        and pre_open == pre_close
                    ):
                        continue
                    elif (
                        pre_close <= pre_stop
                        and pre_close == pre_stop
                        and pre_open > pre_close
                        or pre_close > pre_stop
                    ):
                        return True
                    else:
                        return False
        return False

    def index_of_last_rise_above(self, cur):
        return next(
            (i for i in range(cur - 1, 1, -1) if not self.above_stop_loss(i)), -1
        )

    def index_of_last_fall_below(self, cur):
        return next(
            (i for i in range(cur - 1, 1, -1) if not self.below_stop_loss(i)), -1
        )

    def value_of_fall_below(self, cur):
        rank = 0
        cur_high = self.high[cur]
        pivot = cur_high
        end = max(cur - 2 * (self.period) + 1, -1) if self.restrict else -1
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
        end = max(cur - 2 * (self.period) + 1, -1) if self.restrict else -1
        for i in range(cur - 1, end, -1):
            if pivot > self.low[i]:
                rank += 1
                pivot = self.low[i]
                if rank == 2:
                    return self.low[i]
        return pivot

    def process_message(self, channel, message, sig=1):
        print(
            f"callback function client: {self.client_id} recieve message from channel: {channel}"
        )
        code2data: dict = eval(message)
        min_stop_period = 5
        if self.code in code2data:
            pass
        else:
            return
        data = code2data.get(self.code)
        value = self.cal_stop_loss(data=data)
        print(
            f"time: {datetime.fromtimestamp(data['time'] / 1e3, _local_zone).strftime('%Y-%m-%d %H:%M:%S')} close: {data['close']} stop_loss: {value}"
        )
        if sig:
            if self.buy == False and value and value > data["close"]:
                print(
                    f"buy at time: {datetime.fromtimestamp(data['time'] / 1e3).strftime('%Y-%m-%d %H:%M:%S')}"
                )
                self.buy_time = data['time'] / 1e3
                self.buy = True
                self.buy_price = data["close"]
                self.clientAPI.handleOrder(code=self.code, buyOrSell=0, lot=10, price=self.buy_price)
                return
            if self.buy and self.buy_price > data["close"] and (data['time'] / 1e3 - self.buy_time - min_stop_period*60):
                print(
                    f"sell at time: {datetime.fromtimestamp(data['time'] / 1e3, _local_zone).strftime('%Y-%m-%d %H:%M:%S')}"
                )
                self.buy = False
                self.sell_price = data["close"]
                self.clientAPI.handleOrder(code=self.code, buyOrSell=1, EntryOrExit = 1, lot=10, price=self.sell_price)

    def cal_stop_loss(
        self,
        cur_open: float = None,
        cur_close: float = None,
        cur_high: float = None,
        cur_low: float = None,
        data: dict = None,
    ) -> float:
        """
        Args:
            cur_open
            cur_close
            cur_high
            cur_low
        Retures:
            stop_loss (list): 止损值
        """
        if data is not None:
            self.close_.append(data["close"])
            self.open_.append(data["open"])
            self.high.append(data["high"])
            self.low.append(data["low"])
        else:
            self.close_.append(cur_close)
            self.open_.append(cur_open)
            self.high.append(cur_high)
            self.low.append(cur_low)
        if len(self.stop_loss) < self.period:
            self.stop_loss.append(None)
            return None
        i = len(self.stop_loss)
        if self.stop_loss[-1] is None:
            value = self.value_of_begin(i) if self.new_lowest(i) else None
        elif self.fall_below_stop_loss(i) and not self.new_highest(i):
            # 跌破且没有创新高
            if self.volatile and i > 1 and self.rise_above_stop_loss(i - 1):
                # 震荡: 昨日涨破, 今日跌破, 出现震荡, 止损值取昨日的前一天
                value = self.stop_loss[i - 1 - 1]
            elif self.backtrack and self.new_highest(i - 1):
                # 回调: 昨日创新高, 取上一次涨破日的前一天的止损值
                index = self.index_of_last_rise_above(i)
                if index != -1:
                    value = self.stop_loss[index - 1]
            else:
                value = self.value_of_fall_below(i)
        elif self.rise_above_stop_loss(i) and not self.new_lowest(i):
            # 涨破且没有创新低
            if self.volatile and i > 1 and self.fall_below_stop_loss(i - 1):
                # 震荡: 昨日跌破, 今日涨破, 出现震荡, 止损值取昨日的前一天
                value = self.stop_loss[i - 1 - 1]
            elif self.backtrack and self.new_lowest(i - 1):
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
            value = self.stop_loss[-1]
        self.stop_loss.append(value)
        return value


if __name__ == "__main__":
    stoploss_strategy = StopLossStrategy(
        client_id="stoploss-client", code="A2203.XDCE"
    )
    stoploss_strategy.subscribe("1m", stoploss_strategy.process_message)
