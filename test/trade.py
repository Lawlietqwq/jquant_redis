from model.stock import Stock
from strategy.stop_loss import StopLossIndicator


class TradingData:
    def __init__(self, data):
        self.data = data
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.data):
            raise StopIteration
        result = self.data.iloc[self.index, :]
        self.index += 1
        return result


def get_newest_data():
    open_ = 0
    close = 0
    high = 0
    low = 0
    return open_, close, high, low


def strategy(trading_data):
    real_stoploss = StopLossIndicator(
        code="test", period=6, restrict=False, backtrack=False, volatile=False
    )
    for piece in trading_data:
        trade_date = piece["trade_date"]
        cur_open = piece["open"]
        cur_close = piece["close"]
        cur_high = piece["high"]
        cur_low = piece["low"]
        cur_stoploss = real_stoploss.cal_stop_loss(
            cur_open, cur_close, cur_high, cur_low
        )
        print(trade_date, cur_stoploss)


if __name__ == "__main__":
    stock = Stock(
        "A2207.XDCE",
        "2022-05-01",
        "2022-12-31",
        frequency="1m",
        table="future_m",
        data_src="mysql",
    )
    trading_data = TradingData(stock.data)
    strategy(trading_data)
