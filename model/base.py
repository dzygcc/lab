# -*- coding: utf-8 -*-
import redis

from tools.szzs_tool import ShangHaiIndex
from tools.utils import SymbolUtils


class Base:
    def __init__(self):
        client = redis.StrictRedis(host='172.28.48.5', port=6379, db=4, password="tiger")
        self.utils = SymbolUtils(client, [])
        self.sh_tool = ShangHaiIndex(client)

    def symbols(self):
        return self.utils.get_symbols()
        #return ["600030"]
        #return ["600030", "002230", "000501", "002411"]  # 中信证券["600797"]

    def name(self):
        return "base"

    def strategy(self, symbol):
        pre_days = 18
        klines = self.utils.get_bday_kline(symbol)
        price_ma5 = SymbolUtils.get_stock_price_ma(klines, 5)
        price_ma10 = SymbolUtils.get_stock_price_ma(klines, 10)
        price_ma20 = SymbolUtils.get_stock_price_ma(klines, 20)
        trades = []
        buy_price = -1
        buy_index = -1
        i = 20
        while i < len(klines):
            danger = self.sh_tool.is_danger_signal(klines[i][0])
            if not danger and buy_price < 0 and self.can_buy(klines[i][4], klines[i-1][4]) \
                    and self.is_price_buy_signal(price_ma5, price_ma10, price_ma20, klines[i - pre_days:i], i):
                buy_price = klines[i][4]
                buy_date = klines[i][0]
                buy_index = i
            if danger or self.is_price_sell_signal(price_ma5, price_ma10, price_ma20, klines[i - pre_days:i], i):
                if buy_price > 0:
                    if not self.utils.dirty_area(symbol, klines[buy_index:i + 1:], buy_date, klines[i][0]) \
                            and klines[i][0] > buy_date:
                        trades.append((symbol, buy_price, klines[i][4], buy_date, klines[i][0]))
                buy_price = -1
            i += 1
        return trades

    def is_price_buy_signal(self, ma5, ma10, ma20, bars, i, check_index=True):
        if not ma5 or not ma20 or len(ma20) <= i:
            return False
        if ma5[i] > ma10[i] > ma20[i] and ma5[i] > ma5[i-1] and \
                (not check_index or self.sh_tool.is_index_up(bars[-1][0])):
            return True
        return False

    def is_price_sell_signal(self, ma5, ma10, ma20, bars, i):
        if ma5[i] < ma20[i]:
            return True
        if self.sh_tool.is_index_down(bars[-1][0]) and ma5[i] < ma10[i]:
            return True
        return False

    # 日期0 开盘1 最高2 最低3 收盘4 成交量5
    @staticmethod
    def can_buy(close, pre_close):
        # 涨停板限制, 可能买不到票
        if not close or not pre_close:
            return False
        change = (close - pre_close) / pre_close
        if change > 0.09899:
            return False
        return True

    def trade(self):
        symbols = self.symbols()
        self.utils.set_symbols(symbols)
        trades = []
        for i in range(0, len(symbols)):
            trades.extend(self.strategy(symbols[i]))
        print("trade num = %d" % (len(trades)))
        x, y = self.utils.net_value_trends(trades)
        self.utils.visulize(x, y, self.name())


if __name__ == "__main__":
    test = Base()
    test.trade()