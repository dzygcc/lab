# -*- coding: utf-8 -*-
import redis

from back.LowTest import LowTest
from tools.utils import SymbolUtils
from datetime import datetime


# 前复权日K
# 日期0 开盘1 最高2 最低3 收盘4 成交量5
class Lower:
    def __init__(self, client):
        self.client = client
        self.utils = SymbolUtils(client)
        self.lower = LowTest(client)
        self.LOOKING_PRE_DAYS = 18

    def make(self):
        dt = datetime.now()
        for symbol in self.utils.get_symbols():
            bars = self.utils.get_pre_bday_bars(symbol, dt, 25)
            if len(bars) > 20:
                self.ma(symbol, bars)

    def ma(self, symbol, lines):
        ma10 = SymbolUtils.get_stock_price_ma(lines, 10)
        ma5 = SymbolUtils.get_stock_price_ma(lines, 5)
        ma20 = SymbolUtils.get_stock_price_ma(lines, 20)
        if self.lower.is_price_buy_signal(ma5, ma10, ma20, lines, -1, False):
            max_vol = self.utils.max_vol(lines)
            avg_vol = self.utils.avg_vol(lines)
            if max_vol < 2.0 * avg_vol:
                print(symbol)

    def run(self):
        self.make()

if __name__ == "__main__":
    r = redis.StrictRedis(host='172.28.48.5', port=6379, db=4, password="tiger")
    low = Lower(r)
    low.run()