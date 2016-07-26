# -*- coding: utf-8 -*-
import redis

from back.LowTest import LowTest
from tools.utils import SymbolUtils


# 前复权日K
# 日期0 开盘1 最高2 最低3 收盘4 成交量5
class Lower:
    def __init__(self, client):
        self.client = client
        self.utils = SymbolUtils(client, False)
        self.LOOKING_PRE_DAYS = 18

    # slow !!!
    def get_all_klines(self, symbols):
        print("start get all Klines")
        ret = {}
        for symbol in symbols:
            bar = self.utils.get_bday_kline(symbol)
            ret[symbol] = bar
        print("end get all Klines")
        return ret

    def ma(self, kmap):
        targets = []
        for symbol in kmap.keys():
            lines = kmap[symbol]
            ma5 = LowTest.get_stock_price_ma(lines, 5)
            ma10 = LowTest.get_stock_price_ma(lines, 10)
            ma20 = LowTest.get_stock_price_ma(lines, 20)
            if LowTest.is_price_buy_signal(ma5, ma10, ma20, -1):
                index, min_price = LowTest.min_price(lines[-self.LOOKING_PRE_DAYS:])
                if len(lines) < self.LOOKING_PRE_DAYS:
                    continue
                bounce = (lines[-1][4] - min_price) / min_price
                max_vol = self.utils.max_vol(lines[-self.LOOKING_PRE_DAYS:])
                if bounce < 0.10 and lines[-1][5] >= max_vol:
                    targets.append(symbol)
        print("%d : %s" % (len(targets), targets))

    def run(self):
        symbols = self.utils.get_symbols()
        kmap = self.get_all_klines(symbols)
        self.ma(kmap)

if __name__ == "__main__":
    r = redis.StrictRedis(host='124.250.34.23', port=6379, db=4)
    low = Lower(r)
    low.run()