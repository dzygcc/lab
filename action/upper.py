# -*- coding: utf-8 -*-
from datetime import timedelta
from datetime import datetime
import redis

from tools.utils import SymbolUtils


class Upper:
    def __init__(self, cli):
        self.abc = 0
        self.client = cli
        self.utils = SymbolUtils(cli, False)

    # 分钟0 分钟收盘价1 分钟成交量2 均价3 总成交量4
    def get_close_bellow_avg(self, symbols, dt):
        for symbol in symbols:
            minute_bars = self.utils.get_minute_quote("1min", dt, symbol)
            pre_day_bar = self.utils.get_pre_kline(symbol, dt-timedelta(1))
            if not pre_day_bar or not minute_bars:
                continue
            pre_close = pre_day_bar[4]
            # 分钟线低于均价
            if minute_bars[-1][3] > minute_bars[-1][1] or minute_bars[-10][3] > minute_bars[-10][1]:
                for mbar in minute_bars:
                    change = (mbar[1] - pre_close)/pre_close
                    if 0.095 < change < 0.11:
                        print("%s %s %0.4f" % (symbol, mbar[0], change))
                        break

    def run(self):
        symbols = self.utils.get_symbols()
        yest = datetime.now() - timedelta(1)
        self.get_close_bellow_avg(symbols, yest)


if __name__ == "__main__":
    r = redis.StrictRedis(host='124.250.34.23', port=6379, db=4)
    tool = Upper(r)
    tool.run()

