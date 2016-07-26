# -*- coding: utf-8 -*-
from datetime import timedelta
from datetime import datetime
import redis

from tools.utils import SymbolUtils


# 日期0 开盘1 最高2 最低3 收盘4 成交量5 DAY_BAR
# 分钟0 分钟收盘价1 分钟成交量2 均价3 总成交量4
class UpperTest:
    def __init__(self, cli):
        self.client = cli
        self.utils = SymbolUtils(cli, False)

    @staticmethod
    def filter_by_avg(minute_bars):
        # avg > close at 14:30 or 15:00
        if minute_bars[-1][3] > minute_bars[-1][1] or minute_bars[-30][3] > minute_bars[-30][1]:
            return True
        return False

    def find_trade(self, dt, symbol):
        minute_bars = self.utils.get_minute_quote("1min", dt, symbol)
        pre_day_bar = self.utils.get_pre_kline(symbol, dt - timedelta(1))
        if not pre_day_bar or len(minute_bars) == 0:
            return None
        pre_close = pre_day_bar[4]
        if minute_bars[-1][4] < 1000000:
            return [1]
        if UpperTest.filter_by_avg(minute_bars):
            for mbar in minute_bars:
                change = (mbar[1] - pre_close) / pre_close
                if 0.090 < change < 0.11:  # 有拉涨停
                    tn_bars = self.utils.get_after_klines(symbol, dt + timedelta(1), 5)
                    if tn_bars:
                        profit_close = (tn_bars[-1][4] - minute_bars[-1][1]) / minute_bars[-1][1]
                        tmp, max_price = self.utils.max_price(tn_bars)
                        profit_max = (max_price - minute_bars[-1][1]) / minute_bars[-1][1]
                        today_bar = self.utils.get_after_kline(symbol, dt)
                        tmp_bars = [today_bar]
                        for bar in tn_bars:
                            tmp_bars.append(bar)
                        if not self.has_company_act(tmp_bars):
                            all_klines = self.utils.get_klines_with_last_date(symbol, dt.strftime("%Y%m%d"))
                            max_vol = self.utils.max_vol(all_klines[-18:])
                            if max_vol > today_bar[5]:
                                print("%s %s: profit max=%s, close=%s" % (dt.strftime("%Y%m%d"), symbol, profit_max, profit_close))
                                return profit_max, profit_close
                    break
        return [1]

    def trade(self):
        symbols = self.utils.get_symbols()
        index_kmap = self.utils.get_sh_index_kmap()
        dt = datetime.now()
        if datetime.now().hour < 15:
            dt = datetime.now() - timedelta(1)
        has_quotes = True
        trades = []
        while has_quotes:
            dt = dt - timedelta(1)
            date_str = dt.strftime("%Y%m%d")
            while date_str not in index_kmap.keys():
                dt = dt - timedelta(1)
                date_str = dt.strftime("%Y%m%d")
            print("process minute bars on %s" % date_str)
            has_quotes = False
            for symbol in symbols:
                ret = self.find_trade(dt, symbol)
                if ret:
                    has_quotes = True
                    if len(ret) == 2:
                        trades.append(ret)
        return trades

    @staticmethod
    def has_company_act(bars):
        preclose = 0
        for bar in bars:
            if preclose == 0:
                preclose = bar[4]
            else:
                change = (bar[4] - preclose) / preclose
                if change < -0.109 or change > 0.109:
                    return True
                preclose = bar[4]
        return False

    @staticmethod
    def stat(profit_arr):
        good_trade_close_cnt = 0
        good_trade_max_cnt = 0
        close_expect = 0.0
        max_expect = 0.0
        for profit_max, profit_close in profit_arr:
            if profit_max > 0.02:
                good_trade_max_cnt += 1
            if profit_close > 0.0:
                good_trade_close_cnt += 1
            close_expect += profit_close
            max_expect += profit_max
        n = len(profit_arr)
        print("trade num = %d" % n)
        print("trade on close: expect = %0.4f, success rate = %0.4f" % (close_expect / n, good_trade_close_cnt / n))
        print("trade on max: expect = %0.4f, success rate = %0.4f" % (max_expect / n, good_trade_max_cnt / n))

    def run(self):
        self.stat(self.trade())


if __name__ == "__main__":
    r = redis.StrictRedis(host='172.28.48.5', port=6379, db=4, password="tiger")
    #r = redis.StrictRedis(host='124.250.34.23', port=6379, db=4)
    tool = UpperTest(r)
    tool.run()
