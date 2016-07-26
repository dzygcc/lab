# -*- coding: utf-8 -*-
import redis
from tools.utils import SymbolUtils
from datetime import datetime, timedelta
import operator
# 前复权日K
# 日期 开盘 最高 最低 收盘 成交量


class LowTest:
    def __init__(self, client):
        self.client = client
        self.utils = SymbolUtils(client)
        self.PRE_DAYS = 18
        self.AFTER_DAYS = 2
        self.index_klines = self.utils.get_sh_index_klines()
        self.index_ma5 = self.get_stock_price_ma(self.index_klines, 5)
        self.index_ma10 = self.get_stock_price_ma(self.index_klines, 10)
        self.index_ma20 = self.get_stock_price_ma(self.index_klines, 20)
        self.bad_date = datetime.strptime("20150611", "%Y%m%d")

    def find_index_pos(self, date):
        b, e = 0, len(self.index_klines) - 1
        while b <= e:
            mid = int((b + e) / 2)
            if self.index_klines[mid][0] == date:
                return mid
            elif self.index_klines[mid][0] < date:
                b = mid + 1
            else:
                e = mid - 1
        return -1

    def test_all(self):
        symbols = self.utils.get_symbols()
        price_trades = []
        for i in range(0, len(symbols)):
            price_trades.extend(self.trade_with_price(symbols[i]))
            if i % 100 == 0:
                print("=== %0.2f ===" % (i*100 / len(symbols)))
        print("trade num = %d" % (len(price_trades)))
        self.utils.net_value_trends(price_trades)

    def trade_with_price_vol(self, symbol):
        klines = self.utils.get_bday_kline(symbol)
        price_ma5 = self.get_stock_price_ma(klines, 5)
        price_ma20 = self.get_stock_price_ma(klines, 20)
        vol_ma20 = self.get_stock_vol_ma(klines, 20)
        vol_ma30 = self.get_stock_vol_ma(klines, 30)
        trades = []
        buy_price = -1
        i = 20
        while i < len(klines):
            if self.is_price_buy_signal(price_ma5, price_ma20, i) and self.is_vol_buy_signal(vol_ma20, vol_ma30, i):
                buy_price = klines[i][4]
                buy_date = klines[i][0]
            if self.is_price_sell_signal(price_ma5, price_ma20, i):
                if buy_price > 0:
                    # 脏数据，复权出现问题
                    if not self.utils.dirty_area(symbol, klines[i-100:i], buy_date, klines[i][0]):
                        trades.append((symbol, buy_price, klines[i][4], buy_date, klines[i][0]))
                        i += 4
                buy_price = -1
        return trades

    # line= [日期0 开盘1 最高2 最低3 收盘4 成交量5]
    def trade_with_price(self, symbol):
        klines = self.utils.get_bday_kline(symbol)
        price_ma5 = self.get_stock_price_ma(klines, 5)
        price_ma10 = self.get_stock_price_ma(klines, 10)
        price_ma20 = self.get_stock_price_ma(klines, 20)
        vol_ma20 = self.get_stock_vol_ma(klines, 20)
        trades = []
        buy_price = -1
        buy_index = -1
        i = 20
        while i < len(klines):
            danger = self.is_danger_signal(klines[i][0])
            if not danger and buy_price < 0 and self.can_buy(klines) \
                    and self.is_price_buy_signal(price_ma5, price_ma10, price_ma20, klines[i-self.PRE_DAYS:i], i):
                buy_price = klines[i][4]
                buy_date = klines[i][0]
                buy_index = i
            if klines[i][5] > 1. * vol_ma20[i]:
                danger = True
            if danger or self.is_price_sell_signal(price_ma5, price_ma10, price_ma20, klines[i-self.PRE_DAYS:i], i):
                if buy_price > 0:
                    if not self.utils.dirty_area(symbol, klines[buy_index:i+1:], buy_date, klines[i][0]) \
                            and klines[i][0] > buy_date:
                        trades.append((symbol, buy_price, klines[i][4], buy_date, klines[i][0]))
                buy_price = -1
            i += 1
        return trades

    def is_price_buy_signal(self, ma5, ma10, ma20, bars, i):
        if not ma5 or not ma20 or len(ma20) <= i:
            return False
        if ma5[i] > ma10[i] > ma20[i] and ma5[i] > ma5[i-1]:
            index_i = self.find_index_pos(bars[-1][0])
            if index_i >= 0 and self.index_ma5[index_i] > self.index_ma10[index_i] > self.index_ma20[index_i]:
                return True
        return False

    def is_danger_signal(self, date):
        dt = datetime.strptime(date, "%Y%m%d")
        bad_end_day = self.bad_date + timedelta(365)
        if self.bad_date < dt < bad_end_day:
            return True
        pre_days = 10
        index_i = self.find_index_pos(date)
        if index_i > pre_days:
            pre_close = -1
            for i in range(index_i - pre_days, index_i):
                if pre_close > 0:
                    change = (self.index_klines[i][4] - pre_close) / pre_close
                    if change <= -0.03:
                        return True
                pre_close = self.index_klines[i][4]
        return False

    def is_price_sell_signal(self, ma5, ma10, ma20, bars, i):
        if ma5[i] < ma20[i]:
            return True
        pos = self.find_index_pos(bars[-1][0])
        if pos > 0 and self.index_ma5[pos] < self.index_ma10[pos] < self.index_ma20[pos] and ma5[i] < ma10[i]:
            return True
        return False

    @staticmethod
    def is_vol_buy_signal(mab, mae, i, default_change=0.05):
        check_num = i
        if i < 0:
            check_num = -1 * i - 1
        if not mab or not mae or len(mae) <= check_num or len(mab) <= check_num:
            return False
        if len(mab) > check_num and len(mae) > check_num and mab[i] > 0:
            slow_vol = mab[i]
            quick_vol = mae[i]
            change = abs(slow_vol - quick_vol) / slow_vol
            if change < default_change:
                return True
        return False

    @staticmethod
    def get_stock_price_ma(lines, n):
        if n <= 0:
            print("error")
            return None
        ma_ret = [0] * len(lines)
        sum_close = [0] * len(lines)
        for i in range(0, len(lines)):
            if i > 0:
                sum_close[i] += sum_close[i - 1] + lines[i][4]
            else:
                sum_close[i] = lines[i][4]
        for i in range(n, len(lines)):
            ma_ret[i] = (sum_close[i] - sum_close[i-n]) / n
        return ma_ret

    @staticmethod
    def get_stock_vol_ma(lines, n):
        if n <= 0:
            print("error")
            return None
        ma_ret = [0] * len(lines)
        sum_vol = [0] * len(lines)
        for i in range(0, len(lines)):
            if i > 0:
                sum_vol[i] += sum_vol[i - 1] + lines[i][5]
            else:
                sum_vol[i] = lines[i][5]
        if len(sum_vol) <= n:
            return ma_ret
        ma_ret[n] = sum_vol[n-1] / n
        for i in range(n+1, len(lines)):
            ma_ret[i] = (sum_vol[i-1] - sum_vol[i-1-n]) / n
        return ma_ret

    @staticmethod
    def avg_close(bars):
        if not bars:
            return 0.0
        avg = 0.0
        for bar in bars:
            avg += bar[4]
        return avg / len(bars)

    # 日期0 开盘1 最高2 最低3 收盘4 成交量5
    @staticmethod
    def can_buy(pre_bars):
        # 涨停板限制, 可能买不到票
        if len(pre_bars) < 2:
            return False
        pre_close = pre_bars[-2][4]
        close = pre_bars[-1][4]
        change = (close - pre_close) / pre_close
        if change > 0.09899:
            return False
        return True

        # i, max_price = LowTest.max_price(pre_bars)
        # j, min_price = LowTest.min_price(pre_bars)
        # change = (max_price - min_price) / max_price
        # if change > 0.15 and i < j:
        #     return False
        # return True
        # if bar[5]/w > pre_max_vol and 0 < change < 0.098 and bounce < 0.30 \
        #          and head_avg > tail_avg:
        #     return True
        # return False

    @staticmethod
    def max_vol(klines):
        vol = 0
        for line in klines:
            if vol < line[5]:
                vol = line[5]
        return vol

    @staticmethod
    def max_price(klines):
        price = 0
        index = 0
        for i in range(0, len(klines)):
            if price < klines[i][2]:
                price = klines[i][2]
                index = i
        return index, price

    @staticmethod
    def min_price(klines):
        index = 0
        price = 10000000
        for i in range(0, len(klines)):
            if klines[i][3] < price:
                price = klines[i][3]
                index = i
        return index, price

    @staticmethod
    def stat_change_probability(self, trade_pair):
        y = [0] * 20
        x = [i for i in range(-10, 10, 1)]
        for pair in trade_pair:
            change = (pair[2] - pair[1]) / pair[1]
            index = 10 + int(100 * change)
            if index >= 20:
                index = 19
            if index <= 0:
                index = 0
            y[index] += 1
        n = len(trade_pair)
        y = [i / n for i in y]
        self.utils.visulize(x, y)

    def find_max_profit(self, trade_pairs):
        profit_map = {}
        for pair in trade_pairs:
            buy_date = datetime.strptime(pair[3], "%Y%m%d")
            sell_date = datetime.strptime(pair[4], "%Y%m%d")
            if buy_date.year != 2016:
                continue
            change = (pair[2] - pair[1]) / pair[1]
            e = change / (sell_date - buy_date).days
            key = "%s:%s:%s" % (pair[0], buy_date.strftime("%Y%m%d"), sell_date.strftime("%Y%m%d"))
            profit_map[key] = e
        sorted_profit = sorted(profit_map.items(), key=operator.itemgetter(1))
        for i in range(0, 100):
            print(sorted_profit[i])
        for i in range(-100, 0):
            print(sorted_profit[i])


    # trade_pair (symbol, buy price, sell price, buy date, sell date)
    def stat(self, trade_pairs, file_prefix):
        good_expect = {}
        bad_expect = {}
        days_expect = {}
        trade_counter = {}
        for pair in trade_pairs:
            buy_date = datetime.strptime(pair[3], "%Y%m%d")
            sell_date = datetime.strptime(pair[4], "%Y%m%d")
            change = (pair[2] - pair[1]) / pair[1]
            year = str(buy_date.year)
            if year not in days_expect:
                good_expect[year] = 0
                bad_expect[year] = 0
                days_expect[year] = 0
                trade_counter[year] = 0
            if change >= 0.0:
                good_expect[year] += change
            else:
                bad_expect[year] += change
                if change < -0.02:
                    print("%s, %s, %s" % (pair[0], pair[3], pair[4]))
            trade_counter[year] += 1
            days_expect[year] += (sell_date - buy_date).days
        # {"year":expect,...}
        y1 = [good_expect[i]/trade_counter[i] for i in sorted(good_expect.keys())]
        y2 = [bad_expect[i]/trade_counter[i] for i in sorted(bad_expect.keys())]
        y3 = [trade_counter[i] for i in sorted(good_expect.keys())]
        y4 = [days_expect[i]/trade_counter[i] for i in sorted(days_expect)]
        x = [i for i in sorted(good_expect.keys())]
        self.utils.visulize_yy(x, y1, y2, file_prefix + "goodExpect-badExpect")
        self.utils.visulize_yy(x, y1, y3, file_prefix + "goodExpect-tradeCounter")
        self.utils.visulize_yy(x, y1, y4, file_prefix + "goodExpect-dayNum")

    def test_index_ma(self):
        dt = datetime.strptime("20150501", "%Y%m%d")
        now = datetime.now()
        while dt < now:
            dt = dt + timedelta(1)
            datestr = dt.strftime("%Y%m%d")
            flag = False
            index_i = self.find_index_pos(datestr)
            if index_i >= 0 and self.index_ma5[index_i] > self.index_ma10[index_i] > self.index_ma20[index_i]:
                flag = True
            print("%s %s" % (datestr, flag))

if __name__ == "__main__":
    r = redis.StrictRedis(host='127.0.0.1', port=6379, db=4)
    test = LowTest(r)
    test.test_all()





