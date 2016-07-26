# -*- coding: utf-8 -*-
import redis
from tools.utils import SymbolUtils
from datetime import datetime, timedelta


class BanTest:
    def __init__(self, client):
        self.client = client
        self.MIN_VOL = 100*10000
        self.utils = SymbolUtils(client, False)
        self.LOOKING_PRE_DAYS = 2

    # 日K
    # 日期0 开盘1 最高2 最低3 收盘4 成交量5
    def stat(self):
        symbols = self.utils.get_symbols()
        good_expect = {}
        bad_expect = {}
        for symbol in symbols:
            lines = self.utils.get_bday_kline(symbol)
            preclose = -1
            for i in range(0, len(lines)):
                buy_date = datetime.strptime(lines[i][0], "%Y%m%d")
                year = str(buy_date.year)
                if preclose < 0 or int(year) < 1997:
                    preclose = lines[i][4]
                    continue
                else:
                    change = (lines[i][4]-preclose)/preclose
                    if 0.01 < change < 0.02:
                        if i < len(lines) - 1 and lines[i][5] > self.MIN_VOL:
                            next_change = (lines[i+1][4]-lines[i][4])/lines[i][4]
                            if next_change > 0:
                                if year not in good_expect.keys():
                                    good_expect[year] = []
                                good_expect[year].append(next_change)
                            else:
                                if year not in bad_expect.keys():
                                    bad_expect[year] = []
                                bad_expect[year].append(next_change)
                preclose = lines[i][4]
        ge = []
        be = []
        x = []
        for year in sorted(good_expect.keys()):
            x.append(year)
            gs = 0
            for e in good_expect[year]:
                gs += e
            print("year=%s, good num = %d, expect = %0.4f" % (year, len(good_expect[year]), gs/len(good_expect[year])))
            ge.append(gs/len(good_expect[year]))
            bs = 0
            for e in bad_expect[year]:
                bs += e
            be.append(bs/len(bad_expect[year]))
            print("year=%s, bad num = %d, expect = %0.4f" % (year, len(bad_expect[year]), bs / len(bad_expect[year])))
            print("expect=%f" % ((gs+bs) / (len(bad_expect[year]) + len(good_expect[year]))))
        self.utils.visulize_yy(x, be, ge, "ban")

    # return dt代表的时间 （涨停的标的, preClose)
    # 日期0 开盘1 最高2 最低3 收盘4 成交量5
    def filter_ban_symbols(self, symbols, dt):
        ret = []
        for symbol in symbols:
            line = self.client.hget("chart:brday:" + symbol, dt.strftime("%Y%m%d"))
            if not line:
                continue
            yest_bar = self.utils.get_pre_kline(symbol, dt - timedelta(1))
            today_bar = self.utils.get_pre_kline(symbol, dt)
            if yest_bar and today_bar:
                change = (today_bar[4] - yest_bar[4])/yest_bar[4]
                if change > 0.0989 and today_bar[5] > 100*10000:
                    ret.append((symbol, yest_bar[4]))
        return ret

    # 分钟0 分钟收盘价1 分钟成交量2 均价3 总成交量4
    def stat_ban_minute_avg(self):
        all_symbols = self.utils.get_symbols()
        dt = datetime.now()
        avg = []
        for i in range(0, self.LOOKING_PRE_DAYS):
            current = dt - timedelta(i)
            ban_symbols = self.filter_ban_symbols(all_symbols, current)
            for symbol, preclose in ban_symbols:
                minute_bars = self.utils.get_minute_quote("1min", current, symbol)
                for j in range(len(minute_bars)-1, -1, -1):
                    bar = minute_bars[j]
                    change = (bar[1] - preclose) / preclose
                    if change < 0.0989 and j >= 0:
                        pre_minute_change = (minute_bars[j-1][3] - preclose) / preclose
                        tomorrow_bar = self.utils.get_after_kline(symbol, current + timedelta(1))
                        max_change = 0
                        if tomorrow_bar:
                            max_change = (tomorrow_bar[2]-preclose) / preclose
                        print("%s %s : pre minute change = %0.4f, max_change = %0.4f" %
                              (symbol, bar[0], pre_minute_change, max_change))
                        avg.append(pre_minute_change)
                        break
            expect = 0.0
            for e in avg:
                expect += e
            print("%s, avg = %0.4f" % (current.strftime("%Y%m%d"), expect/len(avg)))
            avg = []

if __name__ == "__main__":
    r = redis.StrictRedis(host='124.250.34.23', port=6379, db=4)
    ban = BanTest(r)
    ban.stat_ban_minute_avg()