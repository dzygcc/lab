# -*- coding: utf-8 -*-

from model.base import Base
from functools import reduce


class Top(Base):
    def __init__(self):
        Base.__init__(self)
        self.name = "TOP T+1"

    def top_stat(self):
        symbols = self.utils.get_symbols()
        close_trades = []
        max_trades = []
        for symbol in symbols:
            klines = self.utils.get_bday_kline(symbol)
            for i in range(100, len(klines) - 1):
                close_change = (klines[i][4] - klines[i - 1][4]) / klines[i - 1][4]
                max_change = (klines[i][2] - klines[i - 1][4]) / klines[i - 1][4]
                if close_change > 0.0989:
                    close_trades.append((klines[i], klines[i + 1]))
                if max_change > 0.0989 > close_change:
                    max_trades.append((klines[i], klines[i + 1]))
                    if klines[i][0] > "20160101":
                        print("symbol=%s date=%s" % (symbol, klines[i][0]))
        self.stat_max(max_trades)
        print("########################################################################################")
        self.stat_close(close_trades)

    # buy at max
    @staticmethod
    def stat_max(trades):
        max_change = {}
        close_change = {}
        for buy_line, sell_line in trades:
            year = buy_line[0][:4]
            max_c = (sell_line[2] - buy_line[2]) / buy_line[2]
            close_c = (sell_line[4] - buy_line[2]) / buy_line[2]
            if year not in max_change:
                max_change[year] = [max_c]
            else:
                max_change[year].append(max_c)
            if year not in close_change:
                close_change[year] = [close_c]
            else:
                close_change[year].append(close_c)
        for year in sorted(close_change.keys()):
            max_change_avg = reduce(lambda x, y: x + y, max_change[year]) / len(max_change[year])
            close_change_avg = reduce(lambda x, y: x + y, close_change[year]) / len(close_change[year])
            print("stat trade at max : year = %s, trade num = %d, max_change_avg= %.3f, close_change_avg= %.3f" %
                  (year, len(max_change[year]), max_change_avg, close_change_avg))

    # buy at close
    @staticmethod
    def stat_close(trades):
        max_change = {}
        close_change = {}
        for buy_line, sell_line in trades:
            year = buy_line[0][:4]
            max_c = (sell_line[2] - buy_line[4]) / buy_line[4]
            close_c = (sell_line[4] - buy_line[4]) / buy_line[4]
            if year not in max_change:
                max_change[year] = [max_c]
            else:
                max_change[year].append(max_c)
            if year not in close_change:
                close_change[year] = [close_c]
            else:
                close_change[year].append(close_c)
        for year in sorted(close_change.keys()):
            max_change_avg = reduce(lambda x, y: x + y, max_change[year]) / len(max_change[year])
            close_change_avg = reduce(lambda x, y: x + y, close_change[year]) / len(close_change[year])
            print("stat trade at close : year = %s, trade num = %d, max_change_avg= %.3f, close_change_avg= %.3f" %
                  (year, len(max_change[year]), max_change_avg, close_change_avg))


if __name__ == "__main__":
    top = Top()
    top.top_stat()
