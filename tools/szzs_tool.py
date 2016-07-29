# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from tools.utils import SymbolUtils


class ShangHaiIndex:
    def __init__(self, cli):
        self.utils = SymbolUtils(cli)
        self.index_klines = self.utils.get_sh_index_klines()
        self.index_ma5 = SymbolUtils.get_stock_price_ma(self.index_klines, 5)
        self.index_ma10 = SymbolUtils.get_stock_price_ma(self.index_klines, 10)
        self.index_ma20 = SymbolUtils.get_stock_price_ma(self.index_klines, 20)
        self.DANGER_CHANGE = -0.02
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

    def is_index_up(self, date):
        index_i = self.find_index_pos(date)
        if index_i >= 0:
            if self.index_ma5[index_i] > self.index_ma10[index_i] > self.index_ma20[index_i]:
                return True
            else:
                return False
        return True

    def is_index_down(self, date):
        index_i = self.find_index_pos(date)
        if index_i >= 0:
            if self.index_ma5[index_i] < self.index_ma10[index_i] < self.index_ma20[index_i]:
                return True
            else:
                return False
        return True

    def get_change(self, date):
        index_i = self.find_index_pos(date)
        if index_i > 0:
            pre_close = self.index_klines[index_i-1][4]
            change = (self.index_klines[index_i][4] - pre_close) / pre_close
            return change
        return 0

    def is_danger_signal(self, date):
        dt = datetime.strptime(date, "%Y%m%d")
        bad_end_day = self.bad_date + timedelta(265)
        if self.bad_date < dt < bad_end_day:
            return True
        danger_days = 3
        index_i = self.find_index_pos(date)
        if index_i > danger_days:
            pre_close = -1
            for i in range(index_i - danger_days, index_i):
                if pre_close > 0:
                    change = (self.index_klines[i][4] - pre_close) / pre_close
                    if change <= self.DANGER_CHANGE:
                        return True
                pre_close = self.index_klines[i][4]
        return False

    def test_index_ma(self):
        dt = datetime.strptime("20150501", "%Y%m%d")
        now = datetime.now()
        while dt < now:
            dt = dt + timedelta(1)
            date_str = dt.strftime("%Y%m%d")
            flag = False
            index_i = self.sh_tool.find_index_pos(date_str)
            if index_i >= 0 and self.index_ma5[index_i] > self.index_ma10[index_i] > self.index_ma20[index_i]:
                flag = True
            print("%s %s" % (date_str, flag))