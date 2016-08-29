# -*- coding: utf-8 -*-
import pandas as pd
import redis

from tools.utils import SymbolUtils
from datetime import datetime, timedelta
import numpy as np

class Tech:
    def __init__(self):
        self.name = ""

    def macd(self, df, n_fast, n_slow):
        fast = pd.Series(pd.ewma(df['close'], span=n_fast, min_periods=n_fast - 1))
        slow = pd.Series(pd.ewma(df['close'], span=n_slow, min_periods=n_slow - 1))
        md = pd.Series(fast - slow, name='macd')
        sign = pd.Series(pd.ewma(md, span=9, min_periods=8), name='signal')
        diff = pd.Series(md - sign, name="diff")
        df = df.join(md)
        df = df.join(sign)
        df = df.join(diff)
        return df

    # Relative Strength Index
    @staticmethod
    def rsi(df, n):
        delta = df[["close"]].diff()
        u = delta * 0
        d = u.copy()
        i_pos = delta > 0
        i_neg = delta < 0
        u[i_pos] = delta[i_pos]
        d[i_neg] = delta[i_neg]
        wu = pd.Series(pd.ewma(u["close"], span=n, min_periods=n - 1))
        wd = pd.Series(pd.ewma(d["close"], span=n, min_periods=n - 1)).abs()
        rs = pd.Series(wu / wd, name='rsi')
        rsi = pd.Series(100 - 100.0 / (1 + rs))
        df = df.join(rsi)
        return df

    @staticmethod
    def convert_klines(klines):
        close_arr = []
        high_arr = []
        low_arr = []
        for row in klines:
            high_arr.append(row[1])
            low_arr.append(row[2])
            close_arr.append(row[3])
        cls = {"close": close_arr, "high": high_arr, "low": low_arr}
        df = pd.DataFrame(cls)
        return df

    def get_quotes(self, num=-1):
        r = redis.StrictRedis(host='172.28.48.5', port=6379, db=4, password="tiger")
        su = SymbolUtils(r)
        symbols = su.get_symbols()
        if num > 0:
            symbols = symbols[:num]
        ago = datetime.now() - timedelta(60)
        ret = []
        for symbol in symbols:
            klines = su.get_after_klines(symbol, ago, 60)
            ret.append((symbol, klines))
        return ret

    def find_with_macd(self, symbol, klines):
            df = Tech.convert_klines(klines)
            df = self.macd(df, 12, 26)
            if len(df.index) >= 2:
                if df["macd"].iloc[-1] > df["signal"].iloc[-1] and df["macd"].iloc[-2] <= df["signal"].iloc[-2]:
                    print("buy" + symbol)
                if df["macd"].iloc[-1] < df["signal"].iloc[-1] and df["macd"].iloc[-2] >= df["signal"].iloc[-2]:
                    print("sell" + symbol)

    def find_with_rsi(self, symbol, klines):
        df = Tech.convert_klines(klines)
        df = self.rsi(df, 14)
        rsi = df["rsi"].iloc[-1]
        if rsi > 80 or rsi < 20:
                print("symbol = %s, rsi = %f" % (symbol, rsi))

    def run(self):
        quotes = self.get_quotes()
        print("get quotes ended.")
        for symbol, klines in quotes:
            if len(klines) > 20:
                #self.find_with_rsi(symbol, klines)
                self.find_with_macd(symbol, klines)

if __name__ == "__main__":
    t = Tech()
    t.run()


