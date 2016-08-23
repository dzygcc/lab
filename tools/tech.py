# -*- coding: utf-8 -*-
import pandas as pd
import redis

from tools.utils import SymbolUtils
from datetime import datetime, timedelta


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

    def find_with_macd(self):
        r = redis.StrictRedis(host='172.28.48.5', port=6379, db=4, password="tiger")
        su = SymbolUtils(r)
        symbols = su.get_symbols()
        ago = datetime.now() - timedelta(250)
        for symbol in symbols:
            klines = su.get_after_klines(symbol, ago, 180)
            cls = {"close": [row[4] for row in klines]}
            df = pd.DataFrame(cls)
            df = self.macd(df, 12, 26)
            if len(df.index) >= 2 and\
                        df["macd"].iloc[-1] > df["signal"].iloc[-1] and df["macd"].iloc[-2] <= df["signal"].iloc[-2]:
                print(symbol)

if __name__ == "__main__":
    t = Tech()
    t.find_with_macd()