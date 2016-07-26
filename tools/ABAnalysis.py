# -*- coding: utf-8 -*-
import json
import time
from datetime import datetime

import redis

from tools.utils import SymbolUtils


class Probe:
    def __init__(self, cli):
        self.client = cli
        self.STOP_RATE = 0.098
        self.CANDIDATE_RATE = 0.05
        self.candidates = set()
        self.stop_symbols = set()
        self.utils = SymbolUtils(cli, False)

    def get_ask_bid(self, symbols):
        symbols = list(symbols)
        pipe = self.client.pipeline()
        for s in symbols:
            pipe.hget("citic:askbid", s)
        raws = pipe.execute()
        ret = []
        for i in range(0, len(symbols)):
            if raws[i]:
                raw = json.loads(raws[i].decode("utf-8"))
                ab = {"ask": self.parse_ab_array(raw.get("ask"), False), # ASC
                 "bid": self.parse_ab_array(raw.get("bid"), True),  # ASC
                 "symbol": symbols[i],
                 "preClose": raw["preClose"],
                 "time": raw["serverTime"]}
                ret.append(ab)
        return ret

    def filter(self, abs):
        now = datetime.now()
        for ab in abs:
            preclose = ab["preClose"]
            max_ask_price = ab["ask"][-1][0]
            vol = ab["ask"][-1][1]
            ch_rate = self.change(preclose, max_ask_price)
            if ch_rate >= self.STOP_RATE and vol > 0:
                if ab["symbol"] not in self.stop_symbols:
                    self.stop_symbols.add(ab["symbol"])
                    minutes_quotes = self.utils.get_minute_quote("1min", now, ab["symbol"])
                    last_bar = minutes_quotes[-1]
                    avg_ch = (last_bar[3] - preclose) / preclose
                    print("%s, avg = %0.4f" % (ab["symbol"], avg_ch))
            if ch_rate >= self.CANDIDATE_RATE:
                self.candidates.add(ab["symbol"])

    @staticmethod
    def parse_ab_array(arr, reverse):
        ret = []
        for ask in arr:
            parts = ask.split(' ')
            if len(parts) == 2:
                price = float(parts[0])
                vol = int(parts[1])
                ret.append((price, vol))
        if reverse:
            ret.reverse()
        return ret

    def change(self, pre, price):
        if not pre or not price:
            return 0.0
        return (price - pre) / pre


    def parse_timestamp(timestamp):
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    def refresh_abs(self):
        counter = 0
        all_symbols = self.utils.get_symbols()
        while True:
            if counter % 2000 == 0:
                self.filter(self.get_ask_bid(all_symbols))
                stop_symbols = list(self.stop_symbols)
                print("All : %s" % str(stop_symbols))
            self.filter(self.get_ask_bid(self.candidates))
            counter += 1
            time.sleep(0.05)

def print_cur_time():
    print(datetime.now())

if __name__ == "__main__":
    r = redis.StrictRedis(host='124.250.34.23', port=6379, db=4)
    tool = Probe(r)
    tool.refresh_abs()