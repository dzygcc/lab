# -*- coding: utf-8 -*-
import json
import time
from datetime import datetime
import redis
from tools.utils import SymbolUtils

try:
    import winsound

    def make_sound():
        winsound.Beep(3072, 60)
        winsound.Beep(2048, 100)
except ImportError:
    def make_sound():
        pass


#   local redis:
#       ab:date:symbol -> [H:M:S, bidSize, totalVolume, ... , ]
#
class Probe:
    def __init__(self, cli):
        self.client = cli
        self.STOP_RATE = 0.098
        self.CANDIDATE_RATE = 0.07
        self.SCAN_ALL_PERIOD = 360      # second
        self.SCAN_HIGH_PERIOD = 10      # second
        self.candidates = set()
        self.stop_symbols = set()
        self.date = datetime.now().strftime("%Y%m%d")
        self.local_redis = redis.StrictRedis(host='127.0.0.1', port=7777, db=1)
        self.utils = SymbolUtils(cli)
        self.stat_map = {}

    def get_ab_stat_key(self, symbol):
        return "ab:" + self.date + ":" + symbol

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
                ab = {
                    "ask": self.parse_ab_array(raw.get("ask"), False),      # ASC
                    "bid": self.parse_ab_array(raw.get("bid"), True),       # ASC
                    "symbol": symbols[i],
                    "preClose": raw["preClose"],
                    "time": raw["serverTime"]
                }
                ret.append(ab)
        return ret

    # 分钟0 分钟收盘价1 分钟成交量2 均价3 总成交量4
    def get_last_minute_bar(self, symbol):
        minutes_quotes = self.utils.get_minute_quote("1min", datetime.now(), symbol)
        if not minutes_quotes or len(minutes_quotes) == 0:
            return 0, 0, 0, 0, 0
        return minutes_quotes[-1]

    # 新股/奇葩
    def ipo_filter(self, symbol):
        bars = self.utils.get_bday_kline(symbol)
        if len(bars) < 20:
            return True
        count = 0
        for i in range(-3, 0, 1):
            change = self.change(bars[i-1][4], bars[i][4])
            if change > self.STOP_RATE:
                count += 1
        if count >= 3:
            return True
        return False

    def stat_bid_vol(self):
        askbids = self.get_ask_bid(self.stop_symbols)
        for ab in askbids:
            symbol = ab["symbol"]
            bid_size = ab["bid"][-1][1]
            vol = int(self.get_last_minute_bar(symbol)[4] / 100)
            t = self.format_hms(ab["time"])
            if symbol not in self.stat_map:
                self.stat_map[symbol] = {"bs": bid_size}
            elif abs(bid_size - self.stat_map[symbol]["bs"]) > 88:
                self.stat_map[symbol]["bs"] = bid_size
                k = self.get_ab_stat_key(symbol)
                v = t + " " + str(bid_size) + " " + str(vol)
                self.local_redis.rpush(k, v)

    def alpha_filter(self, symbols):
        askbids = self.get_ask_bid(symbols)
        ret = []
        for ab in askbids:
            preclose = ab["preClose"]
            max_bid_price = ab["ask"][-1][0]
            if max_bid_price <= 0.0:
                max_bid_price = ab["bid"][-1][0]
            ch_rate = self.change(preclose, max_bid_price)
            if ch_rate > 0.01:
                ret.append(ab["symbol"])
            postfix = "*" * (int(ab["symbol"]) % 50)
            if ch_rate >= self.STOP_RATE:
                if not self.ipo_filter(ab["symbol"]):
                    if ab["symbol"] not in self.stop_symbols:
                        self.stop_symbols.add(ab["symbol"])
                        print("***  %s %s" % (ab["symbol"], postfix))
                        make_sound()
            elif ab["symbol"] in self.stop_symbols:
                self.stop_symbols.remove(ab["symbol"])
            if self.CANDIDATE_RATE <= ch_rate < self.STOP_RATE:
                self.candidates.add(ab["symbol"])
            elif ab["symbol"] in self.candidates:
                self.candidates.remove(ab["symbol"])
        return ret

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

    def parse_timestamp(t):
        return datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def is_market_close():
        dt = datetime.now()
        minute_of_day = dt.hour * 60 + dt.minute
        if dt.hour < 9 or dt.hour >= 15  \
            or 690 < minute_of_day < 780 \
                or dt.isoweekday() >= 6:
            return True
        return False

    def refresh_abs(self):
        counter = 0
        all_symbols = self.utils.get_symbols()
        min_tick = 0.1
        while True:
            if self.is_market_close():
                time.sleep(min_tick*30)
                continue
            # 全量
            if counter % (self.SCAN_ALL_PERIOD/min_tick) == 0:
                all_symbols = self.utils.get_symbols()
            # 子集
            if counter % (self.SCAN_HIGH_PERIOD/min_tick) == 0:
                all_symbols = self.alpha_filter(all_symbols)
                # print("all stop symbols: %s" % str(list(self.stop_symbols)))
            # 增量
            self.alpha_filter(self.candidates)
            self.stat_bid_vol()
            counter += 1
            time.sleep(min_tick)

    # format 1477630919
    @staticmethod
    def format_hms(t):
        return datetime.fromtimestamp(t).strftime('%H:%M:%S')

if __name__ == "__main__":
    r = redis.StrictRedis(host='172.28.48.5', port=6379, db=4, password="tiger")#(host='124.250.34.23', port=6379, db=4)
    tool = Probe(r)
    tool.refresh_abs()
