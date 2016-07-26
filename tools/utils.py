# -*- coding: utf-8 -*-
import json
import re
from datetime import datetime
import plotly
import plotly.graph_objs as go
from datetime import timedelta
import redis

# plotly.sign_in(username='dzy', api_key='4dkuzf1c70')
class SymbolUtils:
    def __init__(self, redis_client, construct_kline_map=True):
        self.client = redis_client
        # 000001.SH
        # {date->kline}
        #self.index_map = self.get_sh_index_kmap()
        # all symbols kline map
        # {symbol -> {date -> kline}}
        if construct_kline_map:
            self.construct_kline_map()

    def get_symbols(self):
        symbols = self.client.smembers("symbols")
        ret = []
        for s in symbols:
            val = s.decode("utf-8")
            tmp = val.split(":")
            if len(tmp) == 3 and re.match(r"60\d+$|00\d+$|30\d+$", tmp[0]):
                ret.append(tmp[0])
        return ret

    def get_sh_index_kmap(self):
        symbol = "000001.SH"
        key = "chart:day:" + symbol
        raws = self.client.hgetall(key)
        ret = {}
        for day in raws.keys():
            line = raws[day].decode("utf-8")
            bar = self.parse_day_bar(line)
            if bar:
                ret[day.decode("utf-8")] = bar
        return ret

    def get_sh_index_klines(self):
        symbol = "000001.SH"
        key = "chart:day:" + symbol
        raws = self.client.hgetall(key)
        ret = []
        for day in sorted(raws.keys()):
            line = raws[day].decode("utf-8")
            bar = self.parse_day_bar(line)
            if bar:
                ret.append(bar)
        return ret

    def construct_kline_map(self):
        symbols = self.get_symbols()
        self.kmap = {}
        for symbol in symbols:
            key = "chart:brday:" + symbol
            raws = self.client.hgetall(key)
            self.kmap[symbol] = {}
            for day in raws.keys():
                line = raws[day].decode("utf-8")
                bar = self.parse_day_bar(line)
                self.kmap[symbol][day.decode("utf-8")] = bar

    def get_index_change(self, date):
        if not self.index_map or len(self.index_map) == 0:
            self.index_map = self.get_sh_index_kmap()
        pre_dt = datetime.strptime(date, "%Y%m%d")
        pre_kline = None
        count = 0
        while not pre_kline:
            count += 1
            if count > 30:
                return False
            pre_dt = pre_dt - timedelta(1)
            pre_date = pre_dt.strftime("%Y%m%d")
            if pre_date in self.index_map.keys():
                pre_kline = self.index_map[pre_date]
        if date not in self.index_map:
            return -1
        kline = self.index_map[date]
        change = (kline[4] - pre_kline[4]) / pre_kline[4]
        return change

    def is_index_support(self, date):
        if int(date) < int("20000101"):
            return False
        if not self.index_map or len(self.index_map) == 0:
            self.index_map = self.get_sh_index_kmap()
        pre_dt = datetime.strptime(date, "%Y%m%d")
        pre_kline = None
        count = 0
        while not pre_kline:
            count += 1
            if count > 30:
                return False
            pre_dt = pre_dt - timedelta(1)
            pre_date = pre_dt.strftime("%Y%m%d")
            if pre_date in self.index_map.keys():
                pre_kline = self.index_map[pre_date]
        kline = self.index_map[date]
        change = (kline[4] - pre_kline[4]) / pre_kline[4]
        if change > -0.01:
            return True
        return False

    # "circulatedValue" 流通市值
    def get_meta(self):
        meta = self.client.hgetall("citic:meta")
        ret = {}
        for symbol in meta.keys():
            val = meta[symbol].decode("utf-8")
            ret[symbol.decode("utf-8")] = json.loads(val)
        return ret

    @staticmethod
    def dirty_area(symbol, klines, begin_date, end_date):
        pre_close = -1
        for bar in klines:
            if bar[0] == begin_date:
                pre_close = bar[4]
            if pre_close > 0:
                change = (bar[4] - pre_close) / pre_close
                if change > 0.11 or change < -0.11:
                    return True
                pre_close = bar[4]
            if bar[0] == end_date:
                return False
        return False

    def get_all_closes(self, date):
        key = "chart:close:" + date
        hmap = self.client.hgetall(key)
        ret = {}
        for k in hmap.keys():
            ret[k.decode("utf-8")] = float(hmap[k].decode("utf-8"))
        return ret

    @staticmethod
    def max_vol(klines):
        vol = 0
        for line in klines:
            if vol < line[5]:
                vol = line[5]
        return vol

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
    def max_price(klines):
        index = 0
        price = 0
        for i in range(0, len(klines)):
            if klines[i][2] > price:
                price = klines[i][2]
                index = i
        return index, price

    # 取前复权日K
    # return klines list sorted by date
    def get_bday_kline(self, symbol):
        key = "chart:brday:" + symbol
        raws = self.client.hgetall(key)
        ret = []
        # 按日期排序
        for day in sorted(raws.keys()):
            line = raws[day].decode("utf-8")
            bar = self.parse_day_bar(line)
            if bar:
                ret.append(bar)
        return ret

    def get_klines_with_last_date(self, symbol, last_day):
        key = "chart:brday:" + symbol
        raws = self.client.hgetall(key)
        ret = []
        # 按日期排序
        for day in sorted(raws.keys()):
            line = raws[day].decode("utf-8")
            bar = self.parse_day_bar(line)
            if bar and bar[0] < last_day:
                ret.append(bar)
        return ret

    # 返回当日k线，如果没有返回上一个交易日
    def get_pre_kline(self, symbol, dt):
        line = self.client.hget("chart:brday:" + symbol, dt.strftime("%Y%m%d"))
        yest = dt
        count = 0
        while not line:
            count += 1
            if count > 30:
                return None
            yest = yest - timedelta(1)
            line = self.client.hget("chart:brday:" + symbol, yest.strftime("%Y%m%d"))
        return self.parse_day_bar(line)

    # 返回当日k线，如果没有返回后一个交易日
    def get_after_kline(self, symbol, dt):
        line = self.client.hget("chart:brday:" + symbol, dt.strftime("%Y%m%d"))
        yest = dt
        count = 0
        while not line:
            count += 1
            if count > 30:
                return None
            yest = yest + timedelta(1)
            line = self.client.hget("chart:brday:" + symbol, yest.strftime("%Y%m%d"))
        return self.parse_day_bar(line)

    def get_after_klines(self, symbol, dt, days):
        ret = []
        now = datetime.now()
        while days > 0 and dt < now:
            line = self.client.hget("chart:brday:" + symbol, dt.strftime("%Y%m%d"))
            if not line:
                dt = dt + timedelta(1)
                continue
            ret.append(self.parse_day_bar(line))
            days -= 1
            dt = dt + timedelta(1)
        return ret

    # 日期0 开盘1 最高2 最低3 收盘4 成交量5
    @staticmethod
    def parse_day_bar(line):
        if not line:
            return None
        if not isinstance(line, str):
            line = line.decode("utf-8")
        parts = line.split(" ")
        if len(parts) != 6:
            return None
        open = float(parts[1])
        high = float(parts[2])
        low = float(parts[3])
        close = float(parts[4])
        vol = int(parts[5])
        return parts[0], open, high, low, close, vol

    def visulize(self, xdata, ydata, file_name='xy'):
        data = [go.Scatter(
                x=xdata,
                y=ydata)
                ]
        plotly.offline.plot(data, filename=file_name)

    def visulize_yy(self, xd, yd1, yd2, file_name="xyy"):
        trace1 = go.Scatter(
            x=xd,
            y=yd1,
            yaxis='y1'
        )
        trace2 = go.Scatter(
            x=xd,
            y=yd2,
            yaxis='y2'
        )
        data = [trace1, trace2]
        layout = go.Layout(
            title='',
            yaxis=dict(
                title='yaxis title'
            ),
            yaxis2=dict(
                title='yaxis2 title',
                titlefont=dict(
                    color='rgb(148, 103, 189)'
                ),
                tickfont=dict(
                    color='rgb(148, 103, 189)'
                ),
                overlaying='y',
                side='right'
            )
        )
        fig = go.Figure(data=data, layout=layout)
        plotly.offline.plot(fig, filename=file_name)

    # since format = "%Y%m%d"
    def get_all_trading_days(self, since):
        symbol = "000001.SH"
        key = "chart:day:" + symbol
        raws = self.client.hgetall(key)
        ret = [since]
        for day in sorted(raws.keys()):
            day = day.decode("utf-8")
            if day > since and day > ret[-1]:
                ret.append(day)
        return ret

    # portfolio = { symbol -> {"open_date"->, "open_price"->, "quantity":->} }
    # trades = [(symbol, buy_price, sell_price, buy_date, sell_date)]
    def net_value_trends(self, trades):
        trade_map = {}
        for trade in trades:
            buy_date = trade[3]
            if buy_date not in trade_map.keys():
                trade_map[buy_date] = []
            trade_map[buy_date].append(trade)
        cash = 1.0
        portfolio = {}
        net_value_arr = []
        x = []
        trading_days = self.get_all_trading_days("20000101")
        for d in trading_days:
            for symbol in portfolio.keys():
                position = portfolio[symbol]
                if d not in self.kmap[symbol]:
                    continue
                bar = self.kmap[symbol][d]
                if not bar:
                    continue
                position["market_price"] = bar[4]
            x.append(datetime.strptime(d, "%Y%m%d"))
            cash += self.close_position(portfolio, d)
            if d in trade_map.keys():
                if cash > 0.05:
                    cash = self.filled(cash, portfolio, trade_map[d])
                else:
                    # print("cash not enough, close max profit position.")
                    for i in range(0, int((len(portfolio)+1)/2)):
                        cash += self.close_one_position(portfolio)
                    cash = self.filled(cash, portfolio, trade_map[d])
            nlv = self.compute_net_value(cash, portfolio)
            net_value_arr.append(nlv)
            # print("%s : %0.4f" % (d, len(portfolio)))
        self.visulize(x, net_value_arr, "nlv")


    def compute_net_value(self, cash, portfolio):
        if not portfolio:
            return cash
        for symbol in portfolio:
            p = portfolio[symbol]
            cash += p["quantity"] * p["market_price"]
        return cash

    # 平最大盈利持仓
    def close_one_position(self, portfolio):
        max_change = -1.0
        max_position = None
        for symbol in portfolio.keys():
            position = portfolio[symbol]
            change = (position["market_price"] - position["open_price"])/position["open_price"]
            if change > max_change:
                max_change = change
                max_position = position
        if not max_position:
            return 0.0
        else:
            portfolio.pop(max_position["symbol"])
            market_value = max_position["quantity"] * max_position["market_price"]
            return market_value - SymbolUtils.commission(market_value)

    # 根据平仓日期平仓
    @staticmethod
    def close_position(portfolio, day):
        cash = 0.0
        if not portfolio:
            return cash
        closed_symbols = []
        for symbol, position in portfolio.items():
            if position["close_date"] < day:
                print("close_position error: current date = %s, close date= %s" % (day, position["close_date"]))
            if position["close_date"] == day:
                market_value = position["quantity"] * position["close_price"]
                cash = cash + market_value - SymbolUtils.commission(market_value)
                closed_symbols.append(symbol)
        for symbol in closed_symbols:
            portfolio.pop(symbol)
        return cash

    @staticmethod
    def commission(market_value):
        return market_value * 0.0013

    def filled(self, cash, portfolio, trades):
        if len(trades) == 0:
            return cash
        for trade in trades:
            quantity = (cash / len(trades)) / trade[1]
            position = {}
            symbol = trade[0]
            position["quantity"] = quantity
            position["open_price"] = trade[1]
            position["close_price"] = trade[2]
            position["open_date"] = trade[3]
            position["close_date"] = trade[4]
            position["symbol"] = symbol
            position["market_price"] = trade[1]
            if symbol in portfolio.keys():
                print("openOrder error. %s %s %s %0.4f" % (symbol, portfolio[symbol]["close_date"], trade[3],
                                                           portfolio[symbol]["quantity"]))
                portfolio[symbol]["quantity"] += quantity
            else:
                portfolio[symbol] = position
        return 0.0

    # m = "1min" or "5min"
    # 取当日分时线
    def get_minute_quote(self, m, symbol):
        return self.get_minute_quote(m, datetime.now(), symbol)

    def get_minute_quote(self, m, dt, symbol):
        key = ":".join(["chart", m, symbol, dt.strftime("%Y%m%d")])
        quote_dict = self.client.hgetall(key)
        ret = []
        for k in sorted(quote_dict.keys()):
            bar = self.parse_minute_bar(quote_dict[k])
            if bar:
                ret.append(bar)
        return ret

    # 分钟0 分钟收盘价1 分钟成交量2 均价3 总成交量4
    @staticmethod
    def parse_minute_bar(line):
        if line:
            parts = line.decode("utf-8").split(' ')
            if len(parts) == 5:
                return parts[0], float(parts[1]), int(parts[2]), float(parts[3]), int(parts[4])
        return None

if __name__ == "__main__":
    r = redis.StrictRedis(host='127.0.0.1', port=6379, db=4)
    su = SymbolUtils(r)
