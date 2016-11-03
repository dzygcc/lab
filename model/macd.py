# -*- coding: utf-8 -*-
from model.base import Base
from tools.tech import Tech


class Macd(Base):
    def strategy(self, symbol):
        klines = self.utils.get_bday_kline(symbol)
        input_frame = Tech.convert_klines(klines)
        df = Tech.macd(input_frame, 12, 26)
        trades = []
        i = 1
        num = len(df.index)
        buy_date, sell_date, buy_price, sell_price = None, None, None, None
        pre_close = None
        while i < num:
            date = df["date"].iloc[i]
            close = df["close"].iloc[i]
            can_buy = self.can_buy(close, pre_close)
            buy_sig = self.buy_signal(df, i)
            if buy_sig:
                while not can_buy and i < num - 1:
                    print("can't buy %s" % date)
                    i += 1
                    pre_close = close
                    date = df["date"].iloc[i]
                    close = df["close"].iloc[i]
                    can_buy = self.can_buy(close, pre_close)
                if df["macd"].iloc[i] >= df["signal"].iloc[i]:
                    buy_date, buy_price = date, close
                    # print("buy on %s price=%0.3f" % (date, close))
            if self.sell_sinal(df, i):
                if buy_price and buy_date and date > buy_date:
                    sell_date, sell_price = date, close
                    # print("sell on %s price=%0.3f" % (date, close))
                    trades.append((symbol, buy_price, sell_price, buy_date, sell_date))
                    buy_date, buy_price = None, None
            i += 1
            pre_close = close
        return trades

    def name(self):
        return "macd"

    def buy_signal(self, df, i):
        if df["macd"].iloc[i] >= df["signal"].iloc[i] \
                and df["macd"].iloc[i - 1] <= df["signal"].iloc[i - 1]:
            return True
        return False

    def sell_sinal(self, df, i):
        if df["macd"].iloc[i] <= df["signal"].iloc[i] and \
                        df["macd"].iloc[i-1] >= df["signal"].iloc[i-1]:
            return True
        return False


    def trade(self):
        symbols = self.symbols()
        self.utils.set_symbols(symbols)
        trades = []
        for i in range(0, len(symbols)):
            trades.extend(self.strategy(symbols[i]))
        print("trade num = %d" % (len(trades)))
        x, y = self.utils.net_value_trends(trades)
        self.utils.visulize(x, y, self.name())

    def compare(self):
        symbols = self.symbols()
        count = 0
        m = -1
        for i in range(0, len(symbols)):
            if symbols[i] == '600030':
                continue
            trades = self.strategy("600030")
            trades.extend(self.strategy(symbols[i]))
            x1, y1 = self.utils.net_value_trends(trades)     # with index warn
            if y1[-1] > m:
                count += 1
                m = y1[-1]
                print("%s : %0.3f", symbols[i], m)


if __name__ == "__main__":
    test = Macd()
    #test.compare()
    test.trade()