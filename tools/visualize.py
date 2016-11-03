# -*- coding: utf-8 -*-
from datetime import datetime
import plotly
import plotly.graph_objs as go
import redis


class Visualize:
    def __init__(self):
        self.local_redis = redis.StrictRedis(host='127.0.0.1', port=6379, db=1)

    @staticmethod
    def get_ab_stat_key(symbol, date):
        return "ab:" + date + ":" + symbol

    def get_ab_stat(self, symbol, date):
        k = self.get_ab_stat_key(symbol, date)
        rows = self.local_redis.lrange(k, 0, -1)
        ret = []
        for row in rows:
            parts = row.decode("utf-8").split(" ")
            hms = parts[0]
            dt = datetime.strptime(date + " " + hms, "%Y%m%d %H:%M:%S")
            t = datetime.strftime(dt, "%Y-%m-%d %H:%M:%S")
            bid_size = int(parts[1])
            vol = int(parts[2])
            ret.append({"time": t, "bidSize": bid_size, "vol": vol})
        return ret

    @staticmethod
    def visualize_ab_stat(stat, symbol):
        x = []
        bid = []
        vol = []
        for s in stat:
            x.append(s["time"])
            bid.append(s["bidSize"])
            vol.append(s["vol"])
        data = [go.Scatter(x=x, y=bid, yaxis='bid', name='BidSize'),
                go.Scatter(x=x, y=vol, yaxis='vol', name='Vol')]
        layout = go.Layout(
            xaxis=dict(
                autorange=True,
                showgrid=True,
                zeroline=True,
                showline=True,
                mirror='ticks',
                gridcolor='#bdbdbd',
                gridwidth=2,
                zerolinecolor='#969696',
                zerolinewidth=4,
                linecolor='#636363',
                linewidth=6,
            ),
            yaxis=dict(
                autorange=True,
                showgrid=True,
                zeroline=True,
                showline=True,
                mirror='ticks',
                gridcolor='#bdbdbd',
                gridwidth=2,
                zerolinecolor='#969696',
                zerolinewidth=4,
                linecolor='#636363',
                linewidth=6,
            )
        )
        file_name = "../img/" + symbol + ".html"
        fig = go.Figure(data=data, layout=layout)
        plotly.offline.plot(fig, filename=file_name)

if __name__ == "__main__":
    tool = Visualize()
    date = "20161031"
    symbol = "002623"
    tool.visualize_ab_stat(tool.get_ab_stat(symbol, date), symbol)