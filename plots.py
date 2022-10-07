import numpy as np
import pandas as pd

from matplotlib import pyplot as plt

def read_btc(nrows=100000):
    
    lobs = pd.read_csv('md/btcusdt:Binance:LinearPerpetual/lobs.csv', nrows=nrows)

    trades = pd.read_csv('md/btcusdt:Binance:LinearPerpetual/trades.csv', nrows=nrows)

    names = lobs.columns.values
    ln = len('btcusdt:Binance:LinearPerpetual_')
    renamer = { name:name[ln:] for name in names[2:]}
    renamer[' exchange_ts'] = 'exchange_ts'
    lobs.rename(renamer, axis=1, inplace=True)
    
    lobs["exchange_dt"] = pd.to_datetime(lobs.exchange_ts)
    trades["exchange_dt"] = pd.to_datetime(trades.exchange_ts)
    
    return lobs, trades


def get_cum_volume(df):
    N_steps = 10
    S = np.zeros((df.shape[0], ))
    for i in range(N_steps):
        S += df[f'vol_{i}'].values
        df[f"vol_cum_{i}"] = S
    return df


def get_grids(x_ask, y_ask, x_bid, y_bid):
    x_grid_ask = np.linspace(x_ask[0], x_ask[-1] + (x_ask[-1] - x_ask[0]) / 2)
    y_grid_ask = np.zeros_like(x_grid_ask)
    S = 0.0
    j = 0
    for i in range(len(x_grid_ask)):
        if j < len(x_ask) and x_grid_ask[i] >= x_ask[j]:
            S = y_ask[j]
            j += 1
        y_grid_ask[i] = S

    x_grid_bid = np.linspace(x_bid[-1] - (x_bid[0] - x_bid[-1]) / 2, x_bid[0])[::-1]
    y_grid_bid = np.zeros_like(x_grid_bid)
    S = 0.0
    j = 0
    for i in range(len(x_grid_bid)):
        if j < len(y_bid) and x_grid_bid[i] <= x_bid[j]:
            S = y_bid[j]
            j += 1
        y_grid_bid[i] = S
    return x_grid_bid, y_grid_bid, x_grid_ask, y_grid_ask

def main():
    lobs, trades = read_btc(1e4)
    ask_columns = list(filter(lambda x: x.startswith('ask'), lobs.columns.values))

    bid_columns = list(filter(lambda x: x.startswith('bid'), lobs.columns.values))

    dt_columns = list(set(lobs.columns) - set(ask_columns) - set(bid_columns))
    ask_lobs = lobs[dt_columns + ask_columns].rename({name : name[4:] for name in ask_columns}, axis=1)
    bid_lobs = lobs[dt_columns + bid_columns].rename({name : name[4:] for name in bid_columns}, axis=1)

    bid_lobs = get_cum_volume(bid_lobs)
    ask_lobs = get_cum_volume(ask_lobs)

    # enable interactive mode
    plt.ion()
    
    # creating subplot and figure
    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    for t in range(0, 100):
        x_ask = list(ask_lobs[[f"price_{i}" for i in range(10)]].iloc[t].values)
        y_ask = list(ask_lobs[[f"vol_cum_{i}" for i in range(10)]].iloc[t].values)
        
        x_bid = list(bid_lobs[[f"price_{i}" for i in range(10)]].iloc[t].values)
        y_bid = list(bid_lobs[[f"vol_cum_{i}" for i in range(10)]].iloc[t].values)
        
        x_grid_bid, y_grid_bid, x_grid_ask, y_grid_ask = get_grids(x_ask, y_ask, x_bid, y_bid)
        
        
        mid = 0.5 * (x_ask[0] + x_bid[0])

        fig.clear()



        plt.bar(x_grid_bid, y_grid_bid, width=0.2, color='r')
        plt.bar(x_grid_ask, y_grid_ask, width=0.2, color='b')
        plt.axvline(x=mid, c='k')
        plt.title(f"{t}")
        plt.grid()
        plt.xlim(19970, 19985)
        plt.ylim(0, 20)
        plt.pause(0.1)
    plt.show()

if __name__ == '__main__':
    main()