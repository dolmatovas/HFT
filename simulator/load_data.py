import pandas as pd

from typing import List

from simulator import AnonTrade, OrderbookSnapshotUpdate, MdUpdate


def load_trades(path, nrows=10000) -> List[AnonTrade]:
    trades = pd.read_csv(path + 'trades.csv', nrows=nrows)
    
    #переставляю колонки, чтобы удобнее подавать их в конструктор AnonTrade
    trades = trades[ ['exchange_ts', 'receive_ts', 'aggro_side', 'size', 'price' ] ].sort_values(["exchange_ts", 'receive_ts'])
    receive_ts = trades.receive_ts.values
    exchange_ts = trades.exchange_ts.values 
    trades = [ AnonTrade(*args) for args in trades.values]
    md = [ MdUpdate(  exchange_ts[i], receive_ts[i], None, trades[i]) for i in range(len(trades)) ]
    return md


def load_books(path, nrows=10000) -> List[OrderbookSnapshotUpdate]:
    lobs   = pd.read_csv(path + 'lobs.csv', nrows=nrows)
    
    #rename columns
    names = lobs.columns.values
    ln = len('btcusdt:Binance:LinearPerpetual_')
    renamer = { name:name[ln:] for name in names[2:]}
    renamer[' exchange_ts'] = 'exchange_ts'
    lobs.rename(renamer, axis=1, inplace=True)
    
    #timestamps
    receive_ts = lobs.receive_ts.values
    exchange_ts = lobs.exchange_ts.values 
    #список ask_price, ask_vol для разных уровней стакана
    #размеры: len(asks) = 10, len(asks[0]) = len(lobs)
    asks = [list(zip(lobs[f"ask_price_{i}"],lobs[f"ask_vol_{i}"])) for i in range(10)]
    #транспонируем список
    asks = [ [asks[i][j] for i in range(len(asks))] for j in range(len(asks[0]))]
    #тоже самое с бидами
    bids = [list(zip(lobs[f"bid_price_{i}"],lobs[f"bid_vol_{i}"])) for i in range(10)]
    bids = [ [bids[i][j] for i in range(len(bids))] for j in range(len(bids[0]))]
    
    book = list( OrderbookSnapshotUpdate(*args) for args in zip(exchange_ts, receive_ts, asks, bids) )
    md = [ MdUpdate(exchange_ts[i], receive_ts[i], book[i], None) for i in range(len(book)) ]
    return md


def load_md_from_file(path: str, nrows=10000) -> List[MdUpdate]:
    md_trades = load_trades(path, nrows)
    md_books  = load_books(path, nrows)
    md = sorted( md_trades + md_books, key=lambda x: x.receive_ts )
    return md