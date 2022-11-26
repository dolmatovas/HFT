from collections import deque
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union, Deque, Dict

import numpy as np
from sortedcontainers import SortedDict


@dataclass
class Order:  # Our own placed order
    place_ts : float # ts when we place the order
    exchange_ts : float # ts when exchange(simulator) get the order    
    order_id: int
    side: str
    size: float
    price: float

        
@dataclass
class CancelOrder:
    exchange_ts: float
    id_to_delete : int

@dataclass
class AnonTrade:  # Market trade
    exchange_ts : float
    receive_ts : float
    side: str
    size: float
    price: float


@dataclass
class OwnTrade:  # Execution of own placed order
    place_ts : float # ts when we call place_order method, for debugging
    exchange_ts: float
    receive_ts: float
    trade_id: int
    order_id: int
    side: str
    size: float
    price: float
    execute : str # BOOK or TRADE


    def __post_init__(self):
        assert isinstance(self.side, str)


@dataclass
class OrderbookSnapshotUpdate:  # Orderbook tick snapshot
    exchange_ts : float
    receive_ts : float
    asks: List[Tuple[float, float]]  # tuple[price, size]
    bids: List[Tuple[float, float]]


@dataclass
class MdUpdate:  # Data of a tick
    exchange_ts : float
    receive_ts : float
    orderbook: Optional[OrderbookSnapshotUpdate] = None
    trade: Optional[AnonTrade] = None



def update_best_positions(best_bid:float, best_ask:float, md:MdUpdate) -> Tuple[float, float]:
    if not md.orderbook is None:
        best_bid = md.orderbook.bids[0][0]
        best_ask = md.orderbook.asks[0][0]
    elif not md.trade is None:
        if md.trade.side == 'BID':
            best_ask = max(md.trade.price, best_ask)
        elif md.trade.side == 'ASK':
            best_bid = min(best_bid, md.trade.price)
        else:
            assert False, "WRONG TRADE SIDE"
    assert best_ask > best_bid, "wrong best positions"
    return best_bid, best_ask


def get_mid_price(mid_price:float, md:MdUpdate):
    book = md.orderbook
    if book is None:
        return mid_price

    price = 0.0
    pos   = 0.0

    for i in range( len(book.asks) ):
        price += book.asks[i][0] * book.asks[i][1]
        pos   += book.asks[i][1]
        price += book.bids[i][0] * book.bids[i][1]
        pos   += book.bids[i][1]
    price /= pos
    return price


class PriorQueue:
    def __init__(self, default_key=np.inf, default_val = None):
        self._queue = SortedDict()
        self._min_key = np.inf

    
    def push(self, key, val):
        if key not in self._queue:
            self._queue[key] = []
        self._queue[key].append(val)
        self._min_key = min(self._min_key, key)

    
    def pop(self):
        if len(self._queue) == 0:
            return np.inf, None
        res = self._queue.popitem(0)
        self._min_key = np.inf
        if len(self._queue):
            self._min_key = self._queue.peekitem(0)[0]
        return res
    

    def min_key(self):
        return self._min_key