from collections import deque
from sortedcontainers import SortedDict

from dataclasses import dataclass
from typing import Tuple, List, Optional, Union

import numpy as np



def Min(iterable):
    res = np.inf
    for x in iterable:
        res = min(x, res)
    return res


def Max(iterable):
    res = -np.inf
    for x in iterable:
        res = max(x, res)
    return res


@dataclass
class Order:  # Our own placed order
    timestamp: int    
    order_id: int
    side: str
    size: float
    price: float

        
@dataclass
class CancelOrder:
    timestamp: int
    id_to_delete : int

@dataclass
class AnonTrade:  # Market trade
    exchange_ts : int
    receive_ts : int
    side: str
    size: float
    price: float


@dataclass
class OwnTrade:  # Execution of own placed order
    exchange_ts: int
    receive_ts: int
    trade_id: int
    order_id: int
    side: str
    size: float
    price: float


@dataclass
class OrderbookSnapshotUpdate:  # Orderbook tick snapshot
    exchange_ts : int
    receive_ts : int
    asks: List[Tuple[float, float]]  # tuple[price, size]
    bids: List[Tuple[float, float]]


@dataclass
class MdUpdate:  # Data of a tick
    exchange_ts : int
    receive_ts : int
    orderbook: Optional[OrderbookSnapshotUpdate] = None
    trade: Optional[AnonTrade] = None


def update_best_positions(best_bid:float, best_ask:float, md:MdUpdate) -> Tuple[float, float]:
    if not md.orderbook is None:
        best_bid = md.orderbook.bids[0][0]
        best_ask = md.orderbook.asks[0][0]
    elif not md.trade is None:
        if md.trade.side == 'BID':
            best_ask = md.trade.price
        else:
            best_bid = md.trade.price
    return best_bid, best_ask


class Sim:
    def __init__(self, market_data: List[MdUpdate], execution_latency: float, md_latency: float) -> None:
        """
            Args:
                execution_latency(float): latency in nanoseconds
                md_latency(float): latency in nanoseconds
                nrows: number of rows to read
        """
        #nrows -- сколько строк маркет даты скачивать   
        self.md_queue = deque( market_data )
        
        self.actions_queue = deque()
        #SordetDict: receive_ts -> [updates]
        self.strategy_updates_queue = SortedDict()
        
        #map : order_id -> Order
        self.ready_to_execute_orders = {}
        
        #current md
        self.md = None
        #current ids
        self.order_id = 0
        self.trade_id = 0
        #latency
        self.latency = execution_latency
        self.md_latency = md_latency
        #current bid and ask
        self.best_bid = -np.inf
        self.best_ask = np.inf
        
    
    def get_md_queue_event_time(self) -> np.float:
        return np.inf if len(self.md_queue) == 0 else self.md_queue[0].exchange_ts
    
    
    def get_actions_queue_event_time(self) -> np.float:
        return np.inf if len(self.actions_queue) == 0 else self.actions_queue[0].timestamp
    
    
    def get_strategy_updates_queue_event_time(self) -> np.float:
        return np.inf if len(self.strategy_updates_queue) == 0 else self.strategy_updates_queue.keys()[0]
    
    
    def get_order_id(self) -> int:
        res = self.order_id
        self.order_id += 1
        return res
    
    
    def get_trade_id(self) -> int:
        res = self.trade_id
        self.trade_id += 1
        return res
    
    
    def place_order(self, ts:float, size:float, side:str, price:float) -> Order:
        #добавляем заявку в список всех заявок
        #ts равен времени, когда заявка придет на биржу
        ts += self.latency
        order = Order(ts, self.get_order_id(), side, size, price)
        self.actions_queue.append(order)
        return order

    
    def cancel_order(self, ts:float, id_to_delete:int) -> CancelOrder:
        #добавляем заявку на удаление
        ts += self.latency
        delete_order = CancelOrder(ts, id_to_delete)
        self.actions_queue.append(delete_order)
        return delete_order
    
    
    def update_md(self, md:MdUpdate) -> None:        
        #current md
        self.md = md
        self.best_bid, self.best_ask = update_best_positions(self.best_bid, self.best_ask, md) 
        
        if not md.receive_ts in self.strategy_updates_queue.keys():
            self.strategy_updates_queue[md.receive_ts] = []
        self.strategy_updates_queue[md.receive_ts].append(md)
        
    
    def update_action(self, action:Union[Order, CancelOrder]) -> None:
        if isinstance(action, Order):
            self.ready_to_execute_orders[action.order_id] = action
        elif isinstance(action, CancelOrder):
            #удаляем ордер из списка отстоявшихся стратегий
            if action.id_to_delete in self.ready_to_execute_orders:
                self.ready_to_execute_orders.pop(action.id_to_delete)
        else:
            assert False, "Wrong action type!"

        
    def tick(self) -> Tuple[ float, List[ Union[OwnTrade, MdUpdate] ] ]:
        while True:
            strategy_updates_queue_et = self.get_strategy_updates_queue_event_time()
            md_queue_et = self.get_md_queue_event_time()
            actions_queue_et = self.get_actions_queue_event_time()
            
            #both queue are empty
            if md_queue_et == np.inf and actions_queue_et == np.inf:
                break
            #strategy queue has minimum event time
            if min(md_queue_et, actions_queue_et) >= strategy_updates_queue_et:
                break
            #md queue has minimum event time
            if md_queue_et < actions_queue_et:
                self.update_md( self.md_queue.popleft() )
            else:
                self.update_action( self.actions_queue.popleft() )
            
            self.execute_orders()
        #end of simulation
        if len(self.strategy_updates_queue) == 0:
            return np.inf, None
        key = self.strategy_updates_queue.keys()[0]
        res = self.strategy_updates_queue.pop(key)
        return key, res


    def execute_orders(self) -> None:
        executed_orders_id = []
        for order_id, order in self.ready_to_execute_orders.items():
            executed_order = None
            if order.side == 'BID' and order.price >= self.best_ask:
                executed_order = OwnTrade(
                    self.md.exchange_ts, 
                    self.md.exchange_ts + self.md_latency,
                    self.get_trade_id(),
                    order_id,
                    order.side,
                    order.size,
                    self.best_ask,
                )
            if order.side == 'ASK' and order.price <= self.best_bid:
                executed_order = OwnTrade(
                    self.md.exchange_ts, 
                    self.md.exchange_ts + self.md_latency,
                    self.get_trade_id(),
                    order_id,
                    order.side,
                    order.size,
                    self.best_bid,
                )
            if not executed_order is None:
                executed_orders_id.append(order_id)
                if not executed_order.receive_ts in self.strategy_updates_queue:
                    self.strategy_updates_queue[ executed_order.receive_ts ] = []
                self.strategy_updates_queue[ executed_order.receive_ts ].append(executed_order)
        for k in executed_orders_id:
            self.ready_to_execute_orders.pop(k)
        pass