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
    place_ts : int # ts when we place the order
    exchange_ts : int # ts when exchange(simulator) get the order    
    order_id: int
    side: str
    size: float
    price: float

        
@dataclass
class CancelOrder:
    exchange_ts: int
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
    place_ts : int # ts when we call place_order method, for debugging
    exchange_ts: int
    receive_ts: int
    trade_id: int
    order_id: int
    side: str
    size: float
    price: float
    execute : str # BOOK or TRADE


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
        #current trade 
        self.trade_price = {}
        self.trade_price['BID'] = -np.inf
        self.trade_price['ASK'] = np.inf
        
    
    def get_md_queue_event_time(self) -> np.float:
        return np.inf if len(self.md_queue) == 0 else self.md_queue[0].exchange_ts
    
    
    def get_actions_queue_event_time(self) -> np.float:
        return np.inf if len(self.actions_queue) == 0 else self.actions_queue[0].exchange_ts
    
    
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
    

    def update_best_pos(self) -> None:
        if not self.md.orderbook is None:
            self.best_bid = self.md.orderbook.bids[0][0]
            self.best_ask = self.md.orderbook.asks[0][0]
    
    
    def update_last_trade(self) -> None:
        if not self.md.trade is None:
            self.trade_price[self.md.trade.side] = self.md.trade.price


    def delete_last_trade(self) -> None:
        self.trade_price['BID'] = -np.inf
        self.trade_price['ASK'] = np.inf


    def update_md(self, md:MdUpdate) -> None:        
        #current orderbook
        self.md = md 
        #update position
        self.update_best_pos()
        #update info about last trade
        self.update_last_trade()

        #add md to strategy_updates_queue
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
            #get event time for all the queues
            strategy_updates_queue_et = self.get_strategy_updates_queue_event_time()
            md_queue_et = self.get_md_queue_event_time()
            actions_queue_et = self.get_actions_queue_event_time()
            
            #if both queue are empty
            if md_queue_et == np.inf and actions_queue_et == np.inf:
                break

            #strategy queue has minimum event time
            if strategy_updates_queue_et < min(md_queue_et, actions_queue_et):
                break

            if md_queue_et <= actions_queue_et:
                self.update_md( self.md_queue.popleft() )
            if actions_queue_et <= md_queue_et:
                self.update_action( self.actions_queue.popleft() )

            #execute orders with current orderbook
            self.execute_orders()
            #deleting last trade
            self.delete_last_trade()
        #end of simulation
        if len(self.strategy_updates_queue) == 0:
            return np.inf, None
        key = self.strategy_updates_queue.keys()[0]
        res = self.strategy_updates_queue.pop(key)
        return key, res


    def execute_orders(self) -> None:
        executed_orders_id = []
        for order_id, order in self.ready_to_execute_orders.items():

            executed_price, execute = None, None

            if order.side == 'BID' and order.price >= self.best_ask:
                    executed_price = self.best_ask
                    execute = 'BOOK'    
            elif order.side == 'ASK' and order.price <= self.best_bid:
                    executed_price = self.best_bid
                    execute = 'BOOK'
            elif order.side == 'BID' and order.price >= self.trade_price['ASK']:
                    executed_price = order.price
                    execute = 'TRADE'    
            elif order.side == 'ASK' and order.price <= self.trade_price['BID']:
                    executed_price = order.price
                    execute = 'TRADE'

            if not executed_price is None:
                executed_order = OwnTrade(
                    order.place_ts, # when we place the order
                    self.md.exchange_ts, #exchange ts
                    self.md.exchange_ts + self.md_latency, #receive ts
                    self.get_trade_id(), #trade id
                    order_id, order.side, order.size, executed_price, execute)
        
                executed_orders_id.append(order_id)

                #added order to strategy update queue
                #there is no defaultsorteddict so i have to do this
                if not executed_order.receive_ts in self.strategy_updates_queue:
                    self.strategy_updates_queue[ executed_order.receive_ts ] = []
                self.strategy_updates_queue[ executed_order.receive_ts ].append(executed_order)
        
        #deleting executed orders
        for k in executed_orders_id:
            self.ready_to_execute_orders.pop(k)


    def place_order(self, ts:float, size:float, side:str, price:float) -> Order:
        #добавляем заявку в список всех заявок
        #ts равен времени, когда заявка придет на биржу
        order = Order(ts, ts + self.latency, self.get_order_id(), side, size, price)
        self.actions_queue.append(order)
        return order

    
    def cancel_order(self, ts:float, id_to_delete:int) -> CancelOrder:
        #добавляем заявку на удаление
        ts += self.latency
        delete_order = CancelOrder(ts, id_to_delete)
        self.actions_queue.append(delete_order)
        return delete_order