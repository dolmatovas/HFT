from collections import deque
from sortedcontainers import SortedDict

from dataclasses import dataclass
from typing import Tuple, List, Optional, Union, Set

import numpy as np

from simulator import Sim, Order, CancelOrder, AnonTrade, OwnTrade, OrderbookSnapshotUpdate, MdUpdate


class PriorHeap():
    def __init__(self, *args, **kwargs):
        self._map = {}
        self._heap = SortedDict()


    def __str__(self):
        return "map: " + str(self._map) + "\nheap: " + str(self._heap)


    def push(self, order_id : int, order_price : float):
        #add order to map
        self._map[order_id] = order_price
        
        if order_price not in self._heap.keys():
            self._heap[order_price] = set()
        self._heap[order_price].add(order_id)


    def erase(self, order_id):
        if not order_id in self._map:
            return False
        order_price = self._map.pop(order_id)
        self._heap[order_price].remove(order_id)
        if len( self._heap[order_price] ) == 0:
            self._heap.pop( order_price )
        return True


    def greater_or_eq(self, price:float) -> Set[int]:
        ind = self._heap.bisect_left(price)
        res = []
        for _price in self._heap.keys()[ind:]:
            res += self._heap[_price]
        return res

    
    def less_or_eq(self, price:float) -> Set[int]:
        ind = self._heap.bisect_right(price)
        res = []
        for _price in self._heap.keys()[:ind]:
            res += self._heap[_price]
        return res


class SimOptim(Sim):
    def __init__(self, *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)   

        self.bid_orders = PriorHeap()
        self.ask_orders = PriorHeap()
        
    
    def update_action(self, action:Union[Order, CancelOrder]) -> None:
        
        if isinstance(action, Order):
            #self.ready_to_execute_orders[action.order_id] = action
            #save last order to try to execute it aggressively
            self.last_order = action
        elif isinstance(action, CancelOrder):    
            #cancel order
            if action.id_to_delete in self.ready_to_execute_orders:
                self.ready_to_execute_orders.pop(action.id_to_delete)
                self.ask_orders.erase(action.id_to_delete)
                self.bid_orders.erase(action.id_to_delete)
        else:
            assert False, "Wrong action type!"


    def execute_last_order(self) -> None:
        '''
            this function tries to execute self.last order aggressively
        '''
        #nothing to execute
        if self.last_order is None:
            return

        executed_price, execute = None, None
        #
        if self.last_order.side == 'BID' and self.last_order.price >= self.best_ask:
                executed_price = self.best_ask
                execute = 'BOOK'
        #    
        elif self.last_order.side == 'ASK' and self.last_order.price <= self.best_bid:
                executed_price = self.best_bid
                execute = 'BOOK'

        if not executed_price is None:
            executed_order = OwnTrade(
                self.last_order.place_ts, # when we place the order
                self.md.exchange_ts, #exchange ts
                self.md.exchange_ts + self.md_latency, #receive ts
                self.get_trade_id(), #trade id
                self.last_order.order_id, 
                self.last_order.side, 
                self.last_order.size, 
                executed_price, execute)
            #add order to strategy update queue
            #there is no defaultsorteddict so I have to do this
            self.strategy_updates_queue.push(executed_order.receive_ts, executed_order)
        else:
            self.ready_to_execute_orders[self.last_order.order_id] = self.last_order
            if self.last_order.side == 'BID':
                self.bid_orders.push(self.last_order.order_id, self.last_order.price)
            elif self.last_order.side == 'ASK':
                self.ask_orders.push(self.last_order.order_id, self.last_order.price)
        self.last_order = None
        

    def execute_orders(self) -> None:
        
        executed_orders_id = list()
        #order book bid order execution
        executed_orders_id += ( self.bid_orders.greater_or_eq(self.best_ask) )
        executed_orders_id += ( self.ask_orders.less_or_eq(self.best_bid) )

        n = len(executed_orders_id)

        executed_orders_id += ( self.bid_orders.greater_or_eq(self.trade_price['ASK']) ) 
        executed_orders_id += ( self.ask_orders.less_or_eq(self.trade_price['BID']) )

        for i, order_id in enumerate(executed_orders_id):
            order = self.ready_to_execute_orders.pop(order_id)
            
            self.ask_orders.erase(order_id)
            self.bid_orders.erase(order_id)

            execute = "BOOK" if i < n else "TRADE"
            executed_order = OwnTrade(
                order.place_ts, # when we place the order
                self.md.exchange_ts, #exchange ts
                self.md.exchange_ts + self.md_latency, #receive ts
                self.get_trade_id(), #trade id
                order_id, order.side, order.size, order.price, execute)

            self.strategy_updates_queue.push(executed_order.receive_ts, executed_order)