from typing import List, Optional, Tuple, Union, Dict, Deque

from collections import deque, defaultdict
import numpy as np
import pandas as pd

from simulator import MdUpdate, Order, OwnTrade, Sim, update_best_positions


class BaseStoikovStrategy:
    '''
        This strategy places ask and bid order every `delay` nanoseconds.
        If the order has not been executed within `hold_time` nanoseconds, it is canceled.
    '''
    def __init__(self, delay: float, min_pos:float, gamma: float, k:float, T:float) -> None:
        '''
            Args:
                delay(float): delay between orders in nanoseconds

                gamma(float): gamma
                k(float): k
                T(float): time window for variance calculation
        '''
        self.delay = delay
        
        self.min_pos = min_pos

        #expiration time in nanoseconds
        self.T = T
        self.gamma = gamma
        self.k = k


    def _warm_up(self, sim:Sim):
        while True:
            self.receive_ts, updates = sim.tick()
            if updates is None:
                break
            self.lists['update'] += updates
            for md in updates:
                assert isinstance(md, MdUpdate), "wrong update type!"

                self.lists['md'].append(md)
                
                self.best_bid, self.best_ask = update_best_positions(self.best_bid, self.best_ask, md)
                
                mid_price = 0.5 * (self.best_ask + self.best_bid)

                self.queues['mid_price'].append(mid_price)
                self.queues['ask_price'].append(self.best_ask)
                self.queues['bid_price'].append(self.best_bid)
                self.queues['receive_ts'].append(self.receive_ts)

                self.lists['mid_price'].append(mid_price)
                self.lists['ask_price'].append(self.best_ask)
                self.lists['bid_price'].append(self.best_bid)
                self.lists['res_price'].append(np.nan)
                self.lists['spread'].append(np.nan)            

            if self.receive_ts - self.queues['receive_ts'][0] >= self.T:
                break


    def _update_queues(self):
        while self.queues['receive_ts'][0] < self.receive_ts - self.T:
            for _, queue in self.queues.items():
                queue.popleft()


    def _calc_reservation_price(self):
        assert False, f"{__name__} is not defined in base class"


    def run(self, sim: Sim ) ->\
        Tuple[ List[OwnTrade], List[MdUpdate], List[ Union[OwnTrade, MdUpdate] ], List[Order] ]:
        '''
            This function runs simulation

            Args:
                sim(Sim): simulator
            Returns:
                trades_list(List[OwnTrade]): list of our executed trades
                md_list(List[MdUpdate]): list of market data received by strategy
                updates_list( List[ Union[OwnTrade, MdUpdate] ] ): list of all updates 
                received by strategy(market data and information about executed trades)
                all_orders(List[Orted]): list of all placed orders
        '''

        self.receive_ts = 0.0

        self.queues = defaultdict(deque)
        self.lists = defaultdict(list)
        
        #current best positions
        self.best_bid = -np.inf
        self.best_ask = np.inf

        #last order timestamp
        prev_time = -np.inf
        #orders that have not been executed/canceled yet
        ongoing_orders: Dict[int, Order] = {}
        all_orders = []

        self._warm_up(sim)    
        btc_pos = 0.0

        while True:
            #get update from simulator
            self.receive_ts, updates = sim.tick()
            if updates is None:
                break
            #save updates
            self.lists['update'] += updates
            for update in updates:
                #update best position and volatility
                if isinstance(update, MdUpdate):
                    self.best_bid, self.best_ask = update_best_positions(self.best_bid, self.best_ask, update)
                    self.lists['md'].append(update)
                    mid_price = 0.5 * (self.best_ask + self.best_bid)
                #update btc_position
                elif isinstance(update, OwnTrade):
                    self.lists['trade'].append(update)
                    #delete executed trades from the dict
                    if update.order_id in ongoing_orders.keys():
                        ongoing_orders.pop(update.order_id)
                    sgn = 1.0 if update.side == 'BID' else -1.0
                    btc_pos += sgn * update.size
                else: 
                    assert False, 'invalid type of update!'
            
            self.queues['mid_price'].append(mid_price)
            self.queues['ask_price'].append(self.best_ask)
            self.queues['bid_price'].append(self.best_bid)
            self.queues['receive_ts'].append(self.receive_ts)
            self._update_queues()

            
            
            inventory = btc_pos / self.min_pos
            res_price, spread, bid_price, ask_price = self._calc_reservation_price(inventory)

            self.lists['mid_price'].append(mid_price)
            self.lists['ask_price'].append(self.best_ask)
            self.lists['bid_price'].append(self.best_bid)
            self.lists['res_price'].append(res_price)
            self.lists['spread'].append(spread)


            if self.receive_ts - prev_time >= self.delay:
                prev_time = self.receive_ts
                
                #place order
                bid_order = sim.place_order( self.receive_ts, self.min_pos, 'BID', bid_price )
                ask_order = sim.place_order( self.receive_ts, self.min_pos, 'ASK', ask_price )
                ongoing_orders[bid_order.order_id] = bid_order
                ongoing_orders[ask_order.order_id] = ask_order

                all_orders += [bid_order, ask_order]
            
            #cancel orders
            to_cancel = []
            for ID, order in ongoing_orders.items():
                if order.place_ts < self.receive_ts - self.delay:
                    sim.cancel_order( self.receive_ts, ID )
                    to_cancel.append(ID)
            for ID in to_cancel:
                ongoing_orders.pop(ID)

        res = dict(self.lists)
        array_keys = ['mid_price', 'ask_price', 'bid_price', 'res_price', 'spread']

        for k in array_keys:
            res[k] = np.asarray(self.lists[k])
        
        res['trade'] = self.lists['trade']
        res['md'] = self.lists['md']
        res['update'] = self.lists['update']

        return res
