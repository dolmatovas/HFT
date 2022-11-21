from typing import List, Optional, Tuple, Union, Dict, Deque

from collections import deque, defaultdict
import numpy as np
import pandas as pd

from simulator import MdUpdate, Order, OwnTrade, Sim


from utils import get_mid_price, update_best_positions, Order, OwnTrade, MdUpdate



class BaseStrategy:
    '''
        This strategy places ask and bid order every `delay` nanoseconds.
    '''
    def __init__(self, delay: float, min_pos:float, T:int, inventory_policy='neutral', q0=1.0) -> None:
        '''
            Args:
                delay(float): delay between orders in nanoseconds

                gamma(float): gamma
                k(float): k
                T(float): time window for variance calculation

                theta_policy(str): if 'std', we calculate theta as standard deviation of mid price over the last T nanoseconds
                                   if 'spread', we calculate theta as mean bid-ask spread over the last T nanoseconds
                res_policy(str): strategy for calculating reservation price
                                can be either mid_price or stoikov
        '''
        self.delay = delay
        
        self.min_pos = min_pos

        #expiration time in nanoseconds
        self.T = T

        self.mid_price = None

        self._inventory_policy = inventory_policy
        self.q0 = q0

        keys = ['receive_ts', 'mid_price', 'best_ask', 'best_bid']
        self.queues = { k:deque() for k in keys}
        assert inventory_policy in ['neutral', 'aggressive', 'linear']



    def _update_md(self, md):
        '''
            updates best positions
        '''
        self.best_bid, self.best_ask = update_best_positions(self.best_bid, self.best_ask, md)
        if self.mid_price is None:
            self.mid_price = 0.5 * (self.best_ask + self.best_bid)
        self.mid_price = get_mid_price(self.mid_price, md)


    def _update_queues(self):
        self.queues['mid_price'].append(self.mid_price)
        self.queues['best_ask'].append(self.best_ask)
        self.queues['best_bid'].append(self.best_bid)
        self.queues['receive_ts'].append(self.receive_ts)

        while len(self.queues['receive_ts']) and self.receive_ts - self.queues['receive_ts'][0] > self.T:
            for k, queue in self.queues.items():
                queue.popleft()


    def _update_lists(self):
        self.lists['mid_price'].append(self.mid_price)
        self.lists['best_ask'].append(self.best_ask)
        self.lists['best_bid'].append(self.best_bid)

        self.lists['ask_price'].append(self.ask_price)
        self.lists['bid_price'].append(self.bid_price)


    def _warm_up(self, sim:Sim):
        go = True
        while go:
            
            self.receive_ts, updates = sim.tick()
            
            if updates is None:
                break
            
            self.lists['update'] += updates
            
            if len(self.queues['receive_ts']) and self.receive_ts - self.queues['receive_ts'][0] > self.T:
                go = False

            for md in updates:
                assert isinstance(md, MdUpdate), "wrong update type!"

                self.lists['md'].append(md)
                
                self._update_md(md)
                self._update_lists()
                self._update_queues()            

    def _calculate_order_position(self, inventory):
        self.ask_pos = self.min_pos
        self.bid_pos = self.min_pos

        if self._inventory_policy == 'neutral':
            pass
        elif self._inventory_policy == 'aggressive':
            fct = 3.0
            self.ask_pos = self.min_pos * (1 + (inventory > self.q0) )
            self.bid_pos = self.min_pos * (1 + (inventory < -self.q0) )
        elif self._inventory_policy == 'linear':
            self.ask_pos = 1.0
            if inventory > self.q0:
                self.ask_pos = 1 + inventory - self.q0
            elif inventory < -self.q0:
                self.ask_pos = 1 + inventory + self.q0
            self.ask_pos = max(self.ask_pos, 0.0)
            self.ask_pos *= self.min_pos

            self.bid_pos = 1.0
            if inventory > self.q0:
                self.bid_pos = 1 - (inventory - self.q0)
            elif inventory < -self.q0:
                self.bid_pos = 1 - (inventory + self.q0)
            self.bid_pos = max(self.bid_pos, 0.0)
            self.bid_pos *= self.min_pos
        else:
            assert False, "unreachable"


    def _calculate_order_prices(self, inventory):
        self.ask_price = self.best_ask
        self.bid_price = self.best_bid



    def run(self, sim: Sim ):
        '''
            This function runs simulation

            Args:
                sim(Sim): simulator
        '''

        self.receive_ts = 0.0

        self.lists = defaultdict(list)
        
        #current best positions
        self.best_bid = -np.inf
        self.best_ask = np.inf
        #
        self.bid_price = self.best_bid
        self.ask_price = self.best_ask


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
                if isinstance(update, MdUpdate):
                    self.lists['md'].append(update)
                    self._update_md(update)
                elif isinstance(update, OwnTrade):
                    self.lists['trade'].append(update)
                    #delete executed trades from the dict
                    if update.order_id in ongoing_orders.keys():
                        ongoing_orders.pop(update.order_id)
                    sgn = 1.0 if update.side == 'BID' else -1.0
                    btc_pos += sgn * update.size
                else: 
                    assert False, 'invalid type of update!'
            
            inventory = btc_pos / self.min_pos
            self._calculate_order_prices(inventory)
            self._calculate_order_position(inventory)

            self._update_queues()
            self._update_lists()

            if self.receive_ts - prev_time >= self.delay:
                prev_time = self.receive_ts
                #place order
                bid_order = sim.place_order( self.receive_ts, self.bid_pos, 'BID', self.bid_price )
                ask_order = sim.place_order( self.receive_ts, self.ask_pos, 'ASK', self.ask_price )
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
        array_keys = ['mid_price', 'best_ask', 'best_bid', 'res_price', 'ask_price', 'bid_price']

        for k in array_keys:
            res[k] = np.asarray(self.lists[k])
        
        res['trade'] = self.lists['trade']
        res['md'] = self.lists['md']
        res['update'] = self.lists['update']

        return res
