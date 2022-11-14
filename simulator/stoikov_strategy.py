from typing import List, Optional, Tuple, Union, Dict, Deque

from collections import deque, defaultdict
import numpy as np
import pandas as pd

from simulator import MdUpdate, Order, OwnTrade, Sim, update_best_positions


class StoikovStrategy:
    '''
        This strategy places ask and bid order every `delay` nanoseconds.
        If the order has not been executed within `hold_time` nanoseconds, it is canceled.
    '''
    def __init__(self, delay: float, min_pos:float, gamma: float, k:float, T:float,
                 theta_policy:str ='std',
                 res_policy:str   ='stoikov'
                 ) -> None:
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
        self.gamma = gamma
        self.k = k

        self.theta_policy = theta_policy
        self.res_policy = res_policy

        assert theta_policy in ['std', 'spread'], "Wrong theta policy!"
        assert res_policy in ['mid_price', 'stoikov'], "Wrong theta policy!"



    def update_md(self, md):
        '''
            updates best positions
        '''
        self.best_bid, self.best_ask = update_best_positions(self.best_bid, self.best_ask, md)
        return self.best_bid, self.best_ask


    def _warm_up(self, sim:Sim):
        while True:
            self.receive_ts, updates = sim.tick()
            if updates is None:
                break
            self.lists['update'] += updates
            for md in updates:
                assert isinstance(md, MdUpdate), "wrong update type!"

                self.lists['md'].append(md)
                
                self.best_bid, self.best_ask = self.update_md(md)
                
                self.mid_price = 0.5 * (self.best_ask + self.best_bid)

                self.queues['mid_price'].append(self.mid_price)
                self.queues['ask_price'].append(self.best_ask)
                self.queues['bid_price'].append(self.best_bid)
                self.queues['receive_ts'].append(self.receive_ts)

                self.lists['mid_price'].append(self.mid_price)
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


    def _calc_reservation_price(self, inventory):
        theta = self._calc_theta() 

        if self.res_policy == 'stoikov':       
            res_price = self.mid_price - inventory * self.gamma * theta
        elif self.res_policy == 'mid_price':
            res_price = self.mid_price
        else:
            assert False, 'Wrong reservation price policy'
        
        #k = 4.0 / np.mean( np.asarray( self.queues['ask_price'] ) - np.asarray( self.queues['bid_price'] ))
        spread = self.gamma * theta + 2 / self.gamma * np.log( 1 + self.gamma / self.k )
        bid_price = res_price - 0.5 * spread
        ask_price = res_price + 0.5 * spread
        return res_price, spread, bid_price, ask_price


    def _calc_theta(self):
        if self.theta_policy == 'std':
            theta = np.std( self.queues['mid_price'] )
        elif self.theta_policy == 'spread':
            theta = np.mean( np.asarray( self.queues['ask_price'] ) - np.asarray( self.queues['bid_price'] ))
        else:
            assert False, "wrong theta policy"
        return theta


    def run(self, sim: Sim ):
        '''
            This function runs simulation

            Args:
                sim(Sim): simulator
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
                    self.best_bid, self.best_ask = self.update_md(update)
                    self.lists['md'].append(update)
                    self.mid_price = 0.5 * (self.best_ask + self.best_bid)
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
            
            self.queues['mid_price'].append(self.mid_price)
            self.queues['ask_price'].append(self.best_ask)
            self.queues['bid_price'].append(self.best_bid)
            self.queues['receive_ts'].append(self.receive_ts)
            self._update_queues()

            
            
            inventory = btc_pos / self.min_pos
            res_price, spread, bid_price, ask_price = self._calc_reservation_price(inventory)

            self.lists['mid_price'].append(self.mid_price)
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
