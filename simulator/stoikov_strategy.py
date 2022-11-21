from typing import List, Optional, Tuple, Union, Dict, Deque

from collections import deque, defaultdict
import numpy as np
import pandas as pd

from simulator import MdUpdate, Order, OwnTrade, Sim


from utils import get_mid_price, update_best_positions

from base_strategy import BaseStrategy


class StoikovStrategy(BaseStrategy):
    '''
        This strategy places ask and bid order every `delay` nanoseconds.
        If the order has not been executed within `hold_time` nanoseconds, it is canceled.
    '''
    def __init__(self, delay: float, 
                    min_pos:float, 
                    T:int, 
                    gamma: float,
                    theta_policy:str = 'std',
                    res_policy:str = 'stoikov') -> None:
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
        super().__init__(delay, min_pos, T, 'neutral', q0=1.0)

        self.gamma = gamma

        self._theta_policy = theta_policy
        self._res_policy = res_policy

        self.mid_price = None
        self.res_price = np.nan

        assert theta_policy in ['std', 'spread'], "Wrong theta policy!"
        assert res_policy in ['mid_price', 'stoikov'], "Wrong theta policy!"


    def _update_lists(self):
        super()._update_lists()
        self.lists['res_price'].append(self.res_price)


    def _calc_theta(self):
        if self._theta_policy == 'std':
            theta = np.std( self.queues['mid_price'] )
        elif self._theta_policy == 'spread':
            theta = np.mean( np.asarray( self.queues['best_ask'] ) - np.asarray( self.queues['best_bid'] ))
        else:
            assert False, "Unreachable"
        return theta


    def _calculate_order_prices(self, inventory):
        theta = self._calc_theta()
        
        if self._res_policy == 'stoikov':       
            self.res_price = self.mid_price - inventory * self.gamma * theta
        elif self._res_policy == 'mid_price':
            self.res_price = self.mid_price
        else:
            assert False, 'Unreachable'
        

        spread = (1 + self.gamma) * theta
        self.bid_price = self.res_price - 0.5 * spread
        self.ask_price = self.res_price + 0.5 * spread
        return


class FutureStrategy(BaseStrategy):
    '''
        This strategy places ask and bid order every `delay` nanoseconds.
        If the order has not been executed within `hold_time` nanoseconds, it is canceled.
    '''
    def __init__(self, delay: float, 
                    min_pos:float, 
                    T:int, 
                    gamma: float,
                    md: Deque[MdUpdate],
                    theta_policy:str = 'std',
                    inventory_policy:str = 'neutral',
                    q0:float = 1.0
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
        super().__init__(delay, min_pos, T, inventory_policy, q0)

        self.gamma = gamma

        self._theta_policy = theta_policy

        self.mid_price = None
        self.fut_price = None
        assert theta_policy in ['std', 'spread'], "Wrong theta policy!"

        md = sorted(md, key=lambda x:x.receive_ts)
        self.md_queue = deque( md )

        self.future_md = self.md_queue.popleft()
        self.future_bid_price, self.future_ask_price = \
            update_best_positions(-np.inf, np.inf, self.future_md)
        self.future_mid_price = 0.5 * (self.future_bid_price + self.future_ask_price)
        self.future_mid_price = get_mid_price(self.future_mid_price, self.future_md)


    def _update_md(self, md):
        super()._update_md(md)
        if len(self.md_queue):
            self.future_md = self.md_queue.popleft()
        self.future_bid_price, self.future_ask_price = \
            update_best_positions(-np.inf, np.inf, self.future_md)
        self.future_mid_price = 0.5 * (self.future_bid_price + self.future_ask_price)
        self.future_mid_price = get_mid_price(self.future_mid_price, self.future_md)


    def _update_lists(self):
        super()._update_lists()
        self.lists['future_mid_price'].append(self.future_mid_price)
        return


    def _calc_theta(self):
        if self._theta_policy == 'std':
            theta = np.std( self.queues['mid_price'] )
        elif self._theta_policy == 'spread':
            theta = np.mean( np.asarray( self.queues['best_ask'] ) - np.asarray( self.queues['best_bid'] ))
        else:
            assert False, "Unreachable"
        return theta


    def _calculate_order_prices(self, inventory):
        theta = self._calc_theta()
        spread = (1 + self.gamma) * theta
        self.bid_price = self.future_mid_price - 0.5 * spread
        self.ask_price = self.future_mid_price + 0.5 * spread
        return