from typing import List, Optional, Tuple, Union, Dict, Deque

from collections import deque, defaultdict
import numpy as np
import pandas as pd

from simulator import MdUpdate, Order, OwnTrade, Sim, update_best_positions

from stoikov_strategy import StoikovStrategy


class PredictStrategy(StoikovStrategy):
    '''
        This strategy places ask and bid order every `delay` nanoseconds.
        If the order has not been executed within `hold_time` nanoseconds, it is canceled.
    '''
    def __init__(self, *args, **kwargs) -> None:
        assert 'market_data' in kwargs, 'You should provide market data!'
        
        md = sorted(kwargs['market_data'], key=lambda x:x.receive_ts)
        del kwargs['market_data']   
        self.md_queue = deque( md )


        self.future_md = self.md_queue.popleft()
        self.future_bid_price, self.future_ask_price = \
            update_best_positions(-np.inf, np.inf, self.future_md)
        
        
        super().__init__(*args, **kwargs)
        


    def _calc_reservation_price(self, inventory):
        theta = self._calc_theta() 

        res_price = self.future_mid_price
        spread = self.gamma * theta + 2 / self.gamma * np.log( 1 + self.gamma / self.k )
        bid_price = res_price - 0.5 * spread
        ask_price = res_price + 0.5 * spread
        return res_price, spread, bid_price, ask_price


    def update_md(self, md):
        self.best_bid, self.best_ask = update_best_positions(self.best_bid, self.best_ask, md)

        if len(self.md_queue):
            self.future_md = self.md_queue.popleft()
        self.future_bid_price, self.future_ask_price = \
            update_best_positions(self.future_bid_price, self.future_ask_price, self.future_md)
        self.future_mid_price = 0.5 * (self.future_bid_price + self.future_ask_price)

        return self.best_bid, self.best_ask
