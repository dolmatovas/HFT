from base_stoikov_strategy import BaseStoikovStrategy

import numpy as np


class StoikovStrategy(BaseStoikovStrategy):
    def _calc_reservation_price(self, inventory:float):
        mid_price = self.queues['mid_price'][-1]
        
        theta = np.std( self.queues['mid_price'] )
        theta = np.mean( np.asarray( self.queues['ask_price'] ) - np.asarray( self.queues['bid_price'] ))
        
        res_price = mid_price - inventory * self.gamma * theta
        spread = self.gamma * theta
        # + 2 / self.gamma * np.log( 1 + self.gamma / self.k )
        bid_price = res_price - 0.5 * spread
        ask_price = res_price + 0.5 * spread
        return res_price, spread, bid_price, ask_price


class MidPriceStoikovStrategy(BaseStoikovStrategy):
    def _calc_reservation_price(self, inventory:float):
        mid_price = self.queues['mid_price'][-1]
        
        theta = np.std( self.queues['mid_price'] )
        theta = np.mean( np.asarray( self.queues['ask_price'] ) - np.asarray( self.queues['bid_price'] ))
        res_price = mid_price
        spread = self.gamma * theta
        # + 2 / self.gamma * np.log( 1 + self.gamma / self.k )
        bid_price = res_price - 0.5 * spread
        ask_price = res_price + 0.5 * spread
        return res_price, spread, bid_price, ask_price