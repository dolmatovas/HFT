from typing import List, Optional, Tuple, Union, Dict, Deque

from collections import deque
import numpy as np
import pandas as pd

from simulator import MdUpdate, Order, OwnTrade, Sim, update_best_positions


class StoikovStrategy:
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

        #market data list
        md_list:List[MdUpdate] = []
        #mid prices list
        mid_price_list:List[float] = []
        #
        bid_price_list:List[float] = []
        ask_price_list:List[float] = []
        #reservation price list
        res_price_list:List[float] = []
        #
        spread_list:List[float] = []

        mid_price_queue:Deque[float] = deque()
        receive_ts_queue:Deque[float] = deque()

        #executed trades list
        trades_list:List[OwnTrade] = []
        #all updates list
        updates_list = []
        #current best positions
        best_bid = -np.inf
        best_ask = np.inf

        #last order timestamp
        prev_time = -np.inf
        #orders that have not been executed/canceled yet
        ongoing_orders: Dict[int, Order] = {}
        all_orders = []

        receive_ts = 0.0
        sigs:List[float] = []
        while True:
            receive_ts, updates = sim.tick()
            if updates is None:
                break
            updates_list += updates
            for md in updates:
                assert isinstance(md, MdUpdate), "wrong update type!"
                
                md_list.append(md)
                
                best_bid, best_ask = update_best_positions(best_bid, best_ask, md)
                
                mid_price = 0.5 * (best_ask + best_bid)

                mid_price_queue.append(mid_price)
                receive_ts_queue.append(receive_ts)

                mid_price_list.append(mid_price)
                ask_price_list.append(best_ask)
                bid_price_list.append(best_bid)
                res_price_list.append(np.nan)
                spread_list.append(np.nan)

            if receive_ts_queue[-1] - receive_ts_queue[0] >= self.T:
                break
    
        btc_pos = 0.0

        while True:
            #get update from simulator
            receive_ts, updates = sim.tick()
            if updates is None:
                break
            #save updates
            updates_list += updates
            for update in updates:
                #update best position and volatility
                if isinstance(update, MdUpdate):
                    best_bid, best_ask = update_best_positions(best_bid, best_ask, update)
                    md_list.append(update)
                    mid_price = 0.5 * (best_ask + best_bid)
                #update btc_position
                elif isinstance(update, OwnTrade):
                    trades_list.append(update)
                    #delete executed trades from the dict
                    if update.order_id in ongoing_orders.keys():
                        ongoing_orders.pop(update.order_id)

                    sgn = 1.0 if update.side == 'BID' else -1.0
                    btc_pos += sgn * update.size
                else: 
                    assert False, 'invalid type of update!'
            

            mid_price_queue.append(mid_price)
            receive_ts_queue.append(receive_ts)
            while receive_ts_queue[0] < receive_ts_queue[-1] - self.T:
                receive_ts_queue.popleft()
                mid_price_queue.popleft()
            
            inventory = btc_pos / self.min_pos
            theta = np.std(mid_price_queue) ** 2
            reserv_price = mid_price - inventory * self.gamma * theta
            spread = self.gamma * theta + 2 / self.gamma * np.log( 1 + self.gamma / self.k )
            bid_price = reserv_price - 0.5 * spread
            ask_price = reserv_price + 0.5 * spread


            mid_price_list.append(mid_price)
            ask_price_list.append(best_ask)
            bid_price_list.append(best_bid)
            res_price_list.append(reserv_price)
            spread_list.append(spread)


            if receive_ts - prev_time >= self.delay:
                prev_time = receive_ts
                
                #place order
                bid_order = sim.place_order( receive_ts, self.min_pos, 'BID', bid_price )
                ask_order = sim.place_order( receive_ts, self.min_pos, 'ASK', ask_price )
                ongoing_orders[bid_order.order_id] = bid_order
                ongoing_orders[ask_order.order_id] = ask_order

                all_orders += [bid_order, ask_order]
            
            #cancel orders
            to_cancel = []
            for ID, order in ongoing_orders.items():
                if order.place_ts < receive_ts - self.delay:
                    sim.cancel_order( receive_ts, ID )
                    to_cancel.append(ID)
            for ID in to_cancel:
                ongoing_orders.pop(ID)
            
        res = {"trades":trades_list, "md":md_list, "updates":updates_list, "orders":all_orders, 
            "mid_price":np.asarray(mid_price_list),
            "ask_price":np.asarray(ask_price_list),
            "bid_price":np.asarray(bid_price_list), 
            "res_price":np.asarray(res_price_list), 
            "spread":np.asarray(spread_list)}       
        return res
