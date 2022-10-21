import pandas as pd
import numpy as np
from typing import List, Union, Tuple, Optional

from simulator import Sim, update_best_positions, OwnTrade, MdUpdate


class BestPosStrategy:
    '''
        This strategy places ask and bid order every `delay` nanoseconds.
        If the order has not been executed within `hold_time` nanoseconds, it is canceled.
    '''
    def __init__(self, delay: float, hold_time:Optional[float] = None) -> None:
        '''
            Args:
                delay(float): delay between orders in nanoseconds
                hold_time(Optional[float]): holding time
        '''
        self.delay = delay
        if hold_time is None:
            hold_time = max( delay * 5, pd.Timedelta(10, 's').delta )
        self.hold_time = hold_time


    def run(self, sim: "Sim") ->\
        Tuple[ List[OwnTrade], List[MdUpdate], List[ Union[OwnTrade, MdUpdate] ] ]:
        #market data list
        md_list = []
        #executed trades list
        trades_list = []
        #all updates list
        updates_list = []
        #current best positions
        best_bid = -np.inf
        best_ask = np.inf

        #last order timestamp
        prev_time = 0
        #orders that have not been executed/canceled yet
        ongoing_orders = {}
        
        while True:
            #get update from simulator
            receive_ts, updates = sim.tick()
            if updates is None:
                break
            #save updates
            updates_list += updates
            for update in updates:
                #update best position
                if isinstance(update, MdUpdate):
                    best_bid, best_ask = update_best_positions(best_bid, best_ask, update)
                    md_list.append(update)
                elif isinstance(update, OwnTrade):
                    trades_list.append(update)
                    #delete executed trades from the dict
                    if update.order_id in ongoing_orders.keys():
                        ongoing_orders.pop(update.order_id)
                else: 
                    assert False, 'invalid type of update!'
            

            if receive_ts - prev_time >= self.delay:
                prev_time = receive_ts
                #place order
                bid_order = sim.place_order( receive_ts, 0.001, 'BID', best_bid )
                ask_order = sim.place_order( receive_ts, 0.001, 'ASK', best_ask )
                bid_order.timestamp = receive_ts
                ask_order.timestamp = receive_ts
                ongoing_orders[bid_order.order_id] = bid_order
                ongoing_orders[ask_order.order_id]  = ask_order
            
            to_cancel = []
            for ID, order in ongoing_orders.items():
                if order.timestamp < receive_ts - self.hold_time:
                    sim.cancel_order( receive_ts, ID )
                    to_cancel.append(ID)
            for ID in to_cancel:
                ongoing_orders.pop(ID)
            
                
        return trades_list, md_list, updates_list