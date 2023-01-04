import numpy as np
from collections import Counter


class MidPriceEncoder:
    def __init__(self):
        self.tick_size = 0.01
        self.thresholds = None
        self.codes      = None
        
        self.n_bins = 200

        
    def predict(self, dm):
        #previous threshold
        prv = 0
        dm_int = (np.abs(dm) / self.tick_size).astype(int)
        for thr, code in zip(self.thresholds, self.codes):
            idx = (prv < dm_int ) & ( dm_int <= thr) 
            dm_int[idx] = code
            prv = thr
        dm_int = dm_int * np.sign(dm) * self.tick_size
        assert set(dm_int).issubset(self.dm_set)
        return dm_int 
    
    
    def fit(self, dm):
        #to int
        dm_int = np.round_((np.abs(dm) / self.tick_size))
        #ignore zeros
        dm_int = dm_int[dm_int > 0]
        #build array of thresholds and codes
        self.thresholds = []
        self.codes = []
        
        counter = sorted(Counter(dm_int).items())   
        dcount = len(dm_int) / (self.n_bins)
        
        cnts = 0
        vals = []
        for val, cnt in counter:
            
            cnts += cnt
            vals.append(val)
            
            if cnts >= dcount:
                self.thresholds.append(val)
                self.codes.append(int(np.median(vals)))
                
                cnts = 0
                vals = []
        self.thresholds[-1] = np.inf
        self.dm_set = sorted( [-code * self.tick_size for code in self.codes] + [0]\
                            + [ code * self.tick_size for code in self.codes] ) 
        self.k_dm = len(self.codes)



class DummyMidPriceEncoder:    
    def predict(self, dm):
        return dm
    
    
    def fit(self, dm):
        self.dm_set = {}