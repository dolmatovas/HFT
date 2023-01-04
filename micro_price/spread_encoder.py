import numpy as np
from collections import Counter

class BaseSpreadEncoder:
    def __init__(self, tick_size):
        self.tick_size = tick_size
        
        self.thresholds = None
        self.codes      = None
    
    
    def fit(self, spread):
        assert False, "not implemented yet"

        
    def predict(self, spread):
        #clip to be sure spread >= tick_size
        spread = np.clip(spread, self.tick_size, np.inf)
        #to int
        spread = (spread / self.tick_size).astype(int)
        #previous threshold
        prv = 0
        for thr, code in zip(self.thresholds, self.codes):
            idx = (prv < spread) & (spread <= thr) 
            spread[idx] = code
            prv = thr
        assert set(spread).issubset(self.spread_set)
        return spread


class UniformSpreadEncoder(BaseSpreadEncoder):
    def __init__(self, tick_size, max_quantile=0.999, n_bins1=10, n_bins2=10):
        super().__init__(tick_size)
        self.tick_size    = tick_size
        self.max_quantile = max_quantile
        self.n_bins1 = n_bins1
        self.n_bins2 = n_bins2
    
    
    def fit(self, spread):
        #clip to be sure spread >= tick_size
        spread = np.clip(spread, self.tick_size, np.inf)
        #to int
        spread = (spread / self.tick_size).astype(int)
        #max spread to consider
        max_spread = np.quantile(spread, self.max_quantile)
        spread = np.clip(spread, 1, max_spread)
        #build array of thresholds and codes
        self.thresholds = [i for i in range(1, self.n_bins1)]
        self.codes = [i for i in range(1, self.n_bins1)]
        
        counter = sorted(Counter(spread).items())
        
        total = len(spread) 
        
        current = sum( cont for _, cont in counter[:self.n_bins1]  )
        
        total -= current

        dcount = total / (self.n_bins2)

        n_bins2 = 0
        
        cnts = 0
        vals = []
        for val, cnt in counter[self.n_bins1:]:
            if n_bins2 == self.n_bins2:
                break
            cnts += cnt
            vals.append(val)
            
            if cnts > dcount:
                self.thresholds.append(val)
                self.codes.append(int(np.median(vals)))
                
                n_bins2 += 1
                total -= cnts
                dcount = total / (self.n_bins2 - n_bins2)

                cnts = 0
                vals = []
        self.thresholds[-1] = np.inf
        self.spread_set = self.codes
        self.m_spread = len(self.codes)


class SpreadEncoder(BaseSpreadEncoder):
    def __init__(self, tick_size, max_quantile=0.99, max_steps=5, step_inc=4):
        super().__init__(tick_size)
        self.tick_size    = tick_size
        self.max_quantile = max_quantile
        self.max_steps    = max_steps
        self.step_inc     = step_inc
    
    
    def fit(self, spread):
        #clip to be sure spread >= tick_size
        spread = np.clip(spread, self.tick_size, np.inf)
        #to int
        spread = (spread / self.tick_size).astype(int)
        #max spread to consider
        max_spread = np.quantile(spread, self.max_quantile)
        #build array of thresholds and codes
        self.thresholds = [1]
        self.codes = [1]
        step = 1
        n_steps = 1
        while self.thresholds[-1] < max_spread:
            self.codes.append(self.thresholds[-1] + (step+1) // 2)
            self.thresholds.append(self.thresholds[-1] + step)
            
            n_steps += 1
            if n_steps == self.max_steps:
                step += self.step_inc
                n_steps = 0    
        self.thresholds[-1] = np.inf
        #for example, if step_inc = 2, max_step = 4
        #thresholds = [1, 2, 3, 4, 7, 10, 13, 16, 21, 26, 31, 36, ..., np.inf]
        self.m_spread = len(self.codes)
        self.spread_set = self.codes