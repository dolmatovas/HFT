import numpy as np
from spread_encoder import BaseSpreadEncoder, SpreadEncoder, UniformSpreadEncoder
from mid_price_encoder import MidPriceEncoder, DummyMidPriceEncoder


class Encoder:
    def __init__(self, n_imb, spread_encoder, dm_encoder):
        self.n_imb = n_imb
        self.spread_encoder = spread_encoder
        self.dm_encoder = dm_encoder
    
    
    def fit(self, imb, spread, dm):
        self.imb_set = list(range(0, self.n_imb))
        #fit spread encoder
        self.spread_encoder.fit(spread)
        self.spread_set = self.spread_encoder.spread_set
        self.m_spread = len(self.spread_set)     
        #fit dm encoder
        self.dm_encoder.fit(dm)
        self.dm_set = self.dm_encoder.dm_set
        self.k_dm = len(self.dm_set)
    
    
    def predict(self, imb, spread, dm=None):
        #round imbalance
        imb = (imb * self.n_imb).astype(int)
        assert set(imb).issubset(self.imb_set)
        #round spread
        spread = self.spread_encoder.predict(spread)
        if dm is None:
            return imb, spread
        #round dm
        dm = self.dm_encoder.predict(dm)
        return imb, spread, dm