import numpy as np
from encoder import Encoder


class SimpleMicroPrice:
    def __init__(self, encoder:Encoder):
        self.encoder = encoder
    
    
    def fit(self, I, S, dM):
        self.encoder.fit(I, S, dM)
        I, S, dM = self.encoder.predict(I, S, dM)
        
        self.imb_set    = self.encoder.imb_set
        self.spread_set = self.encoder.spread_set
        self.dm_set     = self.encoder.dm_set
        
        n, m, k = self.encoder.n_imb, self.encoder.m_spread, self.encoder.k_dm
        
        #map from state variable values to index
        self.i_map  = dict( zip( self.imb_set,     range(n) ) )
        self.s_map  = dict( zip( self.spread_set,  range(m) ) )
        self.dm_map = dict( zip( self.dm_set,      range(k) ) )
        #memory for probabilities
        R = np.zeros((n * m , k))
        T = np.zeros((n * m , n * m))
        Q = np.zeros((n * m , n * m))
        #memory for counter
        cont = np.zeros((n * m, 1))
        
        N = len(dM)
        for idx in range(N):

            i, i_ = I[idx], I[idx + 1]
            s, s_ = S[idx], S[idx + 1]
            dm = dM[idx]

            x = self.i_map[i] * m +  self.s_map[s]
            y = self.i_map[i_] * m + self.s_map[s_]
            cont[x] += 1
            j = self.dm_map[dm]
            if dm != 0.0:
                R[x, j] += 1
                T[x, y] += 1
            else:
                Q[x, y] += 1

            #Symmetrize the data
            i, i_ = n - 1 - I[idx], n - 1 - I[idx + 1]
            dm = -dM[idx]

            x = self.i_map[i] * m +  self.s_map[s]
            y = self.i_map[i_] * m + self.s_map[s_]
            cont[x] += 1
            j = self.dm_map[dm]
            if dm != 0.0:
                R[x, j] += 1
                T[x, y] += 1
            else:
                Q[x, y] += 1

        R /= (cont + 1e-10)
        Q /= (cont + 1e-10)
        T /= (cont + 1e-10)

        Q_inv = np.linalg.inv(np.eye(n * m) - Q)
        G = Q_inv @ (R @ np.asarray(self.dm_set))
        B = Q_inv @ T

        n_iter = 100

        errors = []
        G_star = G
        for i in range(n_iter):
            G_star = G + B @ G_star
            
        G_spread = {}
        for s in self.spread_set:
            G_spread[s] = []
            for i in self.imb_set:
                x = self.i_map[i] * m +  self.s_map[s]
                G_spread[s].append( G_star[x] )
        self.G = G_spread    
        

    def predict(self, I:np.ndarray, S:np.ndarray):
        I, S = self.encoder.predict(I, S)
        predict = np.asarray([ self.G[s][i] for s, i in zip(S, I) ])
        return predict


class DummyMicroPrice:
    """
        this class just calculate average mid price increments for given imbalance and spread
    """
    def __init__(self, encoder:Encoder):
        self.encoder = encoder
    
    
    def fit(self, I, S, dM):
        self.encoder.fit(I, S, dM)
        I, S = self.encoder.predict(I, S)
        #memory for probabilities
        self.dms = {}
        N = len(dM)
        for idx in range(N):
            i  = I[idx]
            s  = S[idx]
            dm = dM[idx]
            if s not in self.dms:
                self.dms[s] = [ []  for _ in range(self.encoder.n_imb)]
            self.dms[s][i].append(dm)
        
        self.G = {}
        for s in self.dms.keys():
            self.G[s] = np.zeros((self.encoder.n_imb, ))
            for i in range(self.encoder.n_imb):
                self.G[s][i] = np.mean(self.dms[s][i])

                
    def predict(self, I:np.ndarray, S:np.ndarray):
        I, S = self.encoder.predict(I, S)
        pred = np.asarray([ self.G[s][i] for s, i in zip(S, I) ])
        return pred


    def apply(self, I, S, func):
        """
            return Dict[ int, np.ndarray ] G
            where G.keys() are unique values of spread, e.g. 1,2,3,5,...
            and G[s] is array of length n_imbalance
        """
        I, S = self.encoder.predict(I, S)
        G = {}
        for s in self.dms.keys():
            G[s] = np.zeros((self.encoder.n_imb, ))
            for i in range(self.encoder.n_imb):
                G[s][i] = func(self.dms[s][i])
        return G

    
    def predict_apply(self, I:np.ndarray, S:np.ndarray, func):
        I, S = self.encoder.predict(I, S)
        G = {}
        for s in self.dms.keys():
            G[s] = np.zeros((self.encoder.n_imb, ))
            for i in range(self.encoder.n_imb):
                G[s][i] = func(self.dms[s][i])
        pred = np.asarray( [ G[s][i] for s, i in zip(S, I) ] )
        return pred