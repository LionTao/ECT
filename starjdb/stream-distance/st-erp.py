#!/usr/bin/env python
# coding: utf-8

# In[13]:


import numpy as np
from scipy.spatial.distance import cdist
def dist(x,y):
    return abs(x-y)


######################
# Euclidean Geometry #
######################

def e_erp(t0, t1, g):
    """
    Usage
    -----
    The Edit distance with Real Penalty between trajectory t0 and t1.
    Parameters
    ----------
    param t0 : len(t0)x2 numpy_array
    param t1 : len(t1)x2 numpy_array
    Returns
    -------
    dtw : float
          The Dynamic-Time Warping distance between trajectory t0 and t1
    """

    n0 = len(t0)
    n1 = len(t1)
    C = np.zeros((n0 + 1, n1 + 1))

    gt0_dist = [abs(dist(g, x)) for x in t0]
    gt1_dist = [abs(dist(g, x)) for x in t1]
    mdist = cdist(t0, t1,'euclidean')

    C[1:, 0] = sum(gt0_dist)
    C[0, 1:] = sum(gt1_dist)
    for i in np.arange(n0) + 1:
        for j in np.arange(n1) + 1:
            derp0 = C[i - 1, j] + gt0_dist[i-1]
            derp1 = C[i, j - 1] + gt1_dist[j-1]
            derp01 = C[i - 1, j - 1] + mdist[i-1, j-1]
            C[i, j] = min(derp0, derp1, derp01)
    erp = C[n0, n1]
    print(C[1:,1:])
    return erp


# In[14]:


a=np.random.randint(1,20,(5,1))
b=np.random.randint(1,20,(5,1))
print(a)
print(b)


# In[18]:


e_erp(a,b,0)
e_erp(a[3:],b[3:],0)==3


# In[ ]:




