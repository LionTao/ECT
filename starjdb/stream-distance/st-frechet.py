#!/usr/bin/env python
# coding: utf-8

# In[11]:


import numpy as np

def dist(x,y):
    return abs(x-y)


def discret_frechet(t0, t1):
    """
    Usage
    -----
    Compute the discret frechet distance between trajectories P and Q
    Parameters
    ----------
    param t0 : px2 numpy_array, Trajectory t0
    param t1 : qx2 numpy_array, Trajectory t1
    Returns
    -------
    frech : float, the discret frechet distance between trajectories t0 and t1
    """
    n0 = len(t0)
    n1 = len(t1)
    C = np.zeros((n0 + 1, n1 + 1))
    C[1:, 0] = float('inf')
    C[0, 1:] = float('inf')
    for i in np.arange(n0) + 1:
        for j in np.arange(n1) + 1:
            C[i, j] = max(dist(t0[i - 1], t1[j - 1]),
                          min(
                              C[i, j - 1],
                              C[i - 1, j - 1],
                              C[i - 1, j]
                          ))
    dtw = C[n0, n1]
    print(C)
    return dtw


# In[16]:


a=np.random.randint(1,20,np.random.randint(5,10))
b=np.random.randint(1,20,np.random.randint(5,10))
print(a)
print(b)


# In[18]:


origin=discret_frechet(a,b)
slice=discret_frechet(a[3:],b)
slice==13


# In[ ]:




