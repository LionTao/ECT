#!/usr/bin/env python
# coding: utf-8

# In[20]:


import numpy as np
def dist(x,y):
    return abs(x-y)
def e_lcss(t0, t1, eps):
    """
    Usage
    -----
    The Longuest-Common-Subsequence distance between trajectory t0 and t1.
    Parameters
    ----------
    param t0 : len(t0)x2 numpy_array
    param t1 : len(t1)x2 numpy_array
    eps : float
    Returns
    -------
    lcss : float
           The Longuest-Common-Subsequence distance between trajectory t0 and t1
    """
    n0 = len(t0)
    n1 = len(t1)
    # An (m+1) times (n+1) matrix
    C = np.zeros((n0 + 1, n1 + 1))
    for i in range(1, n0 + 1):
        for j in range(1, n1 + 1):
            if dist(t0[i - 1], t1[j - 1]) == 0:
                C[i][j] = C[i - 1][j - 1] + 1
            else:
                C[i][j] = max(C[i][j - 1], C[i - 1][j])
    lcss = 1 - float(C[n0][n1]) / min([n0, n1])
    print(C[1:,1:])
    print(C[n0][n1])
    return lcss


# In[24]:


a=[6,1,2,3,4,5,6]
b=[2,1,2,3,10,33,2]
e_lcss(a,b,0)
e_lcss(a[3:],b[3:],0)


# In[ ]:




