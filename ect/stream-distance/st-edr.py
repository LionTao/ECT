#!/usr/bin/env python
# coding: utf-8

# In[9]:


import numpy as np
def dist(x,y):
    return abs(x-y)
def e_edr(t0, t1, eps):
    """
    Usage
    -----
    The Edit Distance on Real sequence between trajectory t0 and t1.
    Parameters
    ----------
    param t0 : len(t0)x2 numpy_array
    param t1 : len(t1)x2 numpy_array
    eps : float
    Returns
    -------
    edr : float
           The Longuest-Common-Subsequence distance between trajectory t0 and t1
    """
    n0 = len(t0)
    n1 = len(t1)
    # An (m+1) times (n+1) matrix
    C = np.zeros((n0 + 1, n1 + 1))
    for i in range(1, n0 + 1):
        for j in range(1, n1 + 1):
            if dist(t0[i - 1], t1[j - 1]) ==0:
                subcost = 0
            else:
                subcost = 1
            C[i][j] = min(C[i][j - 1] + 1, C[i - 1][j] + 1, C[i - 1][j - 1] + subcost)
    edr = float(C[n0][n1]) / max([n0, n1])
    print(C[1:,1:])
    print(C[n0][n1])
    return edr


# In[10]:


a=np.random.randint(1,20,(5,1))
b=np.random.randint(1,20,(5,1))
print(a)
print(b)


# In[ ]:


e_edr(a,b,0)
print(e_edr(a[1:],b,0)==4)
print(e_edr(a[1:],b[1:],0)==3)


# In[ ]:




