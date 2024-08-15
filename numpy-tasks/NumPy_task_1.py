#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np

# Practical Task 1: Basic Array Creation and Manipulation with NumPy

# 1. Array Creation
a=np.arange(1,11)
b=np.random.randint(1,10, size=(3, 3))

# 2. Basic Operations
a[2]
b[:2,:2]

a+5
b*2

# 3. Output Function
def print_array(array,message=''):
    print(message,array,sep='\n')
    
c=np.arange(1,11)
d=np.random.randint(1,10, size=(3, 3))
print_array(c,"Initial one-dimensional array:")
print_array(d,"Initial two-dimensional array:")
print_array(c[2],"Third element of the one-dimensional array:")
print_array(d[:2,:2],"First two rows and columns of the two-dimensional array:")
print_array(c+5,"Add 5 to each element of the one-dimensional array:")
print_array(d*2,"Multiply each element of the two-dimensional array by 2:")


# In[ ]:




