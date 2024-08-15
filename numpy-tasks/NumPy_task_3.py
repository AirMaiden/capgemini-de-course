#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Practical Task 3: Array Manipulation with Separate Output Function in NumPy
import numpy as np

# 1. Array Creation
def create_random_array(rows,cols,max_value=10):
    return np.random.randint(0,max_value, size=(rows, cols))

# 2. Array Manipulation Functions
def transpose_array(array):
    return np.transpose(array)

def reshape_array(array, new_shape):
    return np.reshape(array, new_shape)

def split_array(array, num_splits, axis=0):
    return np.array_split(array, num_splits, axis=axis)

def combine_arrays(arrays, axis=0):
    return np.concatenate(arrays, axis=axis)

# 3. Output Function
def print_array(array,message=''):
    print("\n"+message,array,sep='\n')
   
    
# Execution and Verification
array = create_random_array(6,6,101)
print_array(array, "Initial array:")

transposed_array = transpose_array(array)
print_array(transposed_array, "Transposed array:")
assert transposed_array.shape == (6, 6), f"Expected shape (6, 6), but got {transposed_array.shape}"

reshaped_array = reshape_array(transposed_array, (3, 12))
print_array(reshaped_array, "Reshaped array (3x12):")
assert reshaped_array.shape == (3, 12), f"Expected shape (3, 12), but got {reshaped_array.shape}"

splitted_arrays = split_array(reshaped_array, 3, axis=0)
for i, splitted_array in enumerate(splitted_arrays):
    print_array(splitted_array, f"Splitted array {i+1}:")
assert len(splitted_arrays) == 3, f"Expected 3 splitted arrays, but got {len(splitted_arrays)}"
for splitted_array in splitted_arrays:
    assert splitted_array.shape[1] == 12, f"Expected shape with 12 columns, but got {splitted_array.shape[1]}"

combined_array = combine_arrays(splitted_arrays, axis=0)
print_array(combined_array, "Combined array:")
assert combined_array.shape == (3, 12), f"Expected shape (3, 12), but got {combined_array.shape}"

assert np.array_equal(combined_array, reshaped_array), "Combined array does not match the reshaped array."


# In[ ]:




