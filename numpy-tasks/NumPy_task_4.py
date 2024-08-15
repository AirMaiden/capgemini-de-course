#!/usr/bin/env python
# coding: utf-8

# In[3]:


# Practical Task 4: Comprehensive Data Handling and Analysis with NumPy
import numpy as np
import os

# 1. Array Creation
def create_random_array(rows,cols,max_value=10):
    return np.random.randint(0,max_value, size=(rows, cols))

# 2. Data I/O Functions
def save_array(array, filename='random_array'):
    np.savetxt(f'{filename}.txt', array, fmt='%d')
    np.savetxt(f'{filename}.csv', array, delimiter=',', fmt='%d')
    np.save(f'{filename}.npy', array)

def load_array(filename='random_array'):
    array_txt = np.loadtxt(f'{filename}.txt', dtype=int)
    array_csv = np.loadtxt(f'{filename}.csv', delimiter=',', dtype=int)
    array_npy = np.load(f'{filename}.npy')
    return array_txt, array_csv, array_npy

# 3. Aggregate Functions
def calculate_sum(array):
    return np.sum(array)

def calculate_mean(array):
    return np.mean(array)

def calculate_median(array):
    return np.median(array)

def calculate_std(array):
    return np.std(array)

def axis_based_sum(array, axis):
    return np.sum(array, axis=axis)

# 4. Output Function
def print_array(array,message=''):
    print("\n"+message,array,sep='\n')


# Execution and Verification
array = create_random_array(10,10,101)
print_array(array, "Initial array:")
assert array.shape == (10, 10), f"Expected shape (10, 10), but got {array.shape}"

sum_result = calculate_sum(array)
mean_result = calculate_mean(array)
median_result = calculate_median(array)
std_result = calculate_std(array)
print_array(sum_result,"Sum of all elements:")
print_array(mean_result,"Mean of the array:")
print_array(median_result,"Median of the array:")
print_array(std_result,"Standard Deviation of the array:")

save_array(array)  
assert os.path.exists('random_array.txt'), "Saving a text file failed."
assert os.path.exists('random_array.csv'), "Saving a csv file failed."
assert os.path.exists('random_array.npy'), "Saving a binary file failed."

loaded_arrays = load_array()
print_array(loaded_arrays[0], "Loaded array from a txt file:")
print_array(loaded_arrays[1], "Loaded array from a csv file:")
print_array(loaded_arrays[2], "Loaded array from a binary file:")
assert np.array_equal(loaded_arrays[0], loaded_arrays[1]), "Loaded txt and csv arrays do not match."
assert np.array_equal(loaded_arrays[1], loaded_arrays[2]), "Loaded csv and binary arrays do not match."

for a in loaded_arrays:
    assert calculate_sum(a) == sum_result, "Sum results do not match."
    assert calculate_mean(a) == mean_result, "Mean results do not match."
    assert calculate_median(a) == median_result, "Median results do not match."
    assert calculate_std(a) == std_result, "Standard deviation results do not match."
    
row_wise_sum = axis_based_sum(array, axis=1)
col_wise_sum = axis_based_sum(array, axis=0)
print_array(row_wise_sum, "Row-wise sum:")
print_array(col_wise_sum, "Column-wise sum:")


# In[ ]:




