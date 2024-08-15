#!/usr/bin/env python
# coding: utf-8

# In[90]:


# Practical Task 2: Analyzing and Visualizing E-Commerce Transactions with NumPy
import numpy as np
import datetime

# 1. Array Creation
def create_transactions_data(rows):
    transaction_ids = np.arange(1, rows + 1)
    user_ids = np.random.randint(1, 100, rows)
    product_ids = np.random.randint(1, 50, rows)
    quantities = np.random.randint(1, 11, rows)
    prices = np.round(np.random.uniform(10, 100, rows), 2)
    
    base_timestamp = datetime.datetime.now()
    timestamps = np.array([base_timestamp - datetime.timedelta(days=np.random.randint(1, 31)) for _ in range(rows)])

    transactions = np.column_stack((transaction_ids, user_ids, product_ids, quantities, prices, timestamps))
    return transactions


# 2. Data Analysis Functions
def get_total_revenue(transactions):
    return np.sum(transactions[:,3]*transactions[:,4])

def get_unique_users(transactions):
    return np.size(np.unique(transactions[:,1]))

def get_most_purchased_product(transactions):
    product_quantities = {}
    for product_id, quantity in get_product_quantity_array(transactions):
        if product_id in product_quantities:
            product_quantities[product_id] += quantity
        else:
            product_quantities[product_id] = quantity
    return max(product_quantities, key=product_quantities.get)

def convert_price_to_integer(transactions):
    transactions[:, 4] = transactions[:,4].astype(int)
    return transactions

def get_column_data_types(transactions):
    return [type(transactions[0, i]) for i in range(transactions.shape[1])]
    
    num_columns = transactions.shape[1]
    for col in range(num_columns):
        for row in transactions[:, col]:
            column_data_type = type(row)
        print(f"Column {col + 1} data type: {column_data_type.__name__}")


# 3. Array Manipulation Functions
def get_product_quantity_array(transactions):
    return np.column_stack((transactions[:,2], transactions[:,3]))

def get_user_transaction_count(transactions):
    unique_user_ids, counts = np.unique(transactions[:,1], return_counts=True)
    return np.column_stack((unique_user_ids, counts))

def mask_zero_quantity_transactions(transactions):
    isZero = transactions[:,3] == 0
    expanded_mask = np.expand_dims(isZero, axis=1)
    masked_array = np.ma.masked_array(transactions, mask=np.repeat(expanded_mask, transactions.shape[1], axis=1))
    return masked_array


# 4. Arithmetic and Comparison Functions
def increase_prices(transactions, percentage_increase):
    updated_prices = transactions[:,4] * (1+(percentage_increase/100))
    transactions[:, 4] = updated_prices
    return transactions

def filter_transactions(transactions):
    return transactions[transactions[:,3] > 1]

def compare_revenue(transactions, start_date1, end_date1, start_date2, end_date2):
    start_date1 = datetime.datetime.strptime(start_date1, '%Y-%m-%d')
    end_date1 = datetime.datetime.strptime(end_date1, '%Y-%m-%d')
    start_date2 = datetime.datetime.strptime(start_date2, '%Y-%m-%d')
    end_date2 = datetime.datetime.strptime(end_date2, '%Y-%m-%d')
    
    timestamps = transactions[:,5]
    
    transactions_period1 = transactions[(timestamps >= start_date1) & (timestamps <= end_date1)]
    revenue_period1 = get_total_revenue(transactions_period1)
    
    transactions_period2 = transactions[(timestamps >= start_date2) & (timestamps <= end_date2)]
    revenue_period2 = get_total_revenue(transactions_period2)
    
    return revenue_period1, revenue_period2


# 5. Indexing and Slicing Functions
def get_transactions_for_specific_user(transactions, user_id):
    return transactions[transactions[:,1] == user_id]

def slice_transactions_by_date_range(transactions, start_date, end_date):
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    timestamps = transactions[:, 5]
    return transactions[(timestamps >= start_date) & (timestamps <= end_date)]

def top_products_by_revenue(transactions, top_n=5):
    product_ids = transactions[:,2]
    quantities = transactions[:,3]
    prices = transactions[:,4]
    revenue = np.column_stack((product_ids,quantities * prices))
    
    unique_product_ids = np.unique(product_ids)
    total_revenue_per_product = np.zeros(unique_product_ids.shape[0])
    
    for i, product_id in enumerate(unique_product_ids):
        total_revenue_per_product[i] = np.sum(revenue[revenue[:,0] == product_id])
    
    top_indices = np.argsort(total_revenue_per_product)[::-1][:top_n]
    top_product_ids = unique_product_ids[top_indices]
    
    return transactions[np.isin(product_ids, top_product_ids)]


# 6. Output Function
def print_array(array,message=''):
    print("\n"+message,array,sep='\n')


# Execution and Verification
sample_size=10
transactions_sample=create_transactions_data(sample_size)
print_array(transactions_sample,"Initial transactions:")
assert transactions_sample.shape == (sample_size, 6), f"Expected shape ({sample_size}, 6), but got {transactions.shape}"

print_array(get_total_revenue(transactions_sample),"Total Revenue:")
print_array(get_unique_users(transactions_sample),"Unique Users:")
print_array(get_most_purchased_product(transactions_sample),"Most Purchased Product:")

transactions_sample=convert_price_to_integer(transactions_sample)
print_array(transactions_sample,"Converting prices to integer:")
assert [type(transactions_sample[i, 4]) == int for i in range(sample_size)], f"Expected dtype int for price column, but got {transactions_sample[:, 4].dtype}"

data_types=get_column_data_types(transactions_sample)
print("\n")
for i, dtype in enumerate(data_types):
    print(f"Column {i + 1} data type: {dtype}")

product_quantity_array=get_product_quantity_array(transactions_sample)
print_array(product_quantity_array,"Product Quantity Array:")
assert product_quantity_array.shape == (sample_size, 2), f"Expected shape ({sample_size}, 2), but got {product_quantity_array.shape}"

user_transaction_count=get_user_transaction_count(transactions_sample)
print_array(user_transaction_count,"User Transaction Count:")

print_array(mask_zero_quantity_transactions(transactions_sample),"Masked Array (quantity!=0):")

percentage_increase=10
increase_prices(transactions_sample,percentage_increase)
print_array(transactions_sample,"Transactions with prices increased by " + str(percentage_increase) + "%:")
assert transactions_sample.shape == (sample_size, 6), f"Expected shape ({sample_size}, 6), but got {transactions.shape}"

transactions_sample=filter_transactions(transactions_sample)
print_array(transactions_sample,"Filtered Transactions (quantity>1):")

start_date1 = "2024-07-08"
end_date1 = "2024-08-07"
start_date2 = "2024-07-23"
end_date2 = "2024-07-28"

revenue_period1, revenue_period2 = compare_revenue(transactions_sample, start_date1, end_date1, start_date2, end_date2)
print_array(revenue_period1,"Revenue for " + start_date1 + " - " + end_date1 + ":")
print_array(revenue_period2,"Revenue for " + start_date2 + " - " + end_date2 + ":")

user=transactions_sample[np.random.randint(sample_size-1),1]
print_array(get_transactions_for_specific_user(transactions_sample,user),"All transactions for user " + str(user) + ":")

print_array(slice_transactions_by_date_range(transactions_sample,start_date2,end_date2),"Transactions for date range " + start_date2 + " - " + end_date2 + ":")
print_array(top_products_by_revenue(transactions_sample),"Transactions of the top 5 products by revenue:")


# In[ ]:





# In[ ]:




