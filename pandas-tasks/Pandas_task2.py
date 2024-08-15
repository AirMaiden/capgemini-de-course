# Practical Task 2: Data Selection, Filtering, and Aggregation with Pandas
import pandas as pd

def load_data(file_path):
    return pd.read_csv(file_path)

# 1. Data Selection and Filtering
def select_data_iloc(df, start_row, end_row, start_col, end_col):
    return df.iloc[start_row:end_row, start_col:end_col]

def select_data_loc(df, rows, cols):
    return df.loc[rows, cols]

def filter_data(df):
    filtered_df = df[df['neighbourhood_group'].isin(['Manhattan', 'Brooklyn'])]
    filtered_df = filtered_df[(filtered_df['price'] > 100) & (filtered_df['number_of_reviews'] > 10)]
    filtered_df = filtered_df[['neighbourhood_group', 'price', 'minimum_nights', 'number_of_reviews', 'price_category', 'availability_365']]
    return filtered_df

# 2. Aggregation and Grouping
def group_data(df):
    return df.groupby(['neighbourhood_group', 'price_category']).agg({
        'price': 'mean',
        'minimum_nights': 'mean',
        'number_of_reviews': 'mean',
        'availability_365': 'mean'
    }).reset_index()

# 3. Data Sorting and Ranking
def sort_data(df):
    return df.sort_values(by=['price', 'number_of_reviews'], ascending=[False, True])

def rank_neighborhoods(df):
    ranked_df = df.groupby('neighbourhood_group').agg({
        'price': 'mean',
        'id': 'count'
    }).rename(columns={'id': 'total_listings'}).reset_index()
    ranked_df['price_rank'] = ranked_df['price'].rank(ascending=False)
    ranked_df['listings_rank'] = ranked_df['total_listings'].rank(ascending=False)
    return ranked_df.sort_values(by=['listings_rank', 'price_rank'])

# 4. Output
def print_grouped_data(grouped_data, message=''):
    print("\n"+message)
    print(grouped_data)

# 5. Execution and Verification
df = load_data('cleaned_airbnb_data.csv') # load the cleaned dataset (from task 1)
    
selected_data_iloc = select_data_iloc(df, 0, 5, 0, 3)
print("Selected data using iloc:\n", selected_data_iloc)
    
selected_data_loc = select_data_loc(df, slice(0, 5), ['neighbourhood_group', 'price', 'minimum_nights'])
print("\nSelected data using loc:\n", selected_data_loc)
    
filtered_df = filter_data(df)
print("\nFiltered data:\n", filtered_df.head())
    
grouped_df = group_data(filtered_df)
print_grouped_data(grouped_df, "Grouped data by neighbourhood and price with calculated averages:")
    
sorted_df = sort_data(df)
print("\nSorted data:\n", sorted_df.head())
    
ranked_df = rank_neighborhoods(df)
print("\nRanked neighborhoods:\n", ranked_df)
    
grouped_df.to_csv('aggregated_airbnb_data.csv', index=False)
print("\nData saved to 'aggregated_airbnb_data.csv'.")