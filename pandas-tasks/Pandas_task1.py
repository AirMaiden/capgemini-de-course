# Practical Task 1: Basic Data Exploration and Cleaning with Pandas
import pandas as pd

# 1. Data Loading and Initial Inspection
def load_data(file_path):
    df = pd.read_csv(file_path)
    print("Data is loaded from file: " + file_path)
    return df

def initial_inspection(df):
    print("Initial inspection:")
    print(df.head())
    print(df.info())

# 2. Handling Missing Values
def handle_missing_values(df):
    missing_values = df.isnull().sum()
    print("\nMissing Values before handling:\n", missing_values)
    
    df['name'] = df['name'].fillna("Unknown")
    df['host_name'] = df['host_name'].fillna("Unknown")
    df['last_review'] = df['last_review'].fillna("NaT") # I cannot use special pd.NaT because then furhter validation for missing values will fail

    print("\nMissing Values after handling:\n", df.isnull().sum())
    return df

# 3. Data Transformation
def categorize_price(df):
    bins = [0, 100, 300, float('inf')]
    labels = ['Low', 'Medium', 'High']
    df['price_category'] = pd.cut(df['price'], bins=bins, labels=labels, right=False)
    return df

def categorize_length_of_stay(df):
    bins=[0, 3, 14, float('inf')]
    labels=['Short-term', 'Medium-term', 'Long-term']
    df['length_of_stay_category'] = pd.cut(df['minimum_nights'], bins=bins, labels=labels, right=False)
    return df

# 4. Data Validation
def validate_data(df):
    assert df['name'].isnull().sum() == 0, "Missing values in 'name' column"
    assert df['host_name'].isnull().sum() == 0, "Missing values in 'host_name' column"
    assert df['last_review'].isnull().sum() == 0, "Missing values in 'last_review' column"

    initial_df_size = df.shape[0]
    invalid_prices = df[df['price'] <= 0]
    if not invalid_prices.empty:
        print("Invalid prices found: ", invalid_prices.shape[0])
        df = df[df['price'] > 0]
    print(f"{initial_df_size - df.shape[0]} rows were removed.")

    print("\nSummary of price categorization:")
    print(f"Low prices: {df[df['price_category']=='Low'].shape[0]}")
    print(f"Medium prices: {df[df['price_category']=='Medium'].shape[0]}")
    print(f"High prices: {df[df['price_category']=='High'].shape[0]}")

    print("\nSummary of length_of_stay categorization:")
    print(f"Short-term prices: {df[df['length_of_stay_category']=='Short-term'].shape[0]}")
    print(f"Medium-term prices: {df[df['length_of_stay_category']=='Medium-term'].shape[0]}")
    print(f"Long-term prices: {df[df['length_of_stay_category']=='Long-term'].shape[0]}")

    
    return df

# 5. Output Function
def print_dataframe_info(df, message=''):
    print("\n"+message)
    print(df.info())

# 6. Execution and Verification
df = load_data("AB_NYC_2019.csv")
initial_inspection(df)
print_dataframe_info(df, "Before cleaning:")
    
df = handle_missing_values(df)
df = categorize_price(df)
df = categorize_length_of_stay(df)
df = validate_data(df)
    
print_dataframe_info(df, "After cleaning:")
df.to_csv("cleaned_airbnb_data.csv", index=False)
print("\nCleaned data saved to cleaned_airbnb_data.csv")