# Practical Task 3: Advanced Data Manipulation, Descriptive Statistics, and Time Series Analysis with Pandas
import pandas as pd

def load_data(file_path):
    return pd.read_csv(file_path)

# 1. Advanced Data Manipulation
def analyze_pricing_trends(df):
    return df.pivot_table(values='price', index='neighbourhood_group', columns='room_type', aggfunc='mean')

def prepare_data_long_format(df):
    return df.melt(id_vars=['id', 'name', 'host_id', 'host_name', 'neighbourhood_group', 'neighbourhood', 'latitude', 'longitude', 'room_type'], 
                      value_vars=['price', 'minimum_nights'], 
                      var_name='metric', 
                      value_name='value')

def classify_listings_by_availability(df):
    def availability_status(row):
        if row['availability_365'] < 50:
            return 'Rarely Available'
        elif 50 <= row['availability_365'] <= 200:
            return 'Occasionally Available'
        else:
            return 'Highly Available'
    
    df['availability_status'] = df.apply(availability_status, axis=1)
    return df

# 2. Descriptive Statistics
def perform_descriptive_statistics(df):
    return df[['price', 'minimum_nights', 'number_of_reviews']].describe()

# 3. Time Series Analysis
def convert_and_index_time_data(df):
    df['last_review'] = pd.to_datetime(df['last_review'])
    df.set_index('last_review', inplace=True)
    return df

def identify_monthly_trends(df):
    return df.resample('ME').agg({'number_of_reviews': 'sum', 'price': 'mean'})

def analyze_monthly_patterns(df):
    df['month'] = df.index.month
    return df.groupby('month').agg({'number_of_reviews': 'mean', 'price': 'mean'})

def analyze_seasonal_patterns(df):
    season_mapping = {
        12: 'Winter', 1: 'Winter', 2: 'Winter',
        3: 'Spring', 4: 'Spring', 5: 'Spring',
        6: 'Summer', 7: 'Summer', 8: 'Summer',
        9: 'Autumn', 10: 'Autumn', 11: 'Autumn'
    }

    df['season'] = df.index.month.map(season_mapping)
    seasonal_data = df.groupby('season').agg({
        'number_of_reviews': 'mean',
        'price': 'mean'
    })

    return seasonal_data

# 4. Output Function
def print_analysis_results(data, message=''):
    print("\n"+message)
    print(data)

# 5. Execution and Verification
df = load_data('AB_NYC_2019.csv')
    
pricing_trends = analyze_pricing_trends(df)
print_analysis_results(pricing_trends, "Pricing trends by neighbourhood and room type:")
    
long_format_df = prepare_data_long_format(df)
print_analysis_results(long_format_df.head(), "Long format data:")
    
classified_df = classify_listings_by_availability(df)
print_analysis_results(classified_df[['availability_status', 'price', 'number_of_reviews', 'neighbourhood_group']].head(), "Listings classified by availability:")
    
descriptive_stats = perform_descriptive_statistics(df)
print_analysis_results(descriptive_stats, "Descriptive statistics:")
    
time_df = convert_and_index_time_data(df)
monthly_trends = identify_monthly_trends(time_df)
print_analysis_results(monthly_trends, "Monthly trends in reviews and prices:")

monthly_patterns = analyze_monthly_patterns(time_df)
print_analysis_results(monthly_patterns, "Monthly patterns by month:")

seasonal_patterns = analyze_seasonal_patterns(time_df)
print_analysis_results(seasonal_patterns, "Seasonal patterns by season:")
    
monthly_trends.to_csv('time_series_airbnb_data.csv', index=True)
print("Time series analysis data saved to 'time_series_airbnb_data.csv'.")