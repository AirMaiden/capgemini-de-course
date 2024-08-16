# Visualization Task: Analyzing Airbnb Listings in New York City
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# 1. Neighborhood Distribution of Listings
def plot_neighborhood_distribution(df):
    counts = df['neighbourhood_group'].value_counts()
    plt.figure(figsize=(10, 6))
    plt.bar(counts.index, counts.values, color=['#F0E442', '#56B4E9', '#009E73', '#8F77B5', '#FF9999'])
    plt.title('Distribution of Listings Across Neighborhood Groups')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Number of Listings')
    for i, count in enumerate(counts.values):
        plt.text(i, count + 50, str(count), ha='center')
    plt.savefig('neighborhood_distribution.png')
    plt.show()
    plt.close()

# 2. Price Distribution Across Neighborhoods
def plot_price_distribution(df):
    neighborhoods = df['neighbourhood_group'].unique()
    plt.figure(figsize=(12, 6))
    data = [df[df['neighbourhood_group'] == n]['price'] for n in neighborhoods]
    plt.boxplot(data, patch_artist=True, notch=True)
    plt.xticks(range(1, len(neighborhoods) + 1), neighborhoods)
    plt.title('Price Distribution Across Neighborhood Groups')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Price')
    plt.savefig('price_distribution.png')
    plt.show()
    plt.close()

# 3. Room Type vs. Availability
def plot_room_type_vs_availability(df):
    means = df.groupby(['neighbourhood_group', 'room_type'])['availability_365'].mean().unstack()
    stds = df.groupby(['neighbourhood_group', 'room_type'])['availability_365'].std().unstack()
    means.plot(kind='bar', yerr=stds, capsize=4, figsize=(10, 6), color=['#F0E442', '#56B4E9', '#FF9999'])
    plt.title('Average Availability by Room Type Across Neighborhoods')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Average Availability (365 days)')
    plt.legend(title='Room Type')
    plt.savefig('room_type_vs_availability.png')
    plt.show()
    plt.close()

# 4. Correlation Between Price and Number of Reviews
def plot_price_vs_reviews(df):
    room_type_markers = {
        'Entire home/apt': 'o',
        'Private room': '^',
        'Shared room': 's'
    }
    
    plt.figure(figsize=(10, 6))
    for room_type, marker in room_type_markers.items():
        subset = df[df['room_type'] == room_type]
        plt.scatter(subset['price'], subset['number_of_reviews'], alpha=0.6, edgecolors='w', label=room_type, marker=marker)
    
    plt.title('Correlation Between Price and Number of Reviews')
    plt.xlabel('Price')
    plt.ylabel('Number of Reviews')
    plt.legend(title='Room Type')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('price_vs_reviews.png')
    plt.show()
    plt.close()

# 5. Time Series Analysis of Reviews
def plot_time_series_reviews(df):
    df['last_review'] = pd.to_datetime(df['last_review'])
    df.set_index('last_review', inplace=True)
    reviews_by_month = df.groupby([df.index.to_period('M'), 'neighbourhood_group'])['number_of_reviews'].mean().unstack()
    reviews_by_month.rolling(window=3).mean().plot(figsize=(12, 6), linewidth=2)
    plt.title('Time Series Analysis of Reviews by Neighborhood Group')
    plt.xlabel('Time')
    plt.ylabel('Average Number of Reviews')
    plt.legend(title='Neighborhood Group')
    plt.savefig('time_series_reviews.png')
    plt.show()
    plt.close()

# 6. Price and Availability Heatmap
def plot_price_availability_heatmap(df):
    plt.figure(figsize=(10, 6))
    pivot = df.pivot_table(values='availability_365', index='neighbourhood_group', columns='price_category', aggfunc='mean')
    pivot = pivot[['Low', 'Medium', 'High']]  # Ensure the correct order
    plt.imshow(pivot, cmap='Reds', aspect='auto')
    plt.colorbar(label='Average Availability (Days)')
    plt.xticks(ticks=np.arange(len(pivot.columns)), labels=pivot.columns)
    plt.yticks(ticks=np.arange(len(pivot.index)), labels=pivot.index)
    plt.title('Heatmap of Price and Availability by Neighborhood Group')
    plt.xlabel('Price Category')
    plt.ylabel('Neighborhood Group')
    plt.tight_layout()
    plt.savefig('price_availability_heatmap.png')
    plt.show()
    plt.close()

# 7. Room Type and Review Count Analysis
def plot_room_type_reviews(df):
    grouped = df.groupby(['neighbourhood_group', 'room_type'])['number_of_reviews'].sum().unstack().fillna(0)
    grouped.plot(kind='bar', stacked=True, figsize=(10, 6), color=['#F0E442', '#56B4E9', '#FF9999'])
    plt.title('Review Count by Room Type Across Neighborhood Groups')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Total Number of Reviews')
    plt.legend(title='Room Type')
    plt.savefig('room_type_reviews.png')
    plt.show()
    plt.close()


# Preparation and Execution
with open('../pandas-tasks/Pandas_task1.py') as file:
    exec(file.read())
    
df = pd.read_csv('cleaned_airbnb_data.csv')

plot_neighborhood_distribution(df)
plot_price_distribution(df)
plot_room_type_vs_availability(df)
plot_price_vs_reviews(df)
plot_time_series_reviews(df)
plot_price_availability_heatmap(df)
plot_room_type_reviews(df)