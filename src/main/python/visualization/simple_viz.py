#!/usr/bin/env python3

"""
Simple visualization generator for Yelp Data Analysis
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Create output directory
OUTPUT_DIR = './visualization_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_sample_data():
    """Generate sample data for visualization"""
    
    np.random.seed(42)
    
    # Generate sample business data
    business_count = 1000
    business_data = {
        'business_id': [f'biz_{i}' for i in range(business_count)],
        'stars': np.random.normal(3.5, 1, business_count).clip(1, 5),
        'review_count': np.random.exponential(50, business_count).astype(int).clip(1, 1000),
        'is_open': np.random.binomial(1, 0.7, business_count),
    }
    
    # Add success indicator
    business_data['success'] = (business_data['stars'] >= 4.0).astype(int)
    
    return pd.DataFrame(business_data)

def create_visualizations():
    """Create basic visualizations"""
    
    # Generate sample data
    print("Generating sample data...")
    df = generate_sample_data()
    
    print("Creating visualizations...")
    
    # 1. Star rating distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(df['stars'], bins=9, kde=True)
    plt.title('Distribution of Star Ratings')
    plt.xlabel('Star Rating')
    plt.ylabel('Count')
    plt.savefig(os.path.join(OUTPUT_DIR, 'star_distribution.png'))
    plt.close()
    
    # 2. Success by review count
    plt.figure(figsize=(10, 6))
    sns.boxplot(x='success', y='review_count', data=df)
    plt.title('Review Count by Business Success')
    plt.xlabel('Success (1=Successful, 0=Not Successful)')
    plt.ylabel('Review Count')
    plt.savefig(os.path.join(OUTPUT_DIR, 'success_by_reviews.png'))
    plt.close()
    
    # 3. Stars vs review count scatter
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='stars', y='review_count', hue='success', data=df, alpha=0.6)
    plt.title('Stars vs Review Count')
    plt.xlabel('Star Rating')
    plt.ylabel('Review Count')
    plt.savefig(os.path.join(OUTPUT_DIR, 'stars_vs_reviews.png'))
    plt.close()
    
    print(f"All visualizations created in {OUTPUT_DIR}")

if __name__ == "__main__":
    create_visualizations()
