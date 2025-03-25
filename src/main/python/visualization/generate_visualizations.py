#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visualization Generator for Yelp Data Analysis
"""

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import MaxNLocator
import folium
from folium.plugins import MarkerCluster

# Set plot style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("viridis")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['figure.dpi'] = 100

# Create output directory
OUTPUT_DIR = './visualization_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_data():
    """Load processed data files"""
    
    data = {}
    data_paths = {
        'business': 'sample_data/business_analysis.json',
        'geographic': 'sample_data/geographic.json',
        'temporal': 'sample_data/temporal.json',
        'model': 'sample_data/model_data.json'
    }
    
    # Try loading the sample data
    for key, path in data_paths.items():
        try:
            # In a real scenario, you'd load from HDFS
            # Here we're using local sample data
            data[key] = pd.read_json(path, lines=True)
            print(f"Loaded {key} data: {len(data[key])} records")
        except Exception as e:
            print(f"Error loading {key} data: {e}")
            # Create empty DataFrame if file doesn't exist
            data[key] = pd.DataFrame()
    
    # If no data was loaded, create sample data
    if all(df.empty for df in data.values()):
        print("Creating sample data for visualization...")
        data = generate_sample_data()
    
    return data

def generate_sample_data():
    """Generate sample data for visualization if real data is not available"""
    
    np.random.seed(42)
    
    # Generate sample business data
    business_count = 1000
    business_data = {
        'business_id': [f'biz_{i}' for i in range(business_count)],
        'name': [f'Business {i}' for i in range(business_count)],
        'stars': np.random.normal(3.5, 1, business_count).clip(1, 5),
        'review_count': np.random.exponential(50, business_count).astype(int).clip(1, 1000),
        'is_open': np.random.binomial(1, 0.7, business_count),
        'has_wifi': np.random.binomial(1, 0.6, business_count),
        'price_level': np.random.choice([1, 2, 3, 4], business_count, p=[0.3, 0.4, 0.2, 0.1]),
        'noise_level': np.random.choice([1, 2, 3], business_count, p=[0.3, 0.5, 0.2]),
        'takes_reservations': np.random.binomial(1, 0.4, business_count)
    }
    
    # Add success indicator
    business_data['success'] = (business_data['stars'] >= 4.0).astype(int)
    
    # Generate sample geographic data
    state_counts = {
        'AZ': 300, 'NV': 200, 'OH': 150, 'PA': 100, 
        'CA': 80, 'FL': 70, 'NY': 50, 'TX': 50
    }
    
    geo_data = {
        'business_id': business_data['business_id'],
        'state': np.random.choice(
            list(state_counts.keys()),
            business_count,
            p=[count/sum(state_counts.values()) for count in state_counts.values()]
        ),
        'latitude': np.random.uniform(32, 42, business_count),
        'longitude': np.random.uniform(-120, -80, business_count)
    }
    
    # Generate sample temporal data
    temporal_data = {
        'business_id': business_data['business_id'],
        'avg_growth_rate': np.random.normal(0.1, 0.3, business_count),
        'avg_star_change': np.random.normal(0, 0.2, business_count),
        'years_with_data': np.random.randint(1, 6, business_count)
    }
    
    # Feature importance data
    model_data = {
        'feature': ['review_count', 'price_level', 'has_wifi', 'noise_level', 
                   'takes_reservations', 'years_with_data', 'avg_growth_rate'],
        'importance': [0.35, 0.25, 0.15, 0.1, 0.08, 0.05, 0.02]
    }
    
    return {
        'business': pd.DataFrame(business_data),
        'geographic': pd.DataFrame(geo_data),
        'temporal': pd.DataFrame(temporal_data),
        'model': pd.DataFrame(model_data)
    }

def create_business_visualizations(data):
    """Create visualizations for business success factors"""
    
    business_df = data['business']
    
    print("Creating business visualizations...")
    
    # 1. Star Rating Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(business_df['stars'], bins=9, kde=True)
    plt.title('Distribution of Business Star Ratings', fontsize=16)
    plt.xlabel('Star Rating', fontsize=14)
    plt.ylabel('Count', fontsize=14)
    plt.axvline(x=4.0, color='red', linestyle='--', 
                label='Success Threshold (4+ stars)')
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR, 'star_rating_distribution.png'))
    plt.close()
    
    # 2. Review Count vs. Stars
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=business_df, x='review_count', y='stars', 
                   alpha=0.6, hue='is_open')
    plt.title('Review Count vs. Star Rating', fontsize=16)
    plt.xlabel('Number of Reviews', fontsize=14)
    plt.ylabel('Star Rating', fontsize=14)
    plt.xscale('log')  # Log scale for better visualization
    plt.savefig(os.path.join(OUTPUT_DIR, 'review_count_vs_stars.png'))
    plt.close()
    
    # 3. Price Level vs. Success Rate
    if 'price_level' in business_df.columns:
        # Group by price level and calculate success rate
        price_success = business_df.groupby('price_level')['success'].mean().reset_index()
        price_counts = business_df.groupby('price_level').size().reset_index(name='count')
        price_data = pd.merge(price_success, price_counts, on='price_level')
        
        plt.figure(figsize=(10, 6))
        ax = sns.barplot(data=price_data, x='price_level', y='success')
        plt.title('Success Rate by Price Level', fontsize=16)
        plt.xlabel('Price Level ($ to $$$$)', fontsize=14)
        plt.ylabel('Success Rate (% with 4+ stars)', fontsize=14)
        plt.ylim(0, 1)
        
        # Add count labels
        for i, p in enumerate(ax.patches):
            ax.annotate(f'n={price_data.iloc[i]["count"]}', 
                       (p.get_x() + p.get_width() / 2., p.get_height()), 
                       ha = 'center', va = 'bottom', fontsize=10)
        
        plt.savefig(os.path.join(OUTPUT_DIR, 'price_level_success.png'))
        plt.close()
    
    # 4. WiFi Availability vs. Stars
    if 'has_wifi' in business_df.columns:
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=business_df, x='has_wifi', y='stars')
        plt.title('WiFi Availability vs. Star Rating', fontsize=16)
        plt.xlabel('WiFi Available', fontsize=14)
        plt.ylabel('Star Rating', fontsize=14)
        plt.xticks([0, 1], ['No', 'Yes'])
        plt.savefig(os.path.join(OUTPUT_DIR, 'wifi_vs_stars.png'))
        plt.close()
    
    # 5. Noise Level vs. Success Rate
    if 'noise_level' in business_df.columns:
        noise_success = business_df.groupby('noise_level')['success'].mean().reset_index()
        noise_counts = business_df.groupby('noise_level').size().reset_index(name='count')
        noise_data = pd.merge(noise_success, noise_counts, on='noise_level')
        
        plt.figure(figsize=(10, 6))
        ax = sns.barplot(data=noise_data, x='noise_level', y='success')
        plt.title('Success Rate by Noise Level', fontsize=16)
        plt.xlabel('Noise Level (1=quiet, 2=average, 3=loud)', fontsize=14)
        plt.ylabel('Success Rate (% with 4+ stars)', fontsize=14)
        plt.ylim(0, 1)
        
        # Add count labels
        for i, p in enumerate(ax.patches):
            ax.annotate(f'n={noise_data.iloc[i]["count"]}', 
                       (p.get_x() + p.get_width() / 2., p.get_height()), 
                       ha = 'center', va = 'bottom', fontsize=10)
        
        plt.savefig(os.path.join(OUTPUT_DIR, 'noise_level_success.png'))
        plt.close()

def create_geographic_visualizations(data):
    """Create geographic visualizations"""
    
    business_df = data['business']
    geo_df = data['geographic']
    
    print("Creating geographic visualizations...")
    
    # Merge business and geographic data if needed
    if not geo_df.empty and 'business_id' in geo_df.columns:
        geo_data = pd.merge(business_df, geo_df, on='business_id')
    else:
        geo_data = business_df  # Use business data if geo data is not available
    
    # 1. Success Rate by State
    if 'state' in geo_data.columns:
        # Group by state and calculate metrics
        state_metrics = geo_data.groupby('state').agg({
            'business_id': 'count',
            'stars': 'mean',
            'success': 'mean'
        }).reset_index()
        
        state_metrics.columns = ['state', 'business_count', 'avg_stars', 'success_rate']
        state_metrics = state_metrics.sort_values('business_count', ascending=False)
        
        # Get top 10 states by business count
        top_states = state_metrics.head(10)
        
        plt.figure(figsize=(12, 8))
        ax = sns.barplot(data=top_states, x='state', y='success_rate')
        plt.title('Success Rate by State (Top 10 States by Business Count)', fontsize=16)
        plt.xlabel('State', fontsize=14)
        plt.ylabel('Success Rate (% with 4+ stars)', fontsize=14)
        plt.ylim(0, 1)
        
        # Add count labels
        for i, p in enumerate(ax.patches):
            ax.annotate(f'n={int(top_states.iloc[i]["business_count"])}', 
                       (p.get_x() + p.get_width() / 2., p.get_height()), 
                       ha = 'center', va = 'bottom', fontsize=10)
        
        plt.savefig(os.path.join(OUTPUT_DIR, 'state_success_rate.png'))
        plt.close()
    
    # 2. Map Visualization
    if all(col in geo_data.columns for col in ['latitude', 'longitude', 'stars']):
        # Create a sample of businesses for the map (to avoid overcrowding)
        map_data = geo_data.sample(min(1000, len(geo_data)))
        
        # Create map centered at the average coordinates
        center_lat = map_data['latitude'].mean()
        center_lon = map_data['longitude'].mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=5)
        
        # Create a marker cluster for better visualization
        marker_cluster = MarkerCluster().add_to(m)
        
        # Add markers for each business
        for idx, row in map_data.iterrows():
            # Color based on star rating
            if row['stars'] >= 4.0:
                color = 'green'
            elif row['stars'] >= 3.0:
                color = 'blue'
            else:
                color = 'red'
            
            # Create popup content
            popup_content = f"""
            <b>{row.get('name', 'Business')}</b><br>
            Stars: {row['stars']}<br>
            Reviews: {row.get('review_count', 'N/A')}<br>
            Open: {'Yes' if row.get('is_open', 0) == 1 else 'No'}
            """
            
            # Add marker
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_content, max_width=300),
                icon=folium.Icon(color=color)
            ).add_to(marker_cluster)
        
        # Save map
        m.save(os.path.join(OUTPUT_DIR, 'business_map.html'))

def create_temporal_visualizations(data):
    """Create visualizations for temporal patterns"""
    
    business_df = data['business']
    temporal_df = data['temporal']
    
    print("Creating temporal visualizations...")
    
    # Merge business and temporal data if needed
    if not temporal_df.empty and 'business_id' in temporal_df.columns:
        # Join data
        temporal_data = pd.merge(business_df, temporal_df, on='business_id')
        
        # 1. Growth Rate vs. Star Change
        if all(col in temporal_data.columns for col in ['avg_growth_rate', 'avg_star_change']):
            plt.figure(figsize=(10, 6))
            sns.scatterplot(
                data=temporal_data, 
                x='avg_growth_rate', 
                y='avg_star_change',
                hue='stars',
                palette='viridis',
                alpha=0.7
            )
            plt.title('Review Growth Rate vs. Star Rating Change', fontsize=16)
            plt.xlabel('Average Annual Growth Rate in Review Count', fontsize=14)
            plt.ylabel('Average Annual Change in Star Rating', fontsize=14)
            plt.axhline(y=0, color='red', linestyle='--')
            plt.axvline(x=0, color='red', linestyle='--')
            plt.savefig(os.path.join(OUTPUT_DIR, 'growth_vs_star_change.png'))
            plt.close()
        
        # 2. Success Rate by Years of Data
        if 'years_with_data' in temporal_data.columns:
            years_success = temporal_data.groupby('years_with_data').agg({
                'business_id': 'count',
                'success': 'mean'
            }).reset_index()
            
            years_success.columns = ['years_with_data', 'business_count', 'success_rate']
            
            plt.figure(figsize=(10, 6))
            ax = sns.barplot(data=years_success, x='years_with_data', y='success_rate')
            plt.title('Success Rate by Years of Operation', fontsize=16)
            plt.xlabel('Years with Review Data', fontsize=14)
            plt.ylabel('Success Rate (% with 4+ stars)', fontsize=14)
            plt.ylim(0, 1)
            
            # Add count labels
            for i, p in enumerate(ax.patches):
                ax.annotate(f'n={int(years_success.iloc[i]["business_count"])}', 
                           (p.get_x() + p.get_width() / 2., p.get_height()), 
                           ha = 'center', va = 'bottom', fontsize=10)
            
            plt.savefig(os.path.join(OUTPUT_DIR, 'success_by_years.png'))
            plt.close()
    
    # 3. Simulated Business Review Growth Trajectories
    # This is a simulated visualization without actual data
    plt.figure(figsize=(12, 8))
    
    # Generate simulated data for different business trajectories
    years = np.arange(1, 6)
    
    # Successful business trajectory
    successful_business = 50 * np.exp(0.5 * years)
    plt.plot(years, successful_business, 'g-', linewidth=3, label='High Growth Business')
    
    # Average business trajectory
    average_business = 30 * np.exp(0.2 * years)
    plt.plot(years, average_business, 'b-', linewidth=3, label='Moderate Growth Business')
    
    # Struggling business trajectory
    struggling_business = 20 * np.exp(0.05 * years)
    plt.plot(years, struggling_business, 'r-', linewidth=3, label='Low Growth Business')
    
    plt.title('Typical Review Growth Trajectories', fontsize=16)
    plt.xlabel('Years of Operation', fontsize=14)
    plt.ylabel('Cumulative Review Count', fontsize=14)
    plt.legend(fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(OUTPUT_DIR, 'review_growth_trajectories.png'))
    plt.close()

def create_model_visualizations(data):
    """Create visualizations for predictive models"""
    
    print("Creating model visualizations...")
    
    # 1. Feature Importance for Star Rating Prediction
    # Either use real model data or the synthetic feature importance
    if 'model' in data and not data['model'].empty:
        model_df = data['model']
        if 'feature' in model_df.columns and 'importance' in model_df.columns:
            # Sort by importance
            model_df = model_df.sort_values('importance', ascending=True)
            
            plt.figure(figsize=(12, 8))
            ax = sns.barplot(data=model_df, y='feature', x='importance')
            plt.title('Feature Importance for Business Success Prediction', fontsize=16)
            plt.xlabel('Importance', fontsize=14)
            plt.ylabel('Feature', fontsize=14)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'feature_importance.png'))
            plt.close()
    
    # 2. Success Rate by Feature Combinations (simulated)
    # Create a synthetic visualization showing success rates for different
    # combinations of key features
    
    # Synthetic data for feature combinations
    feature_combos = [
        "High Reviews + WiFi + Low Price",
        "High Reviews + WiFi + High Price",
        "High Reviews + No WiFi + Low Price",
        "High Reviews + No WiFi + High Price",
        "Low Reviews + WiFi + Low Price",
        "Low Reviews + WiFi + High Price",
        "Low Reviews + No WiFi + Low Price",
        "Low Reviews + No WiFi + High Price"
    ]
    
    success_rates = [0.75, 0.68, 0.55, 0.48, 0.42, 0.38, 0.25, 0.18]
    
    plt.figure(figsize=(14, 8))
    ax = sns.barplot(x=success_rates, y=feature_combos)
    plt.title('Business Success Rate by Feature Combinations', fontsize=16)
    plt.xlabel('Success Rate (% with 4+ stars)', fontsize=14)
    plt.ylabel('Feature Combination', fontsize=14)
    plt.xlim(0, 1)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'success_by_features.png'))
    plt.close()
    
    # 3. Predicted vs. Actual Star Ratings (simulated)
    # Create synthetic data for predicted vs. actual scatter plot
    np.random.seed(42)
    actual_stars = np.random.uniform(1, 5, 200)
    predicted_stars = actual_stars + np.random.normal(0, 0.5, 200)
    predicted_stars = np.clip(predicted_stars, 1, 5)
    
    plt.figure(figsize=(10, 10))
    plt.scatter(actual_stars, predicted_stars, alpha=0.6)
    plt.plot([1, 5], [1, 5], 'r--')  # Diagonal line for perfect predictions
    plt.title('Predicted vs. Actual Star Ratings', fontsize=16)
    plt.xlabel('Actual Star Rating', fontsize=14)
    plt.ylabel('Predicted Star Rating', fontsize=14)
    plt.xlim(1, 5)
    plt.ylim(1, 5)
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(OUTPUT_DIR, 'predicted_vs_actual.png'))
    plt.close()

def main():
    """Main function to generate all visualizations"""
    
    print("Starting visualization generation...")
    
    # Load data
    data = load_data()
    
    # Create visualizations
    create_business_visualizations(data)
    create_geographic_visualizations(data)
    create_temporal_visualizations(data)
    create_model_visualizations(data)
    
    print(f"All visualizations generated in {OUTPUT_DIR}")

if __name__ == "__main__":
    main()