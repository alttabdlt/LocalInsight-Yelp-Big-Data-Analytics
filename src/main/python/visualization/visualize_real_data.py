#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visualization Generator for Actual Yelp Data Analysis Results
This script reads the actual output from Spark jobs and creates visualizations
"""

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from folium.plugins import MarkerCluster

# Create output directory
OUTPUT_DIR = './visualization_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_data():
    """Load actual data from extracted HDFS outputs"""
    
    print("Loading data from extracted files...")
    data = {}
    local_paths = {
        'business': './data_for_viz/business_analysis.json',
        'geographic': './data_for_viz/geographic.json',
        'temporal': './data_for_viz/temporal.json',
        'model': './data_for_viz/model_data.json'
    }
    
    # Load data from local files
    for key, path in local_paths.items():
        try:
            # Check if file exists and has content
            if os.path.exists(path) and os.path.getsize(path) > 0:
                # Read JSON lines file
                with open(path, 'r') as f:
                    lines = f.readlines()
                
                # Parse each line as JSON and create DataFrame
                records = []
                for line in lines:
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError:
                        pass
                
                # Convert to pandas DataFrame
                pandas_df = pd.DataFrame(records)
                print(f"Loaded {key} data: {len(pandas_df)} records")
                data[key] = pandas_df
            else:
                print(f"No data found for {key}")
                data[key] = pd.DataFrame()
        except Exception as e:
            print(f"Error loading {key} data: {e}")
            # Create empty DataFrame if file doesn't exist or error occurs
            data[key] = pd.DataFrame()
    
    # Load geojson if available
    try:
        geojson_path = './data_for_viz/geojson.json'
        if os.path.exists(geojson_path) and os.path.getsize(geojson_path) > 0:
            with open(geojson_path, 'r') as f:
                lines = f.readlines()
                
            # There should be only one geojson object in the file
            if lines:
                geojson_record = json.loads(lines[0].strip())
                if 'geojson' in geojson_record:
                    data['geojson'] = json.loads(geojson_record['geojson'])
                    print("Loaded GeoJSON data")
                else:
                    data['geojson'] = None
        else:
            data['geojson'] = None
    except Exception as e:
        print(f"Error loading GeoJSON: {e}")
        data['geojson'] = None
    
    return data

def create_business_visualizations(data):
    """Create visualizations for business analysis data"""
    
    print("Creating business visualizations...")
    
    if data['business'].empty:
        print("No business data available to visualize")
        return
    
    # Get business DataFrame
    business_df = data['business']
    
    # 1. Star rating distribution
    plt.figure(figsize=(12, 7))
    if 'stars' in business_df.columns:
        sns.histplot(business_df['stars'], bins=9, kde=True)
        plt.title('Distribution of Business Star Ratings', fontsize=15)
        plt.xlabel('Star Rating', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'star_distribution.png'))
        plt.close()
    
    # 2. Review count distribution
    if 'review_count' in business_df.columns:
        plt.figure(figsize=(12, 7))
        # Use log scale for review counts
        sns.histplot(business_df['review_count'].clip(upper=1000), bins=30, log_scale=True)
        plt.title('Distribution of Review Counts (log scale)', fontsize=15)
        plt.xlabel('Number of Reviews', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'review_count_distribution.png'))
        plt.close()
    
    # 3. Open vs. Closed business comparison
    if 'is_open' in business_df.columns:
        # Convert to categorical for better visualization
        business_df['Business Status'] = business_df['is_open'].map({1: 'Open', 0: 'Closed'})
        
        plt.figure(figsize=(12, 7))
        sns.boxplot(x='Business Status', y='stars', data=business_df)
        plt.title('Star Ratings: Open vs. Closed Businesses', fontsize=15)
        plt.xlabel('Business Status', fontsize=12)
        plt.ylabel('Star Rating', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'open_vs_closed_stars.png'))
        plt.close()
    
    # 4. Success indicator (if available)
    if 'success' in business_df.columns:
        # Create success categories
        business_df['Success Category'] = business_df['success'].map(
            {1: 'Successful', 0: 'Not Successful'}
        )
        
        plt.figure(figsize=(12, 7))
        sns.countplot(x='Success Category', data=business_df)
        plt.title('Count of Successful vs. Unsuccessful Businesses', fontsize=15)
        plt.xlabel('Success Category', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'business_success_counts.png'))
        plt.close()
        
        # Average review count by success category
        plt.figure(figsize=(12, 7))
        sns.boxplot(x='Success Category', y='review_count', data=business_df)
        plt.title('Review Counts by Business Success', fontsize=15)
        plt.xlabel('Success Category', fontsize=12)
        plt.ylabel('Review Count', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'success_by_reviews.png'))
        plt.close()

def create_geographic_visualizations(data):
    """Create geographic visualizations"""
    
    print("Creating geographic visualizations...")
    
    if data['geographic'].empty:
        print("No geographic data available to visualize")
        return
    
    # Get geographic DataFrame
    geo_df = data['geographic']
    
    # 1. Create a map of businesses if latitude and longitude are available
    if 'latitude' in geo_df.columns and 'longitude' in geo_df.columns:
        # Sample data if too large
        map_df = geo_df
        if len(geo_df) > 1000:
            map_df = geo_df.sample(1000, random_state=42)
        
        # Create map centered at the mean coordinates
        center_lat = map_df['latitude'].mean()
        center_lon = map_df['longitude'].mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=4)
        
        # Add a marker cluster
        marker_cluster = MarkerCluster().add_to(m)
        
        # Add points
        for idx, row in map_df.iterrows():
            try:
                # Skip if missing coordinates
                if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                    continue
                
                # Format popup content
                popup_content = f"""
                <b>{row.get('name', 'Unknown')}</b><br>
                Stars: {row.get('stars', 'N/A')}<br>
                Reviews: {row.get('review_count', 'N/A')}<br>
                City: {row.get('city', 'Unknown')}, {row.get('state', 'Unknown')}
                """
                
                # Set color based on star rating
                stars = row.get('stars', 0)
                if stars >= 4.5:
                    color = 'darkgreen'
                elif stars >= 4.0:
                    color = 'green'
                elif stars >= 3.0:
                    color = 'orange'
                else:
                    color = 'red'
                
                # Add marker
                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=folium.Popup(popup_content, max_width=200),
                    icon=folium.Icon(color=color),
                ).add_to(marker_cluster)
            except Exception as e:
                # Skip points that cause errors
                continue
        
        # Save the map
        m.save(os.path.join(OUTPUT_DIR, 'business_map.html'))
    
    # 2. If geojson is available, create a direct visualization
    if data['geojson'] is not None:
        try:
            # Create a map using the GeoJSON
            m_geojson = folium.Map(location=[37, -100], zoom_start=4)
            
            # Add GeoJSON features
            for feature in data['geojson']['features']:
                try:
                    coords = feature['geometry']['coordinates']
                    props = feature['properties']
                    
                    # Set color based on star rating
                    stars = props.get('stars', 0)
                    if stars >= 4.5:
                        color = 'darkgreen'
                    elif stars >= 4.0:
                        color = 'green'
                    elif stars >= 3.0:
                        color = 'orange'
                    else:
                        color = 'red'
                    
                    # Format popup content
                    popup_content = f"""
                    <b>{props.get('name', 'Unknown')}</b><br>
                    Stars: {stars}<br>
                    Reviews: {props.get('review_count', 'N/A')}<br>
                    City: {props.get('city', 'Unknown')}, {props.get('state', 'Unknown')}
                    """
                    
                    # Add marker
                    folium.Marker(
                        location=[coords[1], coords[0]],
                        popup=folium.Popup(popup_content, max_width=200),
                        icon=folium.Icon(color=color),
                    ).add_to(m_geojson)
                except Exception as e:
                    # Skip features that cause errors
                    continue
            
            # Save the map
            m_geojson.save(os.path.join(OUTPUT_DIR, 'geojson_map.html'))
        except Exception as e:
            print(f"Error creating GeoJSON map: {e}")
    
    # 3. Create state-based visualizations if state data is available
    if 'state' in geo_df.columns:
        # Business count by state
        state_counts = geo_df['state'].value_counts().reset_index()
        state_counts.columns = ['State', 'Count']
        state_counts = state_counts.sort_values('Count', ascending=False).head(15)
        
        plt.figure(figsize=(12, 8))
        sns.barplot(x='Count', y='State', data=state_counts)
        plt.title('Business Count by State (Top 15)', fontsize=15)
        plt.xlabel('Number of Businesses', fontsize=12)
        plt.ylabel('State', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'business_by_state.png'))
        plt.close()
        
        # Star ratings by state
        if 'stars' in geo_df.columns:
            plt.figure(figsize=(12, 8))
            state_stars = geo_df.groupby('state')['stars'].mean().reset_index()
            state_stars.columns = ['State', 'Average Stars']
            state_stars = state_stars.sort_values('Average Stars', ascending=False).head(15)
            
            sns.barplot(x='Average Stars', y='State', data=state_stars)
            plt.title('Average Star Rating by State (Top 15)', fontsize=15)
            plt.xlabel('Average Stars', fontsize=12)
            plt.ylabel('State', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'stars_by_state.png'))
            plt.close()

def create_temporal_visualizations(data):
    """Create visualizations for temporal data"""
    
    print("Creating temporal visualizations...")
    
    if data['temporal'].empty:
        print("No temporal data available to visualize")
        return
    
    # Get temporal DataFrame
    temporal_df = data['temporal']
    
    # 1. Growth rate distribution if available
    if 'avg_growth_rate' in temporal_df.columns:
        plt.figure(figsize=(12, 7))
        # Filter out extreme outliers
        growth_data = temporal_df['avg_growth_rate'].clip(lower=-1, upper=2)
        sns.histplot(growth_data, bins=30, kde=True)
        plt.axvline(x=0, color='red', linestyle='--', alpha=0.7)
        plt.title('Distribution of Business Growth Rates', fontsize=15)
        plt.xlabel('Average Growth Rate', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'growth_rate_distribution.png'))
        plt.close()
    
    # 2. Star change distribution if available
    if 'avg_star_change' in temporal_df.columns:
        plt.figure(figsize=(12, 7))
        # Filter out extreme outliers
        star_change_data = temporal_df['avg_star_change'].clip(lower=-1, upper=1)
        sns.histplot(star_change_data, bins=30, kde=True)
        plt.axvline(x=0, color='red', linestyle='--', alpha=0.7)
        plt.title('Distribution of Star Rating Changes Over Time', fontsize=15)
        plt.xlabel('Average Star Change', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'star_change_distribution.png'))
        plt.close()
    
    # 3. Correlation between star change and growth rate if both available
    if 'avg_growth_rate' in temporal_df.columns and 'avg_star_change' in temporal_df.columns:
        plt.figure(figsize=(12, 7))
        # Filter out extreme outliers for better visualization
        plot_df = temporal_df.copy()
        plot_df['avg_growth_rate'] = plot_df['avg_growth_rate'].clip(lower=-1, upper=2)
        plot_df['avg_star_change'] = plot_df['avg_star_change'].clip(lower=-1, upper=1)
        
        sns.scatterplot(x='avg_star_change', y='avg_growth_rate', data=plot_df, alpha=0.5)
        plt.title('Relationship Between Star Changes and Growth Rate', fontsize=15)
        plt.xlabel('Average Star Change', fontsize=12)
        plt.ylabel('Average Growth Rate', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'star_change_vs_growth.png'))
        plt.close()

def create_model_visualizations(data):
    """Create visualizations for predictive model data"""
    
    print("Creating model visualizations...")
    
    if data['model'].empty:
        print("No model data available to visualize")
        return
    
    model_df = data['model']
    
    # 1. Feature importance visualization if available
    if 'feature' in model_df.columns and 'importance' in model_df.columns:
        # Sort by importance
        feature_importance = model_df[['feature', 'importance']].sort_values('importance', ascending=False)
        
        plt.figure(figsize=(12, 8))
        sns.barplot(x='importance', y='feature', data=feature_importance)
        plt.title('Feature Importance for Business Success Prediction', fontsize=15)
        plt.xlabel('Importance', fontsize=12)
        plt.ylabel('Feature', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'feature_importance.png'))
        plt.close()
    
    # 2. Success prediction distribution if available
    if 'success' in model_df.columns and ('stars' in model_df.columns or 'prediction' in model_df.columns):
        # Calculate success rate by star rating if 'stars' is available
        if 'stars' in model_df.columns:
            plt.figure(figsize=(12, 8))
            success_by_stars = model_df.groupby('stars')['success'].mean().reset_index()
            
            sns.lineplot(x='stars', y='success', data=success_by_stars, marker='o')
            plt.title('Success Rate by Star Rating', fontsize=15)
            plt.xlabel('Star Rating', fontsize=12)
            plt.ylabel('Success Rate', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'success_by_stars.png'))
            plt.close()
        
        # Compare actual vs predicted success if 'prediction' is available
        if 'prediction' in model_df.columns:
            plt.figure(figsize=(12, 8))
            
            # Create confusion matrix
            conf_matrix = pd.crosstab(model_df['success'], model_df['prediction'])
            
            # Plot heatmap
            sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues')
            plt.title('Confusion Matrix: Actual vs Predicted Success', fontsize=15)
            plt.xlabel('Predicted', fontsize=12)
            plt.ylabel('Actual', fontsize=12)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'success_prediction_matrix.png'))
            plt.close()

def main():
    """Main function to generate all visualizations from real data"""
    
    print("Starting visualization generation from real data...")
    
    # Load real data from local files (extracted from HDFS)
    data = load_data()
    
    # Create visualizations
    create_business_visualizations(data)
    create_geographic_visualizations(data)
    create_temporal_visualizations(data)
    create_model_visualizations(data)
    
    print(f"All visualizations generated in {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
