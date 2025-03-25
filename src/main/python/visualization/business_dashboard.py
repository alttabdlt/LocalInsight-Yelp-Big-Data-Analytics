#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Interactive Streamlit Dashboard for Yelp Business Analysis
This dashboard allows users to explore insights from the Yelp data analysis.
"""

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go

# Set page configuration
st.set_page_config(
    page_title="Yelp Business Insights Dashboard",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Create data directory path
DATA_DIR = './data_for_viz'

@st.cache_data
def load_data():
    """Load data from HDFS-extracted files with caching for performance"""
    
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_for_viz")
    
    # Check if data directory exists
    if not os.path.exists(data_dir):
        st.error(f"Data directory not found: {data_dir}")
        return {
            'business': pd.DataFrame(),
            'geographic': pd.DataFrame(),
            'temporal': pd.DataFrame(),
            'model': pd.DataFrame()
        }
    
    # Load each dataset
    data = {}
    
    # Business analysis data
    try:
        business_file = os.path.join(data_dir, "business_analysis.json")
        if os.path.exists(business_file):
            data['business'] = pd.read_json(business_file, lines=True)
        else:
            data['business'] = pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading business data: {e}")
        data['business'] = pd.DataFrame()
    
    # Geographic data
    try:
        geo_file = os.path.join(data_dir, "geographic.json")
        if os.path.exists(geo_file):
            data['geographic'] = pd.read_json(geo_file, lines=True)
        else:
            data['geographic'] = pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading geographic data: {e}")
        data['geographic'] = pd.DataFrame()
    
    # Temporal data
    try:
        temporal_file = os.path.join(data_dir, "temporal.json")
        if os.path.exists(temporal_file):
            data['temporal'] = pd.read_json(temporal_file, lines=True)
        else:
            data['temporal'] = pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading temporal data: {e}")
        data['temporal'] = pd.DataFrame()
    
    # Model data - first try the transformed file, then fall back to original
    try:
        model_file_transformed = os.path.join(data_dir, "model_data_transformed.json")
        model_file = os.path.join(data_dir, "model_data.json")
        
        if os.path.exists(model_file_transformed):
            data['model'] = pd.read_json(model_file_transformed, lines=True)
        elif os.path.exists(model_file):
            data['model'] = pd.read_json(model_file, lines=True)
        else:
            data['model'] = pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading model data: {e}")
        data['model'] = pd.DataFrame()
    
    # Load geojson if available
    try:
        geojson_path = os.path.join(data_dir, 'geojson.json')
        if os.path.exists(geojson_path) and os.path.getsize(geojson_path) > 0:
            with open(geojson_path, 'r') as f:
                lines = f.readlines()
                
            # There should be only one geojson object in the file
            if lines:
                geojson_record = json.loads(lines[0].strip())
                if 'geojson' in geojson_record:
                    data['geojson'] = json.loads(geojson_record['geojson'])
                else:
                    data['geojson'] = None
        else:
            data['geojson'] = None
    except Exception as e:
        st.error(f"Error loading GeoJSON: {e}")
        data['geojson'] = None
    
    return data

def create_business_analytics(data):
    """Business analytics section of the dashboard"""
    
    st.header("Business Analytics")
    
    if data['business'].empty:
        st.warning("No business data available to analyze")
        return
    
    # Get business DataFrame
    business_df = data['business']
    
    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Count of businesses
    with col1:
        st.metric("Total Businesses", f"{len(business_df):,}")
    
    # Average star rating
    if 'stars' in business_df.columns:
        with col2:
            avg_stars = business_df['stars'].mean()
            st.metric("Average Rating", f"{avg_stars:.2f}⭐")
    
    # Open businesses percentage
    if 'is_open' in business_df.columns:
        with col3:
            open_pct = (business_df['is_open'] == 1).mean() * 100
            st.metric("Open Businesses", f"{open_pct:.1f}%")
    
    # Success rate if available
    if 'success' in business_df.columns:
        with col4:
            success_rate = business_df['success'].mean() * 100
            st.metric("Success Rate", f"{success_rate:.1f}%")
    
    # Create tabs for different visualizations
    tabs = st.tabs(["Ratings", "Reviews", "Success Factors"])
    
    # Tab 1: Ratings Distribution
    with tabs[0]:
        if 'stars' in business_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Star rating distribution
                st.subheader("Star Rating Distribution")
                fig = px.histogram(
                    business_df, 
                    x='stars', 
                    nbins=9,
                    title="Distribution of Business Star Ratings",
                    labels={'stars': 'Star Rating', 'count': 'Number of Businesses'},
                    color_discrete_sequence=['#1f77b4']
                )
                fig.update_layout(bargap=0.1)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Star ratings by business status if available
                if 'is_open' in business_df.columns:
                    st.subheader("Ratings by Business Status")
                    business_df['Status'] = business_df['is_open'].map({1: 'Open', 0: 'Closed'})
                    
                    fig = px.box(
                        business_df,
                        x='Status',
                        y='stars',
                        title="Star Ratings: Open vs. Closed Businesses",
                        labels={'Status': 'Business Status', 'stars': 'Star Rating'},
                        color='Status',
                        color_discrete_map={'Open': '#2ca02c', 'Closed': '#d62728'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
    # Tab 2: Review Analysis
    with tabs[1]:
        if 'review_count' in business_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Review count distribution (log scale)
                st.subheader("Review Count Distribution")
                
                # Calculate logarithm of review count for better visualization
                business_df['log_review_count'] = np.log1p(business_df['review_count'])
                
                fig = px.histogram(
                    business_df,
                    x='log_review_count',
                    nbins=30,
                    title="Distribution of Review Counts (log scale)",
                    labels={'log_review_count': 'Log(Review Count + 1)', 'count': 'Number of Businesses'},
                    color_discrete_sequence=['#ff7f0e']
                )
                fig.update_layout(bargap=0.1)
                st.plotly_chart(fig, use_container_width=True)
                
                # Add note about log scale
                st.caption("Note: X-axis is log-transformed for better visualization")
            
            with col2:
                # Review count by stars
                st.subheader("Review Count by Star Rating")
                
                review_by_stars = business_df.groupby('stars')['review_count'].mean().reset_index()
                
                fig = px.bar(
                    review_by_stars,
                    x='stars',
                    y='review_count',
                    title="Average Review Count by Star Rating",
                    labels={'stars': 'Star Rating', 'review_count': 'Average Review Count'},
                    color_discrete_sequence=['#ff7f0e']
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Tab 3: Success Factors
    with tabs[2]:
        if 'success' in business_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Success rate by star rating
                st.subheader("Success Rate by Rating")
                
                success_by_stars = business_df.groupby('stars')['success'].mean().reset_index()
                success_by_stars['success_pct'] = success_by_stars['success'] * 100
                
                fig = px.line(
                    success_by_stars,
                    x='stars',
                    y='success_pct',
                    title="Success Rate by Star Rating",
                    labels={'stars': 'Star Rating', 'success_pct': 'Success Rate (%)'},
                    markers=True,
                    color_discrete_sequence=['#2ca02c']
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Review count by success
                st.subheader("Reviews vs Success")
                
                business_df['Success Category'] = business_df['success'].map({1: 'Successful', 0: 'Not Successful'})
                
                fig = px.box(
                    business_df,
                    x='Success Category',
                    y='review_count',
                    title="Review Counts by Business Success",
                    labels={'Success Category': 'Success Category', 'review_count': 'Review Count'},
                    color='Success Category',
                    color_discrete_map={'Successful': '#2ca02c', 'Not Successful': '#d62728'}
                )
                fig.update_layout(yaxis_type="log")  # Log scale for better visualization
                st.plotly_chart(fig, use_container_width=True)
                
                # Add note about log scale
                st.caption("Note: Y-axis is log-transformed for better visualization")

def create_geographic_analytics(data):
    """Geographic analytics section of the dashboard"""
    
    st.header("Geographic Analysis")
    
    if data['geographic'].empty:
        st.warning("No geographic data available to analyze")
        return
    
    # Get geographic DataFrame
    geo_df = data['geographic']
    
    # State and city analysis
    if 'state' in geo_df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            # Business count by state (top 10)
            st.subheader("Top States by Business Count")
            
            state_counts = geo_df['state'].value_counts().reset_index()
            state_counts.columns = ['State', 'Count']
            top_states = state_counts.sort_values('Count', ascending=False).head(10)
            
            fig = px.bar(
                top_states,
                y='State',
                x='Count',
                title="Business Count by State (Top 10)",
                labels={'State': 'State', 'Count': 'Number of Businesses'},
                orientation='h',
                color='Count',
                color_continuous_scale=px.colors.sequential.Blues
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Star ratings by state (top 10)
            if 'stars' in geo_df.columns:
                st.subheader("Top States by Average Rating")
                
                state_stars = geo_df.groupby('state')['stars'].mean().reset_index()
                state_stars.columns = ['State', 'Average Stars']
                state_stars['Count'] = geo_df.groupby('state').size().values
                state_stars = state_stars[state_stars['Count'] > 50]  # Filter states with enough businesses
                top_rated_states = state_stars.sort_values('Average Stars', ascending=False).head(10)
                
                fig = px.bar(
                    top_rated_states,
                    y='State',
                    x='Average Stars',
                    title="Average Star Rating by State (Top 10)",
                    labels={'State': 'State', 'Average Stars': 'Average Stars'},
                    orientation='h',
                    color='Average Stars',
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Interactive maps section
    st.subheader("Business Location Map")
    
    # Map options
    map_options = []
    
    # Check if we have latitude/longitude data
    has_coordinates = 'latitude' in geo_df.columns and 'longitude' in geo_df.columns
    if has_coordinates:
        map_options.append("Business Distribution Map")
    
    # Check if geojson is available
    if data['geojson'] is not None:
        map_options.append("GeoJSON Visualization")
    
    # If we have map options, create a selectbox
    if map_options:
        map_type = st.selectbox("Select Map Type", map_options)
        
        if map_type == "Business Distribution Map" and has_coordinates:
            # Sample data if too large for map
            map_df = geo_df
            if len(geo_df) > 1000:
                st.info("Sampling 1,000 businesses for map visualization")
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
                except Exception:
                    # Skip points that cause errors
                    continue
            
            # Display map
            folium_static(m, width=1000)
            
            # Add legend
            st.caption("**Map Legend:** 🟢 4.5+ stars | 🟩 4.0-4.4 stars | 🟧 3.0-3.9 stars | 🟥 Below 3.0 stars")
        
        elif map_type == "GeoJSON Visualization" and data['geojson'] is not None:
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
                    except Exception:
                        # Skip features that cause errors
                        continue
                
                # Display map
                folium_static(m_geojson, width=1000)
                
                # Add legend
                st.caption("**Map Legend:** 🟢 4.5+ stars | 🟩 4.0-4.4 stars | 🟧 3.0-3.9 stars | 🟥 Below 3.0 stars")
            except Exception as e:
                st.error(f"Error creating GeoJSON map: {e}")
    else:
        st.warning("No geographic data available for map visualization")

def create_temporal_analytics(data):
    """Temporal analytics section of the dashboard"""
    
    st.header("Temporal Analysis")
    
    if data['temporal'].empty:
        st.warning("No temporal data available to analyze")
        return
    
    # Get temporal DataFrame
    temporal_df = data['temporal']
    
    # Growth rate and star change analysis
    col1, col2 = st.columns(2)
    
    with col1:
        # Growth rate distribution if available
        if 'avg_growth_rate' in temporal_df.columns:
            st.subheader("Business Growth Rates")
            
            # Filter out extreme outliers
            growth_data = temporal_df['avg_growth_rate'].clip(lower=-1, upper=2)
            
            fig = px.histogram(
                growth_data,
                nbins=30,
                title="Distribution of Business Growth Rates",
                labels={'value': 'Average Growth Rate', 'count': 'Number of Businesses'},
                color_discrete_sequence=['#2ca02c']
            )
            fig.add_vline(x=0, line_dash="dash", line_color="red")
            fig.update_layout(bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Star change distribution if available
        if 'avg_star_change' in temporal_df.columns:
            st.subheader("Star Rating Changes")
            
            # Filter out extreme outliers
            star_change_data = temporal_df['avg_star_change'].clip(lower=-1, upper=1)
            
            fig = px.histogram(
                star_change_data,
                nbins=30,
                title="Distribution of Star Rating Changes Over Time",
                labels={'value': 'Average Star Change', 'count': 'Number of Businesses'},
                color_discrete_sequence=['#ff7f0e']
            )
            fig.add_vline(x=0, line_dash="dash", line_color="red")
            fig.update_layout(bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)
    
    # Correlation between star change and growth rate
    if 'avg_growth_rate' in temporal_df.columns and 'avg_star_change' in temporal_df.columns:
        st.subheader("Relationship Between Rating Changes and Growth")
        
        # Filter out extreme outliers for better visualization
        plot_df = temporal_df.copy()
        plot_df['avg_growth_rate'] = plot_df['avg_growth_rate'].clip(lower=-1, upper=2)
        plot_df['avg_star_change'] = plot_df['avg_star_change'].clip(lower=-1, upper=1)
        
        fig = px.scatter(
            plot_df,
            x='avg_star_change',
            y='avg_growth_rate',
            title="Relationship Between Star Changes and Growth Rate",
            labels={'avg_star_change': 'Average Star Change', 'avg_growth_rate': 'Average Growth Rate'},
            opacity=0.7,
            color_discrete_sequence=['#1f77b4']
        )
        
        # Add trend line
        fig.update_traces(marker=dict(size=5))
        fig.update_layout(
            xaxis=dict(zeroline=True, zerolinecolor='red', zerolinewidth=1),
            yaxis=dict(zeroline=True, zerolinecolor='red', zerolinewidth=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate correlation
        correlation = plot_df['avg_star_change'].corr(plot_df['avg_growth_rate'])
        st.metric("Correlation Coefficient", f"{correlation:.3f}")
        
        # Add interpretation
        if correlation > 0.5:
            st.success("Strong positive correlation: Businesses with improving ratings tend to grow faster")
        elif correlation > 0.3:
            st.info("Moderate positive correlation: Some relationship between rating improvements and growth")
        elif correlation > 0.1:
            st.info("Weak positive correlation: Slight relationship between rating improvements and growth")
        elif correlation > -0.1:
            st.warning("No significant correlation: Rating changes don't clearly predict growth")
        else:
            st.error("Negative correlation: Businesses with improving ratings might be growing slower")

def create_predictive_model_analytics(data):
    """Predictive model analytics section of the dashboard"""
    
    st.header("Predictive Modeling")
    
    if data['model'].empty:
        st.warning("No model data available to analyze")
        return
    
    # Print the model data for debugging
    st.write("Debug: Available model data columns:", data['model'].columns.tolist())
    
    model_df = data['model']
    
    # Feature importance visualization if available
    if 'feature' in model_df.columns and 'importance' in model_df.columns:
        st.subheader("Feature Importance for Business Success")
        
        # Sort by importance
        feature_importance = model_df[['feature', 'importance']].sort_values('importance', ascending=False)
        feature_importance = feature_importance.dropna()  # Remove any NaN values
        
        if not feature_importance.empty:
            fig = px.bar(
                feature_importance,
                y='feature',
                x='importance',
                title="Feature Importance for Business Success Prediction",
                labels={'feature': 'Feature', 'importance': 'Importance'},
                orientation='h',
                color='importance',
                color_continuous_scale=px.colors.sequential.Viridis
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Add interpretation
            st.markdown("**Key Success Factors:**")
            top_features = feature_importance.head(3)['feature'].tolist()
            st.write(f"The top 3 factors for business success are: {', '.join(top_features)}")
        else:
            st.info("No feature importance data available")
    else:
        # Create a placeholder chart with sample data
        st.subheader("Sample Feature Importance (Placeholder)")
        sample_features = pd.DataFrame({
            'feature': ['Review Count', 'Star Rating', 'Location', 'Open Hours', 'Category Diversity'],
            'importance': [0.35, 0.28, 0.18, 0.12, 0.07]
        })
        
        fig = px.bar(
            sample_features,
            y='feature',
            x='importance',
            title="Feature Importance for Business Success Prediction (Sample Data)",
            labels={'feature': 'Feature', 'importance': 'Importance'},
            orientation='h',
            color='importance',
            color_continuous_scale=px.colors.sequential.Viridis
        )
        st.plotly_chart(fig, use_container_width=True)
        st.info("Note: This is sample data. Run the full pipeline to see actual model results.")
    
    # Success prediction metrics - check if necessary columns exist
    has_prediction_data = ('success' in model_df.columns and 'prediction' in model_df.columns)
    
    if has_prediction_data:
        success_pred_df = model_df[model_df['success'].notna() & model_df['prediction'].notna()]
        
        if not success_pred_df.empty:
            st.subheader("Model Performance")
            
            # Create confusion matrix
            try:
                conf_matrix = pd.crosstab(success_pred_df['success'], success_pred_df['prediction'])
                
                # Calculate metrics
                tp = conf_matrix.loc[1, 1] if 1 in conf_matrix.index and 1 in conf_matrix.columns else 0
                tn = conf_matrix.loc[0, 0] if 0 in conf_matrix.index and 0 in conf_matrix.columns else 0
                fp = conf_matrix.loc[0, 1] if 0 in conf_matrix.index and 1 in conf_matrix.columns else 0
                fn = conf_matrix.loc[1, 0] if 1 in conf_matrix.index and 0 in conf_matrix.columns else 0
                
                accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0
                precision = tp / (tp + fp) if (tp + fp) > 0 else 0
                recall = tp / (tp + fn) if (tp + fn) > 0 else 0
                f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
                
                # Display metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Accuracy", f"{accuracy:.1%}")
                
                with col2:
                    st.metric("Precision", f"{precision:.1%}")
                
                with col3:
                    st.metric("Recall", f"{recall:.1%}")
                
                with col4:
                    st.metric("F1 Score", f"{f1:.1%}")
                
                # Display confusion matrix
                st.subheader("Confusion Matrix")
                
                # Convert to percentages for better visualization
                conf_pct = conf_matrix.copy()
                for i in conf_pct.index:
                    row_sum = conf_matrix.loc[i, :].sum()
                    if row_sum > 0:
                        conf_pct.loc[i, :] = conf_matrix.loc[i, :] / row_sum * 100
                
                # Create a heatmap
                fig = px.imshow(
                    conf_pct,
                    labels=dict(x="Predicted", y="Actual", color="Percentage"),
                    x=['Not Successful', 'Successful'],
                    y=['Not Successful', 'Successful'],
                    text_auto='.1f',
                    color_continuous_scale='Blues',
                    title="Confusion Matrix (Row Percentages)"
                )
                fig.update_layout(width=600, height=500)
                st.plotly_chart(fig)
                
                # Add interpretation
                st.markdown(f"""
                **Model Performance Summary:**
                - The model correctly identifies {recall:.1%} of actually successful businesses (recall)
                - When the model predicts success, it's correct {precision:.1%} of the time (precision)
                - Overall accuracy is {accuracy:.1%}
                """)
            except Exception as e:
                st.error(f"Error calculating model metrics: {e}")
        else:
            # Show placeholder metrics
            st.info("No prediction data found. Showing sample metrics instead.")
            _show_sample_model_metrics()
    else:
        # Show placeholder metrics
        st.subheader("Sample Model Performance (Placeholder)")
        _show_sample_model_metrics()


def _show_sample_model_metrics():
    """Helper function to show sample model metrics"""
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Accuracy", "78.5%")
    
    with col2:
        st.metric("Precision", "82.3%")
    
    with col3:
        st.metric("Recall", "75.1%")
    
    with col4:
        st.metric("F1 Score", "78.6%")
    
    # Create a sample confusion matrix
    sample_conf_pct = np.array([[81.25, 18.75], [14.29, 85.71]])
    
    # Create a heatmap
    fig = px.imshow(
        sample_conf_pct,
        labels=dict(x="Predicted", y="Actual", color="Percentage"),
        x=['Not Successful', 'Successful'],
        y=['Not Successful', 'Successful'],
        text_auto='.1f',
        color_continuous_scale='Blues',
        title="Sample Confusion Matrix (Row Percentages)"
    )
    fig.update_layout(width=600, height=500)
    st.plotly_chart(fig)
    
    st.info("Note: This is sample data. Run the full pipeline to see actual model results.")

def main():
    """Main function to run the Streamlit dashboard"""
    
    # Add title and description
    st.title("🍽️ Yelp Business Success Insights Dashboard")
    st.markdown("""
    This interactive dashboard presents insights from our big data analysis of Yelp business data.
    Explore patterns in business success, geographic distribution, and temporal trends.
    """)
    
    # Check if data directory exists
    if not os.path.exists(DATA_DIR):
        st.error(f"Data directory not found: {DATA_DIR}")
        st.info("""
        Please run the data extraction script first:
        ```
        ./scripts/extract_hdfs_data.sh
        ```
        """)
        return
    
    # Load data
    with st.spinner("Loading data from Hadoop outputs..."):
        data = load_data()
    
    # Create navigation sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Analysis Section",
        ["Business Analytics", "Geographic Analysis", "Temporal Analysis", "Predictive Modeling"],
        index=0
    )
    
    # Display selected page
    if page == "Business Analytics":
        create_business_analytics(data)
    
    elif page == "Geographic Analysis":
        create_geographic_analytics(data)
    
    elif page == "Temporal Analysis":
        create_temporal_analytics(data)
    
    elif page == "Predictive Modeling":
        create_predictive_model_analytics(data)
    
    # Add footer
    st.sidebar.divider()
    st.sidebar.caption("LocalInsight: Yelp Business Success Factors Analysis")
    st.sidebar.caption(" 2025 Big Data Project")

if __name__ == "__main__":
    main()
