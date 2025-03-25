# Data Visualization for Yelp Business Analysis

This directory contains scripts for generating visualizations from the Yelp data analysis results.

## Script: generate_visualizations.py

This script generates a comprehensive set of visualizations from the processed Yelp data:

### Business Visualizations
- Star rating distributions
- Review count vs. stars scatter plot
- Success rate by price level
- WiFi availability impact on ratings
- Noise level impact on success

### Geographic Visualizations
- Success rate by state
- Interactive business map with success indicators
- Business density visualization

### Temporal Visualizations
- Review growth rate vs. star change
- Success rate by years of operation
- Business review growth trajectories

### Model Visualizations
- Feature importance for business success
- Success rate by feature combinations
- Predicted vs. actual star ratings

## Sample Data

The `sample_data/` directory contains small JSON files for testing visualizations:
- `business_analysis.json` - Business attributes and ratings
- `geographic.json` - Location data for businesses
- `model_data.json` - Model feature importance data
- `temporal.json` - Temporal metrics for businesses

## Usage

To generate all visualizations:

```bash
python3 generate_visualizations.py
```

Visualizations are saved to the `visualization_output/` directory.

## Dependencies

- Python 3.6+
- Pandas
- NumPy
- Matplotlib
- Seaborn
- Folium (for maps)