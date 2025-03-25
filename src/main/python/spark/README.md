# Spark Analysis Scripts for Yelp Data

This directory contains PySpark scripts for analyzing the processed Yelp dataset:

## 1. business_analysis.py

Analyzes business success factors by examining correlations between business attributes and star ratings.

Key components:
- Business attribute extraction
- Feature engineering
- Regression model for star rating prediction
- Classification model for business success prediction
- Feature importance analysis

## 2. geographic_analysis.py

Analyzes geographic patterns in business success.

Key components:
- Business density analysis by location
- Geographic success factors
- State and city comparisons
- GeoJSON output for map visualizations

## 3. temporal_analysis.py

Analyzes temporal patterns in reviews and check-ins.

Key components:
- Time-based review patterns
- Seasonal and daily trends
- Review velocity calculation
- Business hour impact analysis

## 4. predictive_model.py

Builds predictive models for business success metrics.

Key components:
- Star rating prediction model
- Business success classification model
- Business longevity prediction
- Model evaluation and feature importance

## Usage

To run a Spark job:

```bash
spark-submit --master local[*] business_analysis.py
```

Note that these scripts expect data to be available in HDFS at the following locations:
- `/user/localinsight/raw/*` - Raw Yelp data files
- `/user/localinsight/processed/*` - Data processed by MapReduce jobs

## Dependencies

- PySpark 3.0+
- Python 3.6+