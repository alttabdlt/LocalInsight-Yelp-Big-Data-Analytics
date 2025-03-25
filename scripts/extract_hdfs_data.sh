#!/bin/bash
# Script to copy data from HDFS to local filesystem for visualization

# Create local directories
mkdir -p ./data_for_viz

# Copy data from HDFS to local
echo "Copying business analysis data..."
hadoop fs -getmerge /user/localinsight/processed/business_analysis ./data_for_viz/business_analysis.json

echo "Copying geographic analysis data..."
hadoop fs -getmerge /user/localinsight/processed/geographic ./data_for_viz/geographic.json

echo "Copying temporal analysis data..."
hadoop fs -getmerge /user/localinsight/processed/temporal ./data_for_viz/temporal.json

echo "Copying model data..."
hadoop fs -getmerge /user/localinsight/processed/model_data ./data_for_viz/model_data.json

echo "Copying GeoJSON data if available..."
hadoop fs -getmerge /user/localinsight/processed/geojson ./data_for_viz/geojson.json

echo "Data extraction complete. Files saved to ./data_for_viz/"
