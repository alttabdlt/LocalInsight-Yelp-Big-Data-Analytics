#!/bin/bash

# Script to prepare data for the Streamlit dashboard
# This extracts the analysis results from HDFS and puts them in a format
# that the dashboard can easily consume

set -e

# Get directory of script
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

# Allow configuration of HDFS paths
HDFS_BASE_DIR=${HDFS_BASE_DIR:-"/user/localinsight"}
HDFS_PROCESSED_DIR="${HDFS_BASE_DIR}/processed"

# Create data directory for visualizations
DATA_DIR="${PROJECT_ROOT}/src/main/python/visualization/data_for_viz"
# Clear existing data directory to ensure clean results
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

echo "Creating data directory for dashboard: $DATA_DIR"

# Updated HDFS paths with configurable base directory
BUSINESS_ANALYSIS_PATH="${HDFS_PROCESSED_DIR}/business_analysis"
GEOGRAPHIC_ANALYSIS_PATH="${HDFS_PROCESSED_DIR}/geographic"
TEMPORAL_ANALYSIS_PATH="${HDFS_PROCESSED_DIR}/temporal"
PREDICTIVE_MODEL_PATH="${HDFS_PROCESSED_DIR}/model_data"

# Extract business analysis data
echo "Extracting business analysis data..."
hadoop fs -cat ${BUSINESS_ANALYSIS_PATH}/part-* > "${DATA_DIR}/business_analysis.json" || echo "Warning: Could not extract business analysis data"

# Extract geographic analysis data
echo "Extracting geographic analysis data..."
hadoop fs -cat ${GEOGRAPHIC_ANALYSIS_PATH}/part-* > "${DATA_DIR}/geographic.json" || echo "Warning: Could not extract geographic analysis data"

# Extract temporal analysis data
echo "Extracting temporal analysis data..."
hadoop fs -cat ${TEMPORAL_ANALYSIS_PATH}/part-* > "${DATA_DIR}/temporal.json" || echo "Warning: Could not extract temporal analysis data"

# Extract model data
echo "Extracting predictive model data..."
hadoop fs -cat ${PREDICTIVE_MODEL_PATH}/part-* > "${DATA_DIR}/model_data.json" || echo "Warning: Could not extract model data"

# Set permissions
chmod -R 755 "$DATA_DIR"

echo "Data preparation complete. You can now run the dashboard with:"
echo "streamlit run ${PROJECT_ROOT}/src/main/python/visualization/business_dashboard.py"
