#!/bin/bash
# Script to run the complete Yelp data analysis pipeline

# Exit on error
set -e

echo "=========================================================="
echo "LocalInsight: Yelp Business Success Factors Analysis"
echo "=========================================================="

# Define directories
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPTS_DIR")"
JAR_DIR="${PROJECT_ROOT}/target"
LIB_DIR="${PROJECT_ROOT}/lib"

# Check if directories exist
mkdir -p "${PROJECT_ROOT}/logs"
mkdir -p "${JAR_DIR}"

# Source hadoop paths if needed (comment out if not required)
# source /etc/hadoop/conf/hadoop-env.sh
# source /etc/hadoop/conf/yarn-env.sh

echo "Step 1: Skipping HDFS data upload (already completed manually)..."
echo "----------------------------------------"
# Commenting out upload step since we've already uploaded files manually
# bash "${SCRIPTS_DIR}/hdfs_upload.sh"

echo "Step 2: Compiling MapReduce jobs..."
echo "----------------------------------------"
cd "${PROJECT_ROOT}"
# Include the JSON library in the classpath
javac -classpath "$(hadoop classpath):${LIB_DIR}/json-20231013.jar" src/main/java/localinsight/mapreduce/*.java -d "${JAR_DIR}/classes"
mkdir -p "${JAR_DIR}/libs"
# Copy the JSON library to the target libs directory
cp "${LIB_DIR}/json-20231013.jar" "${JAR_DIR}/libs/"
cd "${JAR_DIR}/classes"
# Include the JSON library in the JAR
jar -cf "${JAR_DIR}/yelp-analysis.jar" localinsight/mapreduce/*.class

# Create a combined jar that includes the JSON library
cd "${JAR_DIR}"
mkdir -p "${JAR_DIR}/temp"
cd "${JAR_DIR}/temp"
jar -xf "${JAR_DIR}/yelp-analysis.jar"
jar -xf "${JAR_DIR}/libs/json-20231013.jar"
jar -cf "${JAR_DIR}/yelp-analysis-with-dependencies.jar" .

# Clean up existing output directories
echo "Cleaning up existing output directories..."
echo "----------------------------------------"
hadoop fs -rm -r -f /user/localinsight/processed/business_processed
hadoop fs -rm -r -f /user/localinsight/processed/review_sentiment
hadoop fs -rm -r -f /user/localinsight/processed/temporal_patterns
hadoop fs -rm -r -f /user/localinsight/processed/business_analysis

echo "Step 3: Running Business Processing MapReduce job..."
echo "----------------------------------------"
# Include the JSON library in the classpath for Hadoop (using the correct syntax)
hadoop jar "${JAR_DIR}/yelp-analysis-with-dependencies.jar" localinsight.mapreduce.BusinessProcessing \
  /user/localinsight/raw/business/yelp_academic_dataset_business.json \
  /user/localinsight/processed/business_processed

echo "Step 4: Running Review Sentiment MapReduce job..."
echo "----------------------------------------"
hadoop jar "${JAR_DIR}/yelp-analysis-with-dependencies.jar" localinsight.mapreduce.ReviewSentiment \
  /user/localinsight/raw/reviews/yelp_academic_dataset_review.json \
  /user/localinsight/processed/review_sentiment

echo "Step 5: Running Temporal Patterns MapReduce job..."
echo "----------------------------------------"
hadoop jar "${JAR_DIR}/yelp-analysis-with-dependencies.jar" localinsight.mapreduce.TemporalPatterns \
  /user/localinsight/raw/checkin/yelp_academic_dataset_checkin.json \
  /user/localinsight/processed/temporal_patterns

# Activate virtual environment for Python packages
echo "Activating Python virtual environment..."
source "${PROJECT_ROOT}/venv/bin/activate"

echo "Step 6: Running Spark Business Analysis..."
echo "----------------------------------------"
spark-submit --master local[*] \
  --jars "${JAR_DIR}/libs/json-20231013.jar" \
  "${PROJECT_ROOT}/src/main/python/spark/business_analysis.py" \
  > "${PROJECT_ROOT}/logs/business_analysis.log" 2>&1

echo "Step 7: Running Spark Geographic Analysis..."
echo "----------------------------------------"
spark-submit --master local[*] \
  --jars "${JAR_DIR}/libs/json-20231013.jar" \
  "${PROJECT_ROOT}/src/main/python/spark/geographic_analysis.py" \
  > "${PROJECT_ROOT}/logs/geographic_analysis.log" 2>&1

echo "Step 8: Running Spark Temporal Analysis..."
echo "----------------------------------------"
spark-submit --master local[*] \
  --jars "${JAR_DIR}/libs/json-20231013.jar" \
  "${PROJECT_ROOT}/src/main/python/spark/temporal_analysis.py" \
  > "${PROJECT_ROOT}/logs/temporal_analysis.log" 2>&1

echo "Step 9: Running Predictive Modeling..."
echo "----------------------------------------"
spark-submit --master local[*] \
  --jars "${JAR_DIR}/libs/json-20231013.jar" \
  "${PROJECT_ROOT}/src/main/python/spark/predictive_model.py" \
  > "${PROJECT_ROOT}/logs/predictive_model.log" 2>&1

echo "Step 10: Generating Visualizations..."
echo "----------------------------------------"
# Clear visualization output folder first to ensure clean results
rm -rf "${PROJECT_ROOT}/visualization_output"
mkdir -p "${PROJECT_ROOT}/visualization_output"
spark-submit --master local[*] \
  --jars "${JAR_DIR}/libs/json-20231013.jar" \
  "${PROJECT_ROOT}/src/main/python/visualization/spark_visualizations.py" \
  > "${PROJECT_ROOT}/logs/visualization.log" 2>&1

# Deactivate virtual environment
echo "Deactivating Python virtual environment..."
deactivate

echo "=========================================================="
echo "Pipeline execution complete!"
echo "Logs available in: ${PROJECT_ROOT}/logs"
echo "Visualizations available in: ${PROJECT_ROOT}/visualization_output"
echo "=========================================================="