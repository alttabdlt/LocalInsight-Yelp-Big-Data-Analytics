#!/bin/bash
# Script to upload Yelp dataset to HDFS

# Exit on error
set -e

echo "Creating HDFS directory structure..."
hadoop fs -mkdir -p /user/localinsight/raw/business
hadoop fs -mkdir -p /user/localinsight/raw/reviews
hadoop fs -mkdir -p /user/localinsight/raw/users
hadoop fs -mkdir -p /user/localinsight/raw/checkin
hadoop fs -mkdir -p /user/localinsight/raw/tips
hadoop fs -mkdir -p /user/localinsight/processed

# Define dataset directory
DATASET_DIR="/Users/axel/Desktop/School Projects/Year 2 Trimester 2/Cloud and Big Data/Big-Data-Project/Yelp JSON/yelp_dataset"

echo "Uploading Yelp dataset to HDFS..."
hadoop fs -put "${DATASET_DIR}/yelp_academic_dataset_business.json" /user/localinsight/raw/business/
hadoop fs -put "${DATASET_DIR}/yelp_academic_dataset_review.json" /user/localinsight/raw/reviews/
hadoop fs -put "${DATASET_DIR}/yelp_academic_dataset_user.json" /user/localinsight/raw/users/
hadoop fs -put "${DATASET_DIR}/yelp_academic_dataset_checkin.json" /user/localinsight/raw/checkin/
hadoop fs -put "${DATASET_DIR}/yelp_academic_dataset_tip.json" /user/localinsight/raw/tips/

echo "Verifying files in HDFS..."
hadoop fs -ls /user/localinsight/raw/business/
hadoop fs -ls /user/localinsight/raw/reviews/
hadoop fs -ls /user/localinsight/raw/users/
hadoop fs -ls /user/localinsight/raw/checkin/
hadoop fs -ls /user/localinsight/raw/tips/

echo "Dataset upload complete!"