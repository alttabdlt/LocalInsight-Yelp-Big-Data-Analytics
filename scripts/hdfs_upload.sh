#!/bin/bash

# Exit on error
set -e

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

# Allow configuration of HDFS paths
HDFS_BASE_DIR=${HDFS_BASE_DIR:-"/user/localinsight"}
HDFS_RAW_DIR="${HDFS_BASE_DIR}/raw"
HDFS_PROCESSED_DIR="${HDFS_BASE_DIR}/processed"

echo "Creating HDFS directory structure..."
hadoop fs -mkdir -p ${HDFS_RAW_DIR}/business
hadoop fs -mkdir -p ${HDFS_RAW_DIR}/reviews
hadoop fs -mkdir -p ${HDFS_RAW_DIR}/users
hadoop fs -mkdir -p ${HDFS_RAW_DIR}/checkin
hadoop fs -mkdir -p ${HDFS_RAW_DIR}/tips
hadoop fs -mkdir -p ${HDFS_PROCESSED_DIR}

# Look for dataset directory
echo "Looking for Yelp dataset..."
# Look in common directories relative to project root
YELP_DIR=""
COMMON_LOCATIONS=(
  "${PROJECT_ROOT}/Yelp JSON/yelp_dataset"
  "${PROJECT_ROOT}/data/yelp_dataset"
  "${PROJECT_ROOT}/yelp_dataset"
)

for dir in "${COMMON_LOCATIONS[@]}"; do
  if [ -d "$dir" ]; then
    YELP_DIR="$dir"
    echo "Found Yelp dataset at $YELP_DIR"
    break
  fi
done

# If not found in common locations, ask user
if [ -z "$YELP_DIR" ]; then
  echo "Yelp dataset not found in common locations."
  read -p "Please enter the path to your Yelp dataset directory: " YELP_DIR
  
  if [ ! -d "$YELP_DIR" ]; then
    echo "Error: Directory $YELP_DIR does not exist."
    exit 1
  fi
fi

# Define dataset files
BUSINESS_FILE="${YELP_DIR}/yelp_academic_dataset_business.json"
REVIEW_FILE="${YELP_DIR}/yelp_academic_dataset_review.json"
USER_FILE="${YELP_DIR}/yelp_academic_dataset_user.json"
CHECKIN_FILE="${YELP_DIR}/yelp_academic_dataset_checkin.json"
TIP_FILE="${YELP_DIR}/yelp_academic_dataset_tip.json"

# Check for required files
echo "Checking for required dataset files..."
MISSING_FILES=0
if [ ! -f "$BUSINESS_FILE" ]; then
  echo "Error: Business file not found at $BUSINESS_FILE"
  MISSING_FILES=1
fi

if [ ! -f "$REVIEW_FILE" ]; then
  echo "Error: Review file not found at $REVIEW_FILE"
  MISSING_FILES=1
fi

if [ ! -f "$USER_FILE" ]; then
  echo "Error: User file not found at $USER_FILE"
  MISSING_FILES=1
fi

if [ ! -f "$CHECKIN_FILE" ]; then
  echo "Error: Checkin file not found at $CHECKIN_FILE"
  MISSING_FILES=1
fi

if [ ! -f "$TIP_FILE" ]; then
  echo "Error: Tip file not found at $TIP_FILE"
  MISSING_FILES=1
fi

if [ $MISSING_FILES -eq 1 ]; then
  echo "One or more dataset files are missing. Please ensure all files are available."
  exit 1
fi

# Create a temporary directory with no spaces for easier HDFS upload
TEMP_DIR="${PROJECT_ROOT}/tmp_data"
mkdir -p "${TEMP_DIR}"

# Clean up function to remove temporary links on exit
cleanup() {
  echo "Cleaning up temporary files..."
  rm -rf "${TEMP_DIR}"
}

# Register the cleanup function to run on script exit
trap cleanup EXIT

echo "Creating temporary links for dataset files..."
ln -sf "${BUSINESS_FILE}" "${TEMP_DIR}/business.json"
ln -sf "${REVIEW_FILE}" "${TEMP_DIR}/review.json"
ln -sf "${USER_FILE}" "${TEMP_DIR}/user.json"
ln -sf "${CHECKIN_FILE}" "${TEMP_DIR}/checkin.json"
ln -sf "${TIP_FILE}" "${TEMP_DIR}/tip.json"

echo "Uploading Yelp dataset to HDFS..."
# Verify temporary links exist and upload them
if [ -f "${TEMP_DIR}/business.json" ]; then
  echo "Uploading business dataset..."
  hadoop fs -put -f "${TEMP_DIR}/business.json" ${HDFS_RAW_DIR}/business/
else
  echo "Error: Business file link not created properly"
  exit 1
fi

if [ -f "${TEMP_DIR}/review.json" ]; then
  echo "Uploading review dataset..."
  hadoop fs -put -f "${TEMP_DIR}/review.json" ${HDFS_RAW_DIR}/reviews/
else
  echo "Error: Review file link not created properly"
  exit 1
fi

if [ -f "${TEMP_DIR}/user.json" ]; then
  echo "Uploading user dataset..."
  hadoop fs -put -f "${TEMP_DIR}/user.json" ${HDFS_RAW_DIR}/users/
else
  echo "Error: User file link not created properly"
  exit 1
fi

if [ -f "${TEMP_DIR}/checkin.json" ]; then
  echo "Uploading checkin dataset..."
  hadoop fs -put -f "${TEMP_DIR}/checkin.json" ${HDFS_RAW_DIR}/checkin/
else
  echo "Error: Checkin file link not created properly"
  exit 1
fi

if [ -f "${TEMP_DIR}/tip.json" ]; then
  echo "Uploading tip dataset..."
  hadoop fs -put -f "${TEMP_DIR}/tip.json" ${HDFS_RAW_DIR}/tips/
else
  echo "Error: Tip file link not created properly"
  exit 1
fi

echo "Verifying HDFS upload..."
hadoop fs -ls ${HDFS_RAW_DIR}/business/
hadoop fs -ls ${HDFS_RAW_DIR}/reviews/
hadoop fs -ls ${HDFS_RAW_DIR}/users/
hadoop fs -ls ${HDFS_RAW_DIR}/checkin/
hadoop fs -ls ${HDFS_RAW_DIR}/tips/

echo "Upload complete!"
echo "HDFS files are available at ${HDFS_BASE_DIR}"
echo ""
echo "You can customize the HDFS path by setting the HDFS_BASE_DIR environment variable:"
echo "HDFS_BASE_DIR=/your/custom/path $0"