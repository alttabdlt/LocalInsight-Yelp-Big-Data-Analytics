#!/bin/bash
# Script to upload Yelp dataset to HDFS using relative paths

set -e

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

# Allow configuration of HDFS paths
HDFS_BASE_DIR=${HDFS_BASE_DIR:-"/user/localinsight"}
HDFS_RAW_DIR="${HDFS_BASE_DIR}/raw"
HDFS_PROCESSED_DIR="${HDFS_BASE_DIR}/processed"

# Check if Hadoop is in safe mode and disable it if necessary
echo "Checking Hadoop safe mode status..."
SAFE_MODE=$(hdfs dfsadmin -safemode get)
if [[ $SAFE_MODE == *"ON"* ]]; then
  echo "Hadoop is in safe mode. Attempting to disable..."
  hdfs dfsadmin -safemode leave
  sleep 5  # Give it time to take effect
  echo "Safe mode should now be disabled."
else
  echo "Hadoop is not in safe mode. Proceeding with upload."
fi

# Create necessary HDFS directories
echo "Creating HDFS directories at ${HDFS_BASE_DIR}..."
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

# Check for required files
echo "Checking for required dataset files..."
BUSINESS_FILE="${YELP_DIR}/yelp_academic_dataset_business.json"
REVIEW_FILE="${YELP_DIR}/yelp_academic_dataset_review.json"
USER_FILE="${YELP_DIR}/yelp_academic_dataset_user.json"
CHECKIN_FILE="${YELP_DIR}/yelp_academic_dataset_checkin.json"
TIP_FILE="${YELP_DIR}/yelp_academic_dataset_tip.json"

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

# Create a temporary directory with no spaces for easier handling
TEMP_DIR="${PROJECT_ROOT}/tmp_data"
mkdir -p "${TEMP_DIR}"

# Clean up function to remove temporary links on exit
cleanup() {
  echo "Cleaning up temporary files..."
  rm -rf "${TEMP_DIR}"
}

# Register the cleanup function to run on script exit
trap cleanup EXIT

# Create symbolic links to dataset files in the temporary directory
echo "Creating temporary links for dataset files..."
ln -sf "${BUSINESS_FILE}" "${TEMP_DIR}/business.json"
ln -sf "${REVIEW_FILE}" "${TEMP_DIR}/review.json"
ln -sf "${USER_FILE}" "${TEMP_DIR}/user.json"
ln -sf "${CHECKIN_FILE}" "${TEMP_DIR}/checkin.json"
ln -sf "${TIP_FILE}" "${TEMP_DIR}/tip.json"

# Verify that safe mode is really off before proceeding
echo "Double-checking safe mode status..."
SAFE_MODE=$(hdfs dfsadmin -safemode get)
if [[ $SAFE_MODE == *"ON"* ]]; then
  echo "Error: Hadoop is still in safe mode despite attempts to disable it."
  echo "Please manually disable safe mode with: hdfs dfsadmin -safemode leave"
  echo "Then try running this script again."
  exit 1
fi

# Upload files to HDFS using the temporary directory (no spaces in paths)
echo "Uploading business data..."
hadoop fs -put "${TEMP_DIR}/business.json" ${HDFS_RAW_DIR}/business/

echo "Uploading review data..."
hadoop fs -put "${TEMP_DIR}/review.json" ${HDFS_RAW_DIR}/reviews/

echo "Uploading user data..."
hadoop fs -put "${TEMP_DIR}/user.json" ${HDFS_RAW_DIR}/users/

echo "Uploading checkin data..."
hadoop fs -put "${TEMP_DIR}/checkin.json" ${HDFS_RAW_DIR}/checkin/

echo "Uploading tip data..."
hadoop fs -put "${TEMP_DIR}/tip.json" ${HDFS_RAW_DIR}/tips/

echo "Verifying files in HDFS..."
hadoop fs -ls ${HDFS_RAW_DIR}/business/
hadoop fs -ls ${HDFS_RAW_DIR}/reviews/
hadoop fs -ls ${HDFS_RAW_DIR}/users/
hadoop fs -ls ${HDFS_RAW_DIR}/checkin/
hadoop fs -ls ${HDFS_RAW_DIR}/tips/

echo "Dataset upload complete!"
echo "HDFS files are available at ${HDFS_BASE_DIR}"
echo ""
echo "You can customize the HDFS path by setting the HDFS_BASE_DIR environment variable:"
echo "HDFS_BASE_DIR=/your/custom/path $0"
