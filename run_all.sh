#!/bin/bash

# run_all.sh - Comprehensive script to run the entire LocalInsight pipeline and dashboard
# From data processing to visualization in one go

set -e  # Exit on error

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT=$SCRIPT_DIR

# Allow configuration of HDFS paths
export HDFS_BASE_DIR=${HDFS_BASE_DIR:-"/user/localinsight"}

# Color formatting for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================================"
echo -e "LocalInsight: Yelp Business Success Factors Analysis"
echo -e "Complete Pipeline and Dashboard Runner"
echo -e "========================================================${NC}"

# Create log directory if needed
mkdir -p "$PROJECT_ROOT/logs"
LOG_FILE="$PROJECT_ROOT/logs/complete_run_$(date +%Y%m%d_%H%M%S).log"

# Check if hadoop is running
echo -e "${YELLOW}Checking if Hadoop is running...${NC}"
hadoop fs -ls / > /dev/null 2>&1 || { 
    echo -e "${YELLOW}Hadoop doesn't appear to be running. Starting it now...${NC}" 
    start-dfs.sh
    sleep 5
    start-yarn.sh
    sleep 5
    echo -e "${GREEN}Hadoop started successfully.${NC}"
}

echo -e "${YELLOW}Step 0: Checking if Yelp dataset exists in HDFS...${NC}"
# Check if data already exists in HDFS
if hadoop fs -test -e ${HDFS_BASE_DIR}/raw/business/business.json; then
    echo -e "${GREEN}Yelp dataset already exists in HDFS. Skipping upload step.${NC}" | tee -a "$LOG_FILE"
else
    echo -e "${YELLOW}Uploading Yelp dataset to HDFS...${NC}"
    bash "$PROJECT_ROOT/scripts/upload_data.sh" | tee -a "$LOG_FILE"

    # Check if dataset upload completed successfully
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo -e "${YELLOW}Dataset upload failed. Check logs for details. Exiting.${NC}"
        exit 1
    fi
fi

echo -e "${YELLOW}Step 1: Running data pipeline...${NC}"
bash "$PROJECT_ROOT/scripts/run_pipeline.sh" | tee -a "$LOG_FILE"

# Check if pipeline completed successfully
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo -e "${YELLOW}Pipeline failed. Check logs for details. Exiting.${NC}"
    exit 1
fi

echo -e "${YELLOW}Step 2: Preparing data for dashboard...${NC}"
bash "$PROJECT_ROOT/scripts/prepare_dashboard_data.sh" | tee -a "$LOG_FILE"

echo -e "${YELLOW}Step 3: Transforming model data for visualization...${NC}"
source "$PROJECT_ROOT/new_venv/bin/activate"
python3 "$PROJECT_ROOT/scripts/transform_model_data.py" | tee -a "$LOG_FILE"

echo -e "${GREEN}========================================================="
echo -e "Data pipeline and preparation complete!" 
echo -e "Starting Streamlit dashboard..."
echo -e "==========================================================${NC}"

echo -e "${YELLOW}Note: Press Ctrl+C to stop the dashboard when you're done.${NC}"

# Start the Streamlit app
streamlit run "$PROJECT_ROOT/src/main/python/visualization/business_dashboard.py"

# This part will only execute after the user stops the Streamlit app with Ctrl+C
echo -e "${GREEN}========================================================="
echo -e "LocalInsight pipeline and dashboard execution complete."
echo -e "Logs available at: $LOG_FILE"
echo -e "==========================================================${NC}"
