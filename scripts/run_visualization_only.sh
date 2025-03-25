#!/bin/bash
# Simplified script to run only the visualization component - no Hadoop required

# Exit on error
set -e

echo "=========================================================="
echo "LocalInsight: Running Visualization Component"
echo "=========================================================="

# Define directories
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPTS_DIR")"
VISUALIZATION_OUTPUT="${PROJECT_ROOT}/visualization_output"

# Create required directories
mkdir -p "${VISUALIZATION_OUTPUT}"
mkdir -p "${PROJECT_ROOT}/logs"

# Set up Python environment if not already done
if [ ! -d "${PROJECT_ROOT}/venv" ]; then
    echo "Setting up Python environment..."
    bash "${SCRIPTS_DIR}/setup_env.sh"
    echo ""
fi

# Activate the virtual environment
echo "Activating Python virtual environment..."
source "${PROJECT_ROOT}/venv/bin/activate"
echo ""

echo "Running visualization generator..."
echo "----------------------------------------"
python3 "${PROJECT_ROOT}/src/main/python/visualization/generate_visualizations.py" \
  > "${PROJECT_ROOT}/logs/visualization.log" 2>&1

echo "=========================================================="
echo "Visualization complete!"
echo "Visualizations available in: ${VISUALIZATION_OUTPUT}"
echo ""
echo "To view the visualizations:"
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "  open ${VISUALIZATION_OUTPUT}"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "  xdg-open ${VISUALIZATION_OUTPUT}"
else
    echo "  Browse to: ${VISUALIZATION_OUTPUT}"
fi
echo "=========================================================="