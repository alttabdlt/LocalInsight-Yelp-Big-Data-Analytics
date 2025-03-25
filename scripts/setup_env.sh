#!/bin/bash
# Script to set up the Python environment for the project

# Exit on error
set -e

echo "Setting up Python environment for LocalInsight project..."

# Create a virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists."
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "Installing required packages..."
pip install -r requirements.txt

echo "Setup complete! Use 'source venv/bin/activate' to activate the environment."