#!/bin/bash

# Script to update DAX queries in Airflow Variables

# Set the working directory
cd "$(dirname "$0")"

# Prepare DAX queries
echo "Preparing DAX queries..."
python prepare_dax.py

# Check if the JSON file was created successfully
if [ ! -f "dax_queries.json" ]; then
    echo "Error: Failed to create dax_queries.json"
    exit 1
fi

# Load into Airflow Variables
echo "Loading DAX queries into Airflow Variables..."
airflow variables set dax_queries "$(cat dax_queries.json)"

# Clean up (commented out due to permission issues)
echo "Cleaning up..."
# rm dax_queries.json  # Skipped due to permission issues

echo "DAX queries have been updated successfully!" 