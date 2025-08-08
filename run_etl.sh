#!/bin/bash

# Default values
DAG_ID=${1:-"CompanyProductsETL"}
EXECUTION_DATE=${2:-$(date +%Y-%m-%d)}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Change to project directory
cd /var/www/vhosts/itland.uk/docker

print_status "Starting ETL process..."
print_status "DAG: $DAG_ID"
print_status "Date: $EXECUTION_DATE"

# Check if containers are running
print_status "Checking Airflow containers..."
if ! docker compose ps | grep -q "airflow-webserver.*Up"; then
    print_error "Airflow webserver is not running!"
    exit 1
fi

if ! docker compose ps | grep -q "airflow-scheduler.*Up"; then
    print_error "Airflow scheduler is not running!"
    exit 1
fi

print_success "Airflow containers are running"

# Ask user what type of execution they want
echo ""
print_status "Choose execution type:"
echo "1) Trigger DAG (background execution)"
echo "2) Test DAG (with full logs)"
echo "3) Both (trigger + then show logs)"
read -p "Enter choice (1-3): " choice

case $choice in
    1)
        # Trigger the DAG (background)
        print_status "Triggering DAG: $DAG_ID"
        TRIGGER_OUTPUT=$(docker compose exec airflow-webserver airflow dags trigger $DAG_ID 2>&1)

        if [ $? -eq 0 ]; then
            print_success "DAG triggered successfully"
            echo "$TRIGGER_OUTPUT"
            
            # Extract run ID from output
            RUN_ID=$(echo "$TRIGGER_OUTPUT" | grep "dag_run_id" | awk '{print $2}')
            if [ -n "$RUN_ID" ]; then
                print_status "Run ID: $RUN_ID"
                print_status "You can monitor the run in Airflow UI: http://localhost:8080"
            fi
        else
            print_error "Failed to trigger DAG"
            echo "$TRIGGER_OUTPUT"
            exit 1
        fi
        ;;
    2)
        # Test DAG with full logs
        print_status "Testing DAG with full logs: $DAG_ID"
        docker compose exec airflow-webserver airflow dags test $DAG_ID $EXECUTION_DATE
        ;;
    3)
        # Trigger DAG first
        print_status "Triggering DAG: $DAG_ID"
        TRIGGER_OUTPUT=$(docker compose exec airflow-webserver airflow dags trigger $DAG_ID 2>&1)

        if [ $? -eq 0 ]; then
            print_success "DAG triggered successfully"
            echo "$TRIGGER_OUTPUT"
            
            # Wait a moment for DAG to start
            print_status "Waiting 5 seconds for DAG to start..."
            sleep 5
            
            # Show recent logs
            print_status "Showing recent logs..."
            docker compose exec airflow-webserver airflow dags test $DAG_ID $EXECUTION_DATE
        else
            print_error "Failed to trigger DAG"
            echo "$TRIGGER_OUTPUT"
            exit 1
        fi
        ;;
    *)
        print_error "Invalid choice. Exiting."
        exit 1
        ;;
esac

print_success "ETL process completed!"