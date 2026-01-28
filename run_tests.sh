#!/bin/bash
# Test Runner Script for Docker Environment
# This script manages the test lifecycle for the streaming pipeline

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Real-Time Streaming Pipeline Tests  ${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}Error: Docker is not running. Please start Docker first.${NC}"
        exit 1
    fi
}

# Function to clean up previous test artifacts
cleanup() {
    echo -e "${YELLOW}Cleaning up previous test artifacts...${NC}"
    rm -rf logs/tests/*
    rm -rf data/test_events/*
    # Remove Python cache files aggressively
    find ./src -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find ./tests -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find ./src -type f -name "*.pyc" -delete 2>/dev/null || true
    find ./tests -type f -name "*.pyc" -delete 2>/dev/null || true
    echo -e "${GREEN}✓ Cleanup complete${NC}\n"
}

# Function to run unit tests only
run_unit_tests() {
    echo -e "${BLUE}Running Unit Tests (test_pipeline.py)...${NC}"
    docker-compose run --rm --remove-orphans test pytest tests/test_pipeline.py -v --tb=short --color=yes
}

# Function to run integration tests
run_integration_tests() {
    echo -e "${BLUE}Starting infrastructure for integration tests...${NC}"
    
    # Start postgres and spark
    docker-compose up -d --remove-orphans postgres spark
    
    echo -e "${YELLOW}Waiting for services to be ready (30s)...${NC}"
    sleep 30
    
    echo -e "${BLUE}Running Integration Tests (test_integration.py)...${NC}"
    docker-compose --profile test run --rm --remove-orphans test
}

# Function to run all tests
run_all_tests() {
    echo -e "${BLUE}Running ALL Tests...${NC}\n"
    
    # Run unit tests first
    echo -e "${BLUE}=== UNIT TESTS ===${NC}"
    run_unit_tests
    
    # Run integration tests
    echo -e "\n${BLUE}=== INTEGRATION TESTS ===${NC}"
    run_integration_tests
}

# Function to stop all services
stop_services() {
    echo -e "\n${YELLOW}Stopping Docker services...${NC}"
    docker-compose --profile test down --remove-orphans
    echo -e "${GREEN}✓ Services stopped${NC}"
}

# Function to view test logs
view_logs() {
    if [ -f "logs/tests/test_results.log" ]; then
        echo -e "${BLUE}Test Results:${NC}\n"
        cat logs/tests/test_results.log
    else
        echo -e "${YELLOW}No test logs found. Run tests first.${NC}"
    fi
}

# Parse command line arguments
case "${1:-all}" in
    unit)
        check_docker
        cleanup
        run_unit_tests
        ;;
    integration)
        check_docker
        cleanup
        run_integration_tests
        stop_services
        ;;
    all)
        check_docker
        cleanup
        run_all_tests
        stop_services
        ;;
    logs)
        view_logs
        ;;
    clean)
        cleanup
        stop_services
        ;;
    *)
        echo -e "${YELLOW}Usage: $0 {unit|integration|all|logs|clean}${NC}"
        echo ""
        echo "Commands:"
        echo "  unit          - Run unit tests only (test_pipeline.py)"
        echo "  integration   - Run integration tests (test_integration.py)"
        echo "  all           - Run all tests (default)"
        echo "  logs          - View test results log"
        echo "  clean         - Clean up test artifacts and stop services"
        exit 1
        ;;
esac

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  Test execution complete!${NC}"
echo -e "${GREEN}========================================${NC}"