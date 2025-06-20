#!/bin/bash

# AVESA Staging Pipeline Test Script
# This script runs comprehensive end-to-end testing for the staging environment

set -e

echo "🚀 AVESA Staging Environment Pipeline Test"
echo "=========================================="

# Configuration
REGION="us-east-2"
ENVIRONMENT="staging"
TEST_DIR="$(dirname "$0")/../tests"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Check if AWS credentials are configured
echo "🔧 Checking AWS credentials..."
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    print_status $RED "❌ AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

print_status $GREEN "✅ AWS credentials configured"

# Check if we're in the right directory
if [ ! -f "$TEST_DIR/test-end-to-end-pipeline.py" ]; then
    print_status $RED "❌ Test script not found. Please run from the project root directory."
    exit 1
fi

# Step 1: Check recent pipeline executions and S3 data
print_status $YELLOW "📊 Step 1: Checking recent pipeline executions and S3 data..."
echo "----------------------------------------"

if python3 "$TEST_DIR/test-end-to-end-pipeline.py" --environment $ENVIRONMENT --region $REGION --check-recent; then
    print_status $GREEN "✅ Recent pipeline executions verified"
else
    print_status $YELLOW "⚠️  No recent executions found or S3 verification failed"
    echo "   Proceeding with new test execution..."
fi

echo ""

# Step 2: Run full end-to-end test
print_status $YELLOW "🧪 Step 2: Running full end-to-end pipeline test..."
echo "----------------------------------------"

if python3 "$TEST_DIR/test-end-to-end-pipeline.py" --environment $ENVIRONMENT --region $REGION; then
    print_status $GREEN "✅ End-to-end test PASSED"
    TEST_SUCCESS=true
else
    print_status $RED "❌ End-to-end test FAILED"
    TEST_SUCCESS=false
fi

echo ""

# Step 3: Test specific tables (optional)
print_status $YELLOW "🔍 Step 3: Testing individual tables..."
echo "----------------------------------------"

TABLES=("companies" "contacts" "tickets" "entries")
TABLE_RESULTS=()

for table in "${TABLES[@]}"; do
    echo "Testing table: $table"
    if python3 "$TEST_DIR/test-end-to-end-pipeline.py" --environment $ENVIRONMENT --region $REGION --table $table; then
        print_status $GREEN "✅ Table $table test passed"
        TABLE_RESULTS+=("$table:PASS")
    else
        print_status $RED "❌ Table $table test failed"
        TABLE_RESULTS+=("$table:FAIL")
    fi
    echo ""
done

# Step 4: Generate summary report
print_status $YELLOW "📋 Step 4: Generating test summary..."
echo "----------------------------------------"

echo "Test Results Summary:"
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"
echo "Test Time: $(date)"
echo ""

if [ "$TEST_SUCCESS" = true ]; then
    print_status $GREEN "✅ Overall Test Status: PASSED"
else
    print_status $RED "❌ Overall Test Status: FAILED"
fi

echo ""
echo "Individual Table Results:"
for result in "${TABLE_RESULTS[@]}"; do
    table=$(echo $result | cut -d: -f1)
    status=$(echo $result | cut -d: -f2)
    if [ "$status" = "PASS" ]; then
        print_status $GREEN "  ✅ $table: $status"
    else
        print_status $RED "  ❌ $table: $status"
    fi
done

echo ""

# Step 5: Check S3 data structure
print_status $YELLOW "📁 Step 5: Verifying S3 data structure..."
echo "----------------------------------------"

BUCKET_NAME="data-storage-msp-$ENVIRONMENT"
echo "Checking S3 bucket: $BUCKET_NAME"

if aws s3 ls "s3://$BUCKET_NAME/" > /dev/null 2>&1; then
    print_status $GREEN "✅ S3 bucket accessible"
    
    # List recent data
    echo "Recent data in S3:"
    aws s3 ls "s3://$BUCKET_NAME/" --recursive | tail -10
    
    # Check data size
    TOTAL_SIZE=$(aws s3 ls "s3://$BUCKET_NAME/" --recursive --summarize | grep "Total Size" | awk '{print $3}')
    if [ ! -z "$TOTAL_SIZE" ]; then
        echo "Total data size: $TOTAL_SIZE bytes"
    fi
else
    print_status $RED "❌ S3 bucket not accessible or doesn't exist"
fi

echo ""

# Final summary
print_status $YELLOW "🎯 Final Summary"
echo "================"

if [ "$TEST_SUCCESS" = true ]; then
    print_status $GREEN "🎉 Staging environment pipeline test completed successfully!"
    print_status $GREEN "✅ All systems operational"
    exit 0
else
    print_status $RED "❌ Staging environment pipeline test failed"
    print_status $RED "⚠️  Please review logs and fix issues before proceeding"
    exit 1
fi