#!/bin/bash

# Validation script for ClickHouse API Serverless Express deployment setup
# This script checks if all components are properly configured

set -e

echo "========================================="
echo "ClickHouse API Serverless Setup Validation"
echo "========================================="

API_DIR="src/clickhouse/api"
INFRA_DIR="infrastructure"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

success_count=0
total_checks=0

check_file() {
    local file_path="$1"
    local description="$2"
    ((total_checks++))
    
    if [ -f "$file_path" ]; then
        echo -e "${GREEN}✅ $description${NC}"
        ((success_count++))
        return 0
    else
        echo -e "${RED}❌ $description - File not found: $file_path${NC}"
        return 1
    fi
}

check_directory() {
    local dir_path="$1"
    local description="$2"
    ((total_checks++))
    
    if [ -d "$dir_path" ]; then
        echo -e "${GREEN}✅ $description${NC}"
        ((success_count++))
        return 0
    else
        echo -e "${RED}❌ $description - Directory not found: $dir_path${NC}"
        return 1
    fi
}

echo "Checking API Structure..."
echo "------------------------"

# Core API files
check_file "$API_DIR/app.js" "Express application"
check_file "$API_DIR/lambda.js" "Lambda handler with serverless-express"
check_file "$API_DIR/package.json" "Package configuration"
check_file "$API_DIR/environment_config.json" "Environment configuration"

# Configuration
check_directory "$API_DIR/config" "Configuration directory"
check_file "$API_DIR/config/clickhouse.js" "ClickHouse configuration"

# Middleware
check_directory "$API_DIR/middleware" "Middleware directory"
check_file "$API_DIR/middleware/errorHandler.js" "Error handler middleware"
check_file "$API_DIR/middleware/requestLogger.js" "Request logger middleware"

# Routes
check_directory "$API_DIR/routes" "Routes directory"
check_file "$API_DIR/routes/analytics.js" "Analytics routes"

# Scripts
check_file "$API_DIR/deploy.sh" "Deployment script"
check_file "$API_DIR/dev.sh" "Development script"

# Tests
check_directory "$API_DIR/test" "Test directory"
check_file "$API_DIR/test/api.test.js" "API tests"

echo ""
echo "Checking Infrastructure..."
echo "-------------------------"

# Infrastructure files
check_file "$INFRA_DIR/stacks/web_application_stack.py" "Web Application CDK stack"
check_file "$INFRA_DIR/app.py" "CDK application"
check_file "$INFRA_DIR/requirements.txt" "Infrastructure requirements"

echo ""
echo "Checking Package Dependencies..."
echo "-------------------------------"

if [ -f "$API_DIR/package.json" ]; then
    # Check for required dependencies
    if grep -q "@vendia/serverless-express" "$API_DIR/package.json"; then
        echo -e "${GREEN}✅ Serverless Express dependency${NC}"
        ((success_count++))
    else
        echo -e "${RED}❌ Serverless Express dependency missing${NC}"
    fi
    ((total_checks++))
    
    if grep -q "aws-sdk" "$API_DIR/package.json"; then
        echo -e "${GREEN}✅ AWS SDK dependency${NC}"
        ((success_count++))
    else
        echo -e "${RED}❌ AWS SDK dependency missing${NC}"
    fi
    ((total_checks++))
    
    if grep -q "express" "$API_DIR/package.json"; then
        echo -e "${GREEN}✅ Express dependency${NC}"
        ((success_count++))
    else
        echo -e "${RED}❌ Express dependency missing${NC}"
    fi
    ((total_checks++))
    
    if grep -q "supertest" "$API_DIR/package.json"; then
        echo -e "${GREEN}✅ Testing dependencies${NC}"
        ((success_count++))
    else
        echo -e "${RED}❌ Testing dependencies missing${NC}"
    fi
    ((total_checks++))
fi

echo ""
echo "Checking Script Permissions..."
echo "-----------------------------"

((total_checks++))
if [ -x "$API_DIR/deploy.sh" ]; then
    echo -e "${GREEN}✅ Deploy script is executable${NC}"
    ((success_count++))
else
    echo -e "${RED}❌ Deploy script is not executable${NC}"
fi

((total_checks++))
if [ -x "$API_DIR/dev.sh" ]; then
    echo -e "${GREEN}✅ Development script is executable${NC}"
    ((success_count++))
else
    echo -e "${RED}❌ Development script is not executable${NC}"
fi

echo ""
echo "Checking CDK Integration..."
echo "--------------------------"

if [ -f "$INFRA_DIR/app.py" ]; then
    ((total_checks++))
    if grep -q "WebApplicationStack" "$INFRA_DIR/app.py"; then
        echo -e "${GREEN}✅ Web Application stack imported in CDK app${NC}"
        ((success_count++))
    else
        echo -e "${RED}❌ Web Application stack not imported in CDK app${NC}"
    fi
    
    ((total_checks++))
    if grep -q "AVESAWebApplication" "$INFRA_DIR/app.py"; then
        echo -e "${GREEN}✅ Web Application stack instantiated${NC}"
        ((success_count++))
    else
        echo -e "${RED}❌ Web Application stack not instantiated${NC}"
    fi
fi

echo ""
echo "========================================="
echo "Validation Summary"
echo "========================================="

if [ $success_count -eq $total_checks ]; then
    echo -e "${GREEN}🎉 All checks passed! ($success_count/$total_checks)${NC}"
    echo ""
    echo "Next Steps:"
    echo "1. Install dependencies: cd $API_DIR && npm install"
    echo "2. Test locally: cd $API_DIR && ./dev.sh express"
    echo "3. Deploy to Lambda: cd $API_DIR && ./deploy.sh dev lambda"
    echo "4. Validate deployment with the provided tests"
    echo ""
    echo "Your serverless-express setup is ready! 🚀"
    exit 0
else
    echo -e "${RED}❌ Some checks failed: $success_count/$total_checks passed${NC}"
    echo ""
    echo "Please review the failed checks above and ensure all required files are in place."
    echo "You can re-run this script after making corrections."
    exit 1
fi