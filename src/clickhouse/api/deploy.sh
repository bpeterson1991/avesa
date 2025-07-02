#!/bin/bash

# ClickHouse API Deployment Script
# Supports both Lambda and EC2 deployment modes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage function
usage() {
    echo "Usage: $0 <environment> <deployment_type>"
    echo ""
    echo "Arguments:"
    echo "  environment     - dev, staging, or prod"
    echo "  deployment_type - lambda or ec2"
    echo ""
    echo "Examples:"
    echo "  $0 dev lambda     # Deploy to Lambda for development"
    echo "  $0 staging lambda # Deploy to Lambda for staging"
    echo "  $0 prod ec2       # Deploy to EC2 for production"
    echo ""
    exit 1
}

# Check arguments
if [ $# -ne 2 ]; then
    usage
fi

ENVIRONMENT=$1
DEPLOYMENT_TYPE=$2

# Validate environment
case $ENVIRONMENT in
    dev|staging|prod)
        ;;
    *)
        print_error "Invalid environment: $ENVIRONMENT"
        usage
        ;;
esac

# Validate deployment type
case $DEPLOYMENT_TYPE in
    lambda|ec2)
        ;;
    *)
        print_error "Invalid deployment type: $DEPLOYMENT_TYPE"
        usage
        ;;
esac

print_status "Starting deployment for $ENVIRONMENT environment using $DEPLOYMENT_TYPE..."

# Check if we're in the correct directory
if [ ! -f "package.json" ]; then
    print_error "package.json not found. Please run this script from the API directory."
    exit 1
fi

# Check if CDK is available for Lambda deployments
if [ "$DEPLOYMENT_TYPE" = "lambda" ]; then
    if ! command -v cdk &> /dev/null; then
        print_error "AWS CDK not found. Please install CDK: npm install -g aws-cdk"
        exit 1
    fi
fi

# Install dependencies
print_status "Installing dependencies..."
npm install

# Run tests
print_status "Running tests..."
npm test

if [ $? -ne 0 ]; then
    print_error "Tests failed. Deployment aborted."
    exit 1
fi

# Deploy based on type
if [ "$DEPLOYMENT_TYPE" = "lambda" ]; then
    print_status "Deploying to AWS Lambda via CDK..."
    
    # Navigate to CDK directory
    cd ../../../infrastructure
    
    # Install CDK dependencies
    print_status "Installing CDK dependencies..."
    pip install -r requirements.txt
    
    # Set environment variables for CDK
    export ENVIRONMENT=$ENVIRONMENT
    export DEPLOYMENT_TYPE=lambda
    
    # Deploy the web application stack
    print_status "Deploying CDK stack..."
    cdk deploy WebApplicationStack-$ENVIRONMENT --require-approval never
    
    if [ $? -eq 0 ]; then
        print_status "Lambda deployment completed successfully!"
        print_status "API Gateway endpoint will be shown in the CDK output above."
    else
        print_error "Lambda deployment failed!"
        exit 1
    fi
    
elif [ "$DEPLOYMENT_TYPE" = "ec2" ]; then
    print_status "Preparing for EC2 deployment..."
    
    # Create deployment package
    print_status "Creating deployment package..."
    rm -rf dist/
    mkdir -p dist/
    
    # Copy application files
    cp -r . dist/ 2>/dev/null || true
    
    # Remove development files
    rm -rf dist/node_modules/
    rm -rf dist/test/
    rm -rf dist/dist/
    rm -f dist/*.log
    
    # Install production dependencies
    cd dist/
    npm install --production
    cd ..
    
    # Create archive
    tar -czf "clickhouse-api-$ENVIRONMENT.tar.gz" -C dist/ .
    
    print_status "EC2 deployment package created: clickhouse-api-$ENVIRONMENT.tar.gz"
    print_status ""
    print_status "Next steps for EC2 deployment:"
    print_status "1. Copy the package to your EC2 instance"
    print_status "2. Extract: tar -xzf clickhouse-api-$ENVIRONMENT.tar.gz"
    print_status "3. Set environment variables:"
    print_status "   export NODE_ENV=$ENVIRONMENT"
    print_status "   export CLICKHOUSE_HOST=your-clickhouse-host"
    print_status "   export CLICKHOUSE_USER=your-username"
    print_status "   export CLICKHOUSE_PASSWORD=your-password"
    print_status "4. Start the application: npm start"
    print_status ""
    print_status "For production, consider using PM2:"
    print_status "   npm install -g pm2"
    print_status "   pm2 start app.js --name clickhouse-api"
fi

print_status "Deployment process completed!"