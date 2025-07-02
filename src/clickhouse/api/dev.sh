#!/bin/bash

# ClickHouse API Development Script
# Supports both Express and Lambda simulation modes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

print_info() {
    echo -e "${BLUE}[DEV]${NC} $1"
}

# Usage function
usage() {
    echo "Usage: $0 [mode] [environment]"
    echo ""
    echo "Arguments:"
    echo "  mode        - express (default) or lambda"
    echo "  environment - dev (default), staging, or prod"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run in Express mode with dev environment"
    echo "  $0 express            # Run in Express mode with dev environment"
    echo "  $0 lambda             # Run in Lambda simulation mode with dev environment"
    echo "  $0 express staging    # Run in Express mode with staging environment"
    echo "  $0 lambda prod        # Run in Lambda simulation mode with prod environment"
    echo ""
    echo "Lambda simulation uses serverless-offline to simulate API Gateway + Lambda locally"
    exit 1
}

# Parse arguments
MODE=${1:-express}
ENVIRONMENT=${2:-dev}

# Validate mode
case $MODE in
    express|lambda)
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        print_error "Invalid mode: $MODE"
        usage
        ;;
esac

# Validate environment
case $ENVIRONMENT in
    dev|staging|prod)
        ;;
    *)
        print_error "Invalid environment: $ENVIRONMENT"
        usage
        ;;
esac

print_status "Starting ClickHouse API in $MODE mode with $ENVIRONMENT environment..."

# Check if we're in the correct directory
if [ ! -f "package.json" ]; then
    print_error "package.json not found. Please run this script from the API directory."
    exit 1
fi

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    print_status "Installing dependencies..."
    npm install
fi

# Set environment variables
export NODE_ENV=$ENVIRONMENT
export DEVELOPMENT_MODE=$MODE

# Load environment-specific configuration
if [ -f "environment_config.json" ]; then
    print_info "Using environment configuration for $ENVIRONMENT"
else
    print_warning "environment_config.json not found, using defaults"
fi

# Display configuration info
print_info "Configuration:"
print_info "  Environment: $ENVIRONMENT"
print_info "  Mode: $MODE"
print_info "  Node Environment: $NODE_ENV"

if [ "$MODE" = "express" ]; then
    # Express mode - direct Node.js execution
    print_status "Starting Express server directly..."
    print_info "Server will be available at: http://localhost:3001"
    print_info "Health check: http://localhost:3001/health"
    print_info "API info: http://localhost:3001/api/info"
    print_info ""
    print_info "Press Ctrl+C to stop the server"
    print_info ""
    
    # Start the Express server
    node app.js

elif [ "$MODE" = "lambda" ]; then
    # Lambda simulation mode
    print_status "Starting Lambda simulation with serverless-offline..."
    
    # Check if serverless framework is available
    if ! command -v serverless &> /dev/null && ! command -v sls &> /dev/null; then
        print_warning "Serverless framework not found globally. Installing locally..."
        npm install --save-dev serverless serverless-offline
    fi
    
    # Create temporary serverless.yml for development
    cat > serverless-dev.yml << EOF
service: clickhouse-api-dev

provider:
  name: aws
  runtime: nodejs18.x
  region: us-west-2
  stage: $ENVIRONMENT

functions:
  api:
    handler: lambda.handler
    events:
      - http:
          path: /{proxy+}
          method: ANY
          cors: true
      - http:
          path: /
          method: ANY
          cors: true
    environment:
      NODE_ENV: $ENVIRONMENT
      AWS_LAMBDA_FUNCTION_NAME: clickhouse-api-dev

plugins:
  - serverless-offline

custom:
  serverless-offline:
    host: localhost
    port: 3001
    stage: $ENVIRONMENT
    prefix: api
EOF

    print_info "Lambda simulation will be available at: http://localhost:3001"
    print_info "Health check: http://localhost:3001/health"
    print_info "API info: http://localhost:3001/api/info"
    print_info ""
    print_info "Press Ctrl+C to stop the simulation"
    print_info ""
    
    # Start serverless offline
    if command -v serverless &> /dev/null; then
        serverless offline start --config serverless-dev.yml
    elif command -v sls &> /dev/null; then
        sls offline start --config serverless-dev.yml
    else
        # Use local installation
        npx serverless offline start --config serverless-dev.yml
    fi
    
    # Clean up temporary file
    rm -f serverless-dev.yml
fi