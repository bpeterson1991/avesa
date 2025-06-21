#!/bin/bash

# AVESA Real ClickHouse Development Server Startup Script
# This script starts both the real ClickHouse API and React frontend

set -e

echo "🚀 Starting AVESA with Real ClickHouse Data"
echo "==========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if we're in the right directory
if [ ! -d "src/clickhouse/api" ] && [ ! -d "frontend" ]; then
    print_error "Please run this script from the AVESA project root directory"
    exit 1
fi

# Function to check and configure AWS credentials
check_aws_credentials() {
    local target_profile="AdministratorAccess-123938354448"
    
    print_status "Checking AWS credentials..."
    
    # First, try current credentials
    if aws sts get-caller-identity > /dev/null 2>&1; then
        print_success "AWS credentials already configured and working"
        return 0
    fi
    
    # If no current credentials, try to set the target profile
    print_status "No default credentials found. Trying target profile: $target_profile"
    
    # Check if target profile exists
    if ! aws configure list-profiles 2>/dev/null | grep -q "^$target_profile$"; then
        print_error "Required AWS profile '$target_profile' not found"
        print_status "Available AWS profiles:"
        aws configure list-profiles 2>/dev/null || echo "No profiles found"
        echo ""
        print_status "To fix this issue:"
        echo "   1. Configure the profile: aws configure --profile $target_profile"
        echo "   2. Or use AWS SSO: aws sso login --profile $target_profile"
        echo "   3. Or set AWS_PROFILE environment variable: export AWS_PROFILE=$target_profile"
        exit 1
    fi
    
    # Test the target profile
    if AWS_PROFILE=$target_profile aws sts get-caller-identity > /dev/null 2>&1; then
        print_success "Target profile '$target_profile' works. Setting AWS_PROFILE."
        export AWS_PROFILE=$target_profile
        return 0
    else
        print_error "AWS profile '$target_profile' exists but credentials are not working"
        print_status "This might be due to:"
        echo "   • Expired SSO session - try: aws sso login --profile $target_profile"
        echo "   • Invalid credentials - try: aws configure --profile $target_profile"
        echo "   • Network connectivity issues"
        exit 1
    fi
}

# Check AWS credentials
check_aws_credentials

# Kill any existing processes on these ports
print_status "Cleaning up existing processes..."
lsof -ti:3001 | xargs kill -9 2>/dev/null || true
lsof -ti:3000 | xargs kill -9 2>/dev/null || true
sleep 2

# Function to cleanup background processes
cleanup() {
    print_status "Shutting down services..."
    if [ ! -z "$API_PID" ]; then
        kill $API_PID 2>/dev/null || true
    fi
    if [ ! -z "$FRONTEND_PID" ]; then
        kill $FRONTEND_PID 2>/dev/null || true
    fi
    exit 0
}

# Set trap to cleanup on script exit
trap cleanup SIGINT SIGTERM

# Start Real ClickHouse API server in background
print_status "Starting Real ClickHouse API server on port 3001..."
cd src/clickhouse/api

# Set environment variables for real ClickHouse connection
export AWS_SDK_LOAD_CONFIG=1
# AWS_PROFILE should already be set by check_aws_credentials function
# But ensure it's set to the correct profile if not already configured
if [ -z "$AWS_PROFILE" ]; then
    export AWS_PROFILE=AdministratorAccess-123938354448
fi
export NODE_ENV=development
export PORT=3001
export CLICKHOUSE_SECRET_NAME=arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO
export AWS_REGION=us-east-2
export JWT_SECRET=avesa-demo-secret-key-2024

# Verify AWS credentials are working before starting the server
print_status "Verifying AWS credentials for Node.js process..."
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    print_error "AWS credentials verification failed before starting server"
    exit 1
fi
print_success "AWS credentials verified for Node.js process"

# Start the real server
node server.js > api.log 2>&1 &
API_PID=$!
cd ../../..

# Wait for API to start
sleep 5

# Check if API is running
if kill -0 $API_PID 2>/dev/null; then
    print_success "Real ClickHouse API server started (PID: $API_PID)"
else
    print_error "Real ClickHouse API server failed to start"
    print_status "Checking API logs..."
    cat src/clickhouse/api/api.log
    print_warning "Falling back to mock server..."
    
    # Start mock server as fallback
    cd src/clickhouse/api
    NODE_ENV=development JWT_SECRET=avesa-demo-secret-key-2024 PORT=3001 node mock-server.js > api.log 2>&1 &
    API_PID=$!
    cd ../../..
    sleep 3
    
    if kill -0 $API_PID 2>/dev/null; then
        print_success "Mock API server started as fallback (PID: $API_PID)"
    else
        print_error "Both real and mock API servers failed to start"
        exit 1
    fi
fi

# Test API health
print_status "Testing API health..."
if curl -s http://localhost:3001/health > /dev/null; then
    print_success "API health check passed"
else
    print_warning "API health check failed, but continuing..."
fi

# Start frontend development server
print_status "Starting React frontend on port 3000..."
cd frontend

# Set frontend environment variables
export REACT_APP_API_URL=http://localhost:3001
export PORT=3000

# Add diagnostic logging
print_status "Frontend environment diagnostics:"
echo "   • Working directory: $(pwd)"
echo "   • REACT_APP_API_URL: $REACT_APP_API_URL"
echo "   • PORT: $PORT"
echo "   • Contents of .env file:"
if [ -f ".env" ]; then
    cat .env | sed 's/^/     /'
else
    echo "     No .env file found"
fi
echo "   • package.json proxy setting:"
grep -A1 -B1 '"proxy"' package.json | sed 's/^/     /' || echo "     No proxy setting found"

# Start frontend in background
print_status "Starting frontend with explicit environment variables..."
npm start > frontend.log 2>&1 &
FRONTEND_PID=$!
cd ..

# Wait for frontend to start
sleep 8

# Check if frontend is running
if kill -0 $FRONTEND_PID 2>/dev/null; then
    print_success "Frontend started (PID: $FRONTEND_PID)"
else
    print_error "Frontend failed to start"
    cat frontend/frontend.log
    exit 1
fi

echo ""
echo "🎉 AVESA Development Environment Started!"
echo "========================================"
echo ""
echo "📊 Services:"
echo "   • API Server:       http://localhost:3001"
echo "   • Frontend:         http://localhost:3000"
echo "   • Health Check:     http://localhost:3001/health"
echo ""
echo "🔐 Demo Credentials:"
echo "   • Tenant ID:        sitetechnology"
echo "   • Email:            admin@sitetechnology.com"
echo "   • Password:         demo123"
echo ""
echo "📚 Available API Endpoints:"
echo "   • POST /auth/login           - User authentication"
echo "   • GET  /api/analytics/dashboard - Dashboard data"
echo "   • GET  /api/companies        - Companies list"
echo "   • GET  /api/contacts         - Contacts list"
echo "   • GET  /api/tickets          - Tickets list"
echo "   • GET  /api/time-entries     - Time entries list"
echo ""
echo "💡 Tips:"
echo "   • API logs: src/clickhouse/api/api.log"
echo "   • Frontend logs: frontend/frontend.log"
echo "   • Press Ctrl+C to stop all services"
echo ""
echo "API PID: $API_PID"
echo "Frontend PID: $FRONTEND_PID"
echo ""

# Try to open browser (macOS)
if command -v open &> /dev/null; then
    print_status "Opening browser..."
    sleep 3
    open http://localhost:3000
fi

# Keep script running and monitor processes
while true; do
    # Check if API is still running
    if ! kill -0 $API_PID 2>/dev/null; then
        print_error "API server stopped unexpectedly"
        cleanup
    fi
    
    # Check if frontend is still running
    if ! kill -0 $FRONTEND_PID 2>/dev/null; then
        print_error "Frontend server stopped unexpectedly"
        cleanup
    fi
    
    sleep 5
done