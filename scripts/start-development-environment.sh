#!/bin/bash

# AVESA Unified Development Environment Launcher
# Consolidates all server startup scripts with mode selection

set -e

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

show_usage() {
    echo "🚀 AVESA Development Environment Launcher"
    echo "========================================"
    echo ""
    echo "Usage: $0 [MODE]"
    echo ""
    echo "Available Modes:"
    echo "  real        - Real ClickHouse data with AWS credentials"
    echo "  mock        - Mock data for development"
    echo "  api-only    - API server only (real ClickHouse)"
    echo "  frontend    - Frontend development with hot reload"
    echo ""
    echo "Examples:"
    echo "  $0 real      # Start with real ClickHouse data"
    echo "  $0 mock      # Start with mock data"
    echo "  $0 api-only  # Start only API server"
    echo "  $0 frontend  # Start full-stack with hot reload"
    echo ""
    exit 1
}

check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check if we're in the right directory
    if [ ! -d "src/clickhouse/api" ] && [ ! -d "frontend" ]; then
        print_error "Please run this script from the AVESA project root directory"
        exit 1
    fi
    
    # Check Node.js
    if ! command -v node &> /dev/null; then
        print_error "Node.js is not installed. Please install Node.js 18+ and try again."
        exit 1
    fi
    
    # Check Node.js version
    NODE_VERSION=$(node --version | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$NODE_VERSION" -lt 18 ]; then
        print_error "Node.js version 18+ is required. Current version: $(node --version)"
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

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
        return 1
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
        return 1
    fi
}

install_dependencies() {
    print_status "Installing dependencies if needed..."
    
    # Install API dependencies if needed
    if [ ! -d "src/clickhouse/api/node_modules" ]; then
        print_status "Installing API dependencies..."
        cd src/clickhouse/api
        npm install
        cd ../../..
        print_success "API dependencies installed"
    fi
    
    # Install frontend dependencies if needed
    if [ ! -d "frontend/node_modules" ]; then
        print_status "Installing frontend dependencies..."
        cd frontend
        npm install
        cd ..
        print_success "Frontend dependencies installed"
    fi
}

cleanup_existing_processes() {
    print_status "Cleaning up existing processes..."
    lsof -ti:3001 | xargs kill -9 2>/dev/null || true
    lsof -ti:3000 | xargs kill -9 2>/dev/null || true
    sleep 2
}

cleanup() {
    print_warning "Shutting down services..."
    if [ ! -z "$API_PID" ]; then
        kill $API_PID 2>/dev/null || true
    fi
    if [ ! -z "$FRONTEND_PID" ]; then
        kill $FRONTEND_PID 2>/dev/null || true
    fi
    exit 0
}

start_real_mode() {
    echo "🚀 Starting AVESA with Real ClickHouse Data"
    echo "==========================================="
    
    # Check AWS credentials for real mode
    if ! check_aws_credentials; then
        exit 1
    fi
    
    # Start Real ClickHouse API server
    print_status "Starting Real ClickHouse API server on port 3001..."
    cd src/clickhouse/api
    
    # Set environment variables for real ClickHouse connection
    export AWS_SDK_LOAD_CONFIG=1
    export NODE_ENV=development
    export CLICKHOUSE_MODE=real
    export PORT=3001
    export CLICKHOUSE_SECRET_NAME=arn:aws:secretsmanager:us-east-2:123938354448:secret:clickhouse-connection-dev-V9zSgO
    export AWS_REGION=us-east-2
    export JWT_SECRET=avesa-demo-secret-key-2024
    
    # Verify AWS credentials before starting
    if ! aws sts get-caller-identity > /dev/null 2>&1; then
        print_error "AWS credentials verification failed"
        exit 1
    fi
    
    node server.js > api.log 2>&1 &
    API_PID=$!
    cd ../../..
    
    sleep 5
    
    if kill -0 $API_PID 2>/dev/null; then
        print_success "Real ClickHouse API server started (PID: $API_PID)"
    else
        print_error "Real ClickHouse API server failed to start"
        print_warning "Falling back to mock server..."
        start_mock_api
    fi
    
    start_frontend
}

start_mock_mode() {
    echo "🚀 Starting AVESA with Mock Data"
    echo "================================"
    
    start_mock_api
    start_frontend
}

start_mock_api() {
    print_status "Starting Mock API server on port 3001..."
    cd src/clickhouse/api
    NODE_ENV=development CLICKHOUSE_MODE=mock JWT_SECRET=avesa-demo-secret-key-2024 PORT=3001 node mock-server.js > api.log 2>&1 &
    API_PID=$!
    cd ../../..
    
    sleep 3
    
    if kill -0 $API_PID 2>/dev/null; then
        print_success "Mock API server started (PID: $API_PID)"
    else
        print_error "Mock API server failed to start"
        cat src/clickhouse/api/api.log
        exit 1
    fi
}

start_api_only_mode() {
    echo "🚀 Starting AVESA API Server Only"
    echo "================================="
    
    # Check AWS credentials for API-only mode
    if ! check_aws_credentials; then
        exit 1
    fi
    
    print_status "Starting ClickHouse API server on port 3001..."
    cd src/clickhouse/api
    
    export AWS_SDK_LOAD_CONFIG=1
    export NODE_ENV=development
    export CLICKHOUSE_MODE=real
    export PORT=3001
    export CLICKHOUSE_SECRET_NAME=clickhouse-connection-dev
    export AWS_REGION=us-east-2
    
    npm start &
    API_PID=$!
    cd ../../..
    
    sleep 3
    
    if kill -0 $API_PID 2>/dev/null; then
        print_success "API server started (PID: $API_PID)"
        print_success "API server running at: http://localhost:3001"
        print_success "Health check: http://localhost:3001/health"
    else
        print_error "API server failed to start"
        exit 1
    fi
    
    # Test the health endpoint
    sleep 2
    print_status "Testing API health..."
    curl -s http://localhost:3001/health | jq . || echo "Health check endpoint not ready yet"
}

start_frontend_mode() {
    echo "🚀 Starting AVESA Full-Stack Development"
    echo "======================================="
    
    start_mock_api
    start_frontend
}

start_frontend() {
    print_status "Starting React frontend on port 3000..."
    cd frontend
    
    export REACT_APP_API_URL=http://localhost:3001
    export PORT=3000
    
    npm start > frontend.log 2>&1 &
    FRONTEND_PID=$!
    cd ..
    
    sleep 8
    
    if kill -0 $FRONTEND_PID 2>/dev/null; then
        print_success "Frontend started (PID: $FRONTEND_PID)"
    else
        print_error "Frontend failed to start"
        cat frontend/frontend.log
        exit 1
    fi
}

show_final_status() {
    echo ""
    echo "🎉 AVESA Development Environment Started!"
    echo "========================================"
    echo ""
    echo "📊 Services:"
    if [ ! -z "$API_PID" ]; then
        echo "   • API Server:       http://localhost:3001"
        echo "   • Health Check:     http://localhost:3001/health"
    fi
    if [ ! -z "$FRONTEND_PID" ]; then
        echo "   • Frontend:         http://localhost:3000"
    fi
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
    if [ ! -z "$API_PID" ]; then
        echo "   • API logs: src/clickhouse/api/api.log"
    fi
    if [ ! -z "$FRONTEND_PID" ]; then
        echo "   • Frontend logs: frontend/frontend.log"
    fi
    echo "   • Press Ctrl+C to stop all services"
    echo ""
    if [ ! -z "$API_PID" ]; then
        echo "API PID: $API_PID"
    fi
    if [ ! -z "$FRONTEND_PID" ]; then
        echo "Frontend PID: $FRONTEND_PID"
    fi
    echo ""
    
    # Try to open browser (macOS)
    if [ ! -z "$FRONTEND_PID" ] && command -v open &> /dev/null; then
        print_status "Opening browser..."
        sleep 3
        open http://localhost:3000
    fi
}

monitor_processes() {
    # Keep script running and monitor processes
    while true; do
        # Check if API is still running
        if [ ! -z "$API_PID" ] && ! kill -0 $API_PID 2>/dev/null; then
            print_error "API server stopped unexpectedly"
            cleanup
        fi
        
        # Check if frontend is still running
        if [ ! -z "$FRONTEND_PID" ] && ! kill -0 $FRONTEND_PID 2>/dev/null; then
            print_error "Frontend server stopped unexpectedly"
            cleanup
        fi
        
        sleep 5
    done
}

main() {
    # Parse command line arguments
    MODE=${1:-""}
    
    if [ -z "$MODE" ]; then
        show_usage
    fi
    
    # Set trap to cleanup on script exit
    trap cleanup SIGINT SIGTERM
    
    # Run prerequisite checks
    check_prerequisites
    install_dependencies
    cleanup_existing_processes
    
    # Start services based on mode
    case "$MODE" in
        "real")
            start_real_mode
            ;;
        "mock")
            start_mock_mode
            ;;
        "api-only")
            start_api_only_mode
            ;;
        "frontend")
            start_frontend_mode
            ;;
        *)
            print_error "Unknown mode: $MODE"
            show_usage
            ;;
    esac
    
    # Show final status
    show_final_status
    
    # Monitor processes (only if we have processes to monitor)
    if [ ! -z "$API_PID" ] || [ ! -z "$FRONTEND_PID" ]; then
        monitor_processes
    else
        # For API-only mode, just wait
        echo "Press Ctrl+C to stop the API server"
        wait
    fi
}

# Execute main function
main "$@"