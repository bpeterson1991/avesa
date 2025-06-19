#!/bin/bash
# Migration Readiness Check Script
# Verifies that all required files and dependencies are in place

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}AVESA Migration Readiness Check${NC}"
echo -e "${BLUE}========================================${NC}"

ERRORS=0
WARNINGS=0

# Function to check if file exists
check_file() {
    local file="$1"
    local description="$2"
    
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $description: $file"
    else
        echo -e "${RED}❌${NC} $description: $file (MISSING)"
        ((ERRORS++))
    fi
}

# Function to check if command exists
check_command() {
    local cmd="$1"
    local description="$2"
    
    if command -v "$cmd" >/dev/null 2>&1; then
        local version=$(eval "$cmd --version 2>/dev/null | head -n1" || echo "unknown")
        echo -e "${GREEN}✓${NC} $description: $cmd ($version)"
    else
        echo -e "${RED}❌${NC} $description: $cmd (NOT INSTALLED)"
        ((ERRORS++))
    fi
}

# Function to check environment variable
check_env_var() {
    local var="$1"
    local description="$2"
    local required="$3"
    
    if [ -n "${!var}" ]; then
        echo -e "${GREEN}✓${NC} $description: $var=${!var}"
    else
        if [ "$required" = "true" ]; then
            echo -e "${RED}❌${NC} $description: $var (NOT SET)"
            ((ERRORS++))
        else
            echo -e "${YELLOW}⚠️${NC} $description: $var (NOT SET - will be set during setup)"
            ((WARNINGS++))
        fi
    fi
}

echo -e "\n${YELLOW}Checking required files...${NC}"

# Check migration scripts
check_file "scripts/deploy-prod.sh" "Production deployment script"
check_file "scripts/migrate-production-data.py" "Data migration script"
check_file "scripts/validate-hybrid-setup.py" "Validation script"
check_file "scripts/setup-production-account.sh" "Account setup script"

# Check infrastructure files
check_file "infrastructure/app.py" "CDK application"
check_file "infrastructure/stacks/cross_account_monitoring.py" "Cross-account monitoring stack"
check_file "infrastructure/cdk.json" "CDK configuration"

# Check documentation
check_file "docs/AWS_ACCOUNT_ISOLATION_IMPLEMENTATION_PLAN.md" "Implementation plan"
check_file "docs/PRODUCTION_ACCOUNT_SETUP_GUIDE.md" "Setup guide"
check_file "docs/MIGRATION_CHECKLIST.md" "Migration checklist"

echo -e "\n${YELLOW}Checking required tools...${NC}"

# Check required commands
check_command "aws" "AWS CLI"
check_command "cdk" "AWS CDK"
check_command "python3" "Python 3"
check_command "pip3" "Python package manager"
check_command "jq" "JSON processor (optional but recommended)"

echo -e "\n${YELLOW}Checking environment variables...${NC}"

# Check environment variables
check_env_var "CDK_DEFAULT_REGION" "Default AWS region" "false"
check_env_var "CDK_PROD_ACCOUNT" "Production account ID" "false"

echo -e "\n${YELLOW}Checking AWS configuration...${NC}"

# Check AWS default profile
if aws sts get-caller-identity >/dev/null 2>&1; then
    CURRENT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
    echo -e "${GREEN}✓${NC} AWS default profile configured (Account: $CURRENT_ACCOUNT)"
else
    echo -e "${RED}❌${NC} AWS default profile not configured or not accessible"
    ((ERRORS++))
fi

# Check if production profile exists (optional at this stage)
if aws sts get-caller-identity --profile avesa-production >/dev/null 2>&1; then
    PROD_ACCOUNT=$(aws sts get-caller-identity --profile avesa-production --query Account --output text 2>/dev/null)
    echo -e "${GREEN}✓${NC} AWS production profile configured (Account: $PROD_ACCOUNT)"
else
    echo -e "${YELLOW}⚠️${NC} AWS production profile not configured (will be set up during migration)"
    ((WARNINGS++))
fi

echo -e "\n${YELLOW}Checking Python dependencies...${NC}"

# Check if we're in a virtual environment (recommended)
if [ -n "$VIRTUAL_ENV" ]; then
    echo -e "${GREEN}✓${NC} Python virtual environment active: $VIRTUAL_ENV"
elif [ -d "infrastructure/.venv" ]; then
    echo -e "${YELLOW}⚠️${NC} Virtual environment exists but not active: infrastructure/.venv"
    echo -e "   ${YELLOW}Consider activating: source infrastructure/.venv/bin/activate${NC}"
    ((WARNINGS++))
else
    echo -e "${YELLOW}⚠️${NC} No Python virtual environment detected"
    echo -e "   ${YELLOW}Consider creating one: cd infrastructure && python3 -m venv .venv${NC}"
    ((WARNINGS++))
fi

# Check Python packages
if [ -f "infrastructure/requirements.txt" ]; then
    echo -e "${GREEN}✓${NC} Python requirements file exists"
    
    # Try to check if packages are installed
    if python3 -c "import aws_cdk" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} AWS CDK Python library installed"
    else
        echo -e "${YELLOW}⚠️${NC} AWS CDK Python library not installed"
        echo -e "   ${YELLOW}Install with: pip3 install -r infrastructure/requirements.txt${NC}"
        ((WARNINGS++))
    fi
else
    echo -e "${YELLOW}⚠️${NC} Python requirements file not found"
    ((WARNINGS++))
fi

echo -e "\n${YELLOW}Checking project structure...${NC}"

# Check key directories
for dir in "src" "mappings" "infrastructure" "scripts" "docs"; do
    if [ -d "$dir" ]; then
        echo -e "${GREEN}✓${NC} Directory exists: $dir"
    else
        echo -e "${RED}❌${NC} Directory missing: $dir"
        ((ERRORS++))
    fi
done

# Check if we're in the right directory
if [ -f "infrastructure/app.py" ] && [ -d "src" ] && [ -d "scripts" ]; then
    echo -e "${GREEN}✓${NC} Running from correct project root directory"
else
    echo -e "${RED}❌${NC} Not running from project root directory"
    echo -e "   ${RED}Please run this script from the AVESA project root${NC}"
    ((ERRORS++))
fi

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Readiness Check Summary${NC}"
echo -e "${BLUE}========================================${NC}"

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}🎉 All checks passed! Ready for migration.${NC}"
    echo -e "\n${GREEN}Next steps:${NC}"
    echo -e "1. Create production AWS account"
    echo -e "2. Run: ./scripts/setup-production-account.sh"
    echo -e "3. Follow the migration checklist: docs/MIGRATION_CHECKLIST.md"
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}⚠️  Ready with $WARNINGS warnings.${NC}"
    echo -e "\n${YELLOW}Consider addressing the warnings above for optimal setup.${NC}"
    echo -e "\n${GREEN}Next steps:${NC}"
    echo -e "1. Create production AWS account"
    echo -e "2. Run: ./scripts/setup-production-account.sh"
    echo -e "3. Follow the migration checklist: docs/MIGRATION_CHECKLIST.md"
else
    echo -e "${RED}❌ $ERRORS errors found. Please fix before proceeding.${NC}"
    if [ $WARNINGS -gt 0 ]; then
        echo -e "${YELLOW}⚠️  Also found $WARNINGS warnings.${NC}"
    fi
    echo -e "\n${RED}Please address the errors above before starting migration.${NC}"
    exit 1
fi

echo ""