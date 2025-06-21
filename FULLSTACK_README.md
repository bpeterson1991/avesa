# AVESA - Multi-Tenant SaaS Analytics Platform

A complete full-stack multi-tenant SaaS analytics platform built with React, Node.js, and ClickHouse.

## 🏗️ Architecture Overview

AVESA is a comprehensive analytics platform that provides:

- **Multi-tenant data isolation** with ClickHouse Cloud
- **Real-time analytics dashboard** with interactive charts
- **Secure authentication** with JWT and role-based access control
- **Scalable data pipeline** for processing canonical business data
- **Modern React frontend** with TypeScript and Tailwind CSS

## 🚀 Quick Start

### Prerequisites

- Node.js 18+
- npm or yarn
- ClickHouse Cloud account (for production data)

### Development Setup

1. **Clone and setup the project:**
```bash
git clone <repository-url>
cd avesa
```

2. **Start the full-stack development environment:**
```bash
./scripts/start-fullstack.sh
```

This script will:
- Install all dependencies
- Start the Node.js API server on port 3001
- Start the React frontend on port 3000
- Open your browser automatically

3. **Login with demo credentials:**
- **Tenant ID**: `sitetechnology`
- **Email**: `admin@sitetechnology.com`
- **Password**: `demo123`

## 📁 Project Structure

```
avesa/
├── frontend/                   # React TypeScript frontend
│   ├── src/
│   │   ├── components/        # Reusable UI components
│   │   ├── contexts/          # React contexts (Auth, etc.)
│   │   ├── pages/             # Page components
│   │   ├── services/          # API client services
│   │   └── types/             # TypeScript definitions
│   ├── public/                # Static assets
│   └── package.json
├── src/clickhouse/api/        # Node.js API server
│   ├── routes/                # API route handlers
│   ├── middleware/            # Express middleware
│   ├── config/                # Configuration files
│   └── utils/                 # Utility functions
├── src/clickhouse/            # ClickHouse integration
│   ├── schemas/               # Database schemas
│   ├── data_loader/           # Data loading lambdas
│   └── scd_processor/         # SCD processing
├── infrastructure/            # AWS CDK infrastructure
├── scripts/                   # Deployment and utility scripts
└── docs/                      # Documentation
```

## 🔧 Technology Stack

### Frontend
- **React 18** with TypeScript
- **React Router** for navigation
- **React Query** for data fetching and caching
- **Tailwind CSS** for styling
- **Recharts** for data visualization
- **Heroicons** for icons

### Backend
- **Node.js** with Express
- **JWT** for authentication
- **ClickHouse** for analytics database
- **AWS Lambda** for serverless processing
- **AWS S3** for data storage

### Infrastructure
- **AWS CDK** for infrastructure as code
- **ClickHouse Cloud** for managed analytics database
- **AWS Lambda** for serverless compute
- **AWS S3** for data lake storage

## 🎯 Features

### Authentication & Security
- Multi-tenant JWT authentication
- Role-based access control
- Tenant data isolation
- Secure API endpoints

### Dashboard & Analytics
- Real-time analytics dashboard
- Interactive charts and visualizations
- Key performance indicators (KPIs)
- Customizable time periods

### Data Management
- Companies management
- Contacts management
- Support tickets tracking
- Time entries logging

### Multi-Tenancy
- Complete tenant isolation
- Tenant-specific branding
- Scalable architecture
- Secure data access

## 🔌 API Endpoints

### Authentication
```
POST /auth/login          # User login
POST /auth/logout         # User logout
POST /auth/refresh        # Token refresh
GET  /auth/me            # Current user info
```

### Analytics
```
GET /api/analytics/dashboard           # Dashboard summary
GET /api/analytics/tickets/status     # Ticket status distribution
GET /api/analytics/companies/top      # Top companies by metrics
```

### Data Entities
```
GET /api/companies        # List companies
GET /api/companies/:id    # Get company details
GET /api/contacts         # List contacts
GET /api/tickets          # List tickets
GET /api/time-entries     # List time entries
```

### Health & Monitoring
```
GET /health              # Basic health check
GET /health/detailed     # Detailed health with DB status
```

## 🛠️ Development

### Running Individual Services

**API Server Only:**
```bash
cd src/clickhouse/api
npm install
npm start
```

**Frontend Only:**
```bash
cd frontend
npm install
npm start
```

### Environment Variables

**API Server (.env in src/clickhouse/api/):**
```bash
NODE_ENV=development
PORT=3001
JWT_SECRET=your-secret-key
LOG_LEVEL=info
CLICKHOUSE_SECRET_NAME=your-clickhouse-secret
AWS_REGION=us-east-2
```

**Frontend (.env in frontend/):**
```bash
REACT_APP_API_URL=http://localhost:3001
```

### Testing

**API Tests:**
```bash
cd src/clickhouse/api
npm test
```

**Frontend Tests:**
```bash
cd frontend
npm test
```

## 🚀 Deployment

### Production Build

**Frontend:**
```bash
cd frontend
npm run build
```

**API Server:**
```bash
cd src/clickhouse/api
npm install --production
```

### Infrastructure Deployment

```bash
cd infrastructure
npm install
cdk deploy --all
```

## 📊 Data Flow

1. **Data Ingestion**: Raw data from various sources (ConnectWise, Salesforce, etc.)
2. **Data Processing**: Lambda functions process and transform data
3. **Data Storage**: Canonical data stored in ClickHouse with SCD Type 2
4. **API Layer**: Node.js API provides secure, tenant-aware access
5. **Frontend**: React application displays analytics and manages data

## 🔒 Security Features

- **JWT Authentication**: Secure token-based authentication
- **Tenant Isolation**: Complete data separation between tenants
- **Role-Based Access**: Granular permissions system
- **Input Validation**: Comprehensive request validation
- **Rate Limiting**: API rate limiting for protection
- **CORS Protection**: Configurable CORS policies

## 📈 Monitoring & Logging

- **Structured Logging**: Winston-based logging with multiple transports
- **Health Checks**: Comprehensive health monitoring
- **Error Handling**: Centralized error handling and reporting
- **Performance Metrics**: Built-in performance monitoring

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Code Style
- Use TypeScript for all new code
- Follow existing naming conventions
- Add JSDoc comments for functions
- Use Prettier for code formatting

## 📚 Documentation

- [API Documentation](docs/API_DOCUMENTATION.md)
- [Frontend Guide](frontend/README.md)
- [Deployment Guide](docs/DEPLOYMENT_GUIDE.md)
- [Architecture Overview](docs/SAAS_ARCHITECTURE_REVIEW.md)

## 🐛 Troubleshooting

### Common Issues

**Frontend won't start:**
- Check Node.js version (18+ required)
- Clear node_modules and reinstall
- Check for port conflicts

**API connection errors:**
- Verify API server is running on port 3001
- Check CORS configuration
- Verify JWT secret is set

**Authentication issues:**
- Check demo credentials are correct
- Verify JWT secret matches between frontend and backend
- Check token expiration

### Getting Help

1. Check the documentation in the `docs/` folder
2. Review the troubleshooting section
3. Check existing GitHub issues
4. Create a new issue with detailed information

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- ClickHouse team for the amazing analytics database
- React team for the excellent frontend framework
- AWS for the robust cloud infrastructure
- All contributors and maintainers

---

**Built with ❤️ by the AVESA Team**