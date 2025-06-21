# AVESA Frontend - Multi-tenant SaaS Analytics Platform

A React TypeScript frontend for the AVESA multi-tenant analytics platform.

## Features

- **Multi-tenant Authentication**: JWT-based authentication with tenant isolation
- **Dashboard**: Real-time analytics dashboard with charts and metrics
- **Data Management**: Companies, contacts, tickets, and time entries management
- **Responsive Design**: Mobile-first design with Tailwind CSS
- **Real-time Updates**: React Query for efficient data fetching and caching
- **Type Safety**: Full TypeScript implementation

## Tech Stack

- **React 18** with TypeScript
- **React Router** for navigation
- **React Query** for data fetching
- **Tailwind CSS** for styling
- **Recharts** for data visualization
- **Heroicons** for icons
- **React Hot Toast** for notifications

## Getting Started

### Prerequisites

- Node.js 18+ 
- npm or yarn

### Installation

1. Install dependencies:
```bash
cd frontend
npm install
```

2. Set up environment variables:
```bash
# Create .env file
REACT_APP_API_URL=http://localhost:3001
```

3. Start the development server:
```bash
npm start
```

The application will open at `http://localhost:3000`.

### Demo Credentials

For testing, use these demo credentials:

- **Tenant ID**: `sitetechnology`
- **Email**: `admin@sitetechnology.com`
- **Password**: `demo123`

## Project Structure

```
frontend/
├── public/                 # Static files
├── src/
│   ├── components/        # Reusable UI components
│   │   └── Layout.tsx     # Main layout component
│   ├── contexts/          # React contexts
│   │   └── AuthContext.tsx # Authentication context
│   ├── pages/             # Page components
│   │   ├── Login.tsx      # Login page
│   │   ├── Dashboard.tsx  # Main dashboard
│   │   ├── Companies.tsx  # Companies listing
│   │   └── ...           # Other pages
│   ├── services/          # API services
│   │   └── api.ts         # API client
│   ├── types/             # TypeScript type definitions
│   │   └── index.ts       # Main types
│   ├── App.tsx            # Main app component
│   ├── index.tsx          # App entry point
│   └── index.css          # Global styles
├── package.json
├── tailwind.config.js     # Tailwind configuration
└── tsconfig.json          # TypeScript configuration
```

## Key Components

### Authentication
- JWT-based authentication with automatic token refresh
- Multi-tenant support with tenant isolation
- Protected routes and role-based access control

### Dashboard
- Real-time metrics and KPIs
- Interactive charts using Recharts
- Responsive grid layout

### Data Management
- Paginated data tables
- Search and filtering
- CRUD operations for all entities

## API Integration

The frontend communicates with the Node.js API backend:

- **Base URL**: Configurable via `REACT_APP_API_URL`
- **Authentication**: Bearer token in Authorization header
- **Tenant Isolation**: X-Tenant-ID header for multi-tenancy
- **Error Handling**: Centralized error handling with user-friendly messages

## Building for Production

```bash
npm run build
```

This creates an optimized production build in the `build/` directory.

## Development

### Code Style
- ESLint and Prettier for code formatting
- TypeScript strict mode enabled
- Consistent naming conventions

### Testing
```bash
npm test
```

### Type Checking
```bash
npx tsc --noEmit
```

## Deployment

The application can be deployed to any static hosting service:

1. Build the application: `npm run build`
2. Deploy the `build/` directory to your hosting service
3. Configure environment variables for production API URL

## Environment Variables

- `REACT_APP_API_URL`: Backend API URL (default: `/api`)

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)

## Contributing

1. Follow the existing code style and patterns
2. Add TypeScript types for all new components
3. Test your changes thoroughly
4. Update documentation as needed

## License

MIT License - see LICENSE file for details.