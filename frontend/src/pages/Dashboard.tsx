import React from 'react';
import { useQuery } from '@tanstack/react-query';
import apiService from '../services/api';
import {
  BuildingOfficeIcon,
  UserGroupIcon,
  TicketIcon,
  ClockIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
} from '@heroicons/react/24/outline';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#06B6D4'];

interface StatCardProps {
  title: string;
  value: number;
  change: number;
  icon: React.ComponentType<any>;
  color: string;
}

function StatCard({ title, value, change, icon: Icon, color }: StatCardProps) {
  const isPositive = change >= 0;
  
  return (
    <div className="bg-white overflow-hidden shadow rounded-lg">
      <div className="p-5">
        <div className="flex items-center">
          <div className="flex-shrink-0">
            <div className={`w-8 h-8 ${color} rounded-md flex items-center justify-center`}>
              <Icon className="w-5 h-5 text-white" />
            </div>
          </div>
          <div className="ml-5 w-0 flex-1">
            <dl>
              <dt className="text-sm font-medium text-gray-500 truncate">{title}</dt>
              <dd className="flex items-baseline">
                <div className="text-2xl font-semibold text-gray-900">
                  {value.toLocaleString()}
                </div>
                <div className={`ml-2 flex items-baseline text-sm font-semibold ${
                  isPositive ? 'text-green-600' : 'text-red-600'
                }`}>
                  {isPositive ? (
                    <ArrowTrendingUpIcon className="self-center flex-shrink-0 h-4 w-4 text-green-500" />
                  ) : (
                    <ArrowTrendingDownIcon className="self-center flex-shrink-0 h-4 w-4 text-red-500" />
                  )}
                  <span className="ml-1">
                    {Math.abs(change)}%
                  </span>
                </div>
              </dd>
            </dl>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function Dashboard() {
  const { data: dashboard, isLoading: dashboardLoading } = useQuery({
    queryKey: ['dashboard'],
    queryFn: () => apiService.getDashboard(),
  });

  const { data: ticketStatus } = useQuery({
    queryKey: ['ticket-status'],
    queryFn: () => apiService.getTicketStatusDistribution(),
  });

  const { data: topCompanies } = useQuery({
    queryKey: ['top-companies'],
    queryFn: () => apiService.getTopCompanies('tickets', 5),
  });

  if (dashboardLoading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="bg-white overflow-hidden shadow rounded-lg">
              <div className="p-5">
                <div className="animate-pulse">
                  <div className="flex items-center">
                    <div className="w-8 h-8 bg-gray-200 rounded-md"></div>
                    <div className="ml-5 flex-1">
                      <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
                      <div className="h-6 bg-gray-200 rounded w-1/2"></div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  const stats = dashboard ? [
    {
      title: 'Total Companies',
      value: dashboard.summary.companies.total,
      change: 12,
      icon: BuildingOfficeIcon,
      color: 'bg-blue-500',
    },
    {
      title: 'Total Contacts',
      value: dashboard.summary.contacts.total,
      change: 8,
      icon: UserGroupIcon,
      color: 'bg-green-500',
    },
    {
      title: 'Active Tickets',
      value: dashboard.summary.tickets.current,
      change: -3,
      icon: TicketIcon,
      color: 'bg-yellow-500',
    },
    {
      title: 'Time Entries',
      value: dashboard.summary.time_entries.total,
      change: 15,
      icon: ClockIcon,
      color: 'bg-purple-500',
    },
  ] : [];

  const statusChartData = ticketStatus?.statusDistribution.map(item => ({
    name: item.status,
    value: item.ticketCount,
    hours: item.totalHours,
  })) || [];

  const companiesChartData = topCompanies?.topCompanies.map(company => ({
    name: company.companyName.length > 20 
      ? company.companyName.substring(0, 20) + '...' 
      : company.companyName,
    tickets: company.ticketCount,
    hours: company.totalHours,
  })) || [];

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-semibold text-gray-900">Dashboard</h1>
        <p className="mt-1 text-sm text-gray-500">
          Overview of your analytics data for {dashboard?.period || '30d'}
        </p>
      </div>

      {/* Stats cards */}
      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
        {stats.map((stat) => (
          <StatCard key={stat.title} {...stat} />
        ))}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Ticket Status Distribution */}
        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="px-4 py-5 sm:p-6">
            <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
              Ticket Status Distribution
            </h3>
            {statusChartData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={statusChartData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {statusChartData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-64 text-gray-500">
                No ticket data available
              </div>
            )}
          </div>
        </div>

        {/* Top Companies by Tickets */}
        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="px-4 py-5 sm:p-6">
            <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
              Top Companies by Tickets
            </h3>
            {companiesChartData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={companiesChartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="name" 
                    angle={-45}
                    textAnchor="end"
                    height={80}
                    fontSize={12}
                  />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="tickets" fill="#3B82F6" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-64 text-gray-500">
                No company data available
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Recent Activity */}
      <div className="bg-white shadow rounded-lg">
        <div className="px-4 py-5 sm:p-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
            Recent Activity Summary
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="text-center">
              <div className="text-2xl font-semibold text-blue-600">
                {dashboard?.summary.companies.recent || 0}
              </div>
              <div className="text-sm text-gray-500">New Companies</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-semibold text-green-600">
                {dashboard?.summary.contacts.recent || 0}
              </div>
              <div className="text-sm text-gray-500">New Contacts</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-semibold text-yellow-600">
                {dashboard?.summary.tickets.recent || 0}
              </div>
              <div className="text-sm text-gray-500">New Tickets</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}