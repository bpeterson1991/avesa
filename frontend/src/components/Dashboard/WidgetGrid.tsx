import React, { useCallback } from 'react';
import { Layout, Layouts } from 'react-grid-layout';
import { Responsive, WidthProvider } from 'react-grid-layout';
import { useWidgetStore } from '../../stores/widgetStore';
import { KPIWidget } from './KPIWidget';
import { ChartWidget } from './ChartWidget';
import { WidgetConfig, WidgetData } from '../../types/widget';
import { useQuery } from '@tanstack/react-query';
import apiService from '../../services/api';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';

const ResponsiveGridLayout = WidthProvider(Responsive);

interface WidgetGridProps {
  onEditWidget?: (config: WidgetConfig) => void;
  isEditMode?: boolean;
}

export const WidgetGrid: React.FC<WidgetGridProps> = ({ onEditWidget, isEditMode: propIsEditMode }) => {
  const {
    currentLayout,
    isEditMode: storeIsEditMode,
    updateLayouts,
    removeWidget
  } = useWidgetStore();

  // Use prop if provided, otherwise use store
  const isEditMode = propIsEditMode !== undefined ? propIsEditMode : storeIsEditMode;

  // Fetch data for each widget
  const useWidgetData = (widget: WidgetConfig) => {
    return useQuery({
      queryKey: ['widget', widget.id, widget.dataSource],
      queryFn: async () => {
        // For development, use mock data if backend is not available
        const useMockData = process.env.NODE_ENV === 'development' && !widget.dataSource?.endpoint;
        
        if (useMockData) {
          // Return mock data for development
          switch (widget.type) {
            case 'kpi-card':
              if (widget.id === 'active-tickets') {
                return { value: 156, label: 'Active Tickets', format: 'number' };
              } else if (widget.id === 'total-time-today') {
                return { value: 24.5, label: 'Total Time Today', format: 'hours' };
              } else if (widget.id === 'billable-rate') {
                return { value: 87.3, label: 'Billable Rate', format: 'percentage' };
              } else if (widget.id === 'critical-issues') {
                return { value: 12, label: 'Critical Issues', format: 'number' };
              }
              return { value: 0, label: widget.title, format: 'number' };
            
            case 'chart':
              if (widget.id === 'ticket-status-distribution') {
                return [
                  { name: 'Open', value: 45 },
                  { name: 'In Progress', value: 78 },
                  { name: 'Waiting', value: 23 },
                  { name: 'Closed', value: 156 }
                ];
              }
              return [];
            
            default:
              return null;
          }
        }
        
        // Real API calls based on data source
        try {
          if (widget.dataSource?.endpoint) {
            switch (widget.dataSource.endpoint) {
              case 'analytics/dashboard':
                const dashboardData = await apiService.analytics.getDashboard();
                // Extract appropriate metric based on widget ID
                if (widget.id === 'total-tickets' || widget.dataKey === 'total-tickets') {
                  return {
                    value: dashboardData.summary?.tickets?.current || 0,
                    label: 'Total Tickets',
                    format: 'number'
                  };
                } else if (widget.id === 'total-time-entries' || widget.dataKey === 'total-time-entries') {
                  return {
                    value: dashboardData.summary?.time_entries?.current || 0,
                    label: 'Total Time Entries',
                    format: 'number'
                  };
                } else if (widget.id === 'total-companies' || widget.dataKey === 'total-companies') {
                  return {
                    value: dashboardData.summary?.companies?.current || 0,
                    label: 'Total Companies',
                    format: 'number'
                  };
                } else if (widget.id === 'total-contacts' || widget.dataKey === 'total-contacts') {
                  return {
                    value: dashboardData.summary?.contacts?.current || 0,
                    label: 'Total Contacts',
                    format: 'number'
                  };
                }
                return { value: 0, label: widget.title, format: 'number' };
              
              case 'analytics/tickets/status':
                const ticketStatus = await apiService.analytics.getTicketStatusDistribution();
                return ticketStatus.statusDistribution?.map(item => ({
                  name: item.status,
                  value: item.ticketCount
                })) || [];
              
              case 'analytics/companies/top':
                const topCompanies = await apiService.analytics.getTopCompanies();
                return topCompanies.topCompanies?.map(company => ({
                  name: company.companyName,
                  value: company.ticketCount
                })) || [];
              
              default:
                // Generic API request if endpoint is provided
                const response = await apiService.request({
                  method: 'GET',
                  url: widget.dataSource.endpoint,
                  params: widget.dataSource.parameters
                });
                return response;
            }
          }
        } catch (error) {
          console.error('Failed to fetch widget data:', error);
          // Return mock data on error
          return null;
        }
        
        return null;
      },
      refetchInterval: widget.refreshInterval ? widget.refreshInterval * 1000 : false,
      staleTime: 30000, // 30 seconds
      retry: 1,
      retryDelay: 1000
    });
  };

  // Create a proper component for each widget
  const WidgetWrapper: React.FC<{ widget: WidgetConfig }> = ({ widget }) => {
    const { data, isLoading, error } = useWidgetData(widget);

    const widgetData: WidgetData = {
      loading: isLoading,
      error: error?.message,
      data: data,
      lastUpdated: new Date()
    };

    const commonProps = {
      config: widget,
      data: widgetData,
      onEdit: onEditWidget,
      onRemove: removeWidget,
      isEditMode
    };

    switch (widget.type) {
      case 'kpi-card':
        return <KPIWidget {...commonProps} />;
      
      case 'chart':
        return <ChartWidget {...commonProps} />;
      
      case 'table':
        // TODO: Implement table widget
        return (
          <div className="h-full bg-white rounded-lg shadow-sm border border-gray-200 flex items-center justify-center">
            <p className="text-gray-500">Table Widget (Not implemented)</p>
          </div>
        );
      
      default:
        return (
          <div className="h-full bg-gray-100 rounded-lg flex items-center justify-center">
            <p className="text-gray-500">Unknown widget type: {widget.type}</p>
          </div>
        );
    }
  };

  const handleLayoutChange = useCallback(
    (layout: Layout[], layouts: Layouts) => {
      updateLayouts(layouts as { lg: Layout[]; md: Layout[]; sm: Layout[]; xs: Layout[] });
    },
    [updateLayouts]
  );

  if (!currentLayout) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-50 rounded-lg">
        <p className="text-gray-500">No dashboard layout selected</p>
      </div>
    );
  }

  return (
    <div className={`widget-grid ${isEditMode ? 'edit-mode' : ''}`}>
      <ResponsiveGridLayout
        className="layout"
        layouts={currentLayout.layouts}
        onLayoutChange={handleLayoutChange}
        breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480 }}
        cols={{ lg: 12, md: 8, sm: 4, xs: 4 }}
        rowHeight={80}
        isDraggable={isEditMode}
        isResizable={isEditMode}
        containerPadding={[0, 0]}
        margin={[16, 16]}
        useCSSTransforms={true}
        compactType="vertical"
        preventCollision={false}
      >
        {currentLayout.widgets.map((widget) => (
          <div key={widget.id} className="widget-item">
            <WidgetWrapper widget={widget} />
          </div>
        ))}
      </ResponsiveGridLayout>
    </div>
  );
};