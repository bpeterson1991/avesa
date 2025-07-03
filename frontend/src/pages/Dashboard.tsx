import React, { useState } from 'react';
import { WidgetGrid } from '../components/Dashboard/WidgetGrid';
import { WidgetCanvas } from '../components/Dashboard/WidgetCanvas';
import { WidgetLibrary } from '../components/Dashboard/WidgetLibrary';
import { useWidgetStore } from '../stores/widgetStore';
import { 
  PlusIcon, 
  Cog6ToothIcon,
  ArrowPathIcon,
  CheckCircleIcon 
} from '@heroicons/react/24/outline';
import { WidgetType, WidgetConfig, WidgetLayout } from '../types/widget';
import toast from 'react-hot-toast';

// Pre-configured widgets for MSP dashboard
const defaultWidgets: Array<{
  type: WidgetType;
  title: string;
  dataKey: string;
  dataSource?: { endpoint: string; parameters: Record<string, any> };
  config: any;
  layout: { w: number; h: number; x: number; y: number };
}> = [
  {
    type: 'kpi-card',
    title: 'Total Tickets',
    dataKey: 'total-tickets',
    dataSource: {
      endpoint: 'analytics/dashboard',
      parameters: {}
    },
    config: {
      visualization: {
        type: 'kpi-card',
        options: {
          format: 'number',
          changeType: 'increase',
          icon: 'TicketIcon',
          color: 'bg-blue-500'
        }
      }
    },
    layout: { w: 3, h: 2, x: 0, y: 0 }
  },
  {
    type: 'kpi-card',
    title: 'Total Time Entries',
    dataKey: 'total-time-entries',
    dataSource: {
      endpoint: 'analytics/dashboard',
      parameters: {}
    },
    config: {
      visualization: {
        type: 'kpi-card',
        options: {
          format: 'number',
          changeType: 'increase',
          icon: 'ClockIcon',
          color: 'bg-green-500'
        }
      }
    },
    layout: { w: 3, h: 2, x: 3, y: 0 }
  },
  {
    type: 'kpi-card',
    title: 'Total Companies',
    dataKey: 'total-companies',
    dataSource: {
      endpoint: 'analytics/dashboard',
      parameters: {}
    },
    config: {
      visualization: {
        type: 'kpi-card',
        options: {
          format: 'number',
          changeType: 'increase',
          icon: 'CheckCircleIcon',
          color: 'bg-purple-500'
        }
      }
    },
    layout: { w: 3, h: 2, x: 6, y: 0 }
  },
  {
    type: 'kpi-card',
    title: 'Total Contacts',
    dataKey: 'total-contacts',
    dataSource: {
      endpoint: 'analytics/dashboard',
      parameters: {}
    },
    config: {
      visualization: {
        type: 'kpi-card',
        options: {
          format: 'number',
          changeType: 'increase',
          icon: 'ExclamationTriangleIcon',
          color: 'bg-orange-500'
        }
      }
    },
    layout: { w: 3, h: 2, x: 9, y: 0 }
  },
  {
    type: 'chart',
    title: 'Ticket Status Distribution',
    dataKey: 'ticket-status-distribution',
    dataSource: {
      endpoint: 'analytics/tickets/status',
      parameters: {}
    },
    config: {
      visualization: {
        type: 'pie',
        options: {
          height: 250,
          yAxisKey: 'value'
        }
      }
    },
    layout: { w: 6, h: 4, x: 0, y: 2 }
  }
];

export default function Dashboard() {
  const [isEditMode, setIsEditMode] = useState(false);
  const [isLibraryOpen, setIsLibraryOpen] = useState(false);
  const { dashboards, activeDashboard, addWidget, resetDashboard } = useWidgetStore();
  const dashboard = dashboards[activeDashboard];

  // Initialize with default widgets if empty - run only once
  React.useEffect(() => {
    // Only initialize if the store has been loaded and we have no widgets
    const storeInitialized = localStorage.getItem('widget-store') !== null;
    const hasWidgets = dashboard && dashboard.widgets && dashboard.widgets.length > 0;
    
    if (!hasWidgets && !storeInitialized) {
      // Add a small delay to ensure store is ready
      const timer = setTimeout(() => {
        defaultWidgets.forEach((widget, index) => {
          const widgetConfig: WidgetConfig = {
            id: widget.dataKey, // Use dataKey as ID for consistency
            type: widget.type,
            title: widget.title,
            dataKey: widget.dataKey,
            dataSource: widget.dataSource,
            visualization: widget.config.visualization,
            config: widget.config
          };
          
          const layout: WidgetLayout = {
            i: widgetConfig.id,
            x: widget.layout.x || 0,
            y: widget.layout.y || 0,
            w: widget.layout.w,
            h: widget.layout.h
          };
          
          addWidget(widgetConfig, layout);
        });
      }, 100);
      
      return () => clearTimeout(timer);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // Empty dependency array - run only once on mount

  const handleToggleEditMode = () => {
    setIsEditMode(!isEditMode);
    if (isEditMode) {
      toast.success('Dashboard saved');
    }
  };

  const handleAddWidget = () => {
    setIsLibraryOpen(true);
  };

  const handleResetDashboard = () => {
    if (window.confirm('Are you sure you want to reset the dashboard to default widgets?')) {
      resetDashboard();
      // Re-add default widgets
      defaultWidgets.forEach(widget => {
        const widgetConfig: WidgetConfig = {
          id: widget.dataKey, // Use dataKey as ID for consistency
          type: widget.type,
          title: widget.title,
          dataKey: widget.dataKey,
          dataSource: widget.dataSource,
          visualization: widget.config.visualization,
          config: widget.config
        };
        
        const layout: WidgetLayout = {
          i: widgetConfig.id,
          x: widget.layout.x || 0,
          y: widget.layout.y || 0,
          w: widget.layout.w,
          h: widget.layout.h
        };
        
        addWidget(widgetConfig, layout);
      });
      toast.success('Dashboard reset to defaults');
    }
  };

  return (
    <div className="h-full flex flex-col bg-gray-50">
      {/* Dashboard Header */}
      <div className="flex-shrink-0 bg-white border-b border-gray-200">
        <div className="px-6 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">MSP Dashboard</h1>
              <p className="mt-1 text-sm text-gray-500">
                Monitor your managed services performance
              </p>
            </div>
            <div className="flex items-center space-x-3">
              {isEditMode && (
                <>
                  <button
                    onClick={handleAddWidget}
                    className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                  >
                    <PlusIcon className="h-5 w-5 mr-2" />
                    Add Widget
                  </button>
                  <button
                    onClick={handleResetDashboard}
                    className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                  >
                    <ArrowPathIcon className="h-5 w-5 mr-2" />
                    Reset
                  </button>
                </>
              )}
              <button
                onClick={handleToggleEditMode}
                className={`inline-flex items-center px-4 py-2 border rounded-md shadow-sm text-sm font-medium focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 ${
                  isEditMode
                    ? 'border-transparent text-white bg-primary-600 hover:bg-primary-700'
                    : 'border-gray-300 text-gray-700 bg-white hover:bg-gray-50'
                }`}
              >
                {isEditMode ? (
                  <>
                    <CheckCircleIcon className="h-5 w-5 mr-2" />
                    Save Layout
                  </>
                ) : (
                  <>
                    <Cog6ToothIcon className="h-5 w-5 mr-2" />
                    Edit Dashboard
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Widget Canvas */}
      <div className="flex-1 overflow-hidden">
        <WidgetCanvas isEditMode={isEditMode}>
          <WidgetGrid isEditMode={isEditMode} />
        </WidgetCanvas>
      </div>

      {/* Widget Library Modal */}
      {isLibraryOpen && (
        <WidgetLibrary 
          isOpen={isLibraryOpen}
          onClose={() => setIsLibraryOpen(false)}
        />
      )}
    </div>
  );
}