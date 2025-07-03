// Widget configuration types
export type WidgetType = 'kpi-card' | 'chart' | 'table';

export interface WidgetConfig {
  id: string;
  type: WidgetType;
  title: string;
  dataKey?: string;
  dataSource?: {
    endpoint: string;
    parameters: Record<string, any>;
  };
  visualization: {
    type: string;
    options: any;
  };
  refreshInterval?: number;
  config?: any; // For backward compatibility
}

export interface WidgetLayout {
  i: string; // widget id
  x: number;
  y: number;
  w: number;
  h: number;
  minW?: number;
  minH?: number;
  maxW?: number;
  maxH?: number;
  static?: boolean;
}

export interface DashboardLayout {
  id: string;
  name: string;
  widgets: WidgetConfig[];
  layouts: {
    lg: WidgetLayout[];
    md: WidgetLayout[];
    sm: WidgetLayout[];
    xs: WidgetLayout[];
  };
  isDefault?: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface WidgetData {
  loading: boolean;
  error?: string;
  data?: any;
  lastUpdated?: Date;
}

export interface WidgetProps {
  config: WidgetConfig;
  data: WidgetData;
  onEdit?: (config: WidgetConfig) => void;
  onRemove?: (id: string) => void;
  isEditMode?: boolean;
}

export interface WidgetTemplate {
  id: string;
  name: string;
  description: string;
  type: WidgetConfig['type'];
  defaultConfig: Omit<WidgetConfig, 'id'>;
  preview?: string;
  category: 'kpi' | 'chart' | 'table' | 'custom';
}