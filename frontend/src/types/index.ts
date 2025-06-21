// API Response Types
export interface ApiResponse<T> {
  data?: T;
  error?: string;
  message?: string;
  timestamp: string;
}

export interface PaginationInfo {
  page: number;
  limit: number;
  total: number;
  totalPages: number;
  hasNext: boolean;
  hasPrev: boolean;
}

export interface PaginatedResponse<T> extends ApiResponse<T> {
  pagination: PaginationInfo;
}

// Authentication Types
export interface User {
  id: string;
  email: string;
  tenantId: string;
  roles: string[];
  permissions: string[];
  iat: number;
  exp: number;
}

export interface LoginCredentials {
  email: string;
  password: string;
  tenantId: string;
}

export interface AuthResponse {
  token: string;
  user: User;
  expiresIn: number;
}

// Tenant Types
export interface Tenant {
  id: string;
  name: string;
  domain: string;
  status: 'active' | 'inactive' | 'suspended';
  settings: TenantSettings;
  createdAt: string;
  updatedAt: string;
}

export interface TenantSettings {
  branding: {
    primaryColor: string;
    secondaryColor: string;
    logo?: string;
    companyName: string;
  };
  features: {
    analytics: boolean;
    reporting: boolean;
    customDashboards: boolean;
  };
  limits: {
    maxUsers: number;
    maxDataRetention: number; // days
  };
}

// Business Entity Types
export interface Company {
  id: string;
  identifier: string;
  company_name: string;
  status: string;
  company_type: string;
  phone_number?: string;
  fax_number?: string;
  website?: string;
  address_line1?: string;
  address_line2?: string;
  city?: string;
  state_reference?: string;
  zip?: string;
  country?: string;
  is_current: boolean;
  effective_date: string;
  end_date?: string;
  created_date: string;
  updated_date: string;
}

export interface Contact {
  id: string;
  identifier: string;
  first_name: string;
  last_name: string;
  email?: string;
  phone_number?: string;
  mobile_phone?: string;
  title?: string;
  department?: string;
  company_id: string;
  company_name: string;
  is_current: boolean;
  effective_date: string;
  end_date?: string;
  created_date: string;
  updated_date: string;
}

export interface Ticket {
  id: string;
  identifier: string;
  summary: string;
  description?: string;
  status: string;
  priority: string;
  type?: string;
  sub_type?: string;
  company_id: string;
  company_name: string;
  contact_id?: string;
  contact_name?: string;
  actual_hours: number;
  budget_hours?: number;
  closed_date?: string;
  is_current: boolean;
  effective_date: string;
  end_date?: string;
  created_date: string;
  updated_date: string;
}

export interface TimeEntry {
  id: string;
  identifier: string;
  ticket_id: string;
  company_id: string;
  company_name: string;
  time_start: string;
  time_end: string;
  hours_deduct: number;
  actual_hours: number;
  billable_option: string;
  notes?: string;
  internal_notes?: string;
  date_entered: string;
  member_id: string;
  member_identifier: string;
  is_current: boolean;
  effective_date: string;
  end_date?: string;
  created_date: string;
  updated_date: string;
}

// Analytics Types
export interface DashboardSummary {
  period: string;
  summary: {
    companies: EntitySummary;
    contacts: EntitySummary;
    tickets: EntitySummary;
    time_entries: EntitySummary;
  };
  timestamp: string;
}

export interface EntitySummary {
  total: number;
  current: number;
  recent: number;
}

export interface StatusDistribution {
  status: string;
  ticketCount: number;
  totalHours: number;
  avgHours: number;
}

export interface TopCompany {
  companyId: string;
  companyName: string;
  ticketCount: number;
  totalHours: number;
  avgHours: number;
}

export interface TimeEntrySummary {
  groupId: string;
  groupName: string;
  entryCount: number;
  totalHours: number;
  avgHours: number;
  billableEntries: number;
  billableHours: number;
}

export interface DailySummary {
  date: string;
  entryCount: number;
  totalHours: number;
  billableHours: number;
  nonBillableHours: number;
}

// Chart Data Types
export interface ChartDataPoint {
  name: string;
  value: number;
  [key: string]: any;
}

export interface TimeSeriesDataPoint {
  date: string;
  [key: string]: any;
}

// Filter Types
export interface CompanyFilters {
  search?: string;
  status?: 'active' | 'inactive' | 'all';
  page?: number;
  limit?: number;
}

export interface ContactFilters {
  search?: string;
  company_id?: string;
  status?: 'active' | 'inactive' | 'all';
  page?: number;
  limit?: number;
}

export interface TicketFilters {
  search?: string;
  company_id?: string;
  status?: string;
  priority?: string;
  date_from?: string;
  date_to?: string;
  page?: number;
  limit?: number;
}

export interface TimeEntryFilters {
  ticket_id?: string;
  member_id?: string;
  company_id?: string;
  date_from?: string;
  date_to?: string;
  billable?: boolean;
  page?: number;
  limit?: number;
}

// UI State Types
export interface LoadingState {
  isLoading: boolean;
  error?: string;
}

export interface TableState<T> {
  data: T[];
  loading: boolean;
  error?: string;
  pagination: PaginationInfo;
  filters: Record<string, any>;
}

// Navigation Types
export interface NavItem {
  name: string;
  href: string;
  icon: any; // React component type
  current: boolean;
  children?: NavItem[];
}

// Form Types
export interface FormField {
  name: string;
  label: string;
  type: 'text' | 'email' | 'password' | 'select' | 'textarea' | 'date' | 'number';
  required?: boolean;
  placeholder?: string;
  options?: { value: string; label: string }[];
  validation?: any;
}

// Error Types
export interface ApiError {
  error: string;
  message: string;
  details?: any;
  timestamp: string;
  path?: string;
  method?: string;
}

// Theme Types
export interface Theme {
  colors: {
    primary: string;
    secondary: string;
    success: string;
    warning: string;
    danger: string;
    info: string;
  };
  fonts: {
    primary: string;
    secondary: string;
  };
}

// Export utility types
export type Period = '7d' | '30d' | '90d' | '1y';
export type SortDirection = 'asc' | 'desc';
export type EntityType = 'companies' | 'contacts' | 'tickets' | 'time_entries';