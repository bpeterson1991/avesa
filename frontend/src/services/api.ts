import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import {
  PaginatedResponse,
  AuthResponse,
  LoginCredentials,
  User,
  Company,
  Contact,
  Ticket,
  TimeEntry,
  DashboardSummary,
  StatusDistribution,
  TopCompany,
  TimeEntrySummary,
  DailySummary,
  CompanyFilters,
  ContactFilters,
  TicketFilters,
  TimeEntryFilters
} from '../types';

class ApiService {
  private client: AxiosInstance;
  private token: string | null = null;
  private tenantId: string | null = null;

  constructor() {
    this.client = axios.create({
      baseURL: process.env.REACT_APP_API_URL || 'http://localhost:3000',
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Request interceptor to add auth headers
    this.client.interceptors.request.use(
      (config) => {
        if (this.token) {
          config.headers.Authorization = `Bearer ${this.token}`;
        }
        if (this.tenantId) {
          config.headers['X-Tenant-ID'] = this.tenantId;
        }
        return config;
      },
      (error) => Promise.reject(error)
    );

    // Response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response?.status === 401) {
          this.clearAuth();
          window.location.href = '/login';
        }
        return Promise.reject(error);
      }
    );

    // Load auth from localStorage on initialization
    this.loadAuthFromStorage();
  }

  private loadAuthFromStorage() {
    const token = localStorage.getItem('auth_token');
    const tenantId = localStorage.getItem('tenant_id');
    
    if (token && tenantId) {
      this.setAuth(token, tenantId);
    }
  }

  private saveAuthToStorage(token: string, tenantId: string) {
    localStorage.setItem('auth_token', token);
    localStorage.setItem('tenant_id', tenantId);
  }

  private clearAuthFromStorage() {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('tenant_id');
    localStorage.removeItem('user');
  }

  setAuth(token: string, tenantId: string) {
    this.token = token;
    this.tenantId = tenantId;
    this.saveAuthToStorage(token, tenantId);
  }

  clearAuth() {
    this.token = null;
    this.tenantId = null;
    this.clearAuthFromStorage();
  }

  isAuthenticated(): boolean {
    return !!this.token && !!this.tenantId;
  }

  // Authentication endpoints
  async login(credentials: LoginCredentials): Promise<AuthResponse> {
    // Mock authentication for demo purposes
    if (process.env.NODE_ENV === 'development') {
      // Check for demo credentials
      if (
        credentials.email === 'admin@sitetechnology.com' &&
        credentials.password === 'demo123' &&
        credentials.tenantId === 'sitetechnology'
      ) {
        const mockUser: User = {
          id: '1',
          email: credentials.email,
          tenantId: credentials.tenantId,
          roles: ['admin'],
          permissions: ['dashboard.view', 'widgets.edit'],
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600 * 24 // 24 hours
        };
        const mockToken = 'mock-jwt-token-' + Date.now();
        
        this.setAuth(mockToken, credentials.tenantId);
        localStorage.setItem('user', JSON.stringify(mockUser));
        
        return {
          token: mockToken,
          user: mockUser,
          expiresIn: 3600 * 24 // 24 hours in seconds
        };
      } else {
        throw new Error('Invalid credentials');
      }
    }
    
    // Normal API call for production
    const response = await this.client.post<AuthResponse>('/auth/login', credentials);
    const { token, user } = response.data;
    
    this.setAuth(token, credentials.tenantId);
    localStorage.setItem('user', JSON.stringify(user));
    
    return response.data;
  }

  async logout(): Promise<void> {
    try {
      await this.client.post('/auth/logout');
    } finally {
      this.clearAuth();
    }
  }

  async refreshToken(): Promise<AuthResponse> {
    const response = await this.client.post<AuthResponse>('/auth/refresh');
    const { token, user } = response.data;
    
    if (this.tenantId) {
      this.setAuth(token, this.tenantId);
      localStorage.setItem('user', JSON.stringify(user));
    }
    
    return response.data;
  }

  // Health check
  async healthCheck(): Promise<any> {
    const response = await this.client.get('/health');
    return response.data;
  }

  // Analytics endpoints
  async getDashboard(period: string = '30d'): Promise<DashboardSummary> {
    const response = await this.client.get<DashboardSummary>('/api/analytics/dashboard', {
      params: { period }
    });
    return response.data;
  }

  async getTicketStatusDistribution(period: string = '30d'): Promise<{ statusDistribution: StatusDistribution[] }> {
    const response = await this.client.get('/api/analytics/tickets/status', {
      params: { period }
    });
    return response.data;
  }

  async getTopCompanies(metric: string = 'tickets', limit: number = 10, period: string = '30d'): Promise<{ topCompanies: TopCompany[] }> {
    const response = await this.client.get('/api/analytics/companies/top', {
      params: { metric, limit, period }
    });
    return response.data;
  }

  // Companies endpoints
  async getCompanies(filters: CompanyFilters = {}): Promise<PaginatedResponse<Company[]>> {
    const response = await this.client.get<PaginatedResponse<Company[]>>('/api/companies', {
      params: filters
    });
    return response.data;
  }

  async getCompany(id: string): Promise<{ company: Company }> {
    const response = await this.client.get<{ company: Company }>(`/api/companies/${id}`);
    return response.data;
  }

  async getCompanyTickets(id: string, filters: { page?: number; limit?: number; status?: string } = {}): Promise<PaginatedResponse<Ticket[]>> {
    const response = await this.client.get<PaginatedResponse<Ticket[]>>(`/api/companies/${id}/tickets`, {
      params: filters
    });
    return response.data;
  }

  // Contacts endpoints
  async getContacts(filters: ContactFilters = {}): Promise<PaginatedResponse<Contact[]>> {
    const response = await this.client.get<PaginatedResponse<Contact[]>>('/api/contacts', {
      params: filters
    });
    return response.data;
  }

  async getContact(id: string): Promise<{ contact: Contact }> {
    const response = await this.client.get<{ contact: Contact }>(`/api/contacts/${id}`);
    return response.data;
  }

  async getContactsByCompany(companyId: string, filters: { page?: number; limit?: number } = {}): Promise<PaginatedResponse<Contact[]>> {
    const response = await this.client.get<PaginatedResponse<Contact[]>>(`/api/contacts/by-company/${companyId}`, {
      params: filters
    });
    return response.data;
  }

  // Tickets endpoints
  async getTickets(filters: TicketFilters = {}): Promise<PaginatedResponse<Ticket[]>> {
    const response = await this.client.get<PaginatedResponse<Ticket[]>>('/api/tickets', {
      params: filters
    });
    return response.data;
  }

  async getTicket(id: string): Promise<{ ticket: Ticket }> {
    const response = await this.client.get<{ ticket: Ticket }>(`/api/tickets/${id}`);
    return response.data;
  }

  async getTicketTimeEntries(id: string, filters: { page?: number; limit?: number } = {}): Promise<PaginatedResponse<TimeEntry[]>> {
    const response = await this.client.get<PaginatedResponse<TimeEntry[]>>(`/api/tickets/${id}/time-entries`, {
      params: filters
    });
    return response.data;
  }

  async getTicketStatusSummary(): Promise<{ statusSummary: StatusDistribution[] }> {
    const response = await this.client.get('/api/tickets/status-summary');
    return response.data;
  }

  // Time Entries endpoints
  async getTimeEntries(filters: TimeEntryFilters = {}): Promise<PaginatedResponse<TimeEntry[]>> {
    const response = await this.client.get<PaginatedResponse<TimeEntry[]>>('/api/time-entries', {
      params: filters
    });
    return response.data;
  }

  async getTimeEntry(id: string): Promise<{ timeEntry: TimeEntry }> {
    const response = await this.client.get<{ timeEntry: TimeEntry }>(`/api/time-entries/${id}`);
    return response.data;
  }

  async getTimeEntrySummary(period: string = '30d', groupBy: string = 'member'): Promise<{ summary: TimeEntrySummary[] }> {
    const response = await this.client.get('/api/time-entries/summary', {
      params: { period, group_by: groupBy }
    });
    return response.data;
  }

  async getTimeEntryDailySummary(period: string = '30d'): Promise<{ dailySummary: DailySummary[] }> {
    const response = await this.client.get('/api/time-entries/daily-summary', {
      params: { period }
    });
    return response.data;
  }

  // Generic request method for custom endpoints
  async request<T>(config: AxiosRequestConfig): Promise<T> {
    const response = await this.client.request<T>(config);
    return response.data;
  }

  // Analytics namespace
  analytics = {
    getDashboard: async (period?: string): Promise<DashboardSummary> => {
      const response = await this.client.get<DashboardSummary>('/api/analytics/dashboard', {
        params: period ? { period } : undefined
      });
      return response.data;
    },

    getTicketStatusDistribution: async (period?: string): Promise<{ statusDistribution: StatusDistribution[], period: string }> => {
      const response = await this.client.get<{ statusDistribution: StatusDistribution[], period: string }>('/api/analytics/tickets/status', {
        params: period ? { period } : undefined
      });
      return response.data;
    },

    getTopCompanies: async (metric?: string, limit?: number, period?: string): Promise<{ topCompanies: TopCompany[] }> => {
      const response = await this.client.get<{ topCompanies: TopCompany[] }>('/api/analytics/companies/top', {
        params: { metric, limit, period }
      });
      return response.data;
    }
  };
}

// Create singleton instance
const apiService = new ApiService();

export default apiService;