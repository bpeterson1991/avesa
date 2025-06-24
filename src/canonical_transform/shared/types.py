"""
Shared Type Definitions for AVESA Multi-Tenant Data Pipeline

These Python dataclasses correspond to the TypeScript definitions in shared-types/entities.d.ts
to ensure type consistency between frontend and backend.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime


@dataclass
class Company:
    """Core company entity with SCD Type 2 support."""
    id: str
    identifier: str
    company_name: str
    status: str
    company_type: str
    is_current: bool
    effective_date: str
    created_date: str
    updated_date: str
    phone_number: Optional[str] = None
    fax_number: Optional[str] = None
    website: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state_reference: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class Contact:
    """Core contact entity with SCD Type 2 support."""
    id: str
    identifier: str
    first_name: str
    last_name: str
    company_id: str
    company_name: str
    is_current: bool
    effective_date: str
    created_date: str
    updated_date: str
    email: Optional[str] = None
    phone_number: Optional[str] = None
    mobile_phone: Optional[str] = None
    title: Optional[str] = None
    department: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class Ticket:
    """Core ticket entity with SCD Type 2 support."""
    id: str
    identifier: str
    summary: str
    status: str
    priority: str
    company_id: str
    company_name: str
    actual_hours: float
    is_current: bool
    effective_date: str
    created_date: str
    updated_date: str
    description: Optional[str] = None
    type: Optional[str] = None
    sub_type: Optional[str] = None
    contact_id: Optional[str] = None
    contact_name: Optional[str] = None
    budget_hours: Optional[float] = None
    closed_date: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class TimeEntry:
    """Core time entry entity with SCD Type 2 support."""
    id: str
    identifier: str
    ticket_id: str
    company_id: str
    company_name: str
    time_start: str
    time_end: str
    hours_deduct: float
    actual_hours: float
    billable_option: str
    date_entered: str
    member_id: str
    member_identifier: str
    is_current: bool
    effective_date: str
    created_date: str
    updated_date: str
    notes: Optional[str] = None
    internal_notes: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class ApiResponse:
    """Standard API response wrapper."""
    timestamp: str
    data: Optional[Any] = None
    error: Optional[str] = None
    message: Optional[str] = None


@dataclass
class PaginationInfo:
    """Pagination metadata."""
    page: int
    limit: int
    total: int
    totalPages: int
    hasNext: bool
    hasPrev: bool


@dataclass
class PaginatedResponse:
    """Paginated API response."""
    timestamp: str
    pagination: PaginationInfo
    data: Optional[Any] = None
    error: Optional[str] = None
    message: Optional[str] = None


@dataclass
class User:
    """User authentication and authorization data."""
    id: str
    email: str
    tenantId: str
    roles: List[str]
    permissions: List[str]
    iat: int
    exp: int


@dataclass
class TenantBranding:
    """Tenant branding configuration."""
    primaryColor: str
    secondaryColor: str
    companyName: str
    logo: Optional[str] = None


@dataclass
class TenantFeatures:
    """Tenant feature flags."""
    analytics: bool
    reporting: bool
    customDashboards: bool


@dataclass
class TenantLimits:
    """Tenant resource limits."""
    maxUsers: int
    maxDataRetention: int  # days


@dataclass
class TenantSettings:
    """Tenant configuration settings."""
    branding: TenantBranding
    features: TenantFeatures
    limits: TenantLimits


@dataclass
class Tenant:
    """Tenant entity."""
    id: str
    name: str
    domain: str
    status: str  # 'active' | 'inactive' | 'suspended'
    settings: TenantSettings
    createdAt: str
    updatedAt: str


@dataclass
class EntitySummary:
    """Summary statistics for an entity type."""
    total: int
    current: int
    recent: int


@dataclass
class DashboardSummaryData:
    """Dashboard summary data structure."""
    companies: EntitySummary
    contacts: EntitySummary
    tickets: EntitySummary
    time_entries: EntitySummary


@dataclass
class DashboardSummary:
    """Dashboard summary response."""
    period: str
    summary: DashboardSummaryData
    timestamp: str


@dataclass
class CompanyFilters:
    """Company query filters."""
    search: Optional[str] = None
    status: Optional[str] = None  # 'active' | 'inactive' | 'all'
    page: Optional[int] = None
    limit: Optional[int] = None


@dataclass
class ContactFilters:
    """Contact query filters."""
    search: Optional[str] = None
    company_id: Optional[str] = None
    status: Optional[str] = None  # 'active' | 'inactive' | 'all'
    page: Optional[int] = None
    limit: Optional[int] = None


@dataclass
class TicketFilters:
    """Ticket query filters."""
    search: Optional[str] = None
    company_id: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    page: Optional[int] = None
    limit: Optional[int] = None


@dataclass
class TimeEntryFilters:
    """Time entry query filters."""
    ticket_id: Optional[str] = None
    member_id: Optional[str] = None
    company_id: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    billable: Optional[bool] = None
    page: Optional[int] = None
    limit: Optional[int] = None


# Type aliases for consistency with TypeScript
from typing import Literal

Period = Literal['7d', '30d', '90d', '1y']
SortDirection = Literal['asc', 'desc']
EntityType = Literal['companies', 'contacts', 'tickets', 'time_entries']


# Utility functions for type conversion
def to_dict(obj: Any) -> Dict[str, Any]:
    """Convert dataclass to dictionary, handling nested objects."""
    if hasattr(obj, '__dataclass_fields__'):
        result = {}
        for field_name, field_def in obj.__dataclass_fields__.items():
            value = getattr(obj, field_name)
            if hasattr(value, '__dataclass_fields__'):
                result[field_name] = to_dict(value)
            elif isinstance(value, list):
                result[field_name] = [to_dict(item) if hasattr(item, '__dataclass_fields__') else item for item in value]
            else:
                result[field_name] = value
        return result
    return obj


def from_dict(cls, data: Dict[str, Any]):
    """Create dataclass instance from dictionary."""
    if not hasattr(cls, '__dataclass_fields__'):
        return data
    
    field_types = {f.name: f.type for f in cls.__dataclass_fields__.values()}
    kwargs = {}
    
    for field_name, field_type in field_types.items():
        if field_name in data:
            value = data[field_name]
            # Handle nested dataclasses
            if hasattr(field_type, '__dataclass_fields__'):
                kwargs[field_name] = from_dict(field_type, value)
            else:
                kwargs[field_name] = value
    
    return cls(**kwargs)