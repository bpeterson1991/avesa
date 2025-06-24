-- ClickHouse Multi-Tenant Shared Tables Schema (Optimized)
-- Implements shared tables with optimized indexing for multi-tenant SaaS
-- Supports SCD Type 2 with clean business names (no _scd suffix)

-- =============================================================================
-- COMPANIES TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS companies (
    -- Primary identifiers
    tenant_id String,
    id String,
    
    -- Business fields
    company_name String,
    company_identifier Nullable(String),
    company_type Nullable(String),
    status Nullable(String),
    
    -- Address information
    address_line1 Nullable(String),
    address_line2 Nullable(String),
    city Nullable(String),
    state Nullable(String),
    zip Nullable(String),
    country Nullable(String),
    
    -- Contact information
    phone_number Nullable(String),
    fax_number Nullable(String),
    website Nullable(String),
    
    -- Business details
    territory_id Nullable(String),
    territory_name Nullable(String),
    market_id Nullable(String),
    market_name Nullable(String),
    account_number Nullable(String),
    annual_revenue Nullable(Float64),
    number_of_employees Nullable(UInt32),
    ownership_type Nullable(String),
    time_zone Nullable(String),
    
    -- Marketing and sales
    lead_source Nullable(String),
    lead_flag Nullable(Bool),
    unsubscribe_flag Nullable(Bool),
    
    -- Financial information
    vendor_identifier Nullable(String),
    tax_identifier Nullable(String),
    tax_code Nullable(String),
    billing_terms Nullable(String),
    invoice_delivery_method Nullable(String),
    currency_id Nullable(String),
    currency_symbol Nullable(String),
    
    -- Relationships
    default_contact_id Nullable(String),
    default_contact_name Nullable(String),
    billing_contact_id Nullable(String),
    billing_contact_name Nullable(String),
    bill_to_company_id Nullable(String),
    bill_to_company_name Nullable(String),
    
    -- Audit fields
    source_system String,
    source_id String,
    last_updated DateTime,
    last_updated_by Nullable(String),
    created_date DateTime DEFAULT now(),
    
    -- Data quality
    data_hash String,
    record_version UInt32 DEFAULT 1
)
ENGINE = MergeTree()
ORDER BY (tenant_id, id, created_date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- CONTACTS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS contacts (
    -- Primary identifiers
    tenant_id String,
    id String,
    
    -- Company relationship
    company_id Nullable(String),
    company_name Nullable(String),
    
    -- Personal information
    first_name Nullable(String),
    last_name Nullable(String),
    title Nullable(String),
    school Nullable(String),
    nickname Nullable(String),
    
    -- Personal details
    married_flag Nullable(Bool),
    children_flag Nullable(Bool),
    significant_other Nullable(String),
    gender Nullable(String),
    birth_day Nullable(Date),
    anniversary Nullable(Date),
    
    -- Portal access
    portal_password Nullable(String),
    portal_security_level Nullable(String),
    disable_portal_login_flag Nullable(Bool),
    unsubscribe_flag Nullable(Bool),
    
    -- Digital presence
    presence Nullable(String),
    mobile_guid Nullable(String),
    facebook_url Nullable(String),
    twitter_url Nullable(String),
    linked_in_url Nullable(String),
    
    -- Address information
    address_line1 Nullable(String),
    address_line2 Nullable(String),
    city Nullable(String),
    state Nullable(String),
    zip Nullable(String),
    country Nullable(String),
    
    -- Professional information
    relationship Nullable(String),
    type Nullable(String),
    department Nullable(String),
    inactive_flag Nullable(Bool),
    
    -- Contact methods
    default_phone_type Nullable(String),
    default_phone_number Nullable(String),
    default_phone_extension Nullable(String),
    default_email_type Nullable(String),
    default_email_address Nullable(String),
    
    -- Relationships
    manager_contact_id Nullable(String),
    manager_contact_name Nullable(String),
    assistant_contact_id Nullable(String),
    assistant_contact_name Nullable(String),
    default_merge_contact_id Nullable(String),
    
    -- Integration fields
    sync_guid Nullable(String),
    
    -- Audit fields
    source_system String,
    source_id String,
    last_updated DateTime,
    last_updated_by Nullable(String),
    created_date DateTime DEFAULT now(),
    
    -- Data quality
    data_hash String,
    record_version UInt32 DEFAULT 1
)
ENGINE = MergeTree()
ORDER BY (tenant_id, id, created_date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- TICKETS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS tickets (
    -- Primary identifiers
    tenant_id String,
    id String,
    
    -- Ticket information
    ticket_number String,
    summary String,
    description Nullable(String),
    
    -- Status and priority
    status Nullable(String),
    priority Nullable(String),
    severity Nullable(String),
    impact Nullable(String),
    urgency Nullable(String),
    
    -- Company and contact relationships
    company_id Nullable(String),
    company_name Nullable(String),
    contact_id Nullable(String),
    contact_name Nullable(String),
    
    -- Classification
    board_id Nullable(String),
    board_name Nullable(String),
    type_id Nullable(String),
    type_name Nullable(String),
    subtype_id Nullable(String),
    subtype_name Nullable(String),
    item_id Nullable(String),
    item_name Nullable(String),
    
    -- Assignment
    team_id Nullable(String),
    team_name Nullable(String),
    owner_id Nullable(String),
    owner_name Nullable(String),
    
    -- Time tracking
    budget_hours Nullable(Float64),
    actual_hours Nullable(Float64),
    
    -- Dates
    created_date DateTime,
    created_by Nullable(String),
    required_date Nullable(DateTime),
    closed_date Nullable(DateTime),
    closed_by Nullable(String),
    
    -- Approval
    approved Nullable(Bool),
    
    -- Audit fields
    source_system String,
    source_id String,
    last_updated DateTime,
    last_updated_by Nullable(String),
    
    -- Data quality
    data_hash String,
    record_version UInt32 DEFAULT 1
)
ENGINE = MergeTree()
ORDER BY (tenant_id, id, effective_date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- TIME ENTRIES TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS time_entries (
    -- Primary identifiers
    tenant_id String,
    id String,
    
    -- Company relationship
    company_id Nullable(String),
    company_name Nullable(String),
    
    -- Charge information
    charge_to_id Nullable(String),
    charge_to_type Nullable(String),
    
    -- Member information
    member_id Nullable(String),
    member_name Nullable(String),
    
    -- Location and business unit
    location_id Nullable(String),
    business_unit_id Nullable(String),
    
    -- Work classification
    work_type_id Nullable(String),
    work_type_name Nullable(String),
    work_role_id Nullable(String),
    work_role_name Nullable(String),
    
    -- Agreement
    agreement_id Nullable(String),
    agreement_name Nullable(String),
    
    -- Time information
    time_start Nullable(DateTime),
    time_end Nullable(DateTime),
    hours_deduct Nullable(Float64),
    actual_hours Nullable(Float64),
    
    -- Billing
    billable_option Nullable(String),
    
    -- Notes
    notes Nullable(String),
    internal_notes Nullable(String),
    
    -- Entry information
    date_entered DateTime,
    entered_by Nullable(String),
    
    -- Audit fields
    source_system String,
    source_id String,
    last_updated DateTime,
    last_updated_by Nullable(String),
    
    -- Data quality
    data_hash String,
    record_version UInt32 DEFAULT 1
)
ENGINE = MergeTree()
ORDER BY (tenant_id, id, date_entered)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Companies indexes
ALTER TABLE companies ADD INDEX idx_company_name (company_name) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE companies ADD INDEX idx_company_status (status) TYPE set(100) GRANULARITY 1;

-- Contacts indexes
ALTER TABLE contacts ADD INDEX idx_contact_email (default_email_address) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE contacts ADD INDEX idx_contact_company (company_id) TYPE bloom_filter GRANULARITY 1;

-- Tickets indexes
ALTER TABLE tickets ADD INDEX idx_ticket_status (status) TYPE set(50) GRANULARITY 1;
ALTER TABLE tickets ADD INDEX idx_ticket_priority (priority) TYPE set(20) GRANULARITY 1;
ALTER TABLE tickets ADD INDEX idx_ticket_company (company_id) TYPE bloom_filter GRANULARITY 1;

-- Time entries indexes
ALTER TABLE time_entries ADD INDEX idx_time_member (member_id) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE time_entries ADD INDEX idx_time_company (company_id) TYPE bloom_filter GRANULARITY 1;