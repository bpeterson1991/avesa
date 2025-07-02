/**
 * Schema Synchronization Utility
 *
 * Provides utilities for ensuring API routes use canonical field mappings
 * and handle schema synchronization properly with mixed SCD Type 1 and Type 2 support.
 *
 * This utility dynamically reads field definitions from canonical mapping files
 * to ensure a single source of truth and eliminate hardcoded duplications.
 */

const logger = require('./logger');
const path = require('path');
const fs = require('fs');

// Cache for loaded mapping files to avoid repeated file reads
const mappingCache = new Map();

/**
 * Load canonical mapping for a table
 */
function loadCanonicalMapping(tableName) {
  // Check cache first
  if (mappingCache.has(tableName)) {
    return mappingCache.get(tableName);
  }
  
  try {
    // Try multiple paths to find the mapping file
    // 1. Lambda environment: mappings are bundled at /var/task/mappings
    // 2. Local development: relative path from utils directory
    const possiblePaths = [
      path.join('/var/task/mappings/canonical', `${tableName}.json`), // Lambda environment
      path.join(__dirname, '..', 'mappings', 'canonical', `${tableName}.json`), // Bundled with API
      path.join(__dirname, '..', '..', '..', '..', 'mappings', 'canonical', `${tableName}.json`) // Local dev
    ];
    
    let mappingPath = null;
    
    for (const tryPath of possiblePaths) {
      if (fs.existsSync(tryPath)) {
        mappingPath = tryPath;
        break;
      }
    }
    
    if (!mappingPath) {
      logger.warn(`Canonical mapping file not found for table: ${tableName}. Tried paths:`, possiblePaths);
      return null;
    }
    
    const mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
    
    // Cache the loaded mapping
    mappingCache.set(tableName, mapping);
    
    return mapping;
  } catch (error) {
    logger.error(`Failed to load canonical mapping for ${tableName}: ${error.message}`);
    return null;
  }
}

/**
 * Extract all canonical field names from mapping file
 */
function extractCanonicalFields(mapping) {
  if (!mapping) return [];
  
  // If field_types are explicitly defined in the mapping, use those
  // This ensures we only include fields that actually exist in the table
  if (mapping.field_types) {
    const fields = Object.keys(mapping.field_types);
    
    // Add SCD fields for Type 2 tables if not already included
    if (mapping.scd_type === 'type_2') {
      if (!fields.includes('effective_date')) fields.push('effective_date');
      if (!fields.includes('expiration_date')) fields.push('expiration_date');
      if (!fields.includes('is_current')) fields.push('is_current');
    }
    
    // Always add tenant_id if not included
    // These are required for multi-tenancy and data lineage
    if (!fields.includes('tenant_id')) fields.push('tenant_id');
    
    return fields.sort();
  }
  
  // Fallback to extracting from integration mappings if field_types not defined
  const fieldSet = new Set();
  
  // Minimal truly universal fields
  const standardFields = ['id', 'tenant_id'];
  standardFields.forEach(field => fieldSet.add(field));
  
  // Extract fields from all integration mappings
  Object.keys(mapping).forEach(integrationKey => {
    if (integrationKey === 'scd_type' || integrationKey === 'field_types') return; // Skip metadata
    
    const integration = mapping[integrationKey];
    if (typeof integration === 'object') {
      Object.keys(integration).forEach(endpointKey => {
        const endpoint = integration[endpointKey];
        if (typeof endpoint === 'object') {
          // Add all canonical field names (keys) from this endpoint
          Object.keys(endpoint).forEach(canonicalField => {
            fieldSet.add(canonicalField);
          });
        }
      });
    }
  });
  
  // Add SCD fields for Type 2 tables
  if (mapping.scd_type === 'type_2') {
    fieldSet.add('effective_date');
    fieldSet.add('expiration_date');
    fieldSet.add('is_current');
  }
  
  return Array.from(fieldSet).sort();
}

/**
 * Get canonical fields for a table (dynamically from mapping files)
 */
function getCanonicalFields(tableName) {
  try {
    const mapping = loadCanonicalMapping(tableName);
    if (!mapping) {
      logger.warn(`No canonical mapping found for table: ${tableName}, returning empty field list`);
      return [];
    }
    
    return extractCanonicalFields(mapping);
  } catch (error) {
    logger.error(`Failed to get canonical fields for ${tableName}: ${error.message}`);
    return [];
  }
}

/**
 * Build SELECT clause with all canonical fields
 */
function buildSelectClause(tableName, alias = '') {
  const fields = getCanonicalFields(tableName);
  const prefix = alias ? `${alias}.` : '';
  
  return fields.map(field => `${prefix}${field}`).join(', ');
}

/**
 * Get SCD type for a table from canonical mapping
 */
function getSCDType(tableName) {
  try {
    // Try to load canonical mapping file
    // Try multiple paths to find the mapping file in Lambda and local environments
    const possiblePaths = [
      path.join('/var/task/mappings/canonical', `${tableName}.json`), // Lambda environment
      path.join(__dirname, '..', 'mappings', 'canonical', `${tableName}.json`), // Bundled with API
      path.join(__dirname, '..', '..', '..', 'mappings', 'canonical', `${tableName}.json`) // Local dev (original path)
    ];
    
    let mappingPath = null;
    
    for (const tryPath of possiblePaths) {
      if (fs.existsSync(tryPath)) {
        mappingPath = tryPath;
        break;
      }
    }
    
    if (fs.existsSync(mappingPath)) {
      const mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
      return mapping.scd_type || 'type_1'; // Default to type_1 if not specified
    }
    
    // Try alternate paths for Lambda environment
    const alternatePaths = [
      path.join('/var/task/mappings/canonical', `${tableName}.json`),
      path.join(__dirname, '..', 'mappings', 'canonical', `${tableName}.json`)
    ];
    
    for (const altPath of alternatePaths) {
      if (fs.existsSync(altPath)) {
        try {
          const mapping = JSON.parse(fs.readFileSync(altPath, 'utf8'));
          return mapping.scd_type || 'type_1';
        } catch (e) {
          logger.warn(`Failed to read mapping from alternate path ${altPath}: ${e.message}`);
        }
      }
    }
    
    // Fallback to default SCD types based on business requirements
    const defaultSCDTypes = {
      'companies': 'type_1',    // Simple upsert for companies
      'contacts': 'type_1',     // Simple upsert for contacts
      'tickets': 'type_2',      // Full historical tracking for tickets
      'time_entries': 'type_1'  // Simple upsert for time entries
    };
    
    return defaultSCDTypes[tableName] || 'type_1';
  } catch (error) {
    logger.warn(`Failed to determine SCD type for ${tableName}, defaulting to type_1: ${error.message}`);
    return 'type_1';
  }
}

/**
 * Check if a table uses SCD Type 2
 */
function isSCDType2(tableName) {
  return getSCDType(tableName) === 'type_2';
}

/**
 * Build SELECT clause with only current records (handles mixed SCD types)
 */
function buildCurrentRecordsSelect(tableName, alias = '') {
  const fields = getCanonicalFields(tableName);
  const selectClause = buildSelectClause(tableName, alias);
  const prefix = alias ? `${alias}.` : '';
  
  // Only add SCD filtering for Type 2 tables
  if (isSCDType2(tableName)) {
    return {
      fields: fields,
      select: selectClause,
      where: `${prefix}is_current = true`
    };
  } else {
    // For Type 1 tables, no SCD filtering needed
    return {
      fields: fields,
      select: selectClause,
      where: ''
    };
  }
}

/**
 * Add tenant isolation to WHERE clause
 */
function addTenantIsolation(whereClause, tenantId, alias = '') {
  const prefix = alias ? `${alias}.` : '';
  const tenantFilter = `${prefix}tenant_id = '${tenantId}'`;
  
  if (!whereClause || whereClause.trim() === '') {
    return tenantFilter;
  }
  
  return `${whereClause} AND ${tenantFilter}`;
}

/**
 * Build a complete query with canonical fields, SCD filtering, and tenant isolation
 */
function buildCanonicalQuery(tableName, options = {}) {
  const {
    tenantId = 'sitetechnology',
    alias = '',
    additionalWhere = '',
    orderBy = '',
    limit = null,
    currentOnly = true
  } = options;
  
  const { select, where: scdWhere } = buildCurrentRecordsSelect(tableName, alias);
  
  // Handle SCD filtering based on table type and currentOnly option
  let whereClause = '';
  if (currentOnly && isSCDType2(tableName)) {
    // Only apply SCD filtering for Type 2 tables when currentOnly is true
    whereClause = scdWhere;
  }
  
  // Always add tenant isolation
  whereClause = addTenantIsolation(whereClause, tenantId, alias);
  
  if (additionalWhere) {
    whereClause = `${whereClause} AND ${additionalWhere}`;
  }
  
  let query = `SELECT ${select} FROM ${tableName}`;
  if (alias) {
    query += ` ${alias}`;
  }
  query += ` WHERE ${whereClause}`;
  
  if (orderBy) {
    query += ` ORDER BY ${orderBy}`;
  }
  
  if (limit) {
    query += ` LIMIT ${limit}`;
  }
  
  return query;
}

/**
 * Validate that a query includes tenant isolation
 */
function validateTenantIsolation(query, tenantId) {
  const upperQuery = query.toUpperCase();
  const tenantFilter = `TENANT_ID = '${tenantId.toUpperCase()}'`;
  
  if (!upperQuery.includes('TENANT_ID')) {
    throw new Error('Query must include tenant_id filter for security');
  }
  
  if (!upperQuery.includes(tenantFilter)) {
    logger.warn('Query may not have proper tenant isolation', { query, tenantId });
  }
  
  return true;
}

/**
 * Transform raw ClickHouse results to API format
 */
function transformResults(results, tableName) {
  if (!Array.isArray(results)) {
    return results;
  }
  
  return results.map(row => {
    // Convert ClickHouse types to JavaScript types
    const transformed = {};
    
    for (const [key, value] of Object.entries(row)) {
      // Handle null values
      if (value === null || value === undefined) {
        transformed[key] = null;
        continue;
      }
      
      // Handle boolean fields
      if (key.endsWith('_flag') || key === 'is_current' || key === 'approved') {
        transformed[key] = Boolean(value);
        continue;
      }
      
      // Handle date fields
      if (key.endsWith('_date') || key.includes('date')) {
        if (value) {
          const date = new Date(value);
          // Check if the date is valid
          if (!isNaN(date.getTime())) {
            transformed[key] = date.toISOString();
          } else {
            transformed[key] = null;
          }
        } else {
          transformed[key] = null;
        }
        continue;
      }
      
      // Handle numeric fields
      if (key.endsWith('_hours') || key.includes('revenue') || key.includes('amount')) {
        transformed[key] = value ? parseFloat(value) : null;
        continue;
      }
      
      if (key.endsWith('_count') || key === 'record_version') {
        transformed[key] = value ? parseInt(value) : null;
        continue;
      }
      
      // Default to string
      transformed[key] = String(value);
    }
    
    return transformed;
  });
}

/**
 * Get table statistics for monitoring (handles mixed SCD types)
 */
function buildTableStatsQuery(tableName, tenantId) {
  if (isSCDType2(tableName)) {
    // Full SCD Type 2 statistics
    return `
      SELECT
        count() as total_records,
        countIf(is_current = true) as current_records,
        countIf(is_current = false) as historical_records,
        min(effective_date) as earliest_date,
        max(effective_date) as latest_date,
        uniq(id) as unique_entities,
        max(last_updated) as last_update
      FROM ${tableName}
      WHERE tenant_id = '${tenantId}'
    `;
  } else {
    // Simplified statistics for SCD Type 1 tables
    return `
      SELECT
        count() as total_records,
        count() as current_records,
        0 as historical_records,
        min(created_date) as earliest_date,
        max(last_updated) as latest_date,
        uniq(id) as unique_entities,
        max(last_updated) as last_update
      FROM ${tableName}
      WHERE tenant_id = '${tenantId}'
    `;
  }
}

/**
 * Middleware to add schema synchronization utilities to request
 */
function schemaSyncMiddleware(req, res, next) {
  req.schemaSync = {
    getCanonicalFields,
    buildSelectClause,
    buildCurrentRecordsSelect,
    addTenantIsolation,
    buildCanonicalQuery,
    validateTenantIsolation,
    transformResults,
    buildTableStatsQuery
  };
  
  next();
}

module.exports = {
  getCanonicalFields,
  getSCDType,
  isSCDType2,
  buildSelectClause,
  buildCurrentRecordsSelect,
  addTenantIsolation,
  buildCanonicalQuery,
  validateTenantIsolation,
  transformResults,
  buildTableStatsQuery,
  schemaSyncMiddleware,
  loadCanonicalMapping,
  extractCanonicalFields
};