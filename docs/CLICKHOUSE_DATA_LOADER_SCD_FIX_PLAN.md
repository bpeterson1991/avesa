# 📋 **ClickHouse Data Loader SCD Deduplication Fix - Implementation Plan**

## 🎯 **Root Cause Analysis**

**Current Problem**: `src/clickhouse/data_loader/lambda_function.py` performs **blind INSERTs** without checking for existing records, causing duplicates even when data hasn't changed.

**SCD Configuration**:
- **SCD Type 1**: `companies`, `contacts`, `time_entries` - Should UPDATE existing records
- **SCD Type 2**: `tickets` - Should version records with `is_current` flag

---

## 🏗️ **Implementation Architecture**

### **Core Components**

#### **1. SCD-Aware Data Loader**
```python
# New function in data_loader/lambda_function.py
def load_data_to_clickhouse_with_scd(
    client: Client,
    table_name: str, 
    data: List[Dict[str, Any]],
    tenant_id: str
) -> Dict[str, int]:
    """Load data with SCD logic - returns statistics"""
```

#### **2. SCD Configuration Manager**
```python
# Import existing shared module
from shared.scd_config import get_scd_type

def get_table_scd_type(table_name: str) -> str:
    """Get SCD type from canonical mappings"""
    # Maps to: 'type_1' or 'type_2'
```

#### **3. Type-Specific Processors**
```python
def process_scd_type_1(client, table_name, data, tenant_id) -> Dict[str, int]:
    """Handle Type 1 tables: UPDATE existing, INSERT new"""

def process_scd_type_2(client, table_name, data, tenant_id) -> Dict[str, int]:  
    """Handle Type 2 tables: Version with is_current flag"""
```

---

## 🔧 **Detailed Implementation Strategy**

### **Phase 1: SCD Type 1 Processing (companies, contacts, time_entries)**

#### **Deduplication Logic**:
```sql
-- For each incoming record:
-- 1. Check if ID exists
SELECT id, last_updated FROM {table_name} 
WHERE tenant_id = '{tenant_id}' AND id = '{record_id}'

-- 2. Compare timestamps and decide action:
-- If NOT EXISTS: INSERT
-- If EXISTS + same last_updated: SKIP 
-- If EXISTS + newer last_updated: UPDATE
```

#### **UPDATE Strategy**:
```sql
-- Use ClickHouse ALTER TABLE UPDATE
ALTER TABLE {table_name} 
UPDATE 
    field1 = '{new_value1}',
    field2 = '{new_value2}',
    last_updated = '{new_timestamp}'
WHERE tenant_id = '{tenant_id}' AND id = '{record_id}'
```

### **Phase 2: SCD Type 2 Processing (tickets)**

#### **Versioning Logic**:
```sql
-- For each incoming record:
-- 1. Check for existing current record
SELECT id, last_updated, record_version FROM {table_name}
WHERE tenant_id = '{tenant_id}' AND id = '{record_id}' AND is_current = true

-- 2. If EXISTS + different data:
--    a. Set old record is_current = false
--    b. Insert new record with is_current = true, incremented version

-- 3. If identical: SKIP
-- 4. If NOT EXISTS: INSERT with is_current = true, version = 1
```

#### **Historical Record Management**:
```sql
-- Expire previous version
ALTER TABLE {table_name}
UPDATE 
    is_current = false,
    expiration_date = now()
WHERE tenant_id = '{tenant_id}' AND id = '{record_id}' AND is_current = true

-- Insert new version  
INSERT INTO {table_name} (..., is_current, effective_date, record_version)
VALUES (..., true, now(), {previous_version + 1})
```

---

## 📝 **Implementation Steps**

### **Step 1: Create SCD Helper Functions**

```python
# File: src/clickhouse/data_loader/scd_helpers.py

def check_existing_records(client, table_name: str, ids: List[str], tenant_id: str) -> Dict[str, Dict]:
    """Check existing records for given IDs"""
    
def compare_record_data(existing: Dict, incoming: Dict, ignore_fields: List[str]) -> bool:
    """Compare if record data has meaningfully changed"""
    
def get_max_version(client, table_name: str, record_id: str, tenant_id: str) -> int:
    """Get current max version for SCD Type 2 record"""
```

### **Step 2: Modify Main Data Loader**

**Replace**: `load_data_to_clickhouse()`
**With**: `load_data_to_clickhouse_with_scd()`

```python
def load_data_to_clickhouse_with_scd(client, table_name, data, tenant_id):
    # 1. Get SCD type
    scd_type = get_scd_type(table_name, bucket_name)
    
    # 2. Route to appropriate processor
    if scd_type == 'type_1':
        return process_scd_type_1(client, table_name, data, tenant_id)
    elif scd_type == 'type_2':
        return process_scd_type_2(client, table_name, data, tenant_id)
    else:
        # Fallback to original behavior
        return load_data_to_clickhouse_original(client, table_name, data, tenant_id)
```

### **Step 3: Implement Type 1 Processor**

```python
def process_scd_type_1(client, table_name, data, tenant_id):
    stats = {'inserted': 0, 'updated': 0, 'skipped': 0}
    
    # Check existing records in batch
    ids = [record['id'] for record in data]
    existing_records = check_existing_records(client, table_name, ids, tenant_id)
    
    inserts = []
    updates = []
    
    for record in data:
        record_id = record['id']
        if record_id in existing_records:
            existing = existing_records[record_id]
            
            # Compare timestamps
            existing_timestamp = existing['last_updated']
            incoming_timestamp = record['last_updated']
            
            if incoming_timestamp > existing_timestamp:
                updates.append(record)
            else:
                stats['skipped'] += 1
        else:
            inserts.append(record)
    
    # Batch process updates and inserts
    if updates:
        stats['updated'] += batch_update_records(client, table_name, updates, tenant_id)
    if inserts:
        stats['inserted'] += batch_insert_records(client, table_name, inserts)
    
    return stats
```

### **Step 4: Implement Type 2 Processor**

```python
def process_scd_type_2(client, table_name, data, tenant_id):
    stats = {'inserted': 0, 'versioned': 0, 'skipped': 0}
    
    for record in data:
        record_id = record['id']
        
        # Check current version
        current_record = get_current_record(client, table_name, record_id, tenant_id)
        
        if current_record:
            # Compare data (excluding metadata fields)
            ignore_fields = ['ingestion_timestamp', 'record_version', 'effective_date']
            if not compare_record_data(current_record, record, ignore_fields):
                stats['skipped'] += 1
                continue
            
            # Data has changed - create new version
            expire_current_record(client, table_name, record_id, tenant_id)
            
            max_version = get_max_version(client, table_name, record_id, tenant_id)
            record.update({
                'is_current': True,
                'effective_date': datetime.now(timezone.utc),
                'record_version': max_version + 1,
                'expiration_date': None
            })
            
            insert_record(client, table_name, record)
            stats['versioned'] += 1
        else:
            # New record
            record.update({
                'is_current': True,
                'effective_date': datetime.now(timezone.utc),
                'record_version': 1,
                'expiration_date': None
            })
            
            insert_record(client, table_name, record)
            stats['inserted'] += 1
    
    return stats
```

---

## 🧪 **Testing Strategy**

### **Test Cases**

#### **SCD Type 1 (companies)**
1. **New Record**: Should INSERT
2. **Identical Record**: Should SKIP  
3. **Updated Record**: Should UPDATE existing
4. **Older Record**: Should SKIP (timestamp comparison)

#### **SCD Type 2 (tickets)**
1. **New Record**: Should INSERT with `is_current=true`, `version=1`
2. **Identical Record**: Should SKIP
3. **Changed Record**: Should expire old + INSERT new version
4. **Multiple Changes**: Should maintain proper version history

### **Validation Queries**
```sql
-- Verify no duplicates in Type 1 tables
SELECT id, tenant_id, COUNT(*) as count 
FROM companies 
GROUP BY id, tenant_id 
HAVING count > 1

-- Verify proper versioning in Type 2 tables  
SELECT id, tenant_id, COUNT(*) as current_versions
FROM tickets 
WHERE is_current = true
GROUP BY id, tenant_id
HAVING current_versions > 1
```

---

## 📊 **Performance Considerations**

### **Batch Processing**
- **Existing Record Lookup**: Single query with `IN` clause for all IDs
- **Batch Updates**: Group updates by similarity, execute as batch
- **Batch Inserts**: Process all new records in single operation

### **Query Optimization**
```sql
-- Efficient existing record check
SELECT id, last_updated, record_version
FROM {table_name} 
WHERE tenant_id = '{tenant_id}' 
  AND id IN ({comma_separated_ids})
  AND (is_current = true OR '{table_name}' NOT LIKE '%scd_type_2%')
```

---

## 🚀 **Deployment Plan**

### **Phase 1: Core Implementation**
1. Create `src/clickhouse/data_loader/scd_helpers.py`
2. Modify `src/clickhouse/data_loader/lambda_function.py` 
3. Add unit tests

### **Phase 2: Integration Testing**
1. Test with existing data in development
2. Validate deduplication works end-to-end
3. Performance testing with large datasets

### **Phase 3: Production Deployment**
1. Deploy to staging environment
2. Run parallel processing tests
3. Deploy to production with monitoring

---

## ✅ **Success Criteria**

1. **Zero Duplicates**: No identical records in any table
2. **Proper Versioning**: SCD Type 2 tables maintain history correctly
3. **Performance**: No significant slowdown in data loading
4. **Data Integrity**: All existing data preserved
5. **Monitoring**: Clear statistics on operations performed

---

## 🔍 **Key Insights from Investigation**

### **Discovered Issues**
1. **Blind INSERTs**: Data loader performs direct inserts without checking existing records
2. **Missing SCD Logic**: No differentiation between Type 1 and Type 2 processing
3. **Timestamp Issues**: Found 3,990 records with identical `last_updated` timestamps in tickets table
4. **ReplacingMergeTree Working**: Engine configuration is correct, but input data has identical timestamps

### **Root Cause**
- Data loader at `src/clickhouse/data_loader/lambda_function.py` lacks SCD-aware processing
- Missing deduplication logic during insertion phase
- Canonical transformation timestamp fixes were secondary to the main issue

### **Solution Architecture**
This plan implements proper SCD processing at the data loading stage, preventing duplicates from being created in the first place rather than relying solely on post-insertion cleanup.