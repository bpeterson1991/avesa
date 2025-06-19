import json
import boto3
import base64
import requests
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone


dynamodb = boto3.client('dynamodb')
secrets = boto3.client('secretsmanager')

DYNAMODB_TABLE = 'TenantServices'
STATE_TABLE = 'LastUpdated'
CONFIG_FILE = 'connectwise_config.json'
BUCKET_NAME = 'data-storage-msp'

def get_tenant_services():
    response = dynamodb.scan(TableName=DYNAMODB_TABLE)
    return [
        {
            'tenant_id': item['tenant_id']['S'],
            'service_id': item['service_id']['S'],
            'secret_name': item['secret_name']['S'],
            'database_name': item['database_name']['S']
        }
        for item in response['Items']
        if item['service_id']['S'] == 'connectwise'
    ]

def get_secret(secret_name):
    response = secrets.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def build_auth_header(secret):
    username = f"{secret['company_id']}+{secret['public_key']}"
    password = secret['private_key']
    auth = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {auth}",
        "clientId": secret['client_id'],
        "Accept": "application/json"
    }

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f'{name}{i}_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def get_last_updated(tenant_id, table_name):
    try:
        response = dynamodb.get_item(
            TableName=STATE_TABLE,
            Key={
                'tenant_id': {'S': tenant_id},
                'table_name': {'S': table_name}
            }
        )
        item = response.get('Item')
        return item['last_updated']['S'] if item and 'last_updated' in item else None
    except Exception:
        return None

def update_last_updated(tenant_id, table_name, timestamp):
    dynamodb.put_item(
        TableName=STATE_TABLE,
        Item={
            'tenant_id': {'S': tenant_id},
            'table_name': {'S': table_name},
            'last_updated': {'S': timestamp}
        }
    )

def fetch_paginated_data(base_url, endpoint, headers):
    records = []
    page = 1
    page_size = 25
    max_records = 10000

    while True:
        params = {
            "pageSize": page_size,
            "page": page,
            "orderBy": "id asc"
        }

        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        records.extend(data)
        if len(records) >= max_records:
            break
        page += 1

    return records[:max_records]

def lambda_handler(event, context):
    config = load_config()
    tenants = get_tenant_services()

    for tenant in tenants:
        secret = get_secret(tenant['secret_name'])
        headers = build_auth_header(secret)
        base_url = secret['api_base_url']

        for table in config['connectwise']:
            try:
                prefix = f"s3://{BUCKET_NAME}/{tenant['tenant_id']}/raw/connectwise/{table['table_name']}/"
                existing_ids = set()
                try:
                    df_existing = wr.s3.read_parquet(path=prefix, columns=['id'])
                    existing_ids = set(df_existing['id'].dropna().astype(int).tolist())
                except Exception:
                    pass  # If no existing data, treat as empty set

                raw_records = fetch_paginated_data(base_url, table['endpoint'], headers)
                new_records = [rec for rec in raw_records if rec.get('id') not in existing_ids]

                if not new_records:
                    print(f"⚠️ {tenant['tenant_id']} - {table['table_name']}: No new data")
                    continue

                flattened = [flatten_json(rec) for rec in new_records]
                df = pd.DataFrame(flattened)
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                s3_path = f"s3://{BUCKET_NAME}/{tenant['tenant_id']}/raw/connectwise/{table['table_name']}/{timestamp}.parquet"

                wr.s3.to_parquet(
                    df=df,
                    path=s3_path,
                    dataset=True,
                    mode='overwrite_partitions'
                )

                if '_info__lastUpdated' in df.columns:
                    most_recent = df['_info__lastUpdated'].max()
                    update_last_updated(tenant['tenant_id'], table['table_name'], most_recent)
                else:
                    print(f"⚠️ {tenant['tenant_id']} - {table['table_name']}: Missing _info__lastUpdated in records")

                print(f"✅ {tenant['tenant_id']} - {table['table_name']}")

            except Exception as e:
                print(f"❌ {tenant['tenant_id']} - {table['table_name']}: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('Backfill of ConnectWise data complete.')
    }
