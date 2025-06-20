#!/usr/bin/env python3
"""
Cleanup script for stuck processing jobs in the AVESA pipeline.
Removes jobs that are stuck in "initializing" status for more than 1 hour.
"""

import boto3
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

class JobCleanup:
    """Cleanup utility for stuck processing jobs."""
    
    def __init__(self, region: str = "us-east-2", environment: str = "dev"):
        self.region = region
        self.environment = environment
        self.dynamodb = boto3.client('dynamodb', region_name=region)
        self.processing_jobs_table = f"ProcessingJobs-{environment}"
        
    def scan_stuck_jobs(self, max_age_hours: int = 1) -> List[Dict[str, Any]]:
        """Scan for jobs stuck in initializing status."""
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
            cutoff_timestamp = cutoff_time.isoformat()
            
            print(f"🔍 Scanning for jobs stuck in 'initializing' status older than {max_age_hours} hour(s)")
            print(f"   Cutoff time: {cutoff_timestamp}")
            
            response = self.dynamodb.scan(
                TableName=self.processing_jobs_table,
                FilterExpression='#status = :status AND created_at < :cutoff',
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':status': {'S': 'initializing'},
                    ':cutoff': {'S': cutoff_timestamp}
                }
            )
            
            stuck_jobs = response.get('Items', [])
            print(f"📋 Found {len(stuck_jobs)} stuck jobs")
            
            return stuck_jobs
            
        except Exception as e:
            print(f"❌ Error scanning for stuck jobs: {str(e)}")
            return []
    
    def delete_job(self, job_id: str, tenant_id: str) -> bool:
        """Delete a specific job from the table."""
        try:
            self.dynamodb.delete_item(
                TableName=self.processing_jobs_table,
                Key={
                    'job_id': {'S': job_id},
                    'tenant_id': {'S': tenant_id}
                }
            )
            return True
            
        except Exception as e:
            print(f"❌ Error deleting job {job_id}: {str(e)}")
            return False
    
    def cleanup_stuck_jobs(self, max_age_hours: int = 1, dry_run: bool = True) -> Dict[str, int]:
        """Clean up stuck jobs."""
        print(f"🧹 Starting cleanup of stuck jobs (dry_run={dry_run})")
        
        stuck_jobs = self.scan_stuck_jobs(max_age_hours)
        
        if not stuck_jobs:
            print("✅ No stuck jobs found")
            return {'found': 0, 'deleted': 0, 'failed': 0}
        
        deleted_count = 0
        failed_count = 0
        
        for job in stuck_jobs:
            job_id = job['job_id']['S']
            tenant_id = job['tenant_id']['S']
            created_at = job.get('created_at', {}).get('S', 'unknown')
            
            print(f"📋 Job: {job_id} (tenant: {tenant_id}, created: {created_at})")
            
            if not dry_run:
                if self.delete_job(job_id, tenant_id):
                    print(f"   ✅ Deleted")
                    deleted_count += 1
                else:
                    print(f"   ❌ Failed to delete")
                    failed_count += 1
            else:
                print(f"   🔍 Would delete (dry run)")
        
        result = {
            'found': len(stuck_jobs),
            'deleted': deleted_count,
            'failed': failed_count
        }
        
        if dry_run:
            print(f"\n🔍 DRY RUN SUMMARY:")
            print(f"   Found: {result['found']} stuck jobs")
            print(f"   Would delete: {result['found']} jobs")
            print(f"\n💡 Run with --execute to actually delete the jobs")
        else:
            print(f"\n✅ CLEANUP SUMMARY:")
            print(f"   Found: {result['found']} stuck jobs")
            print(f"   Deleted: {result['deleted']} jobs")
            print(f"   Failed: {result['failed']} jobs")
        
        return result
    
    def list_all_jobs(self) -> List[Dict[str, Any]]:
        """List all jobs in the table for inspection."""
        try:
            print(f"📋 Listing all jobs in {self.processing_jobs_table}")
            
            response = self.dynamodb.scan(TableName=self.processing_jobs_table)
            jobs = response.get('Items', [])
            
            if not jobs:
                print("   No jobs found")
                return []
            
            # Group by status
            status_counts = {}
            for job in jobs:
                status = job.get('status', {}).get('S', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print(f"   Total jobs: {len(jobs)}")
            for status, count in status_counts.items():
                print(f"   {status}: {count}")
            
            return jobs
            
        except Exception as e:
            print(f"❌ Error listing jobs: {str(e)}")
            return []


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Cleanup stuck processing jobs")
    parser.add_argument("--region", default="us-east-2", help="AWS region")
    parser.add_argument("--environment", default="dev", help="Environment (dev/staging/prod)")
    parser.add_argument("--max-age-hours", type=int, default=1, help="Maximum age in hours for stuck jobs")
    parser.add_argument("--execute", action="store_true", help="Actually delete jobs (default is dry run)")
    parser.add_argument("--list-all", action="store_true", help="List all jobs instead of cleaning up")
    
    args = parser.parse_args()
    
    cleanup = JobCleanup(region=args.region, environment=args.environment)
    
    print(f"🚀 AVESA Job Cleanup Utility")
    print(f"============================================================")
    print(f"Region: {args.region}")
    print(f"Environment: {args.environment}")
    print(f"Table: ProcessingJobs-{args.environment}")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    if args.list_all:
        cleanup.list_all_jobs()
    else:
        cleanup.cleanup_stuck_jobs(
            max_age_hours=args.max_age_hours,
            dry_run=not args.execute
        )


if __name__ == "__main__":
    main()