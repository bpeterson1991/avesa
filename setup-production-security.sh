#!/bin/bash
# Production Account Security Setup
# Run these commands in your production account

echo "Setting up production account security..."

# Enable CloudTrail
aws cloudtrail create-trail \
    --name avesa-production-trail \
    --s3-bucket-name avesa-production-cloudtrail-${RANDOM} \
    --include-global-service-events \
    --is-multi-region-trail \
    --profile avesa-production

# Enable GuardDuty
aws guardduty create-detector \
    --enable \
    --profile avesa-production

# Create billing alarm
aws cloudwatch put-metric-alarm \
    --alarm-name "AVESA-Production-Billing-Alert" \
    --alarm-description "Alert when monthly charges exceed $1000" \
    --metric-name EstimatedCharges \
    --namespace AWS/Billing \
    --statistic Maximum \
    --period 86400 \
    --threshold 1000 \
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=Currency,Value=USD \
    --evaluation-periods 1 \
    --profile avesa-production

echo "Security setup complete!"
