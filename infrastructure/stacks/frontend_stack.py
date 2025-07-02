from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_iam as iam,
    CfnOutput,
    Environment
)
from constructs import Construct
import os


class FrontendStack(Stack):
    """
    CDK Stack for React Frontend Deployment
    Deploys React app to S3 with CloudFront CDN for global delivery
    """

    def __init__(self, scope: Construct, construct_id: str, 
                 api_url: str = None, domain_name: str = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get environment from context or environment variables
        environment = self.node.try_get_context("environment") or os.getenv("ENVIRONMENT", "dev")
        
        # Use provided API URL or default to the deployed API
        api_endpoint = api_url or "https://wesjtc1uve.execute-api.us-east-2.amazonaws.com/dev"
        
        # Create S3 bucket for hosting static files (no website hosting - CloudFront will handle that)
        frontend_bucket = s3.Bucket(
            self, "FrontendBucket",
            bucket_name=f"avesa-frontend-{environment}-{self.account}",
            public_read_access=False,  # Will be accessed through CloudFront only
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY if environment == "dev" else RemovalPolicy.RETAIN,
            auto_delete_objects=environment == "dev"
        )

        # Create Origin Access Identity for CloudFront
        origin_access_identity = cloudfront.OriginAccessIdentity(
            self, "FrontendOAI",
            comment=f"OAI for AVESA Frontend - {environment}"
        )

        # Grant CloudFront access to S3 bucket
        frontend_bucket.grant_read(origin_access_identity)

        # Create cache behaviors for different file types
        cache_behaviors = {
            # Cache static assets for 1 year
            "/static/*": cloudfront.BehaviorOptions(
                origin=origins.S3Origin(
                    frontend_bucket,
                    origin_access_identity=origin_access_identity
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                compress=True
            ),
            # Cache HTML files for shorter time
            "*.html": cloudfront.BehaviorOptions(
                origin=origins.S3Origin(
                    frontend_bucket,
                    origin_access_identity=origin_access_identity
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,  # Don't cache HTML for SPA
                compress=True
            )
        }

        # Create CloudFront distribution
        distribution = cloudfront.Distribution(
            self, "FrontendDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3Origin(
                    frontend_bucket,
                    origin_access_identity=origin_access_identity
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,  # Default behavior for SPA
                origin_request_policy=cloudfront.OriginRequestPolicy.CORS_S3_ORIGIN,
                compress=True,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                cached_methods=cloudfront.CachedMethods.CACHE_GET_HEAD_OPTIONS
            ),
            additional_behaviors=cache_behaviors,
            default_root_object="index.html",
            error_responses=[
                # Handle SPA routing - redirect all 404s to index.html
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(0)
                ),
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(0)
                )
            ],
            price_class=cloudfront.PriceClass.PRICE_CLASS_100 if environment == "dev" else cloudfront.PriceClass.PRICE_CLASS_ALL,
            enabled=True,
            comment=f"AVESA Frontend Distribution - {environment}",
            geo_restriction=cloudfront.GeoRestriction.allowlist("US", "CA") if environment == "dev" else None
        )

        # Create deployment for frontend files
        deployment = s3_deployment.BucketDeployment(
            self, "FrontendDeployment",
            sources=[
                s3_deployment.Source.asset(
                    "../frontend/build",
                    exclude=["**/.env*", "**/node_modules/**"]
                )
            ],
            destination_bucket=frontend_bucket,
            distribution=distribution,
            distribution_paths=["/*"],  # Invalidate all paths on deployment
            cache_control=[
                s3_deployment.CacheControl.from_string("max-age=31536000") # 1 year for versioned assets
            ],
            prune=True  # Remove files not in source
        )

        # Optional: Create custom domain if domain_name is provided
        if domain_name:
            # Look up existing hosted zone
            hosted_zone = route53.HostedZone.from_lookup(
                self, "HostedZone",
                domain_name=domain_name
            )
            
            # Create SSL certificate
            certificate = acm.Certificate(
                self, "FrontendCertificate",
                domain_name=f"{environment}.{domain_name}" if environment != "prod" else domain_name,
                validation=acm.CertificateValidation.from_dns(hosted_zone)
            )
            
            # Update distribution with custom domain
            distribution.node.default_child.add_override(
                "Properties.DistributionConfig.Aliases",
                [f"{environment}.{domain_name}" if environment != "prod" else domain_name]
            )
            distribution.node.default_child.add_override(
                "Properties.DistributionConfig.ViewerCertificate",
                {
                    "AcmCertificateArn": certificate.certificate_arn,
                    "SslSupportMethod": "sni-only",
                    "MinimumProtocolVersion": "TLSv1.2_2021"
                }
            )
            
            # Create Route53 record
            route53.ARecord(
                self, "FrontendAliasRecord",
                zone=hosted_zone,
                record_name=f"{environment}.{domain_name}" if environment != "prod" else domain_name,
                target=route53.RecordTarget.from_alias(
                    targets.CloudFrontTarget(distribution)
                )
            )

        # Create environment file for the frontend build
        self._create_environment_config(api_endpoint, environment)

        # Outputs
        CfnOutput(
            self, "FrontendBucketName",
            value=frontend_bucket.bucket_name,
            description=f"S3 bucket name for frontend - {environment}",
            export_name=f"Frontend-Bucket-{environment}"
        )

        CfnOutput(
            self, "FrontendBucketUrl",
            value=f"s3://{frontend_bucket.bucket_name}",
            description=f"S3 bucket ARN - {environment}",
            export_name=f"Frontend-S3-URL-{environment}"
        )

        CfnOutput(
            self, "CloudFrontDistributionId",
            value=distribution.distribution_id,
            description=f"CloudFront distribution ID - {environment}",
            export_name=f"Frontend-CF-ID-{environment}"
        )

        CfnOutput(
            self, "CloudFrontURL",
            value=f"https://{distribution.distribution_domain_name}",
            description=f"CloudFront distribution URL - {environment}",
            export_name=f"Frontend-CF-URL-{environment}"
        )

        if domain_name:
            custom_domain = f"{environment}.{domain_name}" if environment != "prod" else domain_name
            CfnOutput(
                self, "CustomDomainURL",
                value=f"https://{custom_domain}",
                description=f"Custom domain URL - {environment}",
                export_name=f"Frontend-Domain-URL-{environment}"
            )

        # Store values for access in deployment scripts
        self.bucket_name = frontend_bucket.bucket_name
        self.distribution_id = distribution.distribution_id
        self.api_endpoint = api_endpoint

    def _create_environment_config(self, api_endpoint: str, environment: str):
        """Create environment configuration for the frontend build"""
        env_content = f"""# Environment Configuration - {environment}
REACT_APP_API_URL={api_endpoint}
REACT_APP_ENVIRONMENT={environment}
GENERATE_SOURCEMAP=false
BUILD_PATH=build
"""
        
        # Write environment file to frontend directory
        env_file_path = f"../frontend/.env.{environment}"
        with open(env_file_path, 'w') as f:
            f.write(env_content)