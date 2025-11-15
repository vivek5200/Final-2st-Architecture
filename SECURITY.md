# Security Configuration Guide

## ⚠️ Important Security Notice

This repository contains template configuration files. Before deploying to production, you **MUST** replace all placeholder values with your actual AWS account information.

## Files That Need Configuration

### 1. Task Definition Files (Create from templates)

Copy the template files and replace placeholders:

```bash
# Copy templates
cp ecs-api-task-definition.json.template ecs-api-task-definition.json
cp ecs-titiler-task-definition.json.template ecs-titiler-task-definition.json
cp deploy-titiler.ps1.template deploy-titiler.ps1
```

**Replace the following placeholders:**

- `YOUR_ACCOUNT_ID` - Your AWS Account ID (12-digit number)
- `YOUR_REGION` - Your AWS region (e.g., `ap-south-1`, `us-east-1`)
- `YOUR_ALB_NAME` - Your Application Load Balancer name
- `YOUR_ALB_ID` - Your ALB ID
- `YOUR_ALB_DNS_NAME` - Your ALB DNS name
- `YOUR_CLUSTER_NAME` - Your ECS cluster name

### 2. Never Commit These Files

The following files are in `.gitignore` and should **NEVER** be committed to version control:

- `ecs-api-task-definition.json` (contains account-specific ARNs)
- `ecs-titiler-task-definition.json` (contains account-specific ARNs)
- `current-api-task-def.json` (contains production configuration)
- `deploy-titiler.ps1` (contains account-specific values)
- `.env` files (contains credentials)
- `*.pem` or `*.key` files (contains private keys)

### 3. Environment Variables

The application uses environment variables for configuration. Never hardcode:

- AWS Access Keys (use IAM roles instead)
- Passwords or secrets
- API keys
- Database connection strings

All sensitive configuration is loaded from:
- AWS Systems Manager Parameter Store (recommended)
- Environment variables set in ECS task definitions
- IAM role permissions (preferred for AWS credentials)

## AWS Credentials

### For Local Development

Use AWS CLI configuration:
```bash
aws configure
```

Or use environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=your_region
```

### For Production (ECS)

Use IAM roles attached to ECS tasks. The task definition specifies:
- `executionRoleArn` - For pulling images and writing logs
- `taskRoleArn` - For accessing AWS services (S3, DynamoDB, SQS, Lambda)

**Never include AWS credentials in:**
- Source code
- Docker images
- Configuration files committed to git

## Deployment Checklist

Before deploying:

- [ ] All placeholder values replaced in configuration files
- [ ] Sensitive files added to `.gitignore`
- [ ] IAM roles configured with least-privilege permissions
- [ ] Security groups properly configured
- [ ] S3 buckets have proper access policies
- [ ] CloudWatch logs enabled for monitoring
- [ ] Secrets stored in AWS Secrets Manager or Parameter Store
- [ ] SSL/TLS enabled for production endpoints

## Reporting Security Issues

If you discover a security vulnerability, please report it responsibly:

1. **Do not** create a public GitHub issue
2. Email the maintainer directly
3. Include details about the vulnerability
4. Allow time for the issue to be fixed before public disclosure

## Additional Security Best Practices

1. **Enable MFA** on all AWS accounts
2. **Use VPC** for network isolation
3. **Enable CloudTrail** for audit logging
4. **Regular security audits** using AWS Security Hub
5. **Keep dependencies updated** for security patches
6. **Use AWS Secrets Manager** for credential rotation
7. **Implement least-privilege IAM policies**
8. **Enable S3 bucket encryption** at rest
9. **Use HTTPS** for all API endpoints
10. **Regular backup** of critical data
