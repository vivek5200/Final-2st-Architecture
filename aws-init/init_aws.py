#!/usr/bin/env python3
"""
AWS Resource Initialization Script
Creates all necessary AWS resources for the geospatial processing workflow
Configured for REAL AWS deployment with S3 fallback for large Lambda packages
"""

import boto3
import os
import time
import logging
import io
import zipfile
import json
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Define a constant for the direct upload limit (50MB to be safe)
LAMBDA_ZIP_LIMIT_BYTES = 50 * 1024 * 1024

class AWSResourceManager:
    """Manages AWS resource creation and configuration"""

    def __init__(self, region: str = "ap-south-1"):
        self.region = region
        self.session = boto3.Session(region_name=region)
        self._clients = {}

    def get_client(self, service: str):
        if service not in self._clients:
            self._clients[service] = self.session.client(service)
        return self._clients[service]


class LambdaManager:
    """Manages Lambda function creation and deployment with S3 fallback"""

    def __init__(self, aws_manager: AWSResourceManager, bucket_name: str, result_bucket_name: str):
        self.aws = aws_manager
        self.lambda_client = aws_manager.get_client("lambda")
        self.s3_client = aws_manager.get_client("s3")
        self.bucket_name = bucket_name
        self.result_bucket_name = result_bucket_name

    def create_layer(self, layer_name: str, layer_path: str, compatible_runtimes: List[str]) -> str:
        """Create Lambda layer with S3 upload for large files"""
        logger.info(f"Creating layer: {layer_name}")
        layer_zip = self._create_zip_from_directory(layer_path, is_layer=True)
        
        layer_zip_size = len(layer_zip)
        logger.info(f"Layer zip size: {layer_zip_size / 1024 / 1024:.2f} MB")

        try:
            response = self.lambda_client.list_layer_versions(LayerName=layer_name)
            if response['LayerVersions']:
                layer_arn = response['LayerVersions'][0]['LayerVersionArn']
                logger.info(f"Layer {layer_name} already exists: {layer_arn}")
                return layer_arn
        except self.lambda_client.exceptions.ResourceNotFoundException:
            pass

        # Use S3 for large layers, direct upload for small ones
        if layer_zip_size > LAMBDA_ZIP_LIMIT_BYTES:
            logger.info(f"Layer size ({layer_zip_size/1024/1024:.2f}MB) exceeds direct upload limit. Uploading to S3...")
            s3_key = f"lambda-layers/{layer_name}.zip"
            
            # Upload layer zip to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=layer_zip,
                ContentType='application/zip'
            )
            logger.info(f"✅ Layer uploaded to S3: s3://{self.bucket_name}/{s3_key}")
            
            # Create layer from S3 location
            response = self.lambda_client.publish_layer_version(
                LayerName=layer_name,
                Content={
                    'S3Bucket': self.bucket_name,
                    'S3Key': s3_key
                },
                CompatibleRuntimes=compatible_runtimes,
                Description='Geospatial processing dependencies including rasterio, numpy, and numexpr'
            )
        else:
            logger.info(f"Using direct upload for layer ({layer_zip_size/1024/1024:.2f}MB)")
            # Direct upload for smaller layers
            response = self.lambda_client.publish_layer_version(
                LayerName=layer_name,
                Content={'ZipFile': layer_zip},
                CompatibleRuntimes=compatible_runtimes,
                Description='Geospatial processing dependencies including rasterio, numpy, and numexpr'
            )

        layer_arn = response['LayerVersionArn']
        logger.info(f"✅ Layer created: {layer_arn}")
        return layer_arn

    def create_function(self, function_config: Dict[str, Any]) -> str:
        function_name = function_config['FunctionName']
        logger.info(f"Creating/updating function: {function_name}")

        # Always create the zip first
        function_zip = self._create_function_zip(
            function_config['source_dir'],
            function_config['function_name'],
            bundle_layer_path=function_config.get('bundle_layer_path')
        )
        
        function_zip_size = len(function_zip)
        logger.info(f"Final zip size for {function_name}: {function_zip_size / 1024 / 1024:.2f} MB")
        
        # Environment variables for Lambda - REMOVE AWS_REGION (it's reserved)
        environment_vars = {
            'Variables': {
                'S3_BUCKET_NAME': self.bucket_name,
                'RESULT_BUCKET_NAME': self.result_bucket_name,
                # AWS_REGION is automatically set by Lambda - DO NOT include it
            }
        }
        
        # Add Memcached endpoint if configured
        memcached_endpoint = os.environ.get('MEMCACHED_ENDPOINT')
        if memcached_endpoint:
            environment_vars['Variables']['MEMCACHED_ENDPOINT'] = memcached_endpoint

        try:
            # Try to get existing function
            existing_function = self.lambda_client.get_function(FunctionName=function_name)
            logger.info(f"Function {function_name} exists, updating code and configuration...")
            
            # ALWAYS use S3 for code updates to avoid size limits
            s3_key = f"lambda-packages/{function_name}.zip"
            logger.info(f"Uploading function code to S3: s3://{self.bucket_name}/{s3_key}")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name, 
                Key=s3_key, 
                Body=function_zip,
                ContentType='application/zip'
            )
            
            # Update function code from S3
            update_code_response = self.lambda_client.update_function_code(
                FunctionName=function_name,
                S3Bucket=self.bucket_name,
                S3Key=s3_key
            )
            
            # Wait for code update to complete before updating configuration
            logger.info(f"Waiting for function code update to complete for {function_name}...")
            self._wait_for_function_update_complete(function_name)
            
            # Now update function configuration
            self.lambda_client.update_function_configuration(
                FunctionName=function_name,
                Role=function_config['Role'],
                Layers=function_config.get('Layers', []),
                MemorySize=function_config.get('MemorySize', 3008),
                Timeout=function_config.get('Timeout', 300),
                Environment=environment_vars
            )
            
            # Wait for configuration update to complete
            logger.info(f"Waiting for function configuration update to complete for {function_name}...")
            self._wait_for_function_update_complete(function_name)
            
            function_arn = update_code_response['FunctionArn']
            
        except self.lambda_client.exceptions.ResourceNotFoundException:
            logger.info(f"Function {function_name} does not exist, creating...")
            
            # ALWAYS use S3 for function creation to avoid size limits
            s3_key = f"lambda-packages/{function_name}.zip"
            logger.info(f"Uploading function code to S3: s3://{self.bucket_name}/{s3_key}")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name, 
                Key=s3_key, 
                Body=function_zip,
                ContentType='application/zip'
            )
            
            create_response = self.lambda_client.create_function(
                FunctionName=function_name,
                Role=function_config['Role'],
                Runtime=function_config['Runtime'],
                Handler=function_config['Handler'],
                Code={
                    'S3Bucket': self.bucket_name,
                    'S3Key': s3_key
                },
                Timeout=function_config.get('Timeout', 300),
                MemorySize=function_config.get('MemorySize', 3008),
                Layers=function_config.get('Layers', []),
                Environment=environment_vars,
                Description=function_config.get('Description', f'{function_name} for geospatial processing')
            )
            
            function_arn = create_response['FunctionArn']

        self._wait_for_function_active(function_name)
        return function_arn

    def _wait_for_function_update_complete(self, function_name: str, max_attempts: int = 60):
        """Wait for function update (code or configuration) to complete"""
        logger.info(f"Waiting for function {function_name} update to complete...")
        for attempt in range(max_attempts):
            try:
                response = self.lambda_client.get_function(FunctionName=function_name)
                last_update_status = response['Configuration'].get('LastUpdateStatus', '')
                
                if last_update_status == 'Successful':
                    logger.info(f"Function {function_name} update completed successfully.")
                    return
                elif last_update_status == 'Failed':
                    logger.error(f"Function {function_name} update failed.")
                    raise Exception(f"Function {function_name} update failed")
                elif last_update_status == 'InProgress':
                    if attempt % 5 == 0:  # Log every 5 attempts to avoid spam
                        logger.info(f"Function update still in progress... (attempt {attempt + 1})")
                else:
                    logger.info(f"Function update status: {last_update_status}")
                    
                time.sleep(3)
            except Exception as e:
                logger.warning(f"Error checking function update status (attempt {attempt + 1}): {e}")
                time.sleep(3)
                
        logger.warning(f"Function {function_name} update did not complete in time.")

    def _wait_for_function_active(self, function_name: str, max_attempts: int = 60):
        logger.info(f"Waiting for function {function_name} to become active...")
        for attempt in range(max_attempts):
            try:
                response = self.lambda_client.get_function(FunctionName=function_name)
                state = response['Configuration']['State']
                last_update_status = response['Configuration'].get('LastUpdateStatus', '')
                
                if state == 'Active' and last_update_status == 'Successful':
                    logger.info(f"Function {function_name} is now active and updated.")
                    return
                elif state == 'Failed' or last_update_status == 'Failed':
                    logger.error(f"Function {function_name} failed to update. State: {state}, LastUpdateStatus: {last_update_status}")
                    raise Exception(f"Function {function_name} deployment failed")
                else:
                    if attempt % 10 == 0:  # Log every 10 attempts to avoid spam
                        logger.info(f"Function state: {state}, LastUpdateStatus: {last_update_status} - waiting...")
                    
                time.sleep(5)
            except Exception as e:
                logger.warning(f"Error checking function state (attempt {attempt + 1}): {e}")
                time.sleep(5)
                
        logger.warning(f"Function {function_name} did not become active in time.")

    def _create_zip_from_directory(self, path: str, is_layer: bool = False) -> bytes:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Directory not found: {path}")
            
        zip_buffer = io.BytesIO()
        base_path = os.path.join(path, 'python') if is_layer and os.path.exists(os.path.join(path, 'python')) else path
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(base_path):
                for file in files:
                    if file.endswith('.pyc') or '__pycache__' in root:
                        continue
                    file_path = os.path.join(root, file)
                    archive_name = os.path.relpath(file_path, base_path)
                    if is_layer:
                        # For layers, the structure inside zip must be python/...
                        zf.write(file_path, os.path.join('python', archive_name))
                    else:
                        zf.write(file_path, archive_name)
        return zip_buffer.getvalue()

    def _create_function_zip(self, source_dir: str, function_name: str, bundle_layer_path: Optional[str] = None) -> bytes:
        if not os.path.exists(source_dir):
            raise FileNotFoundError(f"Source directory not found: {source_dir}")
            
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            function_path = os.path.join(source_dir, function_name)
            if not os.path.exists(function_path):
                raise FileNotFoundError(f"Function directory not found: {function_path}")
                
            self._add_directory_to_zip(zf, function_path, function_path)

            common_path = os.path.join(source_dir, 'common')
            if os.path.exists(common_path):
                self._add_directory_to_zip(zf, common_path, source_dir)

            if bundle_layer_path and os.path.exists(bundle_layer_path):
                logger.info(f"Bundling dependencies from {bundle_layer_path} into {function_name}.zip")
                layer_python_path = os.path.join(bundle_layer_path, 'python')
                if os.path.exists(layer_python_path):
                    self._add_directory_to_zip(zf, layer_python_path, layer_python_path)

        zip_size = zip_buffer.tell()
        logger.info(f"Final zip size for {function_name}: {zip_size / 1024 / 1024:.2f} MB")
        return zip_buffer.getvalue()
        
    def _add_directory_to_zip(self, zf: zipfile.ZipFile, dir_path: str, base_path: str):
        for root, _, files in os.walk(dir_path):
            for file in files:
                if file.endswith('.pyc') or '__pycache__' in root:
                    continue
                file_path = os.path.join(root, file)
                archive_name = os.path.relpath(file_path, base_path)
                zf.write(file_path, archive_name)

class IAMManager:
    """Manages IAM roles and policies"""
    
    def __init__(self, aws_manager: AWSResourceManager):
        self.aws = aws_manager
        self.iam_client = aws_manager.get_client("iam")
    
    def get_account_id(self) -> str:
        """Get current AWS account ID"""
        sts = self.aws.get_client("sts")
        return sts.get_caller_identity()["Account"]
    
    def create_role(self, role_name: str, service_principal: str) -> str:
        """Create IAM role"""
        logger.info(f"Creating IAM role: {role_name}")
        
        try:
            existing_role = self.iam_client.get_role(RoleName=role_name)
            logger.info(f"Role {role_name} already exists, skipping creation")
            return existing_role['Role']['Arn']
        except self.iam_client.exceptions.NoSuchEntityException:
            pass
        
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": service_principal},
                "Action": "sts:AssumeRole"
            }]
        }
        
        response = self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description=f"Role for {service_principal} execution"
        )
        
        logger.info(f"Role created: {response['Role']['Arn']}")
        return response['Role']['Arn']
    
    def create_and_attach_policy(self, role_name: str, policy_name: str, policy_document: Dict) -> str:
        """Create policy and attach to role"""
        logger.info(f"Creating policy: {policy_name}")
        
        account_id = self.get_account_id()
        policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
        
        try:
            self.iam_client.get_policy(PolicyArn=policy_arn)
            logger.info(f"Policy {policy_name} already exists, reusing")
        except self.iam_client.exceptions.NoSuchEntityException:
            response = self.iam_client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=f"Policy for {role_name}"
            )
            policy_arn = response['Policy']['Arn']
            logger.info(f"Policy created: {policy_arn}")
        
        # Check if policy is already attached
        try:
            attached_policies = self.iam_client.list_attached_role_policies(RoleName=role_name)
            for attached_policy in attached_policies['AttachedPolicies']:
                if attached_policy['PolicyArn'] == policy_arn:
                    logger.info(f"Policy {policy_name} already attached to role {role_name}")
                    return policy_arn
        except Exception as e:
            logger.warning(f"Error checking attached policies: {e}")

        logger.info(f"Attaching policy {policy_name} to role {role_name}")
        self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        logger.info(f"Attached policy {policy_name} to role {role_name}")
        
        return policy_arn

class StepFunctionsManager:
    """Manages Step Functions state machines"""
    
    def __init__(self, aws_manager: AWSResourceManager):
        self.aws = aws_manager
        self.sfn_client = aws_manager.get_client("stepfunctions")
    
    def create_state_machine(self, name: str, definition: Dict, role_arn: str) -> str:
        """Create Step Functions state machine"""
        logger.info(f"Creating state machine: {name}")
        
        try:
            state_machines = self.sfn_client.list_state_machines()
            for sm in state_machines['stateMachines']:
                if sm['name'] == name:
                    logger.info(f"State machine {name} already exists, updating...")
                    response = self.sfn_client.update_state_machine(
                        stateMachineArn=sm['stateMachineArn'],
                        definition=json.dumps(definition),
                        roleArn=role_arn
                    )
                    return sm['stateMachineArn']
        except Exception as e:
            logger.warning(f"Error checking existing state machine: {e}")
        
        response = self.sfn_client.create_state_machine(
            name=name,
            definition=json.dumps(definition),
            roleArn=role_arn
        )
        
        state_machine_arn = response['stateMachineArn']
        logger.info(f"State machine created: {state_machine_arn}")
        return state_machine_arn
    
    @staticmethod
    def create_distributed_map_definition(lambda_arns: Dict[str, str]) -> Dict:
        """Create state machine definition with Distributed Map for large datasets"""
        return {
            "Comment": "Geospatial processing workflow with Distributed Map for large payloads",
            "StartAt": "TranslateDAG",
            "States": {
                "TranslateDAG": {
                    "Type": "Task",
                    "Resource": lambda_arns["translator_lambda"],
                    "Next": "ProcessTilesDistributed",
                    "ResultPath": "$.translation_result"
                },
                "ProcessTilesDistributed": {
                    "Type": "Map",
                    "InputPath": "$.translation_result",
                    "MaxConcurrency": 4,
                    "ItemReader": {
                        "Resource": "arn:aws:states:::s3:getObject",
                        "ReaderConfig": { "InputType": "JSON" },
                        "Parameters": {
                            "Bucket.$": "$.tile_tasks_s3_location.bucket",
                            "Key.$": "$.tile_tasks_s3_location.key"
                        }
                    },
                    "ItemProcessor": {
                        "ProcessorConfig": {
                            "Mode": "DISTRIBUTED",
                            "ExecutionType": "STANDARD"
                        },
                        "StartAt": "ProcessSingleTile",
                        "States": {
                            "ProcessSingleTile": {
                                "Type": "Task",
                                "Resource": lambda_arns["processor_lambda"],
                                "End": True
                            }
                        }
                    },
                    "End": True,
                    "Label": "ProcessTilesDistributed"
                }
            }
        }

class StorageManager:
    """Manages S3 and DynamoDB resources"""
    
    def __init__(self, aws_manager: AWSResourceManager):
        self.aws = aws_manager
        self.s3_client = aws_manager.get_client("s3")
        self.dynamodb_client = aws_manager.get_client("dynamodb")
    
    def create_bucket(self, bucket_name: str):
        """Create S3 bucket with proper region handling"""
        logger.info(f"Creating S3 bucket: {bucket_name}")
        
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} already exists, skipping creation")
            return
        except self.s3_client.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                pass  # Bucket doesn't exist, continue to create
            else:
                logger.warning(f"Error checking bucket {bucket_name}: {e}")
                raise
        
        # Handle bucket creation with proper region configuration
        region = self.aws.region
        
        try:
            # For us-east-1, you don't need LocationConstraint
            if region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                # For all other regions, including ap-south-1, use LocationConstraint
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': region
                    }
                )
            
            # Wait for bucket to be ready
            waiter = self.s3_client.get_waiter('bucket_exists')
            waiter.wait(Bucket=bucket_name)
            
            logger.info(f"✅ Bucket {bucket_name} created successfully in region {region}")
            
        except Exception as e:
            logger.error(f"❌ Failed to create bucket {bucket_name}: {e}")
            raise
    
    def create_table(self, table_name: str, key_schema: List[Dict], attributes: List[Dict]):
        """Create DynamoDB table"""
        logger.info(f"Creating DynamoDB table: {table_name}")
        
        try:
            self.dynamodb_client.describe_table(TableName=table_name)
            logger.info(f"Table {table_name} already exists, skipping creation")
            return
        except self.dynamodb_client.exceptions.ResourceNotFoundException:
            pass
        
        self.dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attributes,
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Wait for table to be active
        waiter = self.dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        
        logger.info(f"Table {table_name} created")

def main():
    """Main initialization function for REAL AWS deployment"""
    CONFIG = {
        'region': os.environ.get('AWS_REGION', 'ap-south-1'),
        's3_bucket_name': "insat-cog-processed-production",
        'result_bucket_name': "insat-processed-results-production",
        'dynamodb_table_name': "WorkflowJobs",
        'state_machine_name': "OnTheFlyProcessorStateMachine",
        'layer_name': "geospatial-dependencies",
        'lambda_source_path': "./src",  # Updated path for real deployment
        'prebuilt_layer_path': "./aws-init/layer",  # Updated path for real deployment
    }
    
    logger.info("🚀 Starting AWS resource initialization for REAL AWS...")
    logger.info(f"🔧 Target Region: {CONFIG['region']}")
    
    # Validate AWS credentials
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        logger.info(f"🔑 AWS Identity: {identity['Arn']}")
        logger.info(f"🏢 Account ID: {identity['Account']}")
    except Exception as e:
        logger.error(f"❌ AWS credentials not configured properly: {e}")
        raise
    
    try:
        aws_manager = AWSResourceManager(CONFIG['region'])
        lambda_manager = LambdaManager(
            aws_manager, 
            CONFIG['s3_bucket_name'],
            CONFIG['result_bucket_name']
        )
        iam_manager = IAMManager(aws_manager)
        sfn_manager = StepFunctionsManager(aws_manager)
        storage_manager = StorageManager(aws_manager)
        
        # Create both S3 buckets
        logger.info("📦 Creating S3 buckets...")
        storage_manager.create_bucket(CONFIG['s3_bucket_name'])
        storage_manager.create_bucket(CONFIG['result_bucket_name'])
        
        # Create DynamoDB table
        storage_manager.create_table(
            CONFIG['dynamodb_table_name'],
            key_schema=[{'AttributeName': 'job_id', 'KeyType': 'HASH'}],
            attributes=[{'AttributeName': 'job_id', 'AttributeType': 'S'}]
        )
        
        # Create IAM roles
        lambda_role_arn = iam_manager.create_role("lambda-execution-role", "lambda.amazonaws.com")
        sfn_role_arn = iam_manager.create_role("stepfunctions-execution-role", "states.amazonaws.com")

        # Create IAM policies
        account_id = iam_manager.get_account_id()
        universal_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow", 
                    "Action": "s3:*", 
                    "Resource": [
                        f"arn:aws:s3:::{CONFIG['s3_bucket_name']}",
                        f"arn:aws:s3:::{CONFIG['s3_bucket_name']}/*",
                        f"arn:aws:s3:::{CONFIG['result_bucket_name']}",
                        f"arn:aws:s3:::{CONFIG['result_bucket_name']}/*"
                    ]
                },
                {
                    "Effect": "Allow", 
                    "Action": "lambda:InvokeFunction", 
                    "Resource": f"arn:aws:lambda:{CONFIG['region']}:{account_id}:function:*"
                },
                {
                    "Effect": "Allow", 
                    "Action": "states:*", 
                    "Resource": f"arn:aws:states:{CONFIG['region']}:{account_id}:stateMachine:*"
                },
                {
                    "Effect": "Allow", 
                    "Action": "logs:*", 
                    "Resource": f"arn:aws:logs:{CONFIG['region']}:{account_id}:log-group:*"
                },
                {
                    "Effect": "Allow", 
                    "Action": "dynamodb:*", 
                    "Resource": f"arn:aws:dynamodb:{CONFIG['region']}:{account_id}:table/{CONFIG['dynamodb_table_name']}"
                },
                {
                    "Effect": "Allow", 
                    "Action": "iam:PassRole", 
                    "Resource": [
                        lambda_role_arn,
                        sfn_role_arn
                    ]
                },
            ]
        }
        
        iam_manager.create_and_attach_policy("lambda-execution-role", "UniversalExecutionPolicy", universal_policy)
        iam_manager.create_and_attach_policy("stepfunctions-execution-role", "UniversalExecutionPolicy", universal_policy)

        # Create Lambda layer
        logger.info("📦 Creating Lambda layer...")
        layer_arn = lambda_manager.create_layer(
            CONFIG['layer_name'],
            CONFIG['prebuilt_layer_path'],
            ['python3.9']
        )
        
        # Create Lambda functions
        lambda_arns = {}
        base_config = {
            'source_dir': CONFIG['lambda_source_path'],
            'Role': lambda_role_arn,
            'Runtime': 'python3.9',
            'Handler': "app.handler",
        }

        logger.info("🔨 Creating Lambda functions...")
        lambda_arns['receiver_lambda'] = lambda_manager.create_function({
            **base_config, 
            'FunctionName': 'receiver_lambda', 
            'function_name': 'receiver_lambda',
            'Description': 'Receives and validates geospatial processing requests',
            'MemorySize': 512,
            'Timeout': 60
        })
        
        lambda_arns['translator_lambda'] = lambda_manager.create_function({
            **base_config, 
            'FunctionName': 'translator_lambda', 
            'function_name': 'translator_lambda', 
            'Layers': [layer_arn],
            'Description': 'Translates processing requests into tile-based tasks',
            'MemorySize': 2048,
            'Timeout': 300
        })
        
        # For processor_lambda, don't use layers since we're bundling dependencies
        lambda_arns['processor_lambda'] = lambda_manager.create_function({
            **base_config, 
            'FunctionName': 'processor_lambda', 
            'function_name': 'processor_lambda', 
            'bundle_layer_path': CONFIG['prebuilt_layer_path'],
            'Layers': [],  # No layers since we bundle dependencies
            'MemorySize': 3008,
            'Timeout': 900,  # 15 minutes for processing
            'Description': 'Processes individual tiles and merges results'
        })
        
        # Create Step Functions state machine
        logger.info("⚡ Creating Step Functions state machine...")
        state_machine_definition = sfn_manager.create_distributed_map_definition(lambda_arns)
        state_machine_arn = sfn_manager.create_state_machine(
            CONFIG['state_machine_name'], 
            state_machine_definition, 
            sfn_role_arn
        )
        
        logger.info("🎉 AWS resources initialized successfully!")
        logger.info("📊 Resource Summary:")
        logger.info(f"   📦 Source Bucket: s3://{CONFIG['s3_bucket_name']}")
        logger.info(f"   📦 Result Bucket: s3://{CONFIG['result_bucket_name']}")
        logger.info(f"   🗄️  DynamoDB Table: {CONFIG['dynamodb_table_name']}")
        logger.info(f"   ⚡ Step Functions: {state_machine_arn}")
        logger.info(f"   🔷 Lambda Functions: {len(lambda_arns)} created")
        logger.info(f"   🔧 Region: {CONFIG['region']}")
        
        if os.environ.get('MEMCACHED_ENDPOINT'):
            logger.info(f"   🚀 Memcached: {os.environ.get('MEMCACHED_ENDPOINT')}")
        else:
            logger.info("   ℹ️  Memcached: Not configured (using in-memory cache)")
        
    except Exception as e:
        logger.error(f"❌ Initialization failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()