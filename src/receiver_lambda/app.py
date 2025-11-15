import os
import json
import uuid
import boto3
import re
import logging
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- CONFIGURATION ---
REQUIRED_ENV_VARS = [
    "STATE_MACHINE_ARN",
    "DYNAMODB_TABLE_NAME",
    "S3_BUCKET_NAME",
    "UDF_METADATA_TABLE"
]

# --- Utilities / Helpers ---
def check_environment():
    """Validate that all required environment variables are set."""
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")

def extract_bands_from_formula(formula):
    """
    Extract band names from formula string.
    Excludes Python keywords and common function names.
    """
    if not formula:
        return set()

    excluded_terms = {
        'and', 'or', 'not', 'if', 'else', 'elif', 'for', 'while',
        'in', 'is', 'abs', 'min', 'max', 'sum', 'pow', 'sqrt'
    }

    potential_bands = set(re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', formula))
    return {band for band in potential_bands if band not in excluded_terms and len(band) > 1}

def get_udf_rules(operation_name, dynamodb_client, table_name):
    """
    Fetch UDF metadata from DynamoDB table.
    Returns None if not found.
    """
    try:
        resp = dynamodb_client.get_item(TableName=table_name, Key={'operation_name': {'S': operation_name}})
        return resp.get('Item')
    except Exception as e:
        logger.error("Error fetching UDF rules for %s: %s", operation_name, str(e))
        return None

def validate_input(workflow_data):
    """Validate the input workflow data structure."""
    if not workflow_data:
        raise ValueError("Workflow data cannot be empty")
    required_fields = ["dataset_id", "tasks"]
    for field in required_fields:
        if field not in workflow_data:
            raise ValueError(f"Missing required field: {field}")
    if not isinstance(workflow_data["tasks"], list):
        raise ValueError("Tasks must be a list")
    if not workflow_data["dataset_id"]:
        raise ValueError("dataset_id cannot be empty")
    for i, task in enumerate(workflow_data["tasks"]):
        if not isinstance(task, dict):
            raise ValueError(f"Task at index {i} must be an object")
        if "operation" not in task:
            raise ValueError(f"Task at index {i} missing 'operation' field")
        if "parameters" not in task:
            raise ValueError(f"Task at index {i} missing 'parameters' field")

# --- DynamoDB helpers ---
def get_dynamo_client(client_config=None):
    if client_config is None:
        client_config = {}
    return boto3.client("dynamodb", **client_config)

def save_initial_state(dynamodb_client, table_name, job_uuid, dataset_id, workflow_data):
    """
    Save initial workflow state to DynamoDB.
    """
    timestamp = datetime.utcnow().isoformat()

    dynamodb_item = {
        'job_id': {'S': job_uuid},
        'dataset_id': {'S': dataset_id},
        'status': {'S': 'ACCEPTED'},
        'received_at': {'S': timestamp},
        'initial_dag': {'S': json.dumps(workflow_data)},
        'last_updated': {'S': timestamp}
    }

    logger.info("Saving initial state for job_id '%s'...", job_uuid)
    dynamodb_client.put_item(TableName=table_name, Item=dynamodb_item)
    logger.info("Initial state saved successfully.")

def update_job_status(dynamodb_client, table_name, job_id, status, extras: dict = None):
    """
    Update job status in DynamoDB. Best-effort: logs errors but doesn't raise.
    Stores extras as JSON string under 'details'.
    """
    if not job_id:
        logger.warning("update_job_status called with empty job_id - skipping")
        return
    timestamp = datetime.utcnow().isoformat()
    update_expr = "SET #s = :s, last_updated = :t"
    expr_names = {"#s": "status"}
    expr_vals = {
        ":s": {"S": status},
        ":t": {"S": timestamp}
    }
    if extras:
        try:
            expr_vals[":d"] = {"S": json.dumps(extras, default=str)}
            update_expr += ", details = :d"
        except Exception as e:
            logger.warning("Failed to JSON-encode extras for DynamoDB details: %s", e)

    try:
        dynamodb_client.update_item(
            TableName=table_name,
            Key={'job_id': {'S': job_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_vals
        )
        logger.info("DynamoDB updated for job_id=%s status=%s", job_id, status)
    except Exception as e:
        logger.exception("Failed to update DynamoDB for job_id=%s: %s", job_id, str(e))

# --- Manifest / UDF Validation ---
def validate_manifest_and_bands(s3_client, bucket_name, dataset_id, user_tasks, udf_metadata_table, client_config):
    """
    Validate manifest existence and band compatibility with UDF operations.
    Returns band_properties mapping.
    """
    manifest_key = f"{dataset_id}/manifest.json"
    try:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
        manifest_content = s3_object['Body'].read().decode('utf-8')
        manifest_data = json.loads(manifest_content)

        band_properties = {
            file_info.get('subdataset', {}).get('name'): file_info.get('subdataset', {})
            for file_info in manifest_data.get('files', []) 
            if file_info.get('subdataset')
        }

        if not band_properties:
            raise ValueError(f"No valid bands found in manifest for dataset: {dataset_id}")

        dynamodb_client = boto3.client("dynamodb", **client_config)

        for task_index, task in enumerate(user_tasks):
            operation = task.get("operation")
            formula = task.get('parameters', {}).get('formula', '')
            required_bands = extract_bands_from_formula(formula)

            if not required_bands:
                logger.warning("Task %s with operation '%s' has no identifiable bands in formula: %s",
                               task_index, operation, formula)
                continue

            missing_bands = required_bands - set(band_properties.keys())
            if missing_bands:
                msg = (f"Invalid request. The following bands in task {task_index} are not available "
                       f"in dataset '{dataset_id}': {', '.join(sorted(missing_bands))}")
                raise ValueError(msg)

            udf_rules = get_udf_rules(operation, dynamodb_client, udf_metadata_table)
            if not udf_rules:
                raise ValueError(f"Operation '{operation}' is not a valid or supported UDF.")

            allowed_dtypes = set()
            if 'allowed_dtypes' in udf_rules:
                dtypes_value = udf_rules['allowed_dtypes']
                if isinstance(dtypes_value, dict) and 'SS' in dtypes_value:
                    allowed_dtypes = set(dtypes_value['SS'])
                elif isinstance(dtypes_value, list):
                    allowed_dtypes = set(dtypes_value)
                elif isinstance(dtypes_value, str):
                    allowed_dtypes = {dtypes_value}

            if not allowed_dtypes:
                logger.warning("No allowed data types specified for operation '%s', skipping dtype validation", operation)
                continue

            for band_name in required_bands:
                band_info = band_properties[band_name]
                band_dtype = band_info.get("dtype")
                if not band_dtype:
                    raise ValueError(f"Band '{band_name}' is missing dtype information in manifest")
                if band_dtype not in allowed_dtypes:
                    raise ValueError(
                        f"Operation '{operation}' cannot be applied to band '{band_name}' "
                        f"because its data type is '{band_dtype}'. Allowed types: {sorted(allowed_dtypes)}."
                    )

        logger.info("Validation successful: dataset, required bands, and UDF compatibility are valid.")
        return band_properties

    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code == 'NoSuchKey':
            raise ValueError(f"Dataset not found: {dataset_id}")
        elif code == 'NoSuchBucket':
            raise ValueError(f"S3 bucket not found: {bucket_name}")
        else:
            raise
    except json.JSONDecodeError:
        raise ValueError(f"Could not parse manifest.json for dataset: {dataset_id}")

# --- Step Functions ---
def start_step_function_execution(sfn_client, state_machine_arn, job_uuid, workflow_data):
    """
    Start Step Functions execution with the workflow data.
    """
    sfn_input = workflow_data.copy()
    sfn_input['job_id'] = job_uuid

    logger.info("Starting Step Functions execution for job_id '%s'...", job_uuid)

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=job_uuid,
        input=json.dumps(sfn_input)
    )
    logger.info("Step Functions execution started. ARN: %s", response.get('executionArn'))
    return response

# --- MAIN HANDLER ---
def handler(event, context):
    """
    Lambda handler for receiving, validating, saving state, and starting a Step Functions workflow.
    Expects an SQS event with a JSON body containing workflow data.
    
    🔧 FIX: Now uses job_id from FastAPI instead of generating a new one
    """
    try:
        check_environment()

        AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
        STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN")
        DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
        S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        UDF_METADATA_TABLE = os.getenv("UDF_METADATA_TABLE")

        logger.info("Receiver Lambda invoked.")

        # Parse SQS message body (first record)
        if 'Records' not in event or not event['Records']:
            raise ValueError("Event is not a valid SQS event or contains no records.")
        message_body = event['Records'][0].get('body')
        if not message_body:
            raise ValueError("SQS message body empty.")
        try:
            workflow_data = json.loads(message_body)
        except json.JSONDecodeError as e:
            logger.error("Failed to decode SQS message body as JSON: %s", e)
            raise ValueError("Could not parse workflow JSON from SQS message body.")

        validate_input(workflow_data)

        dataset_id = workflow_data.get("dataset_id")
        user_tasks = workflow_data.get("tasks", [])

        # 🔧 FIX: Use job_id from FastAPI if present, otherwise generate new one
        job_uuid = workflow_data.get("job_id")
        if not job_uuid:
            # Fallback: generate new job_id if not provided (backwards compatibility)
            job_uuid = str(uuid.uuid4())
            logger.warning("No job_id in workflow_data, generated new one: %s", job_uuid)
        else:
            logger.info("✅ Using job_id from FastAPI: %s", job_uuid)

        client_config = {}
        if AWS_ENDPOINT_URL:
            client_config['endpoint_url'] = AWS_ENDPOINT_URL

        s3_client = boto3.client("s3", **client_config)
        dynamodb_client = boto3.client("dynamodb", **client_config)
        sfn_client = boto3.client("stepfunctions", **client_config)

        # Update: mark job as VALIDATING
        logger.info("Validating manifest and UDF compatibility for dataset '%s'...", dataset_id)

        band_properties = validate_manifest_and_bands(
            s3_client,
            S3_BUCKET_NAME,
            dataset_id,
            user_tasks,
            UDF_METADATA_TABLE,
            client_config
        )

        # 🔧 FIX: Save state using the SAME job_id from FastAPI
        save_initial_state(dynamodb_client, DYNAMODB_TABLE_NAME, job_uuid, dataset_id, workflow_data)

        # Update job to VALIDATED
        update_job_status(dynamodb_client, DYNAMODB_TABLE_NAME, job_uuid, "VALIDATED", {
            "dataset_id": dataset_id,
            "required_bands": list(band_properties.keys())
        })

        # Start Step Functions execution and update status
        try:
            resp = start_step_function_execution(sfn_client, STATE_MACHINE_ARN, job_uuid, workflow_data)
            execution_arn = resp.get('executionArn')
            update_job_status(dynamodb_client, DYNAMODB_TABLE_NAME, job_uuid, "EXECUTION_STARTED", {
                "executionArn": execution_arn
            })

            # Optionally mark RUNNING (Step Functions has its own execution status)
            update_job_status(dynamodb_client, DYNAMODB_TABLE_NAME, job_uuid, "RUNNING", {"executionArn": execution_arn})

            response_body = {
                "message": "Workflow accepted, validated, and execution started.",
                "job_id": job_uuid,
                "executionArn": execution_arn,
                "dataset_id": dataset_id,
                "status": "ACCEPTED"
            }

            logger.info("Successfully accepted workflow for dataset '%s', job_id: %s", dataset_id, job_uuid)
            return {
                "statusCode": 200,
                "body": json.dumps(response_body),
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                }
            }

        except (ClientError, BotoCoreError) as e:
            logger.exception("Failed to start Step Functions execution: %s", e)
            update_job_status(dynamodb_client, DYNAMODB_TABLE_NAME, job_uuid, "EXECUTION_ERROR", {"error": str(e)})
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Failed to start Step Functions execution", "details": str(e)}),
                "headers": {"Content-Type": "application/json"}
            }

    except ValueError as e:
        logger.warning("Input validation error: %s", e)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Input validation error: {str(e)}"}),
            "headers": {"Content-Type": "application/json"}
        }

    except EnvironmentError as e:
        logger.error("Configuration error: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Service configuration error", "details": str(e)}),
            "headers": {"Content-Type": "application/json"}
        }

    except ClientError as e:
        logger.exception("AWS client error: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "AWS service error"}),
            "headers": {"Content-Type": "application/json"}
        }

    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "An internal server error occurred", "details": str(e)}),
            "headers": {"Content-Type": "application/json"}
        }