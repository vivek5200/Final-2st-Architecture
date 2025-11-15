import os
import json
import boto3
import math
import re
from datetime import datetime

def get_required_bands_from_tasks(user_tasks):
    """Extracts all unique band names required by the user's tasks."""
    required_bands = set()
    if not user_tasks:
        return []

    # Handle multiple tasks
    for task in user_tasks:
        formula = task.get('parameters', {}).get('formula', '')

        # Find all words that are likely band names
        bands_in_formula = set(re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', formula))

        # Remove common mathematical operators and functions
        math_keywords = {
            'abs', 'sqrt', 'log', 'exp', 'sin', 'cos', 'tan',
            'min', 'max', 'mean', 'sum', 'pi', 'e'
        }
        bands_in_formula = bands_in_formula - math_keywords

        required_bands.update(bands_in_formula)

    return list(required_bands)

# --- DynamoDB helpers ---
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
AWS_REGION = os.getenv("AWS_REGION")

_dynamo_client = None
def get_dynamo_client():
    global _dynamo_client
    if _dynamo_client is None:
        if AWS_REGION:
            _dynamo_client = boto3.client("dynamodb", region_name=AWS_REGION)
        else:
            _dynamo_client = boto3.client("dynamodb")
    return _dynamo_client

def put_job_item(job_id: str, status: str, extras: dict = None):
    """
    Create or update a job item in DynamoDB. Resilient: logs errors and continues.
    Stores: job_id (PK), status, last_updated, and extras as a JSON string in 'details'.
    Special-case numeric fields (total_tiles, processed_tiles, tiles_x, tiles_y) are stored
    as top-level Number attributes so they are easy to query/update.
    """
    if not DYNAMODB_TABLE_NAME:
        print("⚠️ DYNAMODB_TABLE_NAME not configured; skipping DynamoDB put_item.")
        return

    if not job_id:
        print("⚠️ No job_id provided; skipping DynamoDB put_item.")
        return

    dynamo = get_dynamo_client()
    timestamp = datetime.utcnow().isoformat()

    item = {
        "job_id": {"S": str(job_id)},
        "status": {"S": status},
        "last_updated": {"S": timestamp}
    }

    # If extras contains some numeric fields that we want as top-level attrs, move them
    numeric_keys = ["total_tiles", "processed_tiles", "tiles_x", "tiles_y"]
    extras_copy = dict(extras) if extras else {}

    for k in numeric_keys:
        if k in extras_copy and extras_copy[k] is not None:
            try:
                val = extras_copy.pop(k)
                # store as DynamoDB Number (string representation)
                item[k] = {"N": str(int(val))}
            except Exception as e:
                print(f"⚠️ Failed to convert numeric extras key {k} to int: {e}")

    # Convert remaining extras to a JSON string and store in 'details'
    if extras_copy:
        try:
            item["details"] = {"S": json.dumps(extras_copy, default=str)}
        except Exception as e:
            print(f"⚠️ Failed to encode extras to JSON for DynamoDB: {e}")

    try:
        dynamo.put_item(TableName=DYNAMODB_TABLE_NAME, Item=item)
        print(f"✅ DynamoDB job item put for job_id={job_id} status={status}")
    except Exception as e:
        print(f"⚠️ Failed to put job item into DynamoDB for job_id={job_id}: {e}")

# --- Main translator handler ---
def handler(event, context):
    """
    Production-ready translator Lambda function.
    Translates high-level DAG into parallelizable tile-based tasks and writes a job record to DynamoDB.
    """
    print(f"Translator invoked with event: {json.dumps(event)}")

    # Configuration from environment variables
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "insat-cog-processed")
    AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")

    # Extract job details from the input payload
    dataset_id = event.get("dataset_id")
    user_tasks = event.get("tasks", [])
    job_id = event.get("job_id")

    if not all([dataset_id, user_tasks, job_id]):
        error_msg = "Missing required fields: dataset_id, tasks, or job_id"
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)

    # Initialize S3 client
    try:
        session = boto3.Session()
        s3_config = {}
        if AWS_ENDPOINT_URL:
            s3_config['endpoint_url'] = AWS_ENDPOINT_URL
        s3_client = session.client("s3", **s3_config)
    except Exception as e:
        print(f"❌ Failed to initialize S3 client: {e}")
        raise

    # --- Step 1: Read and parse the manifest.json from S3 ---
    manifest_key = f"{dataset_id}/manifest.json"
    print(f"📁 Reading metadata from: s3://{S3_BUCKET_NAME}/{manifest_key}")

    try:
        s3_object = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=manifest_key)
        manifest_content = s3_object['Body'].read().decode('utf-8')
        manifest_data = json.loads(manifest_content)
        print(f"✅ Successfully parsed manifest with {len(manifest_data.get('files', []))} files")

    except Exception as e:
        print(f"❌ Could not read manifest file: {e}")
        # Create/Update job item with ERROR so downstream can see failure
        put_job_item(job_id, "TRANSLATOR_ERROR", {"error": str(e), "step": "read_manifest"})
        raise

    # --- Step 2: Find highest resolution band for reference ---
    band_info_map = {}
    highest_res_band_info = None
    max_pixels = 0

    # Build band info map and find highest resolution
    for file_info in manifest_data.get('files', []):
        if file_info.get('subdataset'):
            band_name = file_info['subdataset'].get('name')
            band_info_map[band_name] = file_info

            dims = file_info.get('subdataset', {}).get('dimensions', {})
            width = dims.get('width', 0)
            height = dims.get('height', 0)
            current_pixels = width * height

            if current_pixels > max_pixels:
                max_pixels = current_pixels
                highest_res_band_info = file_info

    if not highest_res_band_info:
        error_msg = "Could not determine highest resolution band from manifest"
        print(f"❌ {error_msg}")
        put_job_item(job_id, "TRANSLATOR_ERROR", {"error": error_msg, "step": "find_reference"})
        raise ValueError(error_msg)

    reference_file_info = highest_res_band_info
    ref_band_name = reference_file_info.get('subdataset', {}).get('name')

    # Extract reference geotransform
    reference_geotransform = reference_file_info.get('subdataset', {}).get('geospatial', {}).get('transform')
    if not reference_geotransform:
        error_msg = f"Reference band '{ref_band_name}' missing geospatial transform"
        print(f"❌ {error_msg}")
        put_job_item(job_id, "TRANSLATOR_ERROR", {"error": error_msg, "step": "reference_transform"})
        raise ValueError(error_msg)

    print(f"🎯 Using '{ref_band_name}' as reference band")
    print(f"📐 Reference transform: {reference_geotransform}")

    # --- Step 3: Extract dimensions ---
    subdataset_info = reference_file_info.get('subdataset', {})
    dims = subdataset_info.get('dimensions', {})
    tile_info = subdataset_info.get('tileInfo', {})

    width = dims.get('width')
    height = dims.get('height')
    tile_width = tile_info.get('tileWidth', 512)
    tile_height = tile_info.get('tileHeight', 512)

    if not width or not height:
        error_msg = f"Could not determine dimensions from reference band"
        print(f"❌ {error_msg}")
        put_job_item(job_id, "TRANSLATOR_ERROR", {"error": error_msg, "step": "dimensions"})
        raise ValueError(error_msg)

    print(f"📏 Grid: {width}x{height} pixels, Tile: {tile_width}x{tile_height}")

    # --- Step 4: Calculate grid and generate tile tasks ---
    tiles_x = math.ceil(width / tile_width)
    tiles_y = math.ceil(height / tile_height)
    print(f"🔲 Tile grid: {tiles_x}x{tiles_y}")

    # --- Step 5: Prepare band metadata ---
    required_bands = get_required_bands_from_tasks(user_tasks)
    print(f"📊 Required bands: {required_bands}")

    required_band_metadata = {}
    for band_name in required_bands:
        if band_name in band_info_map:
            band_info = band_info_map[band_name].copy()

            # Flexible path construction
            correct_s3_key = f"{dataset_id}/Image_Band/{band_name}.tif"
            band_info['outputPath'] = correct_s3_key

            required_band_metadata[band_name] = band_info
            print(f"✅ Found band '{band_name}' at: {correct_s3_key}")
        else:
            print(f"⚠️ Band '{band_name}' not found in manifest")

    # Generate tile tasks
    tile_tasks = []
    for y in range(tiles_y):
        for x in range(tiles_x):
            tile_tasks.append({
                "dataset_id": dataset_id,
                "job_id": job_id,
                "tile_x_index": x,
                "tile_y_index": y,
                "tile_width": tile_width,
                "tile_height": tile_height,
                "reference_grid_dimensions": {
                    "width": width,
                    "height": height
                },
                "reference_geotransform": reference_geotransform,
                "reference_band_name": ref_band_name,
                "required_band_metadata": required_band_metadata,
                "user_tasks": user_tasks  # Include all tasks, not just first
            })

    print(f"🎯 Generated {len(tile_tasks)} tile tasks")

    # --- Step 6: Save to S3 (Claim Check Pattern) ---
    tasks_key = f"jobs/{job_id}/tile_tasks.json"

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=tasks_key,
            Body=json.dumps(tile_tasks),
            ContentType='application/json'
        )
        print(f"💾 Stored tile tasks in S3: s3://{S3_BUCKET_NAME}/{tasks_key}")
    except Exception as e:
        print(f"❌ Failed to store tile tasks: {e}")
        put_job_item(job_id, "TRANSLATOR_ERROR", {"error": str(e), "step": "store_tasks"})
        raise

    # --- Step 7: Write initial job record to DynamoDB (TRANSLATOR_DONE) ---
    job_details = {
        "dataset_id": dataset_id,
        "total_tiles": len(tile_tasks),
        # set processed_tiles = 0 at job creation
        "processed_tiles": 0,
        "tiles_x": tiles_x,
        "tiles_y": tiles_y,
        "reference_band": ref_band_name,
        "reference_transform_included": True,
        "tile_tasks_s3_location": {"bucket": S3_BUCKET_NAME, "key": tasks_key},
        "required_bands": required_bands,
        "tasks_preview": user_tasks  # optional, might be large
    }

    try:
        put_job_item(job_id, "TRANSLATOR_DONE", job_details)
    except Exception as e:
        # put_job_item is resilient and already logs; still catch to be safe
        print(f"⚠️ Exception while writing job item: {e}")

    # Return claim check
    claim_check = {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "tile_tasks_s3_location": {
            "bucket": S3_BUCKET_NAME,
            "key": tasks_key
        },
        "total_tiles": len(tile_tasks),
        "tiles_x": tiles_x,
        "tiles_y": tiles_y,
        "reference_band": ref_band_name,
        "reference_transform_included": True
    }

    print(f"✅ Translation complete. Returning claim check.")
    return claim_check
