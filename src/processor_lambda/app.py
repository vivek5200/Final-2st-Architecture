# processor_with_dynamo_updates.py
# Updated: Only difference from your original is added atomic processed_tiles increment
# and conditional finalization logic to prevent marking job COMPLETED per-tile.

import os
import json
import boto3
import re
import numpy as np
import rasterio
from rasterio.merge import merge as rasterio_merge
import numexpr
from rasterio.windows import Window
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
import tempfile
from typing import Dict, List, Any, Optional
import threading
import time
from functools import wraps
import hashlib
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from dataclasses import dataclass
import pickle
from datetime import datetime

# --- Configuration from Environment Variables ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "insat-cog-processed")
RESULT_BUCKET_NAME = os.getenv("RESULT_BUCKET_NAME", "insat-processed-results")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")  # New: DynamoDB table name
MAX_CACHE_SIZE = int(os.getenv("MAX_CACHE_SIZE", "200"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))
BAND_RETRY_COUNT = int(os.getenv("BAND_RETRY_COUNT", "2"))
BAND_RETRY_DELAY = int(os.getenv("BAND_RETRY_DELAY", "1"))
MAX_WORKERS = min(int(os.getenv("MAX_WORKERS", "10")), (os.cpu_count() or 1))
MAX_ZERO_PERCENT = float(os.getenv("MAX_ZERO_PERCENT", "95.0"))
MIN_DATA_RANGE = float(os.getenv("MIN_DATA_RANGE", "10.0"))
MIN_VALID_PIXELS = int(os.getenv("MIN_VALID_PIXELS", "1000"))

# --- Memcached Integration ---
MEMCACHED_AVAILABLE = False  # Disable for initial deployment
MEMCACHED_ENDPOINT = os.environ.get("MEMCACHED_ENDPOINT")
MEMCACHED_CLIENT = None
S3_CLIENT = None
DYNAMO_CLIENT = None

@dataclass
class ProcessingContext:
    """Context object to avoid global state and reduce lock contention"""
    s3_client: Any = None
    memcached_client: Any = None

    def __post_init__(self):
        if self.s3_client is None:
            self.s3_client = get_s3_client()
        if self.memcached_client is None and MEMCACHED_AVAILABLE:
            self.memcached_client = get_memcached_client()

def get_processing_context():
    """Get thread-local processing context"""
    thread_local = threading.local()
    if not hasattr(thread_local, 'context'):
        thread_local.context = ProcessingContext()
    return thread_local.context

def get_memcached_client():
    """Initialize and return Memcached client - disabled for initial deployment"""
    return None

def get_s3_client():
    """Get optimized shared S3 client"""
    global S3_CLIENT
    if S3_CLIENT is None:
        # boto3.session.Config used to set connection pool & retries
        config = boto3.session.Config(
            max_pool_connections=50,
            retries={'max_attempts': 3, 'mode': 'standard'}
        )
        S3_CLIENT = boto3.client('s3', config=config, region_name=AWS_REGION)
    return S3_CLIENT

def get_dynamo_client():
    """Get shared DynamoDB client"""
    global DYNAMO_CLIENT
    if DYNAMO_CLIENT is None:
        DYNAMO_CLIENT = boto3.client('dynamodb', region_name=AWS_REGION)
    return DYNAMO_CLIENT

# --- DynamoDB update helper (resilient) ---
def update_job_status(job_id: str, status: str, extra: Optional[Dict[str, Any]] = None):
    """
    Update DynamoDB table item for job_id with status, last_updated and optional details.
    Resilient: logs failures but doesn't raise to avoid interrupting processing.
    """
    if not DYNAMODB_TABLE_NAME:
        # If table name not configured, skip silently but log
        print("⚠️ DYNAMODB_TABLE_NAME not set; skipping job status updates.")
        return

    if not job_id:
        print("⚠️ Missing job_id; skipping DynamoDB update.")
        return

    dynamo = get_dynamo_client()
    timestamp = datetime.utcnow().isoformat()

    # Prepare expression and attribute values
    update_expr = "SET #s = :s, last_updated = :t"
    expr_attr_names = {"#s": "status"}
    expr_attr_values = {
        ":s": {"S": status},
        ":t": {"S": timestamp}
    }

    # If extra details provided, store them in a map called 'details' as JSON string
    if extra:
        update_expr += ", details = :d"
        expr_attr_values[":d"] = {"S": json.dumps(extra, default=str)}

    try:
        dynamo.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key={"job_id": {"S": job_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values
        )
        print(f"✅ DynamoDB updated for job_id={job_id} with status={status}")
    except Exception as e:
        # Log but don't raise — we don't want status update failures to stop processing
        print(f"⚠️ Failed to update DynamoDB for job_id={job_id}: {e}")

# --- NEW: Atomically mark a tile processed and check for job completion ---
def mark_tile_processed_and_maybe_complete(job_id: str, tile_info: str, output_path: str, operation: str):
    """
    Atomically increments processed_tiles counter in DynamoDB and sets status to TILE_PROCESSED.
    After increment, it reads total_tiles and processed_tiles; if processed_tiles >= total_tiles
    it sets the job status to COMPLETED (and includes a summary).
    This avoids marking the job COMPLETED per tile when multiple processor lambdas run concurrently.
    """
    if not DYNAMODB_TABLE_NAME:
        print("⚠️ DYNAMODB_TABLE_NAME not set; skipping processed tiles accounting.")
        return

    dynamo = get_dynamo_client()
    timestamp = datetime.utcnow().isoformat()

    try:
        # Atomically add 1 to processed_tiles and set status to TILE_PROCESSED and last_updated
        resp = dynamo.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="SET #s = :s, last_updated = :t ADD processed_tiles :inc",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": {"S": "TILE_PROCESSED"},
                ":t": {"S": timestamp},
                ":inc": {"N": "1"}
            },
            ReturnValues="UPDATED_NEW"
        )
        # Read processed_tiles from the update response (if present)
        processed_tiles = 0
        if 'Attributes' in resp and 'processed_tiles' in resp['Attributes']:
            processed_tiles = int(resp['Attributes']['processed_tiles']['N'])
        print(f"🔢 job_id={job_id} processed_tiles incremented -> {processed_tiles}")

    except Exception as e:
        print(f"⚠️ Failed to increment processed_tiles for job_id={job_id}: {e}")
        # Fallback: just set per-tile record without increment
        try:
            update_job_status(job_id, "TILE_PROCESSED", {"tile": tile_info, "output": output_path, "operation": operation})
        except Exception as ee:
            print(f"⚠️ Fallback per-tile update failed for job {job_id}: {ee}")
        return

    # Now fetch total_tiles (and optionally other summary info) to decide completion
    try:
        job_item = dynamo.get_item(TableName=DYNAMODB_TABLE_NAME, Key={"job_id": {"S": job_id}}, ConsistentRead=True)
        if 'Item' in job_item and 'total_tiles' in job_item['Item']:
            total_tiles = int(job_item['Item']['total_tiles']['N'])
        else:
            total_tiles = None
    except Exception as e:
        print(f"⚠️ Failed to read total_tiles for job_id={job_id}: {e}")
        total_tiles = None

    # If total_tiles is known and we've processed enough tiles, finalize job
    if total_tiles is not None:
        if processed_tiles >= total_tiles:
            print(f"🏁 All tiles processed for job {job_id} ({processed_tiles}/{total_tiles}). Finalizing job.")
            try:
                # Add summary details when marking completed
                extra = {
                    "processed_tiles": processed_tiles,
                    "total_tiles": total_tiles,
                    "last_tile": tile_info,
                    "last_output": output_path,
                    "last_operation": operation
                }
                update_job_status(job_id, "COMPLETED", extra)
            except Exception as e:
                print(f"⚠️ Failed to set job COMPLETED for job_id={job_id}: {e}")
        else:
            print(f"ℹ️ job_id={job_id} progress: {processed_tiles}/{total_tiles} tiles processed")
    else:
        # total_tiles unknown — don't finalize automatically; rely on batch/controller to finalize
        print(f"ℹ️ total_tiles unknown for job_id={job_id}. Processed count now {processed_tiles}.")

# --- Optimized Cache Implementation ---
class LockFreeCache:
    """Thread-safe cache without lock contention using copy-on-write"""
    def __init__(self, max_size=200, ttl=300):
        self._cache = {}
        self._timestamps = {}
        self.max_size = max_size
        self.ttl = ttl
        self._lock = threading.RLock()
        self._last_cleanup = time.time()

    def get(self, key):
        current_time = time.time()

        # Periodic cleanup without locking every read
        if current_time - self._last_cleanup > 60:
            self._cleanup_expired()

        with self._lock:
            if key in self._cache:
                if current_time - self._timestamps[key] < self.ttl:
                    result = self._cache[key]
                    return result.copy() if hasattr(result, 'copy') else result
                else:
                    del self._cache[key]
                    del self._timestamps[key]
            return None

    def set(self, key, value):
        with self._lock:
            if len(self._cache) >= self.max_size:
                self._evict_oldest()

            stored_value = value.copy() if hasattr(value, 'copy') else value
            self._cache[key] = stored_value
            self._timestamps[key] = time.time()

    def _cleanup_expired(self):
        current_time = time.time()
        expired_keys = [
            k for k, ts in self._timestamps.items()
            if current_time - ts >= self.ttl
        ]
        for key in expired_keys:
            del self._cache[key]
            del self._timestamps[key]
        self._last_cleanup = current_time

    def _evict_oldest(self):
        if not self._timestamps:
            return
        oldest_key = min(self._timestamps.keys(), key=lambda k: self._timestamps[k])
        del self._cache[oldest_key]
        del self._timestamps[oldest_key]

    def __len__(self):
        with self._lock:
            return len(self._cache)

# Initialize optimized caches
FALLBACK_TILE_CACHE = LockFreeCache(max_size=MAX_CACHE_SIZE, ttl=CACHE_TTL)

def cache_get_optimized(key: str) -> Any:
    """OPTIMIZED: Cache get with pickle serialization"""
    return FALLBACK_TILE_CACHE.get(key)

def cache_set_optimized(key: str, value: Any, expire: int = CACHE_TTL):
    """OPTIMIZED: Cache set with pickle serialization"""
    FALLBACK_TILE_CACHE.set(key, value)

# --- Enhanced Filename Generation ---
def generate_meaningful_filename(job_id: str, operation: str, parameters: dict,
                               tile_count: int, quality_status: str = "HIGH_QUALITY") -> str:
    """
    Generate meaningful, descriptive file names for final outputs
    """
    operation_names = {
        "ndvi": "NDVI",
        "ndwi": "NDWI",
        "band_math": "CustomIndex",
        "evi": "EVI",
        "savi": "SAVI"
    }

    current_time = datetime.utcnow()
    date_str = current_time.strftime("%Y%m%d")
    time_str = current_time.strftime("%H%M%S")

    base_name = operation_names.get(operation, operation.upper())

    band_info = ""
    if operation == "ndvi":
        band_info = f"_NIR{parameters.get('nir_band', '')}-RED{parameters.get('red_band', '')}"
    elif operation == "ndwi":
        band_info = f"_GREEN{parameters.get('green_band', '')}-NIR{parameters.get('nir_band', '')}"
    elif operation == "band_math":
        formula = parameters.get('formula', '')
        bands = set(re.findall(r'\bB[0-9A-Z_]+\b', formula))
        if bands:
            band_info = f"_{'_'.join(sorted(bands))}"

    quality_indicator = "HighQuality" if quality_status == "HIGH_QUALITY" else "Standard"
    tile_info = f"_Tiles{tile_count}"
    satellite_source = "INSAT3D"

    filename = f"{base_name}_{satellite_source}_{date_str}_{time_str}{band_info}{tile_info}_{quality_indicator}.tif"
    filename = re.sub(r'_+', '_', filename)

    print(f"📝 Generated filename: {filename}")
    return filename

# --- Enhanced Upload Function for Result Bucket ---
def upload_final_mosaic_to_result_bucket(mosaic_path: str, filename: str, metadata: dict) -> str:
    """
    Upload final mosaic to the result bucket with meaningful naming
    """
    context = get_processing_context()

    operation = metadata.get("operation", "unknown")
    date_str = datetime.utcnow().strftime("%Y/%m/%d")
    s3_key = f"processed_results/{date_str}/{operation}/{filename}"

    try:
        # Check if result bucket exists
        try:
            context.s3_client.head_bucket(Bucket=RESULT_BUCKET_NAME)
        except ClientError:
            print(f"📦 Creating result bucket: {RESULT_BUCKET_NAME}")
            # Create bucket with location constraint
            if AWS_REGION == 'us-east-1':
                context.s3_client.create_bucket(Bucket=RESULT_BUCKET_NAME)
            else:
                context.s3_client.create_bucket(
                    Bucket=RESULT_BUCKET_NAME,
                    CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                )

        # Upload with metadata
        extra_args = {
            'ContentType': 'image/tiff',
            'Metadata': {
                'processing_job_id': metadata.get('job_id', 'unknown'),
                'operation': operation,
                'processing_date': datetime.utcnow().isoformat(),
                'tiles_merged': str(metadata.get('tiles_merged', 0)),
                'total_tiles': str(metadata.get('total_tiles', 0)),
                'quality_status': metadata.get('quality_status', 'HIGH_QUALITY'),
                'coverage_percentage': f"{metadata.get('coverage_percentage', 0):.1f}%"
            }
        }

        print(f"📤 Uploading final mosaic to result bucket: {s3_key}")
        context.s3_client.upload_file(
            mosaic_path,
            RESULT_BUCKET_NAME,
            s3_key,
            ExtraArgs=extra_args
        )

        final_url = f"s3://{RESULT_BUCKET_NAME}/{s3_key}"
        print(f"✅ Final mosaic uploaded to: {final_url}")
        return final_url

    except Exception as e:
        print(f"❌ Failed to upload to result bucket: {e}")
        raise

# --- Enhanced Tile Cleanup Function ---
def cleanup_tiles_after_merge(job_id: str):
    """
    Remove all individual tile files after successful mosaic creation
    """
    context = get_processing_context()

    try:
        source_prefix = f"outputs/{job_id}/"
        print(f"🗑️ Cleaning up individual tiles from source bucket: {source_prefix}")

        objects_to_delete = []
        paginator = context.s3_client.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=source_prefix):
            if 'Contents' in page:
                objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])

        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i + 1000]
                context.s3_client.delete_objects(
                    Bucket=S3_BUCKET_NAME,
                    Delete={'Objects': batch}
                )

            print(f"✅ Deleted {len(objects_to_delete)} tile files from source bucket")
        else:
            print("ℹ️ No tile files found to delete in source bucket")

    except Exception as e:
        print(f"⚠️ Error during tile cleanup: {e}")

# --- Core Processing Functions ---
def get_cache_key(band_name: str, window: Window, ref_width: int, ref_height: int,
                 orig_width: int, orig_height: int) -> str:
    """Generate cache key"""
    key_data = (f"{band_name}_{window.col_off}_{window.row_off}_{window.width}_"
                f"{window.height}_{ref_width}_{ref_height}_{orig_width}_{orig_height}")
    return hashlib.md5(key_data.encode()).hexdigest()

def check_s3_object_exists(bucket: str, key: str) -> bool:
    """Check if S3 object exists"""
    context = get_processing_context()
    try:
        context.s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
            return False
        else:
            raise RuntimeError(f"S3 error checking existence for {key}: {str(e)}")

def validate_band_data_quick(band_data: np.ndarray, band_name: str) -> bool:
    """Ultra-fast band validation for input quality checking"""
    finite_mask = np.isfinite(band_data)
    finite_count = np.sum(finite_mask)

    if finite_count == 0:
        return False

    valid_data = band_data[finite_mask]
    zero_count = np.sum(valid_data == 0)
    zero_percent = (zero_count / band_data.size) * 100

    if zero_percent > 95:
        return False

    data_range = np.max(valid_data) - np.min(valid_data)
    if data_range < 1.0:
        return False

    return True

def create_output_profile(tile_width: int, tile_height: int, full_geotransform: list, window: Window) -> dict:
    """Create optimized output profile with correct transform for the tile."""
    transform_obj = rasterio.Affine.from_gdal(*full_geotransform)
    window_transform = rasterio.windows.transform(window, transform_obj)

    return {
        'driver': 'GTiff',
        'dtype': 'float32',
        'nodata': -9999,
        'width': tile_width,
        'height': tile_height,
        'count': 1,
        'crs': 'EPSG:4326',
        'transform': window_transform,
        'compress': 'lzw',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
    }

# --- STRATEGIC DATA VALIDATION ---
def validate_image_data_optimized(data: np.ndarray, operation: str, tile_info: str = "") -> Dict[str, Any]:
    """OPTIMIZED: Fast validation focused on key quality metrics"""
    stats = {
        'shape': data.shape,
        'operation': operation,
        'tile_info': tile_info
    }

    total_pixels = data.size
    finite_mask = np.isfinite(data)
    finite_count = np.sum(finite_mask)
    stats['finite_pixels'] = finite_count
    stats['finite_percent'] = (finite_count / total_pixels) * 100 if total_pixels > 0 else 0

    zero_count = np.sum(data[finite_mask] == 0) if finite_count > 0 else 0
    stats['zero_percent'] = (zero_count / total_pixels) * 100 if total_pixels > 0 else 0

    if finite_count > 0:
        valid_data = data[finite_mask]
        stats['min_value'] = float(np.min(valid_data))
        stats['max_value'] = float(np.max(valid_data))
        stats['mean_value'] = float(np.mean(valid_data))

        data_range = stats['max_value'] - stats['min_value']
        stats['data_range'] = data_range
        stats['has_variation'] = data_range > MIN_DATA_RANGE

        non_zero_pixels = finite_count - zero_count
        stats['non_zero_pixels'] = non_zero_pixels
        stats['has_sufficient_data'] = non_zero_pixels >= MIN_VALID_PIXELS
    else:
        stats['min_value'] = None
        stats['max_value'] = None
        stats['mean_value'] = None
        stats['data_range'] = 0
        stats['has_variation'] = False
        stats['non_zero_pixels'] = 0
        stats['has_sufficient_data'] = False

    stats['is_worth_processing'] = (
        stats['zero_percent'] < MAX_ZERO_PERCENT and
        stats['has_sufficient_data'] and
        stats['has_variation']
    )

    quality_status = "✅ GOOD" if stats['is_worth_processing'] else "🚫 POOR"
    print(f"🔍 {operation} validation: {quality_status} - "
          f"{stats['finite_percent']:.1f}% valid, {stats['zero_percent']:.1f}% zeros, "
          f"range: {stats['data_range']:.2f}")

    return stats

# --- OPTIMIZED BAND STREAMING ---
class OptimizedBandStreamer:
    """Completely redesigned band streamer to eliminate lock contention"""

    def __init__(self):
        self._file_cache = LockFreeCache(max_size=10, ttl=300)
        self._download_lock = threading.Lock()

    def _download_file(self, s3_key: str) -> str:
        """Download file with proper locking only for the download operation"""
        local_path = os.path.join(
            tempfile.gettempdir(),
            f"cog_{hashlib.md5(s3_key.encode()).hexdigest()}.tif"
        )

        if os.path.exists(local_path):
            file_age = time.time() - os.path.getctime(local_path)
            if file_age < 300:
                return local_path

        with self._download_lock:
            if os.path.exists(local_path):
                return local_path

            try:
                context = get_processing_context()
                context.s3_client.download_file(S3_BUCKET_NAME, s3_key, local_path)
                return local_path
            except Exception as e:
                print(f"❌ Failed to download {s3_key}: {e}")
                raise

    def stream_band_tile_optimized(self, band_name: str, band_metadata: dict, window: Window,
                                 ref_width: int, ref_height: int,
                                 tile_width: int, tile_height: int) -> np.ndarray:
        """OPTIMIZED: Stream band tile with minimal locking"""
        s3_key = band_metadata["outputPath"]

        if not check_s3_object_exists(S3_BUCKET_NAME, s3_key):
            print(f"Band {band_name} not found at {s3_key}, using fallback array")
            return self._create_fallback_array(tile_height, tile_width)

        subdataset = band_metadata["subdataset"]
        band_dims = subdataset["dimensions"]
        orig_width = band_dims["width"]
        orig_height = band_dims["height"]

        scale_x = orig_width / ref_width
        scale_y = orig_height / ref_height

        orig_window = Window(
            col_off=int(window.col_off * scale_x),
            row_off=int(window.row_off * scale_y),
            width=max(1, int(window.width * scale_x)),
            height=max(1, int(window.height * scale_y))
        )

        orig_window = Window(
            col_off=max(0, min(orig_window.col_off, orig_width - 1)),
            row_off=max(0, min(orig_window.row_off, orig_height - 1)),
            width=min(orig_window.width, orig_width - orig_window.col_off),
            height=min(orig_window.height, orig_height - orig_window.row_off)
        )

        cache_key = get_cache_key(band_name, window, ref_width, ref_height, orig_width, orig_height)

        cached_tile = cache_get_optimized(cache_key)
        if cached_tile is not None:
            print(f"🎯 Cache hit for {band_name} tile")
            return cached_tile

        try:
            local_path = self._download_file(s3_key)

            with rasterio.open(local_path) as src:
                data = src.read(
                    1,
                    window=orig_window,
                    out_shape=(tile_height, tile_width),
                    resampling=Resampling.bilinear
                ).astype(np.float32)

            if not validate_band_data_quick(data, band_name):
                print(f"⚠️ Band {band_name} has poor data quality, using fallback")
                fallback_array = self._create_fallback_array(tile_height, tile_width)
                cache_set_optimized(cache_key, fallback_array)
                return fallback_array

            cache_set_optimized(cache_key, data)
            print(f"✅ Processed {band_name} tile: {data.shape}")
            return data

        except Exception as e:
            print(f"❌ Error processing band {band_name}: {str(e)}")
            fallback_array = self._create_fallback_array(tile_height, tile_width)
            cache_set_optimized(cache_key, fallback_array)
            return fallback_array

    def _create_fallback_array(self, height: int, width: int, fill_value: float = 0.0) -> np.ndarray:
        return np.full((height, width), fill_value, dtype=np.float32)

# Global optimized band streamer
optimized_band_streamer = OptimizedBandStreamer()

def robust_stream_band_with_retry(band_name: str, band_metadata: dict, window: Window,
                                ref_width: int, ref_height: int,
                                tile_width: int, tile_height: int,
                                use_cache: bool = True) -> np.ndarray:
    """Optimized band streaming with retry logic"""
    cache_key = get_cache_key(band_name, window, ref_width, ref_height,
                            band_metadata["subdataset"]["dimensions"]["width"],
                            band_metadata["subdataset"]["dimensions"]["height"])

    if use_cache:
        cached_result = cache_get_optimized(cache_key)
        if cached_result is not None:
            return cached_result

    last_exception = None
    for attempt in range(BAND_RETRY_COUNT + 1):
        try:
            result = optimized_band_streamer.stream_band_tile_optimized(
                band_name, band_metadata, window, ref_width, ref_height,
                tile_width, tile_height
            )
            return result.copy()
        except Exception as e:
            last_exception = e
            if attempt < BAND_RETRY_COUNT:
                print(f"⚠️ Attempt {attempt + 1} failed for {band_name}, retrying...")
                time.sleep(BAND_RETRY_DELAY * (attempt + 1))

    print(f"🔄 Using fallback for {band_name} after {BAND_RETRY_COUNT} failed attempts")
    fallback_array = optimized_band_streamer._create_fallback_array(tile_height, tile_width)
    if use_cache:
        cache_set_optimized(cache_key, fallback_array)
    return fallback_array.copy()

# --- OPTIMIZED PARALLEL PROCESSING ---
def process_single_band_optimized(args) -> tuple:
    """Optimized worker function with reduced state sharing"""
    band_name, band_metadata, window, ref_width, ref_height, tile_width, tile_height = args
    try:
        band_data = robust_stream_band_with_retry(
            band_name, band_metadata, window, ref_width, ref_height,
            tile_width, tile_height, use_cache=True
        )
        return band_name, band_data, None
    except Exception as e:
        return band_name, None, str(e)

def process_bands_parallel_optimized(band_metadata_map: dict, window: Window,
                                   ref_width: int, ref_height: int,
                                   tile_width: int, tile_height: int,
                                   max_workers: int = MAX_WORKERS) -> dict:
    """OPTIMIZED: Process bands in parallel with reduced lock contention"""
    band_arrays = {}

    band_args = [
        (band_name, metadata, window, ref_width, ref_height, tile_width, tile_height)
        for band_name, metadata in band_metadata_map.items()
    ]

    print(f"🔄 Processing {len(band_args)} bands in parallel with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_band = {
            executor.submit(process_single_band_optimized, args): args[0]
            for args in band_args
        }

        for future in as_completed(future_to_band):
            band_name = future_to_band[future]
            try:
                band_name, band_data, error = future.result()
                if error:
                    print(f"❌ Failed to process band {band_name}: {error}")
                    band_arrays[band_name] = optimized_band_streamer._create_fallback_array(tile_height, tile_width)
                else:
                    band_arrays[band_name] = band_data
            except Exception as e:
                print(f"❌ Unexpected error processing band {band_name}: {e}")
                band_arrays[band_name] = optimized_band_streamer._create_fallback_array(tile_height, tile_width)

    return band_arrays

# --- OPTIMIZED UDF Implementations ---
def apply_band_math(band_arrays: dict, formula: str) -> np.ndarray:
    """Optimized band math"""
    print(f"🔢 Applying band math: {formula}")

    try:
        with np.errstate(divide='ignore', invalid='ignore'):
            result = numexpr.evaluate(formula, local_dict=band_arrays)
            result = np.where(np.isfinite(result), result, 0.0)

        return result.astype(np.float32)

    except Exception as e:
        print(f"❌ Band math failed: {e}")
        first_band = next(iter(band_arrays.values()))
        return np.zeros_like(first_band, dtype=np.float32)

def apply_ndvi(band_arrays: dict, nir_band: str, red_band: str) -> np.ndarray:
    """Optimized NDVI calculation"""
    nir = band_arrays[nir_band]
    red = band_arrays[red_band]

    denominator = nir + red
    valid_mask = (denominator != 0) & np.isfinite(nir) & np.isfinite(red)

    result = np.zeros_like(nir, dtype=np.float32)
    result[valid_mask] = (nir[valid_mask] - red[valid_mask]) / denominator[valid_mask]

    return result

def apply_ndwi(band_arrays: dict, green_band: str, nir_band: str) -> np.ndarray:
    """Optimized NDWI calculation"""
    green = band_arrays[green_band]
    nir = band_arrays[nir_band]

    denominator = green + nir
    valid_mask = (denominator != 0) & np.isfinite(green) & np.isfinite(nir)

    result = np.zeros_like(green, dtype=np.float32)
    result[valid_mask] = (green[valid_mask] - nir[valid_mask]) / denominator[valid_mask]

    return result

# --- Enhanced UDF Registry ---
UDF_REGISTRY = {
    "band_math": apply_band_math,
    "ndvi": apply_ndvi,
    "ndwi": apply_ndwi,
}

def get_required_bands_from_task(task: dict) -> List[str]:
    """Extract required bands from task definition"""
    operation = task.get("operation")
    params = task.get("parameters", {})

    if operation == "band_math":
        formula = params.get("formula", "")
        bands = set(re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', formula))
        math_keywords = {
            'abs', 'sqrt', 'log', 'exp', 'sin', 'cos', 'tan',
            'log10', 'log2', 'arcsin', 'arccos', 'arctan'
        }
        return list(bands - math_keywords)
    elif operation == "ndvi":
        return [params.get("nir_band"), params.get("red_band")]
    elif operation == "ndwi":
        return [params.get("green_band"), params.get("nir_band")]
    return []

# --- STRATEGIC VALIDATION IN UPLOAD FUNCTION ---
def upload_to_s3_memory(s3_client, data: np.ndarray, profile: dict,
                       job_id: str, tile_x_idx: int, tile_y_idx: int, task_index: int = 0) -> str:
    """STRATEGIC VALIDATION: Validate final output before upload"""
    tile_info = f"tile_{tile_x_idx}_{tile_y_idx}_task_{task_index}"

    validation_stats = validate_image_data_optimized(data, "Final Output", tile_info)

    if not validation_stats['is_worth_processing']:
        print(f"🚫 Skipping upload for {tile_info} - no meaningful data")
        output_s3_key = f"outputs/{job_id}/tile_{tile_x_idx}_{tile_y_idx}_task_{task_index}_LOW_QUALITY.tif"
        quality_status = "LOW_QUALITY"
    else:
        output_s3_key = f"outputs/{job_id}/tile_{tile_x_idx}_{tile_y_idx}_task_{task_index}.tif"
        quality_status = "HIGH_QUALITY"

    with MemoryFile() as memfile:
        with memfile.open(**profile) as dst:
            dst.write(data, 1)

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=output_s3_key,
            Body=memfile.read(),
            ContentType='image/tiff'
        )

        print(f"📤 Uploaded {tile_info} to S3 ({quality_status}): {output_s3_key}")
        return output_s3_key

# --- FIXED: OPTIMIZED TILE PROCESSING WITH MULTIPLE TASK SUPPORT ---
def process_single_tile_optimized(tile_event: dict) -> dict:
    """OPTIMIZED: Process single tile with support for multiple user_tasks"""
    tile_coords = f"({tile_event.get('tile_x_index','?')},{tile_event.get('tile_y_index','?')})"
    job_id = tile_event.get("job_id", None)
    print(f"🔄 Processing tile {tile_coords} for job {job_id}")

    # Update DynamoDB: tile processing started
    if job_id:
        update_job_status(job_id, "TILE_PROCESSING", {"tile": tile_coords})

    try:
        # --- FIX: Handle both user_task (singular) and user_tasks (plural) ---
        user_tasks = tile_event.get("user_tasks")

        if not user_tasks:
            # Fallback to singular for backward compatibility
            user_tasks = [tile_event.get("user_task")] if tile_event.get("user_task") else None

        if not user_tasks:
            error_msg = f"Input tile_event is missing user tasks. Available keys: {list(tile_event.keys())}"
            print(f"❌ {error_msg}")
            raise ValueError(error_msg)

        # Process each task sequentially for now (can be parallelized later)
        all_results = []
        for task_index, user_task in enumerate(user_tasks):
            print(f"📋 Processing task {task_index + 1}/{len(user_tasks)}: {user_task.get('operation', 'unknown')}")

            required_bands = get_required_bands_from_task(user_task)

            # Validate required_band_metadata exists
            if "required_band_metadata" not in tile_event:
                error_msg = f"Missing 'required_band_metadata' in tile_event. Available keys: {list(tile_event.keys())}"
                print(f"❌ {error_msg}")
                raise ValueError(error_msg)

            required_band_metadata = {
                band: tile_event["required_band_metadata"][band]
                for band in required_bands
            }

            window = Window(
                tile_event["tile_x_index"] * tile_event["tile_width"],
                tile_event["tile_y_index"] * tile_event["tile_height"],
                tile_event["tile_width"],
                tile_event["tile_height"]
            )

            band_arrays = process_bands_parallel_optimized(
                required_band_metadata, window,
                tile_event["reference_grid_dimensions"]["width"],
                tile_event["reference_grid_dimensions"]["height"],
                tile_event["tile_width"],
                tile_event["tile_height"],
                max_workers=min(MAX_WORKERS, len(required_bands))
            )

            operation = user_task.get("operation")
            parameters = user_task.get("parameters", {})

            if operation not in UDF_REGISTRY:
                error_msg = f"Unknown operation '{operation}'. Available operations: {list(UDF_REGISTRY.keys())}"
                print(f"❌ {error_msg}")
                raise ValueError(error_msg)

            processing_function = UDF_REGISTRY[operation]
            result_array = processing_function(band_arrays, **parameters)

            if "reference_geotransform" in tile_event:
                transform = tile_event["reference_geotransform"]
            else:
                first_band_meta = tile_event["required_band_metadata"][required_bands[0]]
                transform = first_band_meta["subdataset"]["geospatial"]["transform"]

            output_profile = create_output_profile(
                tile_event["tile_width"],
                tile_event["tile_height"],
                transform,
                window
            )

            context = get_processing_context()
            output_s3_key = upload_to_s3_memory(
                context.s3_client, result_array, output_profile,
                tile_event["job_id"], tile_event["tile_x_index"], tile_event["tile_y_index"], task_index
            )

            task_result = {
                "status": f"PROCESSED:{operation}",
                "output_path": f"s3://{S3_BUCKET_NAME}/{output_s3_key}",
                "tile_x_index": tile_event["tile_x_index"],
                "tile_y_index": tile_event["tile_y_index"],
                "task_index": task_index,
                "operation": operation,
                "batch_optimized": True,
                "parallel_processed": True
            }

            all_results.append(task_result)

            # --- CHANGED: use atomic increment and finalize check instead of marking COMPLETED per tile ---
            if job_id:
                try:
                    mark_tile_processed_and_maybe_complete(job_id, tile_coords, f"s3://{S3_BUCKET_NAME}/{output_s3_key}", operation)
                except Exception as e:
                    print(f"⚠️ mark_tile_processed_and_maybe_complete failed: {e}")
                    # fallback to simple per-tile update if atomic method fails
                    update_job_status(job_id, "TILE_PROCESSED", {"tile": tile_coords, "output": task_result["output_path"], "operation": operation})

        # Return consolidated results for all tasks
        if len(all_results) == 1:
            return all_results[0]  # Return single task result for backward compatibility
        else:
            return {
                "status": "MULTI_TASK_PROCESSED",
                "tasks_processed": len(all_results),
                "task_results": all_results,
                "tile_x_index": tile_event["tile_x_index"],
                "tile_y_index": tile_event["tile_y_index"],
                "batch_optimized": True,
                "parallel_processed": True
            }

    except Exception as e:
        print(f"❌ Error processing tile {tile_coords}: {e}")
        # Update DynamoDB with tile error
        if job_id:
            update_job_status(job_id, "TILE_ERROR", {"tile": tile_coords, "error": str(e)})
        return {
            "status": "ERROR",
            "tile_x_index": tile_event.get("tile_x_index", -1),
            "tile_y_index": tile_event.get("tile_y_index", -1),
            "error": str(e)
        }

def process_multiple_tiles_optimized(tile_events: List[dict], max_workers: int = MAX_WORKERS) -> List[dict]:
    """OPTIMIZED: Process multiple tiles with reduced lock contention"""
    print(f"🚀 Processing {len(tile_events)} tiles in parallel with {max_workers} workers...")
    start_time = time.time()

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tile = {
            executor.submit(process_single_tile_optimized, tile_event): tile_event
            for tile_event in tile_events
        }

        for future in as_completed(future_to_tile):
            tile_event = future_to_tile[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"❌ Unexpected error processing tile: {e}")
                results.append({
                    "status": "ERROR",
                    "tile_x_index": tile_event.get("tile_x_index", -1),
                    "tile_y_index": tile_event.get("tile_y_index", -1),
                    "error": str(e)
                })

    processing_time = time.time() - start_time
    successful = len([r for r in results if r.get("status") != "ERROR"])

    print(f"📊 Parallel processing completed: {successful}/{len(tile_events)} successful in {processing_time:.2f}s")
    try:
        throughput = len(tile_events) / processing_time
    except Exception:
        throughput = 0.0
    print(f"⚡ Average throughput: {throughput:.2f} tiles/second")

    return results

# --- ENHANCED AUTOMATIC TILE MERGING FUNCTIONS ---
def download_tiles_from_s3(job_id: str, temp_dir: str) -> List[str]:
    """ENHANCED: Download tiles for merging with robust error handling"""
    context = get_processing_context()
    tile_paths = []

    try:
        prefix = f"outputs/{job_id}/"
        print(f"🔍 Looking for tiles with prefix: {prefix}")

        response = context.s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)

        if 'Contents' not in response:
            print("ℹ️ No objects found in S3")
            return []

        print(f"📁 Found {len(response['Contents'])} objects in S3")

        for obj in response['Contents']:
            key = obj['Key']
            file_size = obj['Size']

            if not key.endswith('.tif') or 'LOW_QUALITY' in key:
                continue

            if file_size == 0:
                print(f"⚠️ Skipping empty file: {key}")
                continue

            local_path = os.path.join(temp_dir, os.path.basename(key))

            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                tile_paths.append(local_path)
                continue

            try:
                context.s3_client.download_file(S3_BUCKET_NAME, key, local_path)

                if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                    tile_paths.append(local_path)
                else:
                    print(f"⚠️ File download failed or empty: {key}")

            except Exception as e:
                print(f"❌ Failed to download {key}: {e}")

        print(f"✅ Successfully collected {len(tile_paths)} tiles for merging")
        return tile_paths

    except Exception as e:
        print(f"❌ Error downloading tiles: {str(e)}")
        return []

def merge_tiles_with_rasterio(tile_paths: List[str], output_path: str) -> str:
    """Merge tiles using Rasterio"""
    if not tile_paths:
        raise ValueError("No tile paths provided")

    src_files = []
    try:
        for tile_path in tile_paths:
            try:
                src = rasterio.open(tile_path)
                src_files.append(src)
                print(f"✅ Loaded tile: {os.path.basename(tile_path)} - {src.shape}")
            except Exception as e:
                print(f"⚠️ Could not open tile {tile_path}: {e}")
                continue

        if not src_files:
            raise RuntimeError("No valid tile files")

        print(f"🔄 Merging {len(src_files)} tiles...")
        mosaic, out_transform = rasterio_merge(src_files)
        out_meta = src_files[0].meta.copy()

        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_transform,
            "compress": "lzw",
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
            "nodata": -9999
        })

        with rasterio.open(output_path, "w", **out_meta) as dest:
            dest.write(mosaic)

        print(f"✅ Mosaic created: {output_path} - {mosaic.shape}")
        return output_path

    except Exception as e:
        raise RuntimeError(f"Tile merging failed: {str(e)}")
    finally:
        for src in src_files:
            try:
                src.close()
            except:
                pass

def alternative_merge_strategy(tile_paths: List[str], temp_dir: str, job_id: str) -> str:
    """Alternative merging strategy if primary method fails"""
    print("🔄 Using alternative merging strategy...")

    mosaic_data = None
    output_profile = None

    for i, tile_path in enumerate(tile_paths):
        try:
            with rasterio.open(tile_path) as src:
                data = src.read(1)
                profile = src.profile

                if mosaic_data is None:
                    mosaic_data = data
                    output_profile = profile
                    print(f"   - Initialized mosaic with tile {i+1}: {data.shape}")
                else:
                    mosaic_data = np.maximum(mosaic_data, data)
                    print(f"   - Merged tile {i+1}: {data.shape}")

        except Exception as e:
            print(f"⚠️ Failed to process tile {i+1}: {e}")
            continue

    if mosaic_data is None:
        raise RuntimeError("Alternative merging failed - no valid tiles")

    mosaic_path = os.path.join(temp_dir, f"mosaic_alt_{job_id}_{int(time.time())}.tif")

    with rasterio.open(mosaic_path, 'w', **output_profile) as dst:
        dst.write(mosaic_data, 1)

    print(f"✅ Alternative mosaic created: {mosaic_path}")
    return mosaic_path

def merge_all_tiles_automatically(job_id: str, operation: str, total_tiles: int, parameters: dict = None) -> Dict[str, Any]:
    """ENHANCED: Automatically merge all tiles into mosaic with new bucket and cleanup"""
    start_time = time.time()
    print(f"🔄 Starting ENHANCED automatic tile merging for job {job_id}...")

    # Update DB: merge started
    if job_id:
        update_job_status(job_id, "MERGE_STARTED", {"operation": operation, "expected_tiles": total_tiles})

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Wait for S3 consistency with progressive waiting
            print("⏳ Waiting for S3 consistency (progressive wait)...")
            wait_times = [2, 3, 5]
            tile_paths = []

            for wait_time in wait_times:
                time.sleep(wait_time)
                tile_paths = download_tiles_from_s3(job_id, temp_dir)
                if tile_paths:
                    print(f"✅ Found {len(tile_paths)} tiles after {wait_time}s wait")
                    break
                else:
                    print(f"⚠️ No tiles found after {wait_time}s wait")

            if not tile_paths:
                print("❌ No tiles found after all wait attempts")
                if job_id:
                    update_job_status(job_id, "MERGE_FAILED", {"reason": "no_tiles_found"})
                return {
                    "status": "ERROR",
                    "message": f"No tiles found for job {job_id} after multiple attempts",
                    "processing_time": time.time() - start_time
                }

            print(f"📥 Successfully downloaded {len(tile_paths)}/{total_tiles} tiles for merging")

            # Check if we have enough tiles to proceed
            min_tiles_required = max(1, int(total_tiles * 0.3))
            if len(tile_paths) < min_tiles_required:
                print(f"⚠️ Only {len(tile_paths)}/{total_tiles} tiles available. Minimum {min_tiles_required} required.")
                if job_id:
                    update_job_status(job_id, "INSUFFICIENT_TILES", {"available": len(tile_paths), "required": min_tiles_required})
                return {
                    "status": "INSUFFICIENT_TILES",
                    "message": f"Only {len(tile_paths)}/{total_tiles} tiles available",
                    "tiles_available": len(tile_paths),
                    "tiles_required": min_tiles_required,
                    "processing_time": time.time() - start_time
                }

            # Determine quality status based on tile coverage
            coverage_percentage = (len(tile_paths) / total_tiles) * 100
            quality_status = "HIGH_QUALITY" if coverage_percentage >= 70 else "STANDARD"

            # Generate meaningful filename
            meaningful_filename = generate_meaningful_filename(
                job_id, operation, parameters or {},
                len(tile_paths), quality_status
            )

            # Merge tiles
            mosaic_path = os.path.join(temp_dir, meaningful_filename)
            print("🔄 Merging tiles into mosaic...")

            try:
                merge_tiles_with_rasterio(tile_paths, mosaic_path)
            except Exception as merge_error:
                print(f"❌ Primary tile merging failed: {merge_error}")
                mosaic_path = alternative_merge_strategy(tile_paths, temp_dir, job_id)

            # Verify mosaic was created
            if not os.path.exists(mosaic_path):
                raise RuntimeError("Mosaic file was not created after merging")

            mosaic_size = os.path.getsize(mosaic_path)
            print(f"✅ Mosaic created: {mosaic_size} bytes")

            # Upload to result bucket with meaningful name
            metadata = {
                "job_id": job_id,
                "operation": operation,
                "tiles_merged": len(tile_paths),
                "total_tiles": total_tiles,
                "quality_status": quality_status,
                "coverage_percentage": coverage_percentage
            }

            final_s3_url = upload_final_mosaic_to_result_bucket(
                mosaic_path, meaningful_filename, metadata
            )

            # Clean up individual tiles after successful upload
            print("🧹 Cleaning up individual tile files...")
            cleanup_tiles_after_merge(job_id)

            mosaic_processing_time = time.time() - start_time

            print(f"✅ Mosaic processing COMPLETED successfully!")
            print(f"   - Tiles merged: {len(tile_paths)}")
            print(f"   - Final mosaic: {final_s3_url}")
            print(f"   - Quality: {quality_status}")
            print(f"   - Coverage: {coverage_percentage:.1f}%")
            print(f"   - Processing time: {mosaic_processing_time:.2f}s")

            # Update DB: merge completed successfully
            if job_id:
                update_job_status(job_id, "MOSAIC_CREATED", {"mosaic_s3": final_s3_url, "tiles_merged": len(tile_paths), "quality_status": quality_status, "coverage_percentage": coverage_percentage})

            return {
                "status": "MOSAIC_CREATED",
                "mosaic_path": final_s3_url,
                "output_path": re.sub(r'^s3://[^/]+/', '', final_s3_url),  # relative path
                "tiles_merged": len(tile_paths),
                "total_tiles": total_tiles,
                "coverage_percentage": coverage_percentage,
                "mosaic_size_bytes": mosaic_size,
                "quality_status": quality_status,
                "filename": meaningful_filename,
                "processing_time": mosaic_processing_time,
                "tiles_cleaned_up": True
            }

        except Exception as e:
            error_msg = f"Mosaic creation failed: {str(e)}"
            print(f"❌ {error_msg}")
            if job_id:
                update_job_status(job_id, "MERGE_FAILED", {"error": str(e)})
            return {
                "status": "ERROR",
                "message": error_msg,
                "processing_time": time.time() - start_time
            }

# --- Cache Management ---
def get_cache_stats():
    """Get cache statistics"""
    return {
        "cache_type": "optimized_lock_free",
        "tile_cache_size": len(FALLBACK_TILE_CACHE),
        "max_workers": MAX_WORKERS,
    }

# Helper to make lambda-style response expected by FastAPI
def make_lambda_response(result: dict) -> dict:
    """
    Wrap result into { statusCode: int, body: json_string } so the FastAPI side
    (which expects statusCode and body) can parse output_path/mosaic_path.
    """
    # Choose status code: 200 for success or in-progress, 500 for explicit errors
    status = result.get("status", "").upper()
    if status == "MOSAIC_CREATED":
        status_code = 200
    elif status in ("ERROR", "INSUFFICIENT_TILES"):
        status_code = 500
    else:
        # MERGE_IN_PROGRESS or MULTI_TASK_PROCESSED etc. — still return 200
        status_code = 200

    # Ensure body is JSON-serializable; fallback to stringified fields for safety
    try:
        body_str = json.dumps(result, default=str)
    except Exception:
        # As a last resort, provide a minimal serializable body
        safe_result = {k: str(v) for k, v in result.items()}
        body_str = json.dumps(safe_result)

    return {
        "statusCode": status_code,
        "body": body_str
    }

# --- MAIN HANDLER ---
def handler(event: dict, context) -> dict:
    """ENHANCED: Main handler with new bucket, meaningful naming, automatic cleanup and DynamoDB updates"""
    start_time = time.time()

    print(f"🎯 Target Result Bucket: {RESULT_BUCKET_NAME}")
    print(f"🔧 Processing event type: {type(event)}")
    print(f"🔧 Event keys: {list(event.keys()) if isinstance(event, dict) else 'Not a dict'}")

    try:
        # Handle manual merge request
        if event.get("action") == "merge_tiles":
            job_id = event["job_id"]
            operation = event.get("operation", "band_math")
            total_tiles = event.get("total_tiles", 1)
            parameters = event.get("parameters", {})

            # DB: merge requested
            if job_id:
                update_job_status(job_id, "MERGE_REQUESTED", {"operation": operation})

            result = merge_all_tiles_automatically(job_id, operation, total_tiles, parameters)
            return make_lambda_response(result)

        # Handle batch processing with enhanced automatic merging
        elif "batch_tiles" in event:
            tile_events = event["batch_tiles"]
            max_workers = min(MAX_WORKERS, len(tile_events))
            print(f"🚀 Starting ENHANCED parallel processing of {len(tile_events)} tiles with {max_workers} workers")

            # determine job_id for batch (assume present in first event)
            job_id = tile_events[0].get("job_id", None)
            # DB: mark batch processing started
            if job_id:
                update_job_status(job_id, "PROCESSING", {"total_tiles": len(tile_events)})

            # Process all tiles
            results = process_multiple_tiles_optimized(tile_events, max_workers)

            processing_time = time.time() - start_time

            # Enhanced automatic merging
            mosaic_result = None
            successful_tiles = [r for r in results if r.get("status") != "ERROR"]

            print(f"✅ Tile processing completed: {len(successful_tiles)}/{len(tile_events)} successful")
            print(f"🔄 Checking automatic merging conditions...")

            # Enhanced merge conditions
            should_merge = (
                len(tile_events) > 1 and
                len(successful_tiles) > 0 and
                event.get("auto_merge", True)
            )

            if should_merge:
                print("🔄 Starting ENHANCED AUTOMATIC tile merging...")
                first_event = tile_events[0]

                # Determine operation from user_tasks
                user_tasks = first_event.get("user_tasks", [])
                if user_tasks:
                    operation = user_tasks[0].get("operation", "unknown")
                    parameters = user_tasks[0].get("parameters", {})
                else:
                    operation = "unknown"
                    parameters = {}

                job_id = first_event.get("job_id", job_id)

                try:
                    # DB: merge queued
                    if job_id:
                        update_job_status(job_id, "MERGE_QUEUED", {"expected_tiles": len(tile_events)})
                    mosaic_result = merge_all_tiles_automatically(
                        job_id,
                        operation,
                        len(tile_events),
                        parameters
                    )
                    print(f"📊 Mosaic creation result: {mosaic_result.get('status', 'UNKNOWN')}")

                except Exception as e:
                    print(f"❌ Automatic merging failed: {e}")
                    mosaic_result = {
                        "status": "ERROR",
                        "message": str(e),
                        "processing_time": 0
                    }
                    if job_id:
                        update_job_status(job_id, "MERGE_FAILED", {"error": str(e)})

            response = {
                "batch_results": results,
                "total_tiles": len(results),
                "successful_tiles": len(successful_tiles),
                "processing_time": processing_time,
                "cache_stats": get_cache_stats(),
                "parallel_processed": True,
                "optimized_version": "v4_multi_task_fix",
                "throughput_tiles_per_second": (len(tile_events) / processing_time) if processing_time > 0 else 0,
                "automatic_mosaic_creation": mosaic_result is not None,
                "result_bucket": RESULT_BUCKET_NAME
            }

            # Add mosaic result to response
            if mosaic_result:
                response["mosaic_creation"] = mosaic_result

            print(f"✅ ENHANCED Batch processing COMPLETED")
            if mosaic_result and mosaic_result.get('status') == 'MOSAIC_CREATED':
                print(f"   - Final Mosaic: {mosaic_result.get('mosaic_path', 'N/A')}")
                print(f"   - Meaningful Name: {mosaic_result.get('filename', 'N/A')}")
                print(f"   - Quality: {mosaic_result.get('quality_status', 'N/A')}")
            print(f"   - Total tiles processed: {len(results)}")
            print(f"   - Successful tiles: {len(successful_tiles)}")
            print(f"   - Total processing time: {processing_time:.2f}s")
            print(f"   - Result Bucket: {RESULT_BUCKET_NAME}")

            # Final DB update for batch: completed or partial
            final_status = "COMPLETED" if len(successful_tiles) == len(tile_events) else "PARTIAL_SUCCESS"
            if job_id:
                if mosaic_result and mosaic_result.get('status') == 'MOSAIC_CREATED':
                    update_job_status(job_id, final_status, {"mosaic": mosaic_result.get("mosaic_path"), "successful_tiles": len(successful_tiles), "total_tiles": len(tile_events)})
                else:
                    update_job_status(job_id, final_status, {"successful_tiles": len(successful_tiles), "total_tiles": len(tile_events)})

            return make_lambda_response(response)

        # Single tile processing (no merging for single tiles)
        else:
            try:
                single_batch = [event]
                job_id = event.get("job_id", None)
                if job_id:
                    update_job_status(job_id, "PROCESSING_SINGLE_TILE", {"tile": f"({event.get('tile_x_index')},{event.get('tile_y_index')})"})
                results = process_multiple_tiles_optimized(single_batch, max_workers=1)

                if results and results[0].get("status") != "ERROR":
                    if job_id:
                        update_job_status(job_id, "COMPLETED", {"tile_result": results[0]})
                    return make_lambda_response(results[0])
                else:
                    error_payload = results[0] if results else {"status": "ERROR", "error": "Unknown error"}
                    if job_id:
                        update_job_status(job_id, "ERROR", {"error": error_payload.get("error")})
                    return make_lambda_response(error_payload)

            except Exception as e:
                print(f"Error processing tile: {str(e)}")
                job_id = event.get("job_id", None)
                if job_id:
                    update_job_status(job_id, "ERROR", {"error": str(e)})
                return make_lambda_response({
                    "status": "ERROR",
                    "error": str(e)
                })

    except Exception as e:
        print(f"Fatal handler error: {e}")
        # Try a last-ditch update if possible
        job_id = event.get("job_id") if isinstance(event, dict) else None
        if job_id:
            update_job_status(job_id, "ERROR", {"error": str(e)})
        return make_lambda_response({"status": "ERROR", "error": str(e)})