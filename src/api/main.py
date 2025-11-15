#!/usr/bin/env python3
"""
Enhanced Workflow Backend API with TiTiler Integration (ready-to-run)

Features:
- Bounding box-based cropping via TiTiler (GET/POST)
- TiTiler assumed to be mounted under /viz; code tries /viz/* then falls back.
- Health checks probe /viz/healthz (and other fallbacks)
- WebSocket-based real-time job watcher
- S3 browsing (root/list/presign)
- Visualization endpoints using rio-tiler when available
"""
import os
import json
import uuid
import logging
import asyncio
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set

import boto3
from botocore.exceptions import ClientError
import httpx

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field

# Optional visualization imports (rasterio for fallback bbox processing only)
try:
    import rasterio
    import numpy as np
    from io import BytesIO
    from PIL import Image
    from matplotlib import cm
    VISUALIZATION_ENABLED = True
except Exception:
    VISUALIZATION_ENABLED = False

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("workflow_api")

# -------------------------
# Pydantic models
# -------------------------
class Task(BaseModel):
    task_id: str
    operation: str
    parameters: Dict[str, Any]
    dependencies: List[str] = []

class WorkflowRequest(BaseModel):
    workflow_id: str
    dataset_id: str
    tasks: List[Task]
    inputs: Optional[Dict[str, str]] = None

class BoundingBoxRequest(BaseModel):
    """Request model for bounding box-based downloads"""
    min_lon: float = Field(..., description="Minimum longitude (west)")
    min_lat: float = Field(..., description="Minimum latitude (south)")
    max_lon: float = Field(..., description="Maximum longitude (east)")
    max_lat: float = Field(..., description="Maximum latitude (north)")
    format: str = Field("geotiff", description="Output format: geotiff or png")
    width: Optional[int] = Field(None, description="Output width in pixels")
    height: Optional[int] = Field(None, description="Output height in pixels")
    max_size: Optional[int] = Field(1024, description="Maximum dimension if width/height not specified")
    colormap: Optional[str] = Field("viridis", description="Colormap to apply")
    rescale: Optional[str] = Field(None, description="Rescale values (e.g., '0,100')")
    crs: Optional[str] = Field("EPSG:4326", description="Coordinate reference system (bbox is expected in this CRS)")

# -------------------------
# Config (env)
# -------------------------
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
RECEIVER_LAMBDA_NAME = os.getenv("RECEIVER_LAMBDA_NAME", "receiver_lambda")
PROCESSOR_LAMBDA_NAME = os.getenv("PROCESSOR_LAMBDA_NAME", "processor_lambda")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE_NAME", "WorkflowJobs")
INPUT_BUCKET = os.getenv("S3_BUCKET_NAME", "insat-cog-processed-production")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET_NAME", "insat-processed-results-production")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
API_BASE_URL = os.getenv("API_BASE_URL", "http://api-lb-production-716552440.ap-south-1.elb.amazonaws.com")

# TiTiler Configuration
# Set TITILER_ENDPOINT to the TiTiler service URL:
# - In ECS with ALB: Use the ALB URL with /viz prefix (e.g., http://your-alb.amazonaws.com/viz)
# - In docker-compose: Use service name (e.g., http://titiler:8080)
# - For direct service: Use the direct endpoint (e.g., http://titiler-service:8080)
# Note: If TiTiler is behind ALB at /viz, set TITILER_ENDPOINT to include /viz
# If TITILER_ENDPOINT env var is not set, we'll try both with and without /viz
_titiler_base = os.getenv("TITILER_ENDPOINT")
if _titiler_base:
    TITILER_ENDPOINT = _titiler_base.rstrip("/")
else:
    # Default to ALB without /viz - will try multiple prefixes in the request function
    TITILER_ENDPOINT = API_BASE_URL.rstrip("/")
TITILER_TIMEOUT = float(os.getenv("TITILER_TIMEOUT", "120.0"))

JOB_WATCHER_POLL_INTERVAL = float(os.getenv("JOB_WATCHER_POLL_INTERVAL", "5.0"))
JOB_WATCHER_TIMEOUT = float(os.getenv("JOB_WATCHER_TIMEOUT", "7200.0"))
WS_PERIODIC_PUSH_INTERVAL = float(os.getenv("WS_PERIODIC_PUSH_INTERVAL", "10.0"))

MIN_EXPECTED_FILE_SIZE_MB = float(os.getenv("MIN_EXPECTED_FILE_SIZE_MB", "10.0"))
MIN_TILES_FOR_MERGE = int(os.getenv("MIN_TILES_FOR_MERGE", "50"))

EXCLUDED_ROOT_PREFIXES = {"jobs/", "lambda-packages/", "outputs/"}
AWS_NO_SIGN_REQUEST = os.getenv("AWS_NO_SIGN_REQUEST", "NO")

# -------------------------
# AWS clients/resources
# -------------------------
def get_aws_client(service_name: str):
    client_args = {"region_name": AWS_REGION}
    if AWS_ENDPOINT_URL:
        client_args["endpoint_url"] = AWS_ENDPOINT_URL
        client_args["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
        client_args["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    return boto3.client(service_name, **client_args)

def get_aws_resource(service_name: str):
    resource_args = {"region_name": AWS_REGION}
    if AWS_ENDPOINT_URL:
        resource_args["endpoint_url"] = AWS_ENDPOINT_URL
        resource_args["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
        resource_args["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    return boto3.resource(service_name, **resource_args)

lambda_client = get_aws_client("lambda")
s3_client = get_aws_client("s3")
sqs_client = get_aws_client("sqs")
dynamodb = get_aws_resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)

# -------------------------
# HTTP client for TiTiler
# -------------------------
http_client = httpx.AsyncClient(timeout=TITILER_TIMEOUT)
logger.info("Starting app - initializing HTTP client for TiTiler")

# -------------------------
# WebSocket connection manager
# -------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        async with self.lock:
            if job_id not in self.active_connections:
                self.active_connections[job_id] = set()
            self.active_connections[job_id].add(websocket)
        logger.info(f"WebSocket connected for job {job_id}. Total connections: {len(self.active_connections[job_id])}")

    async def disconnect(self, websocket: WebSocket, job_id: str):
        async with self.lock:
            if job_id in self.active_connections:
                self.active_connections[job_id].discard(websocket)
                if not self.active_connections[job_id]:
                    self.active_connections.pop(job_id, None)
        logger.info(f"WebSocket disconnected for job {job_id}")

    async def send_update(self, job_id: str, message: dict):
        async with self.lock:
            if job_id not in self.active_connections:
                return
            conns = list(self.active_connections[job_id])
        disconnected = []
        for ws in conns:
            try:
                await ws.send_json(message)
            except Exception as e:
                logger.warning(f"Failed to send ws update for job {job_id}: {e}")
                disconnected.append(ws)
        if disconnected:
            async with self.lock:
                if job_id in self.active_connections:
                    for d in disconnected:
                        self.active_connections[job_id].discard(d)
                    if not self.active_connections[job_id]:
                        self.active_connections.pop(job_id, None)

    def get_connection_count(self, job_id: str) -> int:
        return len(self.active_connections.get(job_id, set()))

    def get_total_connections(self) -> int:
        return sum(len(s) for s in self.active_connections.values())

ws_manager = ConnectionManager()

# -------------------------
# Watcher registry
# -------------------------
_JOB_WATCHERS_LOCK = asyncio.Lock()
_JOB_WATCHERS: Dict[str, asyncio.Task] = {}

# -------------------------
# Async AWS helpers (wrap blocking boto calls)
# -------------------------
async def dynamodb_get_item(job_id: str) -> Dict[str, Any]:
    return await asyncio.to_thread(table.get_item, Key={"job_id": job_id})

async def s3_head_bucket(bucket: str):
    return await asyncio.to_thread(s3_client.head_bucket, Bucket=bucket)

async def s3_head_object(bucket: str, key: str) -> Dict[str, Any]:
    return await asyncio.to_thread(s3_client.head_object, Bucket=bucket, Key=key)

async def s3_get_object(bucket: str, key: str) -> Dict[str, Any]:
    return await asyncio.to_thread(s3_client.get_object, Bucket=bucket, Key=key)

async def s3_generate_presigned_get(bucket: str, key: str, expires_in: int = 3600) -> str:
    return await asyncio.to_thread(
        s3_client.generate_presigned_url,
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in
    )

async def lambda_invoke(payload: dict) -> dict:
    return await asyncio.to_thread(lambda_client.invoke,
                                  FunctionName=PROCESSOR_LAMBDA_NAME,
                                  InvocationType="RequestResponse",
                                  Payload=json.dumps(payload).encode("utf-8"))

async def _s3_list_objects_v2(bucket: str, prefix: str = "", delimiter: Optional[str] = None,
                              continuation_token: Optional[str] = None, max_keys: int = 1000) -> dict:
    params = {"Bucket": bucket, "MaxKeys": max_keys}
    if prefix is not None:
        params["Prefix"] = prefix
    if delimiter:
        params["Delimiter"] = delimiter
    if continuation_token:
        params["ContinuationToken"] = continuation_token
    return await asyncio.to_thread(s3_client.list_objects_v2, **params)

# -------------------------
# Helper functions
# -------------------------
def parse_details_field(details_val: Any) -> Dict[str, Any]:
    if not details_val:
        return {}
    if isinstance(details_val, dict):
        return details_val
    if isinstance(details_val, str):
        try:
            return json.loads(details_val)
        except json.JSONDecodeError:
            return {}
    return {}

def extract_mosaic_path(item: Dict[str, Any]) -> Optional[str]:
    if not item:
        return None
    mosaic_path = item.get("mosaic_path")
    if mosaic_path:
        return mosaic_path
    details_obj = parse_details_field(item.get("details"))
    if details_obj.get("mosaic_s3"):
        return details_obj.get("mosaic_s3")
    if details_obj.get("mosaic_path"):
        return details_obj.get("mosaic_path")
    return None

def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if s3_uri.startswith("s3://"):
        parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
    else:
        bucket = PROCESSED_BUCKET
        key = s3_uri.lstrip("/")
    return bucket, key

def _safe_int_from_sources(item: Dict[str, Any], details_obj: Dict[str, Any], key: str) -> int:
    try:
        if key in item and item.get(key) is not None:
            v = item.get(key)
        elif key in details_obj and details_obj.get(key) is not None:
            v = details_obj.get(key)
        else:
            return 0
        return int(float(v))
    except Exception:
        return 0

async def get_mosaic_path_from_job_async(job_id: str) -> Tuple[str, str]:
    """
    Retrieve the mosaic path from a job and return (bucket, key) tuple.
    Raises HTTPException if job not found or mosaic path missing.
    """
    resp = await dynamodb_get_item(job_id)
    if "Item" not in resp:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    item = resp["Item"]
    mosaic_path = extract_mosaic_path(item)
    
    if not mosaic_path:
        raise HTTPException(status_code=404, detail="Mosaic path not found for this job")
    
    return parse_s3_uri(mosaic_path)

# -------------------------
# TiTiler Integration (adapted for /viz)
# -------------------------
def _build_titiler_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"

async def request_bbox_from_titiler(
    s3_url: str,
    bbox: Tuple[float, float, float, float],
    output_format: str = "geotiff",
    width: Optional[int] = None,
    height: Optional[int] = None,
    max_size: int = 1024,
    colormap: Optional[str] = None,
    rescale: Optional[str] = None
) -> bytes:
    """
    Request a bounding box crop from TiTiler using various endpoint patterns.
    Will try both with and without /viz prefix to find working TiTiler service.
    """
    try:
        min_lon, min_lat, max_lon, max_lat = bbox
        bbox_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
        
        fmt = output_format.lower()
        ext = "tif" if fmt == "geotiff" else "png"
        
        # Try multiple TiTiler endpoint patterns with different prefixes
        base_paths = [
            f"/cog/crop/{bbox_str}.{ext}",
            f"/crop/{bbox_str}.{ext}",
            f"/cog/bbox/{bbox_str}.{ext}",
            f"/bbox/{bbox_str}.{ext}",
            f"/cog/part",
            f"/part",
        ]
        
        # Try both with /viz prefix and without
        candidate_paths = []
        for path in base_paths:
            candidate_paths.append(f"/viz{path}")  # With /viz prefix
            candidate_paths.append(path)            # Without /viz prefix
        
        base_params = {"url": s3_url}
        
        if width:
            base_params["width"] = width
        if height:
            base_params["height"] = height
        if not width and not height:
            base_params["max_size"] = max_size
        if rescale:
            base_params["rescale"] = rescale
        if colormap:
            base_params["colormap_name"] = colormap

        last_resp = None
        errors = []
        successful_endpoint = None
        
        for p in candidate_paths:
            endpoint = _build_titiler_url(TITILER_ENDPOINT, p)
            # For endpoints with bbox in URL path, don't add bbox to params
            # For /part endpoint, add bbox to params
            if "/bbox/" in p or "/crop/" in p:
                params = base_params.copy()
            else:
                params = base_params.copy()
                params["bbox"] = bbox_str
            
            # Only log first few attempts to avoid log spam
            if len(errors) < 3:
                logger.info(f"Trying TiTiler endpoint: {endpoint}")
            
            try:
                resp = await http_client.get(endpoint, params=params, timeout=TITILER_TIMEOUT)
                last_resp = resp
                if resp.status_code == 200:
                    successful_endpoint = endpoint
                    logger.info(f"✓ TiTiler crop succeeded via {endpoint}")
                    return resp.content
                elif resp.status_code != 404:
                    # Non-404 errors are more interesting, log them
                    snippet = (resp.text or "")[:200]
                    errors.append(f"{endpoint} -> {resp.status_code} - {snippet}")
                    logger.warning(f"TiTiler attempt {endpoint}: {resp.status_code}")
            except httpx.TimeoutException:
                errors.append(f"{endpoint} -> timeout")
            except Exception as e:
                # Only log unexpected errors, not connection errors for non-existent endpoints
                if len(errors) < 3:
                    errors.append(f"{endpoint} -> error: {str(e)[:100]}")

        # Build a concise error message
        if errors:
            error_summary = " | ".join(errors[:5])  # Only show first 5 errors
            logger.error(f"All TiTiler endpoints failed. Tried {len(candidate_paths)} endpoints. Sample errors: {error_summary}")
            raise HTTPException(
                status_code=502, 
                detail=f"TiTiler service unavailable. Please verify TiTiler is deployed and accessible. Tried {len(candidate_paths)} endpoint combinations."
            )
        else:
            logger.error(f"All {len(candidate_paths)} TiTiler endpoints returned 404 - TiTiler may not be deployed")
            raise HTTPException(
                status_code=503, 
                detail="TiTiler service not found. The service may not be deployed or is not accessible at the expected endpoints."
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error in TiTiler request")
        raise HTTPException(status_code=500, detail=f"Unexpected error processing TiTiler request: {str(e)}")

async def request_bbox_fallback(
    s3_url: str,
    bbox: Tuple[float, float, float, float],
    output_format: str = "geotiff",
    width: Optional[int] = None,
    height: Optional[int] = None,
    max_size: int = 1024,
    colormap: Optional[str] = None,
    rescale: Optional[str] = None
) -> bytes:
    """
    Fallback bbox crop using rasterio directly when TiTiler is unavailable.
    """
    if not VISUALIZATION_ENABLED:
        raise HTTPException(status_code=503, detail="Visualization libraries not available")
    
    def process_bbox():
        from rasterio.windows import from_bounds
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        import numpy as np
        from io import BytesIO
        
        with rasterio.Env(AWS_NO_SIGN_REQUEST=AWS_NO_SIGN_REQUEST):
            with rasterio.open(s3_url) as src:
                # Get window for bbox
                window = from_bounds(*bbox, transform=src.transform)
                
                # Read the data
                data = src.read(1, window=window)
                
                # Calculate output dimensions
                if width and height:
                    out_shape = (height, width)
                elif width:
                    aspect = window.height / window.width
                    out_shape = (int(width * aspect), width)
                elif height:
                    aspect = window.width / window.height
                    out_shape = (height, int(height * aspect))
                else:
                    max_dim = max(window.height, window.width)
                    if max_dim > max_size:
                        scale = max_size / max_dim
                        out_shape = (int(window.height * scale), int(window.width * scale))
                    else:
                        out_shape = (int(window.height), int(window.width))
                
                # Resize if needed
                if out_shape != (window.height, window.width):
                    from skimage.transform import resize
                    data = resize(data, out_shape, preserve_range=True, anti_aliasing=True).astype(data.dtype)
                
                # Apply rescaling
                if rescale:
                    try:
                        min_val, max_val = map(float, rescale.split(","))
                        data = np.clip(data, min_val, max_val)
                    except:
                        pass
                
                # Apply colormap if specified
                output_data = data
                if colormap:
                    from matplotlib import cm as mpl_cm
                    
                    # Normalize to 0-1
                    data_min, data_max = data.min(), data.max()
                    if data_max > data_min:
                        normalized = (data - data_min) / (data_max - data_min)
                    else:
                        normalized = np.zeros_like(data, dtype=np.float32)
                    
                    # Apply colormap
                    cmap_name = colormap
                    try:
                        cmap = mpl_cm.get_cmap(cmap_name)
                        colored = cmap(normalized)
                        # Extract RGB channels (ignore alpha)
                        rgb_data = (colored[:, :, :3] * 255).astype(np.uint8)
                        output_data = rgb_data
                    except:
                        # If colormap fails, use original data
                        output_data = data
                
                fmt = output_format.lower()
                if fmt == "png":
                    # Convert to PNG
                    from PIL import Image
                    
                    if colormap and output_data.ndim == 3:
                        # RGB data from colormap
                        img = Image.fromarray(output_data)
                    else:
                        # Single band data
                        data_min, data_max = output_data.min(), output_data.max()
                        if data_max > data_min:
                            normalized = ((output_data - data_min) / (data_max - data_min) * 255).astype(np.uint8)
                        else:
                            normalized = np.zeros_like(output_data, dtype=np.uint8)
                        img = Image.fromarray(normalized, mode='L')
                    
                    buf = BytesIO()
                    img.save(buf, format="PNG")
                    return buf.getvalue()
                else:
                    # Return as GeoTIFF
                    from rasterio.io import MemoryFile
                    
                    # Calculate transform for the bbox
                    bounds_transform = rasterio.transform.from_bounds(
                        *bbox, output_data.shape[-1] if output_data.ndim == 3 else output_data.shape[1], 
                        output_data.shape[-2] if output_data.ndim == 3 else output_data.shape[0]
                    )
                    
                    with MemoryFile() as memfile:
                        if output_data.ndim == 3:
                            # RGB data from colormap
                            with memfile.open(
                                driver='GTiff',
                                height=output_data.shape[0],
                                width=output_data.shape[1],
                                count=3,
                                dtype=output_data.dtype,
                                crs=src.crs,
                                transform=bounds_transform
                            ) as dataset:
                                for i in range(3):
                                    dataset.write(output_data[:, :, i], i+1)
                        else:
                            # Single band data
                            with memfile.open(
                                driver='GTiff',
                                height=output_data.shape[0],
                                width=output_data.shape[1],
                                count=1,
                                dtype=output_data.dtype,
                                crs=src.crs,
                                transform=bounds_transform
                            ) as dataset:
                                dataset.write(output_data, 1)
                        
                        return memfile.read()
    
    try:
        return await asyncio.to_thread(process_bbox)
    except Exception as e:
        logger.exception("Fallback bbox processing failed")
        raise HTTPException(status_code=500, detail=f"Failed to process bbox: {str(e)}")

# -------------------------
# Job status builder (same as before)
# -------------------------
async def get_job_status_data_async(job_id: str) -> Dict[str, Any]:
    resp = await dynamodb_get_item(job_id)
    if "Item" not in resp:
        raise HTTPException(status_code=404, detail="Workflow not found")
    item = resp["Item"]
    status_val = item.get("status", "UNKNOWN")
    mosaic_path = extract_mosaic_path(item)
    details_obj = parse_details_field(item.get("details"))

    async with _JOB_WATCHERS_LOCK:
        watcher_active = job_id in _JOB_WATCHERS

    mosaic_info = None
    visualization_info = None

    if mosaic_path:
        validation = {"is_valid": False, "file_exists": False, "size_mb": 0.0, "warnings": []}
        try:
            bucket, key = parse_s3_uri(mosaic_path)
            try:
                head = await s3_head_object(bucket, key)
                size_bytes = head.get("ContentLength", 0)
                size_mb = size_bytes / (1024 * 1024)
                validation["file_exists"] = True
                validation["size_mb"] = round(size_mb, 2)
                validation["size_bytes"] = size_bytes
                validation["is_valid"] = size_mb >= MIN_EXPECTED_FILE_SIZE_MB
                if not validation["is_valid"]:
                    validation["warnings"].append(f"File size {size_mb:.2f}MB < minimum {MIN_EXPECTED_FILE_SIZE_MB}MB")
            except ClientError:
                validation["warnings"].append("File not found in S3")
        except Exception as e:
            logger.exception("validate mosaic in job_status")
            validation["warnings"].append(str(e))

        mosaic_info = {
            "path": mosaic_path,
            "size_mb": validation.get("size_mb", 0.0),
            "is_valid": validation.get("is_valid", False),
            "warnings": validation.get("warnings", []),
            "ready_for_download": validation.get("is_valid", False),
            "tiles_merged": details_obj.get("tiles_merged"),
            "coverage_percentage": details_obj.get("coverage_percentage"),
            "quality_status": details_obj.get("quality_status")
        }

        if VISUALIZATION_ENABLED and validation.get("is_valid"):
            visualization_info = {
                "tiles_url": f"{API_BASE_URL.rstrip('/')}/viz/{job_id}/tiles/{{z}}/{{x}}/{{y}}.png?colormap={details_obj.get('colormap','viridis')}",
                "preview_url": f"{API_BASE_URL.rstrip('/')}/viz/{job_id}/preview.png",
                "metadata_url": f"{API_BASE_URL.rstrip('/')}/viz/{job_id}/info",
                "statistics_url": f"{API_BASE_URL.rstrip('/')}/viz/{job_id}/statistics",
                "bbox_download_url": f"{API_BASE_URL.rstrip('/')}/workflows/{job_id}/download/bbox",
                "colormap": details_obj.get("colormap", "viridis")
            }

    processing_progress = {
        "total_tiles": _safe_int_from_sources(item, details_obj, "total_tiles"),
        "processed_tiles": _safe_int_from_sources(item, details_obj, "processed_tiles"),
        "operation": details_obj.get("operation", "unknown")
    }
    if processing_progress["total_tiles"] > 0:
        percentage = (processing_progress["processed_tiles"] / processing_progress["total_tiles"]) * 100
        processing_progress["percentage"] = round(percentage, 2)
    else:
        processing_progress["percentage"] = 0.0

    if mosaic_info and mosaic_info.get("is_valid"):
        human_status = "✅ Mosaic ready - visualization and bbox download available" if VISUALIZATION_ENABLED else "✅ Mosaic ready"
    elif mosaic_info and not mosaic_info.get("is_valid"):
        human_status = "⚠️ Mosaic created but validation failed"
    elif processing_progress["total_tiles"] > 0:
        pct = processing_progress.get("percentage", 0)
        human_status = f"⚙️ Processing tiles ({pct:.1f}% complete)"
    else:
        human_status = f"Status: {status_val}"

    response = {
        "job_id": job_id,
        "status": status_val,
        "human_status": human_status,
        "processing_progress": processing_progress,
        "mosaic_info": mosaic_info,
        "watcher_active": watcher_active,
        "timestamp": datetime.utcnow().isoformat()
    }
    if visualization_info:
        response["visualization"] = visualization_info
    return response

# -------------------------
# Broadcast helper
# -------------------------
async def broadcast_job_update(job_id: str):
    try:
        status_data = await get_job_status_data_async(job_id)
        await ws_manager.send_update(job_id, {"type": "status_update", "data": status_data})
    except Exception as e:
        logger.exception(f"broadcast_job_update failed for job {job_id}: {e}")
        try:
            await ws_manager.send_update(job_id, {"type": "error", "message": str(e)})
        except Exception:
            logger.exception("Failed to notify ws clients about broadcast error")

# -------------------------
# Async watcher task
# -------------------------
async def _job_watcher_async(job_id: str, poll_interval: float, timeout: float):
    logger.info(f"Async watcher started for job_id={job_id}")
    start_ts = time.time()
    check_count = 0
    merge_triggered = False
    last_processed_count = -1

    try:
        while True:
            check_count += 1
            elapsed = time.time() - start_ts
            if elapsed > timeout:
                logger.warning(f"Watcher timeout for job {job_id} after {elapsed:.1f}s")
                break

            try:
                resp = await dynamodb_get_item(job_id)
            except Exception as e:
                logger.exception(f"Error reading job {job_id} from DynamoDB: {e}")
                await asyncio.sleep(poll_interval)
                continue

            item = resp.get("Item")
            if not item:
                logger.debug(f"Check #{check_count}: job {job_id} not found")
                await asyncio.sleep(poll_interval)
                continue

            mosaic_path = extract_mosaic_path(item)
            details_obj = parse_details_field(item.get("details"))
            total_tiles = _safe_int_from_sources(item, details_obj, "total_tiles")
            processed_tiles = _safe_int_from_sources(item, details_obj, "processed_tiles")

            logger.debug(f"Watcher check #{check_count} for job={job_id}: processed_tiles={processed_tiles} total_tiles={total_tiles}")

            if processed_tiles != last_processed_count:
                try:
                    await broadcast_job_update(job_id)
                    logger.info(f"Broadcasted progress for job {job_id}: {processed_tiles}/{total_tiles}")
                except Exception:
                    logger.exception(f"Failed broadcasting update for {job_id}")
                last_processed_count = processed_tiles

            if total_tiles > 0 and processed_tiles >= total_tiles and not merge_triggered and not mosaic_path:
                logger.info(f"Triggering merge for job {job_id}")
                merge_triggered = True

                operation = details_obj.get("operation")
                if not operation:
                    tasks = item.get("tasks", [])
                    if tasks and isinstance(tasks, list) and isinstance(tasks[0], dict):
                        operation = tasks[0].get("operation")

                payload = {
                    "action": "merge_tiles",
                    "job_id": job_id,
                    "operation": operation or "band_math",
                    "store_output_path": True,
                    "total_tiles": total_tiles
                }

                try:
                    try:
                        await ws_manager.send_update(job_id, {"type": "merge_started", "message": "Merging tiles into final mosaic"})
                    except Exception:
                        logger.exception("Failed to send merge_started update")

                    lambda_resp = await lambda_invoke(payload)
                    payload_stream = lambda_resp.get("Payload")
                    if payload_stream:
                        try:
                            resp_text = await asyncio.to_thread(payload_stream.read)
                            if isinstance(resp_text, bytes):
                                resp_text = resp_text.decode("utf-8", errors="ignore")
                            logger.info(f"Merge lambda response for {job_id}: {resp_text}")
                        except Exception:
                            logger.exception("Failed to read lambda response payload")

                    await asyncio.sleep(1.5)
                    await broadcast_job_update(job_id)

                except Exception:
                    logger.exception(f"Merge lambda invocation failed for job {job_id}")
                    try:
                        await ws_manager.send_update(job_id, {"type": "error", "message": "Merge operation failed"})
                    except Exception:
                        logger.exception("Failed to notify clients of merge failure")
                break

            if mosaic_path:
                logger.info(f"Mosaic ready for job {job_id}")
                try:
                    await broadcast_job_update(job_id)
                except Exception:
                    logger.exception("Failed to broadcast final mosaic ready update")
                break

            await asyncio.sleep(poll_interval)

    except asyncio.CancelledError:
        logger.info(f"Watcher task cancelled for job {job_id}")
    except Exception:
        logger.exception(f"Unhandled exception in watcher for job {job_id}")
    finally:
        async with _JOB_WATCHERS_LOCK:
            _JOB_WATCHERS.pop(job_id, None)
        logger.info(f"Async watcher stopped for job {job_id}")

async def start_job_watcher_async(job_id: str):
    async with _JOB_WATCHERS_LOCK:
        if job_id in _JOB_WATCHERS:
            logger.info(f"Watcher already active for {job_id}")
            return
        task = asyncio.create_task(_job_watcher_async(job_id, JOB_WATCHER_POLL_INTERVAL, JOB_WATCHER_TIMEOUT), name=f"watcher-{job_id[:8]}")
        _JOB_WATCHERS[job_id] = task
        logger.info(f"Started async watcher task for {job_id}")

def start_job_watcher(job_id: str):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(start_job_watcher_async(job_id))
        return
    asyncio.create_task(start_job_watcher_async(job_id))

# -------------------------
# FastAPI app
# -------------------------
app = FastAPI(title="Enhanced Workflow Backend API with TiTiler", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    """Log configuration on startup"""
    logger.info(f"=== API Configuration ===")
    logger.info(f"AWS_REGION: {AWS_REGION}")
    logger.info(f"API_BASE_URL: {API_BASE_URL}")
    logger.info(f"TITILER_ENDPOINT: {TITILER_ENDPOINT}")
    logger.info(f"PROCESSED_BUCKET: {PROCESSED_BUCKET}")
    logger.info(f"INPUT_BUCKET: {INPUT_BUCKET}")
    logger.info(f"VISUALIZATION_ENABLED: {VISUALIZATION_ENABLED}")
    logger.info(f"========================")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up HTTP client on shutdown"""
    try:
        await http_client.aclose()
    except Exception:
        logger.exception("Error closing HTTP client")

# -------------------------
# WebSocket endpoint
# -------------------------
@app.websocket("/ws/workflows/{job_id}")
async def websocket_workflow_status(websocket: WebSocket, job_id: str):
    await ws_manager.connect(websocket, job_id)
    try:
        await start_job_watcher_async(job_id)
    except Exception:
        logger.exception(f"Failed to ensure watcher for {job_id}")

    periodic_task = None
    try:
        try:
            initial_status = await get_job_status_data_async(job_id)
            await websocket.send_json({"type": "initial_status", "data": initial_status})
        except HTTPException:
            await websocket.send_json({"type": "error", "message": "Workflow not found"})
        except Exception as e:
            logger.exception("Error sending initial status")
            await websocket.send_json({"type": "error", "message": str(e)})

        async def periodic_updates():
            try:
                while True:
                    await asyncio.sleep(WS_PERIODIC_PUSH_INTERVAL)
                    try:
                        current_status = await get_job_status_data_async(job_id)
                        await websocket.send_json({"type": "status_update", "data": current_status})
                    except HTTPException:
                        await websocket.send_json({"type": "error", "message": "Workflow not found"})
                    except Exception:
                        logger.exception("periodic update failed")
            except asyncio.CancelledError:
                logger.debug("periodic_updates cancelled")

        periodic_task = asyncio.create_task(periodic_updates())

        while True:
            try:
                data = await websocket.receive_text()
                try:
                    message = json.loads(data)
                except json.JSONDecodeError:
                    await websocket.send_json({"type": "error", "message": "Invalid JSON format"})
                    continue

                mtype = message.get("type")
                if mtype == "ping":
                    await websocket.send_json({"type": "pong"})
                elif mtype == "request_update":
                    try:
                        current_status = await get_job_status_data_async(job_id)
                        await websocket.send_json({"type": "status_update", "data": current_status})
                    except HTTPException:
                        await websocket.send_json({"type": "error", "message": "Workflow not found"})
                    except Exception as e:
                        await websocket.send_json({"type": "error", "message": str(e)})
                else:
                    await websocket.send_json({"type": "ack", "message": f"unknown type: {mtype}"})
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket error for job {job_id}: {e}")
                try:
                    await websocket.send_json({"type": "error", "message": str(e)})
                except Exception:
                    logger.exception("Failed to send error to websocket")
    finally:
        if periodic_task:
            periodic_task.cancel()
            try:
                await periodic_task
            except Exception:
                pass
        await ws_manager.disconnect(websocket, job_id)

# -------------------------
# REST endpoints (root/health/workflow create/status/download)
# -------------------------
@app.get("/")
def read_root():
    return {
        "service": "Enhanced Workflow Backend API with TiTiler",
        "version": "2.0.0",
        "titiler_endpoint": TITILER_ENDPOINT,
    }

@app.get("/health")
async def health_check():
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "active_watchers": len(_JOB_WATCHERS),
        "websocket_connections": ws_manager.get_total_connections(),
        "services": {"s3": None, "dynamodb": None, "titiler": None}
    }

    # Check S3
    try:
        await s3_head_bucket(PROCESSED_BUCKET)
        health["services"]["s3"] = "accessible"
    except Exception:
        health["services"]["s3"] = "error"
        health["status"] = "degraded"

    # Check DynamoDB
    try:
        _ = table.table_status
        health["services"]["dynamodb"] = "accessible"
    except Exception:
        health["services"]["dynamodb"] = "error"
        health["status"] = "degraded"

    # Check TiTiler (try several candidate health paths)
    try:
        titiler_ok = False
        for health_path in ("/healthz", "/health", "/docs"):
            try:
                url = f"{TITILER_ENDPOINT.rstrip('/')}{health_path}"
                resp = await http_client.get(url, timeout=5.0)
                logger.info(f"TiTiler health check {url} -> {resp.status_code}")
                if resp.status_code == 200:
                    health["services"]["titiler"] = f"accessible ({health_path})"
                    titiler_ok = True
                    break
            except Exception as e:
                logger.debug(f"TiTiler health attempt {health_path} failed: {e}")
        if not titiler_ok:
            health["services"]["titiler"] = "unavailable"
            logger.warning("TiTiler health check failed - service may be unavailable")
    except Exception as e:
        health["services"]["titiler"] = f"error ({str(e)[:50]})"
        logger.warning(f"TiTiler health check error: {e}")

    # Return healthy even if TiTiler is down (it's optional for core functionality)
    return health

@app.post("/workflows", status_code=200)
async def create_workflow(request: WorkflowRequest):
    if not SQS_QUEUE_URL:
        raise HTTPException(status_code=500, detail="SQS_QUEUE_URL not configured")
    try:
        workflow_data = request.dict()
        job_id = str(uuid.uuid4())
        workflow_data["job_id"] = job_id
        await asyncio.to_thread(sqs_client.send_message, QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(workflow_data))
        await start_job_watcher_async(job_id)
        logger.info(f"Workflow queued: {job_id}")
        ws_url = f"ws://{API_BASE_URL.replace('http://','').replace('https://','')}/ws/workflows/{job_id}"
        return {
            "success": True,
            "message": "Workflow submitted. Connect to WebSocket for real-time updates.",
            "job_id": job_id,
            "status": "QUEUED",
            "websocket_url": ws_url,
            "status_url": f"{API_BASE_URL.rstrip('/')}/workflows/{job_id}",
            "download_endpoints": {
                "full_mosaic": f"{API_BASE_URL.rstrip('/')}/workflows/{job_id}/download",
                "bbox_crop": f"{API_BASE_URL.rstrip('/')}/workflows/{job_id}/download/bbox"
            }
        }
    except Exception:
        logger.exception("Failed to create workflow")
        raise HTTPException(status_code=500, detail="Failed to create workflow")

@app.get("/workflows/{job_id}")
async def get_workflow_status(job_id: str):
    try:
        return await get_job_status_data_async(job_id)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Error fetching workflow status")
        raise HTTPException(status_code=500, detail="Error fetching workflow status")

@app.get("/workflows/{job_id}/download")
async def get_download_url(job_id: str):
    """Get presigned URL for downloading the full mosaic"""
    try:
        resp = await dynamodb_get_item(job_id)
        if "Item" not in resp:
            raise HTTPException(status_code=404, detail="Workflow not found")
        item = resp["Item"]
        mosaic_path = extract_mosaic_path(item)
        if not mosaic_path:
            raise HTTPException(status_code=404, detail="Mosaic not created yet")
        bucket, key = parse_s3_uri(mosaic_path)
        try:
            await s3_head_object(bucket, key)
        except Exception:
            raise HTTPException(status_code=404, detail=f"Mosaic file not found in S3: s3://{bucket}/{key}")
        url = await s3_generate_presigned_get(bucket, key, expires_in=3600)
        details_obj = parse_details_field(item.get("details"))
        is_valid_flag = details_obj.get("mosaic_is_valid", True)
        size_mb = 0.0
        try:
            size_bytes = (await s3_head_object(bucket, key)).get("ContentLength", 0)
            size_mb = round(size_bytes / (1024 * 1024), 2)
        except Exception:
            pass
        return {
            "download_url": url,
            "expires_in": 3600,
            "file_size_mb": size_mb,
            "is_complete": bool(is_valid_flag),
            "s3_path": mosaic_path,
            "format": "GeoTIFF",
            "note": "This downloads the full mosaic. Use /download/bbox for specific areas."
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to produce download URL")
        raise HTTPException(status_code=500, detail="Failed to produce download URL")

# -------------------------
# Bounding Box Download Endpoints (TiTiler-backed)
# -------------------------
@app.post("/workflows/{job_id}/download/bbox")
async def download_bbox_post(job_id: str, bbox_request: BoundingBoxRequest):
    try:
        bucket, key = await get_mosaic_path_from_job_async(job_id)
        s3_url = f"s3://{bucket}/{key}"

        # Validate bbox
        if bbox_request.min_lon >= bbox_request.max_lon:
            raise HTTPException(status_code=400, detail="min_lon must be less than max_lon")
        if bbox_request.min_lat >= bbox_request.max_lat:
            raise HTTPException(status_code=400, detail="min_lat must be less than max_lat")

        fmt = bbox_request.format.lower()
        if fmt not in {"geotiff", "png"}:
            raise HTTPException(status_code=400, detail="format must be 'geotiff' or 'png'")

        bbox = (bbox_request.min_lon, bbox_request.min_lat, bbox_request.max_lon, bbox_request.max_lat)
        logger.info(f"Requesting bbox crop for job {job_id}: {bbox} format={fmt}")

        # Try TiTiler first, fall back to direct rasterio processing
        try:
            image_data = await request_bbox_from_titiler(
                s3_url=s3_url,
                bbox=bbox,
                output_format=fmt,
                width=bbox_request.width,
                height=bbox_request.height,
                max_size=bbox_request.max_size or 1024,
                colormap=bbox_request.colormap,
                rescale=bbox_request.rescale
            )
            logger.info(f"Bbox crop successful using TiTiler for job {job_id}")
        except HTTPException as e:
            if e.status_code in (502, 503):
                # TiTiler unavailable, use fallback
                logger.info(f"TiTiler unavailable, using fallback processing for job {job_id}")
                image_data = await request_bbox_fallback(
                    s3_url=s3_url,
                    bbox=bbox,
                    output_format=fmt,
                    width=bbox_request.width,
                    height=bbox_request.height,
                    max_size=bbox_request.max_size or 1024,
                    colormap=bbox_request.colormap,
                    rescale=bbox_request.rescale
                )
            else:
                raise

        if fmt == "png":
            media_type = "image/png"
            ext = "png"
        else:
            media_type = "image/tiff"
            ext = "tif"

        sanitized = f"{job_id}_bbox_{bbox_request.min_lon}_{bbox_request.min_lat}_{bbox_request.max_lon}_{bbox_request.max_lat}".replace(" ", "_")
        filename = f"{sanitized}.{ext}"

        return Response(
            content=image_data,
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Bounding-Box": f"{bbox_request.min_lon},{bbox_request.min_lat},{bbox_request.max_lon},{bbox_request.max_lat}",
                "X-Job-ID": job_id
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to process bbox download for job {job_id}")
        raise HTTPException(status_code=500, detail=f"Failed to process bbox download: {str(e)}")

@app.get("/workflows/{job_id}/download/bbox")
async def download_bbox_get(
    job_id: str,
    min_lon: float = Query(...),
    min_lat: float = Query(...),
    max_lon: float = Query(...),
    max_lat: float = Query(...),
    format: str = Query("geotiff"),
    width: Optional[int] = Query(None),
    height: Optional[int] = Query(None),
    max_size: int = Query(1024),
    colormap: str = Query("viridis"),
    rescale: Optional[str] = Query(None)
):
    bbox_request = BoundingBoxRequest(
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        format=format,
        width=width,
        height=height,
        max_size=max_size,
        colormap=colormap,
        rescale=rescale
    )
    return await download_bbox_post(job_id, bbox_request)

@app.get("/workflows/{job_id}/bbox/info")
async def get_bbox_info(job_id: str):
    """Get bbox info using TiTiler"""
    try:
        bucket, key = await get_mosaic_path_from_job_async(job_id)
        s3_path = f"s3://{bucket}/{key}"

        # Request info from TiTiler
        titiler_url = f"{TITILER_ENDPOINT}/cog/info"
        params = {"url": s3_path}
        
        try:
            resp = await http_client.get(titiler_url, params=params, timeout=30.0)
            if resp.status_code == 200:
                info = resp.json()
                bounds = info["bounds"]
                center_lon = (bounds[0] + bounds[2]) / 2
                center_lat = (bounds[1] + bounds[3]) / 2
                lon_extent = bounds[2] - bounds[0]
                lat_extent = bounds[3] - bounds[1]

                quarter_box = {
                    "min_lon": center_lon - lon_extent / 4,
                    "min_lat": center_lat - lat_extent / 4,
                    "max_lon": center_lon + lon_extent / 4,
                    "max_lat": center_lat + lat_extent / 4,
                    "description": "Center quarter of the mosaic"
                }

                return {
                    "job_id": job_id,
                    "full_extent": {"min_lon": bounds[0], "min_lat": bounds[1], "max_lon": bounds[2], "max_lat": bounds[3]},
                    "center": {"lon": center_lon, "lat": center_lat},
                    "dimensions": {"width": info["width"], "height": info["height"], "lon_extent": round(lon_extent, 6), "lat_extent": round(lat_extent, 6)},
                    "crs": info.get("crs", "EPSG:4326"),
                    "example_bounding_boxes": {"full": {"min_lon": bounds[0], "min_lat": bounds[1], "max_lon": bounds[2], "max_lat": bounds[3], "description": "Full mosaic extent"}, "center_quarter": quarter_box},
                    "download_url_template": f"{API_BASE_URL.rstrip('/')}/workflows/{job_id}/download/bbox?min_lon={{min_lon}}&min_lat={{min_lat}}&max_lon={{max_lon}}&max_lat={{max_lat}}&format={{format}}",
                    "supported_formats": ["geotiff", "png"],
                    "note": "Use POST /workflows/{job_id}/download/bbox for more options"
                }
            else:
                raise HTTPException(status_code=502, detail=f"TiTiler info request failed: {resp.status_code}")
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timeout")
        except Exception as e:
            logger.exception("TiTiler info request failed")
            raise HTTPException(status_code=502, detail=f"TiTiler error: {str(e)}")

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to get bbox info for job {job_id}")
        raise HTTPException(status_code=500, detail=f"Failed to get bbox info: {str(e)}")

# -------------------------
# Watcher status & websocket connection endpoints
# -------------------------
@app.get("/watchers/status")
async def watchers_status():
    async with _JOB_WATCHERS_LOCK:
        watchers = {jid: {"task_name": t.get_name(), "done": t.done(), "cancelled": t.cancelled()} for jid, t in _JOB_WATCHERS.items()}
    return {"total_active": len(watchers), "watchers": watchers, "websocket_connections": ws_manager.get_total_connections(), "config": {"poll_interval_sec": JOB_WATCHER_POLL_INTERVAL, "timeout_sec": JOB_WATCHER_TIMEOUT, "min_tiles": MIN_TILES_FOR_MERGE}}

@app.get("/ws/connections")
async def websocket_connections():
    return {"total_connections": ws_manager.get_total_connections(), "jobs_with_connections": len(ws_manager.active_connections), "details": {job_id: ws_manager.get_connection_count(job_id) for job_id in ws_manager.active_connections.keys()}}

# -------------------------
# Visualization endpoints using TiTiler
# -------------------------

@app.get("/viz/{job_id}/tiles/{z}/{x}/{y}.png")
async def get_tile(job_id: str, z: int, x: int, y: int, colormap: str = Query("viridis"), rescale: Optional[str] = Query(None), tileMatrixSetId: str = Query("WebMercatorQuad")):
    """Get map tile using TiTiler"""
    try:
        bucket, key = await get_mosaic_path_from_job_async(job_id)
        s3_path = f"s3://{bucket}/{key}"
        
        # Request tile from TiTiler - requires tileMatrixSetId in path
        titiler_url = f"{TITILER_ENDPOINT}/cog/tiles/{tileMatrixSetId}/{z}/{x}/{y}.png"
        params = {
            "url": s3_path,
            "colormap_name": colormap
        }
        if rescale:
            params["rescale"] = rescale
        
        try:
            resp = await http_client.get(titiler_url, params=params, timeout=30.0)
            if resp.status_code == 200:
                return Response(
                    content=resp.content,
                    media_type="image/png",
                    headers={
                        "Cache-Control": "public, max-age=3600",
                        "Access-Control-Allow-Origin": "*"
                    }
                )
            elif resp.status_code == 404:
                # Tile outside bounds - return empty tile
                from io import BytesIO
                from PIL import Image
                empty_tile = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
                buf = BytesIO()
                empty_tile.save(buf, format="PNG")
                return Response(content=buf.getvalue(), media_type="image/png")
            else:
                raise HTTPException(status_code=502, detail=f"TiTiler tile request failed: {resp.status_code}")
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timeout")
        except Exception as e:
            logger.exception("TiTiler tile request failed")
            raise HTTPException(status_code=502, detail=f"TiTiler error: {str(e)}")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Tile generation failed")
        raise HTTPException(status_code=500, detail="Tile generation failed")

@app.get("/viz/{job_id}/preview.png")
async def get_preview(job_id: str, colormap: str = Query("viridis"), rescale: Optional[str] = Query(None), max_size: int = Query(512)):
    """Get preview image using TiTiler"""
    try:
        bucket, key = await get_mosaic_path_from_job_async(job_id)
        s3_path = f"s3://{bucket}/{key}"
        
        # Request preview from TiTiler
        titiler_url = f"{TITILER_ENDPOINT}/cog/preview.png"
        params = {
            "url": s3_path,
            "max_size": max_size,
            "colormap_name": colormap
        }
        if rescale:
            params["rescale"] = rescale
        
        try:
            resp = await http_client.get(titiler_url, params=params, timeout=60.0)
            if resp.status_code == 200:
                return Response(
                    content=resp.content,
                    media_type="image/png",
                    headers={
                        "Cache-Control": "public, max-age=3600",
                        "Access-Control-Allow-Origin": "*"
                    }
                )
            else:
                raise HTTPException(status_code=502, detail=f"TiTiler preview request failed: {resp.status_code}")
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timeout")
        except Exception as e:
            logger.exception("TiTiler preview request failed")
            raise HTTPException(status_code=502, detail=f"TiTiler error: {str(e)}")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Preview generation failed")
        raise HTTPException(status_code=500, detail="Preview generation failed")

@app.get("/viz/{job_id}/info")
async def get_metadata(job_id: str):
    """Get metadata using TiTiler"""
    try:
        bucket, key = await get_mosaic_path_from_job_async(job_id)
        s3_path = f"s3://{bucket}/{key}"
        
        # Request info from TiTiler
        titiler_url = f"{TITILER_ENDPOINT}/cog/info"
        params = {"url": s3_path}
        
        try:
            resp = await http_client.get(titiler_url, params=params, timeout=30.0)
            if resp.status_code == 200:
                info = resp.json()
                return {
                    "job_id": job_id,
                    "s3_path": s3_path,
                    "width": info["width"],
                    "height": info["height"],
                    "bounds": info["bounds"],
                    "center": [(info["bounds"][0] + info["bounds"][2]) / 2, (info["bounds"][1] + info["bounds"][3]) / 2],
                    "minzoom": info.get("minzoom", 0),
                    "maxzoom": info.get("maxzoom", 24),
                    "band_count": info.get("band_count", 1),
                    "dtype": info.get("dtype", "uint16"),
                    "nodata": info.get("nodata"),
                    "crs": info.get("crs"),
                }
            else:
                raise HTTPException(status_code=502, detail=f"TiTiler info request failed: {resp.status_code}")
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timeout")
        except Exception as e:
            logger.exception("TiTiler info request failed")
            raise HTTPException(status_code=502, detail=f"TiTiler error: {str(e)}")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Metadata fetch failed")
        raise HTTPException(status_code=500, detail="Metadata fetch failed")

@app.get("/viz/{job_id}/statistics")
async def get_statistics(job_id: str):
    """Get raster statistics using TiTiler"""
    try:
        bucket, key = await get_mosaic_path_from_job_async(job_id)
        s3_path = f"s3://{bucket}/{key}"
        
        # Request statistics from TiTiler
        titiler_url = f"{TITILER_ENDPOINT}/cog/statistics"
        params = {"url": s3_path}
        
        try:
            resp = await http_client.get(titiler_url, params=params, timeout=30.0)
            if resp.status_code == 200:
                stats_data = resp.json()
                # TiTiler returns statistics per band
                band_stats = stats_data.get("1", stats_data.get("b1", {}))
                
                return {
                    "job_id": job_id,
                    "statistics": {
                        "min": band_stats.get("min", 0.0),
                        "max": band_stats.get("max", 0.0),
                        "mean": band_stats.get("mean", 0.0),
                        "std": band_stats.get("std", 0.0),
                        "percentiles": {
                            "2": band_stats.get("percentile_2", 0.0),
                            "98": band_stats.get("percentile_98", 0.0)
                        }
                    },
                    "recommended_rescale": f"{band_stats.get('percentile_2', 0.0):.2f},{band_stats.get('percentile_98', 0.0):.2f}"
                }
            else:
                raise HTTPException(status_code=502, detail=f"TiTiler statistics request failed: {resp.status_code}")
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timeout")
        except Exception as e:
            logger.exception("TiTiler statistics request failed")
            raise HTTPException(status_code=502, detail=f"TiTiler error: {str(e)}")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Statistics fetch failed")
        raise HTTPException(status_code=500, detail="Statistics failed")

@app.get("/viz/{job_id}/tilejson")
async def get_tilejson(job_id: str, colormap: str = Query("viridis"), rescale: Optional[str] = Query(None), tileMatrixSetId: str = Query("WebMercatorQuad")):
    """Get TileJSON using TiTiler"""
    try:
        bucket, key = await get_mosaic_path_from_job_async(job_id)
        s3_path = f"s3://{bucket}/{key}"
        
        # Request tilejson from TiTiler - requires tileMatrixSetId in path
        titiler_url = f"{TITILER_ENDPOINT}/cog/{tileMatrixSetId}/tilejson.json"
        params = {
            "url": s3_path,
            "colormap_name": colormap
        }
        if rescale:
            params["rescale"] = rescale
        
        try:
            resp = await http_client.get(titiler_url, params=params, timeout=30.0)
            if resp.status_code == 200:
                tilejson = resp.json()
                # Update tiles URL to point to our API endpoint instead of TiTiler
                tiles_url = f"{API_BASE_URL.rstrip('/')}/viz/{job_id}/tiles/{{z}}/{{x}}/{{y}}.png?colormap={colormap}"
                if rescale:
                    tiles_url += f"&rescale={rescale}"
                tilejson["tiles"] = [tiles_url]
                tilejson["name"] = f"Job {job_id}"
                return tilejson
            else:
                raise HTTPException(status_code=502, detail=f"TiTiler tilejson request failed: {resp.status_code}")
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="TiTiler request timeout")
        except Exception as e:
            logger.exception("TiTiler tilejson request failed")
            raise HTTPException(status_code=502, detail=f"TiTiler error: {str(e)}")
    except HTTPException:
        raise
    except Exception:
        logger.exception("TileJSON generation failed")
        raise HTTPException(status_code=500, detail="TileJSON generation failed")

@app.api_route("/titiler/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def titiler_proxy(path: str, request: Request):
    """Proxy any TiTiler request for external access via Postman/browser"""
    try:
        # Build TiTiler URL
        titiler_url = f"{TITILER_ENDPOINT}/{path}"
        
        # Forward query parameters
        query_params = dict(request.query_params)
        
        # Forward request to TiTiler
        method = request.method.lower()
        headers = dict(request.headers)
        headers.pop("host", None)  # Remove host header
        
        if method == "get":
            resp = await http_client.get(titiler_url, params=query_params, headers=headers, timeout=60.0)
        elif method == "post":
            body = await request.body()
            resp = await http_client.post(titiler_url, params=query_params, content=body, headers=headers, timeout=60.0)
        else:
            raise HTTPException(status_code=405, detail=f"Method {method} not supported")
        
        # Return TiTiler response
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=dict(resp.headers),
            media_type=resp.headers.get("content-type", "application/json")
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="TiTiler request timeout")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("TiTiler proxy failed")
        raise HTTPException(status_code=502, detail=f"TiTiler proxy error: {str(e)}")

# -------------------------
# S3 browsing endpoints (root/list/presign)
# -------------------------
async def list_top_level_prefixes(bucket: str, exclude: Optional[Set[str]] = None) -> List[str]:
    if exclude is None:
        exclude = set()
    resp = await _s3_list_objects_v2(bucket, prefix="", delimiter="/")
    prefixes = []
    for p in resp.get("CommonPrefixes", []):
        pref = p.get("Prefix")
        if pref and pref not in exclude:
            prefixes.append(pref)
    return prefixes

async def list_prefix_contents(bucket: str, prefix: str = "", max_keys: int = 1000) -> dict:
    result = {"prefix": prefix, "folders": [], "objects": []}
    continuation = None
    total_fetched = 0
    while True:
        resp = await _s3_list_objects_v2(bucket, prefix=prefix, delimiter="/", continuation_token=continuation, max_keys=1000)
        for cp in resp.get("CommonPrefixes", []):
            p = cp.get("Prefix")
            if p:
                result["folders"].append(p)
        for obj in resp.get("Contents", []):
            key = obj.get("Key")
            if not key:
                continue
            if key.endswith("/") and key == prefix:
                continue
            result["objects"].append({
                "key": key,
                "size_bytes": obj.get("Size", 0),
                "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
                "storage_class": obj.get("StorageClass")
            })
            total_fetched += 1
            if max_keys and total_fetched >= max_keys:
                break
        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
            if max_keys and total_fetched >= max_keys:
                break
            continue
        break
    return result

@app.get("/s3/root")
async def s3_list_root(exclude: Optional[str] = Query(None, description="Comma separated prefixes to exclude")):
    bucket = INPUT_BUCKET
    exclude_set = set(EXCLUDED_ROOT_PREFIXES)
    if exclude:
        for e in exclude.split(","):
            e = e.strip()
            if not e:
                continue
            if not e.endswith("/"):
                e = e + "/"
            exclude_set.add(e)
    try:
        prefixes = await list_top_level_prefixes(bucket, exclude=exclude_set)
        return {"bucket": bucket, "excluded": sorted(list(exclude_set)), "prefixes": prefixes}
    except ClientError as ce:
        logger.exception("S3 error listing root prefixes")
        raise HTTPException(status_code=502, detail=f"S3 error: {ce}")
    except Exception:
        logger.exception("Unexpected error listing S3 root")
        raise HTTPException(status_code=500, detail="Failed to list S3 root prefixes")

@app.get("/s3/list")
async def s3_list(prefix: Optional[str] = Query("", description="Prefix to list"), max_keys: int = Query(1000, ge=1, le=10000)):
    bucket = INPUT_BUCKET
    normalized_prefix = prefix or ""
    try:
        contents = await list_prefix_contents(bucket, prefix=normalized_prefix, max_keys=max_keys)
        return {"bucket": bucket, "prefix": normalized_prefix, "contents": contents}
    except ClientError as ce:
        logger.exception("S3 error listing prefix")
        raise HTTPException(status_code=502, detail=f"S3 error: {ce}")
    except Exception:
        logger.exception("Unexpected error listing S3 prefix")
        raise HTTPException(status_code=500, detail="Failed to list S3 prefix")

@app.get("/s3/presign")
async def s3_presign(key: str = Query(..., description="S3 key to presign"), expires_in: int = Query(3600, ge=60, le=86400)):
    bucket = INPUT_BUCKET
    try:
        try:
            await s3_head_object(bucket, key)
        except Exception:
            raise HTTPException(status_code=404, detail=f"Object not found: s3://{bucket}/{key}")
        url = await s3_generate_presigned_get(bucket, key, expires_in=expires_in)
        return {"bucket": bucket, "key": key, "url": url, "expires_in": expires_in}
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to create presigned URL")
        raise HTTPException(status_code=500, detail="Failed to create presigned URL")

@app.get("/s3/read")
async def s3_read_object(
    key: str = Query(..., description="S3 object key to read")
):
    """Read any S3 object content (JSON, text, etc.) from the configured INPUT_BUCKET"""
    bucket = INPUT_BUCKET
    try:
        # Get the object from S3
        response = await s3_get_object(bucket, key)
        body = response.get("Body")
        
        if not body:
            raise HTTPException(status_code=404, detail=f"Object not found: s3://{bucket}/{key}")
        
        # Read the content (body.read() is synchronous)
        content_bytes = body.read()
        content_type = response.get("ContentType", "application/octet-stream")
        
        # Try to parse as JSON if it's a JSON file
        if key.endswith(".json") or "json" in content_type:
            try:
                content_str = content_bytes.decode("utf-8")
                json_data = json.loads(content_str)
                return {
                    "bucket": bucket,
                    "key": key,
                    "content_type": content_type,
                    "size_bytes": len(content_bytes),
                    "data": json_data
                }
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        
        # For text files, return as string
        if key.endswith((".txt", ".csv", ".log", ".md")) or "text" in content_type:
            try:
                content_str = content_bytes.decode("utf-8")
                return {
                    "bucket": bucket,
                    "key": key,
                    "content_type": content_type,
                    "size_bytes": len(content_bytes),
                    "data": content_str
                }
            except UnicodeDecodeError:
                pass
        
        # For binary files, return base64 encoded
        import base64
        return {
            "bucket": bucket,
            "key": key,
            "content_type": content_type,
            "size_bytes": len(content_bytes),
            "data": base64.b64encode(content_bytes).decode("utf-8"),
            "encoding": "base64"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to read S3 object: s3://{bucket}/{key}")
        raise HTTPException(status_code=500, detail=f"Failed to read S3 object: {str(e)}")

# -------------------------
# Colormaps & Titiler config endpoints
# -------------------------
@app.get("/colormaps")
async def get_colormaps():
    return {
        "recommended_by_operation": {"ndvi": ["RdYlGn", "YlGn", "Greens"], "ndwi": ["Blues", "YlGnBu", "PuBu"], "evi": ["RdYlGn", "YlGn"], "temperature": ["RdYlBu_r", "coolwarm"], "general": ["viridis", "plasma", "inferno"]},
        "all_available": ["viridis", "plasma", "inferno", "magma", "cividis", "Greys", "Purples", "Blues", "Greens", "Oranges", "Reds", "YlOrBr", "YlOrRd", "OrRd", "PuRd", "RdPu"],
        "rescale_suggestions": {"ndvi": "-1,1", "ndwi": "-1,1", "evi": "-1,1", "temperature": "Use statistics endpoint"}
    }

@app.get("/titiler/config")
async def get_titiler_config():
    return {
        "endpoint": TITILER_ENDPOINT,
        "timeout_seconds": TITILER_TIMEOUT,
        "supported_formats": ["geotiff", "png", "jpeg", "webp"],
        "supported_operations": ["crop (bounding box)", "preview", "tiles", "statistics", "point query"],
        "documentation": "https://developmentseed.org/titiler/"
    }

# -------------------------
# Local run
# -------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))