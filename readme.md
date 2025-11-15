# On-The-Fly Geospatial Processor

[![AWS ECS](https://img.shields.io/badge/AWS-ECS-FF9900?logo=amazon-aws)](https://aws.amazon.com/ecs/)
[![TiTiler](https://img.shields.io/badge/TiTiler-Latest-green)](https://github.com/developmentseed/titiler)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109+-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://www.python.org/)

A scalable, cloud-native geospatial data processing pipeline for INSAT satellite imagery. Processes Cloud Optimized GeoTIFFs (COGs) on-demand with real-time visualization, bounding box extraction, and distributed tile processing.

## 🚀 Features

### Core Capabilities
- **On-Demand Processing**: Submit workflows via REST API, process tiles in parallel using AWS Lambda
- **Real-Time Updates**: WebSocket-based progress tracking with automatic status broadcasting
- **Distributed Tile Processing**: Parallel processing of satellite imagery tiles across AWS Lambda functions
- **Automatic Mosaic Generation**: Merges processed tiles into unified GeoTIFF mosaics

### Visualization & Export
- **Interactive Tile Server**: XYZ tile service powered by TiTiler for web map integration
- **Bounding Box Extraction**: Download any geographic area as GeoTIFF or PNG with custom colormaps
- **Preview Generation**: Quick overview images with configurable resolution and styling
- **Statistics & Metadata**: Raster statistics, band information, and spatial metadata endpoints

### Cloud Architecture
- **Serverless Processing**: AWS Lambda for scalable tile processing
- **Container Orchestration**: ECS Fargate for API and TiTiler services with auto-scaling
- **Service Discovery**: AWS Cloud Map for internal service-to-service communication
- **Load Balancing**: Application Load Balancer with health checks and SSL support
- **Object Storage**: S3 for COG storage and processed results with lifecycle policies

## 📋 Prerequisites

- **AWS Account** with permissions for ECS, Lambda, S3, DynamoDB, SQS, CloudFormation
- **Docker Desktop** (for local development and image building)
- **AWS CLI** v2 configured with credentials
- **Python 3.11+** (for Lambda functions and API)
- **PowerShell** (for deployment scripts on Windows)

## 🏗️ Architecture

```
┌─────────────────┐
│  Load Balancer  │ ← Public HTTP endpoint
└────────┬────────┘
         │
    ┌────┴─────┬──────────────┐
    │          │              │
┌───▼───┐  ┌───▼────┐    ┌───▼────────┐
│  API  │  │ TiTiler│    │  WebSocket │
│ (ECS) │  │ (ECS)  │    │   Client   │
└───┬───┘  └───┬────┘    └────────────┘
    │          │
    │   Service Discovery
    │   (Cloud Map)
    │          │
┌───▼──────────▼────┐
│    S3 Buckets     │
│ ├─ Input COGs     │
│ └─ Processed      │
└───────┬───────────┘
        │
┌───────▼───────────┐
│  Lambda Functions │
│ ├─ Receiver       │
│ ├─ Processor      │
│ └─ Translator     │
└───────┬───────────┘
        │
┌───────▼───────────┐
│    DynamoDB       │
│  (Job Tracking)   │
└───────────────────┘
```

## 🔄 How It Works

### Workflow Processing Pipeline

The system processes geospatial data through a distributed, event-driven pipeline:

#### 1. **Workflow Submission**
```
Client → API (POST /workflows) → Receiver Lambda → SQS Queue
```
- User submits workflow with operations (band math, NDVI, custom expressions)
- API creates job record in DynamoDB with status `QUEUED`
- Receiver Lambda validates input manifest and queues tasks

#### 2. **Distributed Tile Processing**
```
SQS → Processor Lambda (parallel) → S3 (processed tiles)
```
- Each tile processed independently by Lambda function
- Operations applied: band math, filtering, transformations
- Processed tiles written to S3 with metadata
- Progress updates written to DynamoDB

**Example**: Processing 100 tiles
- Lambda concurrency: 10 functions run in parallel
- Each Lambda processes 10 tiles sequentially
- Total time: ~30 seconds (vs 5+ minutes sequential)

#### 3. **Mosaic Generation**
```
All tiles complete → Translator Lambda → Merge tiles → Final mosaic
```
- Triggered when all tiles reach `COMPLETED` status
- Uses GDAL to merge tiles into single GeoTIFF
- Applies compression and optimization
- Updates job status to `COMPLETED`

#### 4. **Visualization & Export**
```
Mosaic ready → TiTiler service → Dynamic tiles/crops/previews
```
- TiTiler reads mosaic directly from S3
- Generates tiles on-the-fly for web maps
- Supports bbox extraction with custom colormaps
- No pre-processing required

### Real-Time Monitoring

**WebSocket Updates:**
```javascript
// Client connects
ws://api-lb/ws/workflows/{job_id}

// Receives updates
{
  "type": "status_update",
  "data": {
    "status": "PROCESSING",
    "tiles_processed": 45,
    "tiles_total": 100,
    "progress_percent": 45
  }
}
```

**Status Flow:**
```
QUEUED → PROCESSING → MERGING → COMPLETED
   ↓         ↓           ↓
FAILED ← FAILED  ← FAILED
```

### Data Flow Example

**Input:** INSAT-3D satellite data with 4 spectral bands
```json
{
  "tiles": [
    {"key": "tile_0_0.tif", "bounds": [68, 8, 70, 10]},
    {"key": "tile_0_1.tif", "bounds": [70, 8, 72, 10]},
    ...
  ]
}
```

**Processing:** Calculate NDVI index
```python
# Processor Lambda
ndvi = (NIR - RED) / (NIR + RED)
# Output: tile_0_0_processed.tif
```

**Output:** Merged mosaic with visualization
```
mosaic.tif → TiTiler → XYZ tiles/{z}/{x}/{y}.png
                     → BBox extraction
                     → Statistics & metadata
```

### Key Design Decisions

**1. Why Lambda for Processing?**
- ✅ Automatic scaling (0-1000+ concurrent executions)
- ✅ Pay-per-use (no idle container costs)
- ✅ Built-in retry logic
- ✅ Handles variable workloads

**2. Why ECS for API/TiTiler?**
- ✅ Long-running WebSocket connections
- ✅ Complex dependencies (GDAL, rasterio)
- ✅ Stateful caching for TiTiler
- ✅ Service discovery integration

**3. Why COG (Cloud Optimized GeoTIFF)?**
- ✅ Random access without full download
- ✅ Internal tiling enables parallel processing
- ✅ HTTP range requests for efficiency
- ✅ Industry standard format

## 🛠️ Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd on-the-fly-processor
```

### 2. Configure AWS Infrastructure
```bash
# Deploy CloudFormation stack
aws cloudformation create-stack \
  --stack-name geospatial-prod-stack \
  --template-body file://cloudformation-template.yml \
  --capabilities CAPABILITY_IAM \
  --region ap-south-1
```

### 3. Build Lambda Layer (Required)
```bash
cd aws-init

# Create the layer directory and install dependencies
mkdir -p layer/python
pip install -r ../layer-requirements.txt -t layer/python

# For GDAL binaries (Linux/WSL)
# Download pre-built GDAL binaries or build from source
# Place in layer/bin/

# Create Lambda layer
./init-lambda.sh

# Upload sample data to S3
python upload_data.py
```

**Note**: The `layer/` folder is not included in git due to size. Build it locally using the requirements file.

### 4. Build and Deploy Docker Images

#### API Service
```powershell
# Build
docker build -f api.Dockerfile -t geospatial-api:latest .

# Tag and push to ECR (replace YOUR_ACCOUNT_ID and YOUR_REGION)
aws ecr get-login-password --region YOUR_REGION | docker login --username AWS --password-stdin YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com
docker tag geospatial-api:latest YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com/geospatial-api:latest
docker push YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com/geospatial-api:latest
```

#### TiTiler Service
```powershell
# Deploy TiTiler using pre-built script
./deploy-titiler.ps1
```

### 5. Deploy ECS Services
```bash
# Register task definitions
aws ecs register-task-definition --cli-input-json file://ecs-api-task-definition.json
aws ecs register-task-definition --cli-input-json file://ecs-titiler-task-definition.json

# Create/update services
aws ecs update-service \
  --cluster geospatial-cluster-production \
  --service api-service-production \
  --task-definition geospatial-api-service:latest \
  --force-new-deployment

aws ecs update-service \
  --cluster geospatial-cluster-production \
  --service titiler-service-production \
  --task-definition titiler-service:latest \
  --force-new-deployment
```

## 🚦 Usage

### Base URL
```
http://api-lb-production-716552440.ap-south-1.elb.amazonaws.com
```

### Submit a Workflow
```bash
curl -X POST "http://<alb-url>/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "custom-index-001",
    "dataset_id": "INSAT3D_20251115",
    "tasks": [{
      "task_id": "band_math_1",
      "operation": "band_math",
      "parameters": {
        "expression": "(B1 - B2) / (B1 + B2)",
        "colormap": "viridis"
      },
      "dependencies": []
    }],
    "inputs": {
      "manifest_key": "manifests/INSAT3D_20251115_061713.json"
    }
  }'
```

**Response:**
```json
{
  "success": true,
  "job_id": "d0b2d5df-907b-48cf-ae36-1024e2ec2752",
  "status": "QUEUED",
  "websocket_url": "ws://<alb-url>/ws/workflows/d0b2d5df-907b-48cf-ae36-1024e2ec2752",
  "status_url": "http://<alb-url>/workflows/d0b2d5df-907b-48cf-ae36-1024e2ec2752"
}
```

### Check Job Status
```bash
curl "http://<alb-url>/workflows/{job_id}"
```

### WebSocket Real-Time Updates
```javascript
const ws = new WebSocket('ws://<alb-url>/ws/workflows/{job_id}');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Status:', data);
  // { type: "status_update", data: { job_id, status, processing_progress, mosaic_info } }
};

// Request manual update
ws.send(JSON.stringify({ type: "request_update" }));

// Send heartbeat
ws.send(JSON.stringify({ type: "ping" }));
```

### Download Full Mosaic
```bash
curl "http://<alb-url>/workflows/{job_id}/download" | jq '.download_url'
# Returns presigned S3 URL valid for 1 hour
```

### Extract Bounding Box

#### GeoTIFF Format
```bash
curl -X POST "http://<alb-url>/workflows/{job_id}/download/bbox" \
  -H "Content-Type: application/json" \
  -d '{
    "min_lon": 77.10,
    "min_lat": 22.50,
    "max_lon": 77.40,
    "max_lat": 22.80,
    "format": "geotiff",
    "width": 1024,
    "height": 1024
  }' \
  --output bbox_crop.tif
```

#### PNG with Colormap
```bash
curl -X POST "http://<alb-url>/workflows/{job_id}/download/bbox" \
  -H "Content-Type: application/json" \
  -d '{
    "min_lon": 68.0,
    "min_lat": 8.0,
    "max_lon": 97.0,
    "max_lat": 35.0,
    "format": "png",
    "width": 2048,
    "height": 2048,
    "colormap": "viridis",
    "rescale": "669,1205"
  }' \
  --output india_full.png
```

### Visualization Endpoints

#### XYZ Tiles (for web maps)
```
http://<alb-url>/viz/{job_id}/tiles/{z}/{x}/{y}.png?colormap=viridis&rescale=669,1205
```

**Leaflet Integration:**
```javascript
const map = L.map('map').setView([20.5937, 78.9629], 5);
L.tileLayer('http://<alb-url>/viz/{job_id}/tiles/{z}/{x}/{y}.png?colormap=viridis', {
  attribution: 'INSAT Geospatial Processor'
}).addTo(map);
```

#### Preview Image
```bash
curl "http://<alb-url>/viz/{job_id}/preview.png?colormap=viridis&rescale=669,1205&max_size=512" \
  --output preview.png
```

#### Metadata & Statistics
```bash
# Get raster info
curl "http://<alb-url>/viz/{job_id}/info" | jq

# Get statistics
curl "http://<alb-url>/viz/{job_id}/statistics" | jq
```

**Example Statistics Response:**
```json
{
  "job_id": "d0b2d5df-907b-48cf-ae36-1024e2ec2752",
  "statistics": {
    "min": 420.0,
    "max": 1497.0,
    "mean": 955.75,
    "std": 115.51,
    "percentiles": {
      "2": 669.0,
      "98": 1205.0
    }
  },
  "recommended_rescale": "669.00,1205.00"
}
```

#### TileJSON
```bash
curl "http://<alb-url>/viz/{job_id}/tilejson?colormap=viridis&rescale=669,1205" | jq
```

### S3 Data Management

#### List Root Prefixes
```bash
curl "http://<alb-url>/s3/root"
```

#### Browse Prefix Contents
```bash
curl "http://<alb-url>/s3/list?prefix=manifests/&max_keys=100"
```

#### Read S3 Object (JSON/Text)
```bash
curl "http://<alb-url>/s3/read?key=manifests/INSAT3D_20251115_061713.json" | jq
```

#### Get Presigned URL
```bash
curl "http://<alb-url>/s3/presign?key=processed_results/2025/11/15/output.tif&expires_in=3600"
```

## 📁 Project Structure

```
on-the-fly-processor/
├── src/
│   ├── api/                    # FastAPI backend
│   │   ├── main.py            # Main API application
│   │   └── requirements.txt
│   ├── common/
│   │   └── schemas.py         # Shared data models
│   ├── processor_lambda/      # Lambda: Tile processing
│   │   ├── app.py
│   │   └── requirements.txt
│   ├── receiver_lambda/       # Lambda: Workflow orchestration
│   │   ├── app.py
│   │   └── requirements.txt
│   └── translator_lambda/     # Lambda: Task translation
│       ├── app.py
│       └── requirements.txt
├── aws-init/                  # AWS setup scripts
│   ├── init_aws.py
│   ├── init-lambda.sh
│   ├── upload_data.py
│   └── layer/                 # Lambda layer dependencies
├── Titiler/                   # Custom TiTiler build
│   ├── Dockerfile
│   └── main.py
├── api.Dockerfile             # API Docker image
├── setup.Dockerfile           # Setup utilities
├── ecs-api-task-definition.json
├── ecs-titiler-task-definition.json
├── cloudformation-template.yml
├── deploy-titiler.ps1
└── README.md
```

## 🔧 Configuration

### Environment Variables (API)

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_REGION` | AWS region | `ap-south-1` |
| `API_BASE_URL` | Public API URL | ALB DNS name |
| `TITILER_ENDPOINT` | TiTiler service URL | `http://titiler.geospatial.local:8080` |
| `S3_BUCKET_NAME` | Input COG bucket | `insat-cog-processed-production` |
| `PROCESSED_BUCKET_NAME` | Output bucket | `insat-processed-results-production` |
| `DYNAMODB_TABLE_NAME` | Job tracking table | `WorkflowJobs` |
| `SQS_QUEUE_URL` | Workflow queue | CloudFormation output |
| `PROCESSOR_LAMBDA_NAME` | Processor Lambda | `processor_lambda` |

### Environment Variables (TiTiler)

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS credentials | IAM role |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials | IAM role |
| `AWS_REGION` | AWS region | `ap-south-1` |

## 📊 Monitoring & Health Checks

### Health Endpoints
```bash
# API health
curl "http://<alb-url>/health" | jq

# TiTiler health (via proxy)
curl "http://<alb-url>/titiler/healthz"

# Active watchers
curl "http://<alb-url>/watchers/status" | jq

# WebSocket connections
curl "http://<alb-url>/ws/connections" | jq
```

### CloudWatch Logs
- **API**: `/ecs/geospatial-api-service`
- **TiTiler**: `/ecs/titiler-service`
- **Processor Lambda**: `/aws/lambda/processor_lambda`
- **Receiver Lambda**: `/aws/lambda/receiver_lambda`

### Metrics to Monitor
- ECS Task CPU/Memory utilization
- Lambda invocations, errors, duration
- SQS queue depth
- S3 request rates
- DynamoDB read/write capacity

## 🐛 Troubleshooting

### Common Issues

#### Tiles Return 404
**Symptom**: Map tiles don't load, backend logs show 404 from TiTiler

**Solution**: Ensure rescale parameters match your data range. Use `/viz/{job_id}/statistics` to get recommended rescale values.

```bash
# Get recommended rescale
RESCALE=$(curl "http://<alb-url>/viz/{job_id}/statistics" | jq -r '.recommended_rescale')

# Use in tile requests
curl "http://<alb-url>/viz/{job_id}/tiles/5/27/17.png?colormap=viridis&rescale=$RESCALE"
```

#### Mosaic Size Too Small
**Symptom**: Workflow completes but mosaic file is < 10MB

**Cause**: Insufficient tiles merged (< 50 tiles)

**Solution**: Check tile coverage in job status, adjust processing parameters.

#### Lambda Timeout
**Symptom**: Processor Lambda times out (15 min limit)

**Solution**: Split processing into smaller tile batches, increase Lambda memory.

#### TiTiler Connection Errors
**Symptom**: `TiTiler service unavailable` errors

**Solution**: 
1. Check ECS service health: `aws ecs describe-services --cluster geospatial-cluster-production --services titiler-service-production`
2. Verify Service Discovery: `aws servicediscovery list-services`
3. Check security groups allow ECS tasks to communicate

## 📈 Performance Tuning

### Scaling Configuration

#### ECS Tasks
```json
{
  "desiredCount": 2,
  "minHealthyPercent": 50,
  "maxPercent": 200
}
```

#### Lambda Concurrency
```bash
aws lambda put-function-concurrency \
  --function-name processor_lambda \
  --reserved-concurrent-executions 100
```

### Optimization Tips

1. **COG Optimization**: Ensure input files are properly tiled and overviewed
2. **Lambda Memory**: Increase to 2048MB for faster tile processing
3. **TiTiler Caching**: Enable CloudFront in front of TiTiler for repeated tile requests
4. **S3 Transfer Acceleration**: Enable for faster uploads/downloads
5. **DynamoDB Capacity**: Use on-demand pricing for variable workloads

## 🔐 Security

### IAM Roles
- **ECS Task Role**: S3 read/write, DynamoDB access, Lambda invoke
- **Lambda Execution Role**: CloudWatch Logs, S3 access, DynamoDB
- **ALB**: Health check access to ECS tasks

### Network Security
- ECS tasks in private subnets with NAT gateway
- Security groups restrict inbound to ALB only
- Service Discovery for internal communication (no public IPs)

### Data Security
- S3 buckets with encryption at rest (AES-256)
- Presigned URLs with 1-hour expiration
- API behind ALB (can add SSL/WAF)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [TiTiler](https://github.com/developmentseed/titiler) - Dynamic tile server for COG
- [FastAPI](https://fastapi.tiangolo.com/) - Modern web framework
- [Rasterio](https://rasterio.readthedocs.io/) - Geospatial raster I/O
- [AWS](https://aws.amazon.com/) - Cloud infrastructure

## 📞 Support

For issues, questions, or contributions:
- 📧 Email: bendrevivek0@gmail.com
- 🐛 Issues: [GitHub Issues](https://github.com/vivek5200/Final-2st-Architecture/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/vivek5200/Final-2st-Architecture/discussions)
- 📖 Documentation: See `SECURITY.md` and `GITHUB_PUSH_CHECKLIST.md`

---

**Built with ❤️ for geospatial data processing**
