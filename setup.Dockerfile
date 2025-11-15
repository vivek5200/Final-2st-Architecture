# Stage 1: The Layer Builder
FROM python:3.9-slim-bullseye AS layer-builder

# Install runtime libraries for Rasterio and other geospatial dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    gdal-bin \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /layer

# Create the directory structure Lambda expects
RUN mkdir -p python bin

# Create requirements file for the layer
COPY layer-requirements.txt .

# --- GDAL FIXES ---
# Copy GDAL executables and ensure they're executable
RUN cp /usr/bin/gdal_merge.py /layer/bin/ && \
    cp /usr/bin/gdalbuildvrt /layer/bin/ && \
    cp /usr/bin/gdalwarp /layer/bin/ && \
    chmod +x /layer/bin/gdal_merge.py /layer/bin/gdalbuildvrt /layer/bin/gdalwarp

# Install dependencies to the python directory
RUN pip install --target=./python -r layer-requirements.txt

# Stage 2: The Final Setup Image
FROM python:3.9-slim-bullseye

# Install only essential tools for the AWS CLI and runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    libgdal-dev \
    gdal-bin \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

# Install the AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Install boto3 for the init_aws.py script
RUN pip install boto3

# Copy the pre-built Python layer from the first stage
COPY --from=layer-builder /layer /layer

# --- ADD PATH CONFIGURATION ---
# Add the layer bin directory to PATH
ENV PATH="/layer/bin:${PATH}"
ENV PYTHONPATH="/layer/python:${PYTHONPATH}"

# Copy your application and initialization code
COPY ./aws-init/ /aws-init/
COPY ./src/ /src/

WORKDIR /aws-init

# This command runs the script and then keeps the container running and idle
CMD ["sh", "-c", "python init_aws.py && echo '--- Setup complete. Container is now idle and ready for exec commands. ---' && tail -f /dev/null"]