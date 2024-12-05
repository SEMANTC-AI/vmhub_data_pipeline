#!/bin/bash

# Exit on error
set -e

# Load environment variables
source .env

# Configuration
IMAGE_NAME="vmhub-data-pipeline"
GCP_PROJECT_ID=${GCP_PROJECT_ID:-"your-project-id"}
REGION=${REGION:-"us-central1"}
TAG=$(date +%Y%m%d_%H%M%S)

# Build the Docker image
echo "Building Docker image..."
docker build -t ${IMAGE_NAME}:${TAG} .

# Tag the image for Google Container Registry
echo "Tagging image for GCR..."
docker tag ${IMAGE_NAME}:${TAG} gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}:${TAG}
docker tag ${IMAGE_NAME}:${TAG} gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}:latest

# Push the image to Google Container Registry
echo "Pushing image to GCR..."
docker push gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}:${TAG}
docker push gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}:latest

echo "Image built and pushed successfully!"
echo "Image: gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}:${TAG}"

# Create or update Cloud Run job
echo "Creating/Updating Cloud Run job..."
gcloud run jobs update vmhub-data-pipeline \
    --image gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}:${TAG} \
    --region ${REGION} \
    --project ${GCP_PROJECT_ID} \
    --memory 512Mi \
    --cpu 1 \
    --max-retries 3 \
    --task-timeout 3600 \
    || gcloud run jobs create vmhub-data-pipeline \
    --image gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}:${TAG} \
    --region ${REGION} \
    --project ${GCP_PROJECT_ID} \
    --memory 512Mi \
    --cpu 1 \
    --max-retries 3 \
    --task-timeout 3600

echo "Cloud Run job updated/created successfully!"