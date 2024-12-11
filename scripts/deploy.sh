#!/bin/bash

# exit on error
set -e

# load environment variables
source .env

# configuration
IMAGE_NAME="vmhub-data-pipeline"
GCP_PROJECT_ID=${GCP_PROJECT_ID:-"semantc-ai"}
REGION=${REGION:-"us-central1"}
REPO_NAME=${REPO_NAME:-"docker-repo"}  # name of your artifact registry repository
TAG=$(date +%Y%m%d_%H%M%S)

# artifact registry repository uri
ARTIFACT_REGISTRY_URI="${REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"
ARTIFACT_REGISTRY_LATEST_URI="${REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:latest"

# authenticate docker to artifact registry
echo "configuring docker authentication for artifact registry..."
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# build the docker image
echo "building docker image..."
docker build -t ${IMAGE_NAME}:${TAG} .

# tag the image for artifact registry
echo "tagging image for artifact registry..."
docker tag ${IMAGE_NAME}:${TAG} ${ARTIFACT_REGISTRY_URI}
docker tag ${IMAGE_NAME}:${TAG} ${ARTIFACT_REGISTRY_LATEST_URI}

# push the image to artifact registry
echo "pushing image to Artifact Registry..."
docker push ${ARTIFACT_REGISTRY_URI}
docker push ${ARTIFACT_REGISTRY_LATEST_URI}

echo "image built and pushed successfully!"
echo "image: ${ARTIFACT_REGISTRY_URI}"

# Create or update Cloud Run job
echo "creating/updating cloud run job..."
gcloud run jobs update vmhub-data-pipeline \
    --image ${ARTIFACT_REGISTRY_URI} \
    --region ${REGION} \
    --project ${GCP_PROJECT_ID} \
    --memory 512Mi \
    --cpu 1 \
    --max-retries 3 \
    --task-timeout 3600 \
    || gcloud run jobs create vmhub-data-pipeline \
    --image ${ARTIFACT_REGISTRY_URI} \
    --region ${REGION} \
    --project ${GCP_PROJECT_ID} \
    --memory 512Mi \
    --cpu 1 \
    --max-retries 3 \
    --task-timeout 3600

echo "cloud run job updated/created successfully!"