#!/usr/bin/env bash

# Set up to fail
set -euxo pipefail

# Set DEBIAN_FRONTEND env var to noninteractive
export DEBIAN_FRONTEND=noninteractive

# Update
apt-get update -y -qq

# Install git, unzip and wget
apt-get install -y -qq git unzip wget

# Install aws v2
wget --quiet \
  --output-document "awscliv2.zip" \
  "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
unzip -qq "awscliv2.zip"
./aws/install --update

# Clean up installation
rm -rf "awscliv2.zip" "aws/"

# Set git commit
GIT_COMMIT_ID="${GIT_COMMIT_ID:0:7}"

# Build as latest tag
docker build --tag "${CONTAINER_REPO}/${CONTAINER_NAME}:latest-${STACK_SUFFIX}" ./

# Also add in tag if applicable - for now just build it as the git commit id
docker tag "${CONTAINER_REPO}/${CONTAINER_NAME}:latest-${STACK_SUFFIX}" "${CONTAINER_REPO}/${CONTAINER_NAME}:${GIT_COMMIT_ID}-${STACK_SUFFIX}"

echo "Container version is ${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-latest}" 1>&2

# Login to aws and push Docker image to ECR
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${CONTAINER_REPO}"

docker push "${CONTAINER_REPO}/${CONTAINER_NAME}:latest-${STACK_SUFFIX}"
docker push "${CONTAINER_REPO}/${CONTAINER_NAME}:${GIT_COMMIT_ID}-${STACK_SUFFIX}"
