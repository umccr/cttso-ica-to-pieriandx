#!/usr/bin/env bash

# Set up to fail
set -euo pipefail

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

# Convenience CODEBUILD VARS, need more? Check https://github.com/thii/aws-codebuild-extras
CTTSO_ICA_TO_PIERIANDX_GIT_TAG="$(git describe --tags --exact-match 2>/dev/null)"

# Build as latest tag
docker build --tag "${CONTAINER_REPO}/${CONTAINER_NAME}:latest-${STACK_SUFFIX}" ./

# Also add in tag if applicable
if [[ -n "${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-}" ]]; then
  docker tag "${CONTAINER_REPO}/${CONTAINER_NAME}:latest-${STACK_SUFFIX}" "${CONTAINER_REPO}/${CONTAINER_NAME}:${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}"
fi

echo "Container version is ${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-latest}" 1>&2

# Login to aws and push Docker image to ECR
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${CONTAINER_REPO}"

docker push "${CONTAINER_REPO}/${CONTAINER_NAME}:latest-${STACK_SUFFIX}"

if [[ -n "${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-}" ]]; then
  docker push "${CONTAINER_REPO}/${CONTAINER_NAME}:${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}"
fi