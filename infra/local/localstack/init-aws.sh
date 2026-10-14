#!/usr/bin/env bash
# Runs inside LocalStack once it is ready. Creates the lakehouse buckets.
set -euo pipefail

prefix="${SKYPULSE_LAKE_BUCKET_PREFIX:-skypulse}"

for layer in bronze silver gold checkpoints; do
  bucket="${prefix}-${layer}"
  if ! awslocal s3api head-bucket --bucket "$bucket" 2>/dev/null; then
    awslocal s3api create-bucket \
      --bucket "$bucket" \
      --create-bucket-configuration LocationConstraint=eu-west-1
  fi
done

echo "lakehouse buckets ready"
