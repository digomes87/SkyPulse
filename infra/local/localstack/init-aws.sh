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

# --- disruption events: SNS fan-out to SQS consumers, each with a dead-letter queue -------

region="eu-west-1"
account="000000000000"
topic_arn=$(awslocal sns create-topic --name flight-disruptions --query TopicArn --output text)

create_queue_with_dlq() {
  local name="$1"
  local dlq_url dlq_arn
  dlq_url=$(awslocal sqs create-queue --queue-name "${name}-dlq" \
    --attributes MessageRetentionPeriod=1209600 --query QueueUrl --output text)
  dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
  awslocal sqs create-queue --queue-name "$name" --attributes "{
    \"VisibilityTimeout\": \"60\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${dlq_arn}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }" > /dev/null
}

subscribe() {
  local name="$1" filter="$2"
  local queue_arn="arn:aws:sqs:${region}:${account}:${name}"
  local subscription
  subscription=$(awslocal sns subscribe --topic-arn "$topic_arn" --protocol sqs \
    --notification-endpoint "$queue_arn" --query SubscriptionArn --output text)
  awslocal sns set-subscription-attributes --subscription-arn "$subscription" \
    --attribute-name RawMessageDelivery --attribute-value true
  if [ -n "$filter" ]; then
    awslocal sns set-subscription-attributes --subscription-arn "$subscription" \
      --attribute-name FilterPolicy --attribute-value "$filter"
  fi
}

create_queue_with_dlq eu261-calculator
create_queue_with_dlq notifications
# Early-warning departure delays are not claims; only final outcomes reach the calculator.
subscribe eu261-calculator '{"kind": ["arrival_delay", "cancellation"]}'
subscribe notifications ""

awslocal dynamodb create-table --table-name eu261-claims \
  --attribute-definitions AttributeName=claim_id,AttributeType=S \
  --key-schema AttributeName=claim_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST > /dev/null

echo "disruption messaging ready"
