# Runbook: messages in an SQS dead-letter queue

**Queues:** `eu261-calculator-dlq`, `notifications-dlq`
**Owner:** data platform on-call
**Severity:** medium. Claims are delayed, nothing is lost: DLQs keep messages for 14 days.

## How messages get here

Each consumer queue redrives to its DLQ after **3 receives** (`maxReceiveCount=3`). The
Lambdas use partial batch responses, so only the failing messages are retried, never the
whole batch. A message lands in the DLQ when the same message failed three times, for
example:

| Symptom in logs | Likely cause |
|-----------------|--------------|
| `claim_failed` with `JSONDecodeError` or `KeyError` | Producer sent a payload that does not match `contracts/flight-disruption.v1.json` |
| `claim_failed` with `ProvisionedThroughputExceeded` or a timeout | DynamoDB or network trouble |
| `notification_failed` with `URLError` / `HTTPError` | Webhook endpoint down |
| No log line at all, Lambda `Errors` metric up | Handler crashed before logging (import error, timeout) |

Every log line carries `correlation_id`, which is also on the message as the
`correlation-id` attribute. Use it to find the same event in the detector and producer logs.

## 1. Look before you touch

```bash
# Depth of each DLQ (local mode; drop "awslocal" for real AWS and use "aws")
docker compose exec -T localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/eu261-calculator-dlq \
  --attribute-names ApproximateNumberOfMessages

# Peek at a few messages without deleting them (they become visible again after 30 s)
docker compose exec -T localstack awslocal sqs receive-message \
  --queue-url http://localhost:4566/000000000000/eu261-calculator-dlq \
  --max-number-of-messages 5 --message-attribute-names All --visibility-timeout 30
```

Then read the Lambda logs for those correlation ids:

```bash
docker compose exec -T localstack awslocal logs filter-log-events \
  --log-group-name /aws/lambda/eu261-calculator --filter-pattern '"claim_failed"'
```

## 2. Fix the cause first

Redriving before the cause is fixed only sends the messages back to the DLQ three receives
later.

- **Bad payload:** fix the detector (or the contract), deploy, and decide whether the
  stuck messages can be processed by the fixed Lambda. If not, the detector can re-announce:
  delete the affected rows from `silver/flight_disruptions` and rerun the detector for the
  period; the calculator's conditional write keeps it idempotent.
- **Downstream outage:** wait until DynamoDB or the webhook is healthy again.
- **Code bug in the Lambda:** fix, `make lambdas` (local) or deploy through CI (AWS).

## 3. Redrive

```bash
docker compose exec -T localstack awslocal sqs start-message-move-task \
  --source-arn arn:aws:sqs:eu-west-1:000000000000:eu261-calculator-dlq \
  --destination-arn arn:aws:sqs:eu-west-1:000000000000:eu261-calculator

# Progress
docker compose exec -T localstack awslocal sqs list-message-move-tasks \
  --source-arn arn:aws:sqs:eu-west-1:000000000000:eu261-calculator-dlq
```

Redriving is safe to repeat: claims are written with
`attribute_not_exists(claim_id) OR detected_at < :detected_at`, so a message that was
partially processed before cannot create a duplicate or overwrite a newer claim.

## 4. Verify

- DLQ depth back to 0.
- `make claims` shows the expected flights.
- No new `claim_failed` lines for the redriven correlation ids.

## 5. If a message can never succeed

Leave it in the DLQ, open an issue with the message body and correlation id, and let the
14-day retention expire it. Do not purge the queue to make an alert go away.
