# 0004. Local-first development with LocalStack and replay fixtures

- Status: Accepted
- Date: 2026-10-14

## Context

SkyPulse targets AWS (S3, Kinesis, SNS, SQS, Lambda, DynamoDB, Glue, Athena) but has to be
cheap to develop on and fully reproducible in CI. Three things get in the way of working
against the real cloud and the real APIs all the time:

- **Cost and teardown discipline.** Kinesis shards and idle resources bill by the hour.
- **External APIs are non-deterministic.** OpenSky traffic changes every second, schedule
  providers have small free quotas, and none of them are reachable from CI without secrets.
- **Feedback speed.** A change to a stream job should be testable in minutes, offline.

## Decision

1. **One configuration switch.** `SKYPULSE_RUN_MODE=local|aws` decides where every client
   points. In `local` mode boto3 clients target LocalStack; in `aws` mode an endpoint override
   is rejected at startup so a LocalStack URL cannot leak into a deployment.
2. **docker-compose is the local cloud.** Kafka (KRaft, single node) stands in for Kinesis
   and LocalStack for the AWS services. Topics and buckets are declared in files under
   `infra/local/` and created by init steps, so the local layout is code-reviewed like the
   Terraform one.
3. **Transport behind an interface.** Producers depend on `StreamSink` / `StreamSource`
   (`skypulse.common.streams`), never on a Kafka or Kinesis client directly.
4. **Replay fixtures for external data** (from M2). Recorded API responses live in
   `data/fixtures/` and a replay provider serves them, so `make demo` and CI never need
   credentials or network access to third parties.
5. **Pinned LocalStack image.** Since release 2026.03 LocalStack requires an auth token to
   start. The stack pins `localstack/localstack:4.14.0`, the last image that runs without one.
   A newer image can be used by setting `LOCALSTACK_IMAGE` and `LOCALSTACK_AUTH_TOKEN`.

## Consequences

- `make up` gives a working environment with no cloud account; CI runs the same stack in
  its integration job.
- LocalStack is an emulation. IAM is not enforced and some service edges (Lambda cold starts,
  Kinesis shard limits) behave differently, so the `aws` mode keeps its own smoke tests (M8).
- Kafka and Kinesis differ in semantics (partitions vs shards, consumer groups vs iterators,
  retention). The stream interface is limited to what both support; anything transport
  specific stays inside the adapters.
- Staying on LocalStack 4.14.0 freezes the emulated service versions. Revisit when a needed
  feature only exists in a newer release.
