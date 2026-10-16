# MLflow tracking server and model registry. Metadata in SQLite on a volume, artifacts in
# S3 (LocalStack locally) served through the server, so clients need no bucket access.
FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir "mlflow==3.16.1" "boto3>=1.35"
ENV MLFLOW_DISABLE_AGENT_HINT=1
EXPOSE 5000
ENTRYPOINT ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000", "--backend-store-uri", "sqlite:////mlflow/mlflow.db", "--serve-artifacts", "--artifacts-destination", "s3://skypulse-models", "--allowed-hosts", "*"]
