FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir \
      boto3 \
      confluent-kafka \
      loguru \
      pyarrow \
      python-dotenv \
    && useradd --create-home sinkuser
COPY banking_sink/ ./banking_sink/
USER sinkuser
CMD ["python3", "-m", "banking_sink"]