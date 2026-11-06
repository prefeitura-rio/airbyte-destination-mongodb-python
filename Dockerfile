FROM airbyte/python-connector-base:4.0.0@sha256:d9894b6895923b379f3006fa251147806919c62b7d9021b5cd125bb67d7bbe22

WORKDIR /airbyte/integration_code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY destination_mongodb_python ./destination_mongodb_python
RUN chown -R airbyte:airbyte /airbyte/integration_code /tmp

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
LABEL io.airbyte.name="ghcr.io/prefeitura-rio/airbyte-destination-mongodb-python"
LABEL org.opencontainers.image.source="https://github.com/prefeitura-rio/airbyte-destination-mongodb-python"

USER airbyte
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
