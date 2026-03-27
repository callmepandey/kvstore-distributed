FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir msgpack

COPY src/ ./src/
COPY ui/  ./ui/

COPY docker/entrypoint.sh .
RUN chmod +x entrypoint.sh

VOLUME ["/data"]

# TCP port for node-to-node communication
EXPOSE 7000
# HTTP API port
EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
