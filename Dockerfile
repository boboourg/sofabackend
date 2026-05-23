FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

# Expose the API port
EXPOSE 8000

# Start the local API server by default
CMD ["python", "-m", "schema_inspector.local_api_server", "--host", "0.0.0.0", "--port", "8000"]
