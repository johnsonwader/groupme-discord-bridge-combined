FROM python:3.11-slim

WORKDIR /app

# Copy and install requirements first (for Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Expose port
EXPOSE 8080

# Set environment variable for Python
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "main.py"]
