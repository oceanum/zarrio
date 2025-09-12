# Use an official Python runtime as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements files
COPY requirements.txt requirements_dev.txt ./

# Install production dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install development dependencies
RUN pip install --no-cache-dir -r requirements_dev.txt

# Copy the source code
COPY . .

# Install the package in development mode
RUN pip install --no-cache-dir -e .

# Create a non-root user
RUN adduser --disabled-password --gecos '' onzarr

# Change ownership of the app directory to the non-root user
RUN chown -R onzarr:onzarr /app

# Switch to the non-root user
USER onzarr

# Expose the default port (if your application has a web interface)
# EXPOSE 8000

# Define the command to run the application
ENTRYPOINT ["zarrify"]
CMD ["--help"]