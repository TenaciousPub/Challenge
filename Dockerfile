# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code into Challenge package directory
COPY . ./Challenge/

# Make entrypoint script executable
RUN chmod +x /app/Challenge/entrypoint.sh

# Run the bot using entrypoint script
CMD ["/app/Challenge/entrypoint.sh"]
