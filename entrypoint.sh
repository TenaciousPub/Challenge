#!/bin/bash
set -e

echo "=== Starting entrypoint script ==="

# Create credentials file from environment variable if it exists
if [ -n "$GOOGLE_CREDENTIALS_JSON" ]; then
    echo "Creating credentials file from GOOGLE_CREDENTIALS_JSON..."
    echo "$GOOGLE_CREDENTIALS_JSON" > /app/Challenge/credentials.json
    export GOOGLE_APPLICATION_CREDENTIALS="/app/Challenge/credentials.json"
    echo "Credentials file created at: $GOOGLE_APPLICATION_CREDENTIALS"
    echo "File size: $(wc -c < /app/Challenge/credentials.json) bytes"
else
    echo "Warning: GOOGLE_CREDENTIALS_JSON environment variable not set"
    echo "Current GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS"
fi

echo "=== Starting bot with python -m Challenge ==="

# Start the bot
exec python -m Challenge
