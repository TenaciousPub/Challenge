#!/bin/bash
set -e

echo "=== Starting entrypoint script ==="

# Create credentials file from environment variable if it exists
if [ -n "$GOOGLE_CREDENTIALS_JSON" ]; then
    echo "Creating credentials file from GOOGLE_CREDENTIALS_JSON..."
    # Write to /tmp instead of app directory
    printf '%s' "$GOOGLE_CREDENTIALS_JSON" > /tmp/google-sa.json
    export GOOGLE_APPLICATION_CREDENTIALS="/tmp/google-sa.json"
    echo "Credentials file created at: $GOOGLE_APPLICATION_CREDENTIALS"
    echo "File size: $(wc -c < /tmp/google-sa.json) bytes"
else
    echo "ERROR: GOOGLE_CREDENTIALS_JSON environment variable not set!"
    echo "Please add your service account JSON to Railway variables."
    exit 1
fi

echo "=== Starting bot with python -m Challenge ==="

# Start the bot
exec python -m Challenge
