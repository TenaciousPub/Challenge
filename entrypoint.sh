#!/bin/bash
set -e

# Create credentials file from environment variable if it exists
if [ -n "$GOOGLE_CREDENTIALS_JSON" ]; then
    echo "Creating credentials file from environment variable..."
    echo "$GOOGLE_CREDENTIALS_JSON" > /app/Challenge/credentials.json
    export GOOGLE_APPLICATION_CREDENTIALS="/app/Challenge/credentials.json"
    echo "Credentials file created successfully"
else
    echo "Warning: GOOGLE_CREDENTIALS_JSON environment variable not set"
fi

# Start the bot
exec python -m Challenge
