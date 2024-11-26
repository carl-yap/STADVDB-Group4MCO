#!/bin/bash

# Concurrency Control Test Script

# Ensure PostgreSQL services are running
docker compose up -d

# Run Python concurrency test
python3 concurrency-implementation.py

# Optional: Generate test report
python3 concurrency-report-generator.py

# Cleanup
docker compose down
