#!/bin/bash

# Concurrency Control Test Script

# Ensure PostgreSQL services are running
docker compose up -d

# Run Python concurrency simulation app
python3 flask_simulation.py

# Optional: Generate test report

# Cleanup
docker compose down
