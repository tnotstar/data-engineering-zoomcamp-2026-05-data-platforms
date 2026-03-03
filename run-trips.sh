#!/bin/bash

bruin run --start-date 2022-01-01T00:00:00.000Z --end-date 2022-01-31T23:59:59.999999999Z --environment default --var 'taxi_types=["yellow"]' "/workspaces/data-engineering-zoomcamp-2026-05-data-platforms/pipeline/assets/ingestion/trips.py"
