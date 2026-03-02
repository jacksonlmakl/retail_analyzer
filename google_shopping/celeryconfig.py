"""Celery configuration for Google Shopping worker."""

import os

broker_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
result_backend = os.environ.get("REDIS_URL", "redis://redis:6379/0")

task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
timezone = "UTC"
enable_utc = True

task_track_started = True
task_acks_late = True
worker_prefetch_multiplier = 1
worker_pool = "solo"

task_default_queue = "google_shopping"
task_routes = {"google_shopping.tasks.*": {"queue": "google_shopping"}}
