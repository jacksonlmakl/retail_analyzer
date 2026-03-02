"""Celery configuration for Grailed worker."""

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

task_default_queue = "grailed"
task_routes = {"grailed.tasks.*": {"queue": "grailed"}}
