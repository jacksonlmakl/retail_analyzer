"""Celery configuration and beat schedule."""

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

VERIFY_INTERVAL_HOURS = int(os.environ.get("VERIFY_INTERVAL_HOURS", "24"))
VERIFY_STALE_DAYS = int(os.environ.get("VERIFY_STALE_DAYS", "7"))

beat_schedule = {
    "verify-stale-products": {
        "task": "api.tasks.verify_products",
        "schedule": VERIFY_INTERVAL_HOURS * 3600,
        "kwargs": {"max_stale_days": VERIFY_STALE_DAYS},
    },
}
