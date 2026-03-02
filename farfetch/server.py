"""FastAPI server for Farfetch scraping."""

from celery.result import AsyncResult
from fastapi import FastAPI
from pydantic import BaseModel

from farfetch.tasks import app as celery_app, scrape_and_load

app = FastAPI(title="Farfetch Scraper API", version="1.0.0")


class ScrapeRequest(BaseModel):
    query: str
    pages: int = 1
    country: str = "us"
    language: str = "en"


class TaskResponse(BaseModel):
    task_id: str
    status: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/scrape", response_model=TaskResponse)
def enqueue_scrape(req: ScrapeRequest):
    task = scrape_and_load.apply_async(
        args=[req.query],
        kwargs=dict(pages=req.pages, country=req.country, language=req.language),
        queue="farfetch",
    )
    return TaskResponse(task_id=task.id, status="queued")


@app.get("/scrape/{task_id}")
def scrape_status(task_id: str):
    result = AsyncResult(task_id, app=celery_app)
    response = {"task_id": task_id, "status": result.status}
    if result.ready():
        if result.successful():
            response["result"] = result.result
        else:
            response["error"] = str(result.result)
    return response
