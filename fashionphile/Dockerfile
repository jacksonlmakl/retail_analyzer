FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY fashionphile/ fashionphile/

EXPOSE 8000

CMD ["uvicorn", "fashionphile.server:app", "--host", "0.0.0.0", "--port", "8000"]
