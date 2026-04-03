# Solana Signal System - Railway Deployment
# Single container: FastAPI only (consumer as separate service later)

FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

COPY . .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start API only (consumer deploy separately on Railway)
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
