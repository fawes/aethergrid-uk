FROM python:3.12-slim-bookworm

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv && uv sync --frozen --no-dev

COPY src/ ./src/
COPY dbt_aethergrid/ ./dbt_aethergrid/

RUN useradd -m aetheruser && chown -R aetheruser:aetheruser /app
USER aetheruser

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uv", "run", "uvicorn", "src.day3_integration_and_api:app", "--host", "0.0.0.0", "--port", "8000"]
