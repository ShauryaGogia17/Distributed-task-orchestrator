FROM python:3.11.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY shared ./shared
COPY scripts ./scripts
COPY services ./services
COPY migrations ./migrations
COPY alembic.ini ./
RUN pip install --no-cache-dir . \
    && useradd --uid 10001 --create-home dlq
USER dlq
EXPOSE 8005
CMD ["python", "-m", "services.dlq"]
