FROM docker:29.5.2-cli AS docker-cli
FROM python:3.11.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY pyproject.toml README.md alembic.ini ./
COPY shared ./shared
COPY services ./services
COPY migrations ./migrations
COPY scripts ./scripts
COPY tests ./tests
RUN pip install --no-cache-dir '.[dev]'
CMD ["python", "-m", "pytest", "tests/unit", "tests/integration", "--run-integration", "-q"]
