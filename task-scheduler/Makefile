COMPOSE ?= docker compose
PYTHON ?= python

.PHONY: test-unit-coverage test-compose chaos test-all-compose lint typecheck up down logs reset topics psql redis-cli check check-worker check-retry check-scheduler check-dlq check-agent check-java test-java contracts-check test test-integration migrate migration-check check-observability test-alerts

# --wait is applied only to long-running services; wait for the one-shot separately.
up:
	$(PYTHON) scripts/init_observability.py
	$(COMPOSE) up -d --wait --wait-timeout 180 kafka redis postgres
	$(COMPOSE) up -d kafka-init
	$(COMPOSE) wait kafka-init
	$(COMPOSE) up -d --build --wait --wait-timeout 180 api
	$(COMPOSE) up -d --build --wait --wait-timeout 180 worker
	$(COMPOSE) up -d --build --wait --wait-timeout 180 dlq
	$(COMPOSE) up -d --build --wait --wait-timeout 180 --scale scheduler=2 scheduler
	$(COMPOSE) up -d --build --wait --wait-timeout 180 agent
	@if [ "$(JAVA)" = "1" ]; then \
		$(COMPOSE) --profile java up -d --build --wait --wait-timeout 240 worker-java; \
	fi
	$(COMPOSE) up -d --wait --wait-timeout 180 prometheus grafana
	@if [ "$(UI)" = "1" ]; then \
		$(COMPOSE) --profile debug up -d --wait --wait-timeout 180 kafka-ui; \
	fi

down:
	$(COMPOSE) --profile debug --profile java down --remove-orphans

logs:
	$(COMPOSE) --profile debug --profile java logs -f --tail=100

# Destructive: deletes this Compose project's database, cache, and Kafka volumes.
reset:
	$(COMPOSE) --profile debug --profile java down --volumes --remove-orphans
	$(MAKE) up

topics:
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe

psql:
	$(COMPOSE) exec postgres sh -c 'exec psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

redis-cli:
	$(COMPOSE) exec redis redis-cli

check:
	$(PYTHON) scripts/check_infra.py

check-worker:
	$(PYTHON) scripts/check_worker.py

check-retry:
	$(PYTHON) scripts/check_retry.py

check-scheduler:
	$(PYTHON) scripts/check_scheduler.py

check-dlq:
	$(PYTHON) scripts/check_dlq.py

check-agent:
	$(PYTHON) scripts/check_agent.py

check-java:
	$(PYTHON) scripts/check_java_worker.py

test-java:
	$(PYTHON) scripts/test_java_contract.py

contracts-check:
	$(PYTHON) scripts/export_contracts.py --check

test:
	$(PYTHON) -m pytest tests/unit -q

test-integration:
	$(PYTHON) -m pytest tests/integration --run-integration -q

migrate:
	$(PYTHON) -m alembic upgrade head

migration-check:
	$(PYTHON) -m alembic check

check-observability:
	$(PYTHON) scripts/check_observability.py

test-alerts:
	$(COMPOSE) run --rm --no-deps --entrypoint /bin/promtool -v ./observability/prometheus:/work:ro -w /work prometheus test rules rules.test.yml

# Test projects have dedicated volumes/networks and are removed after collecting artifacts.
test-unit-coverage:
	$(PYTHON) -m pytest tests/unit -q --cov --cov-report=term-missing --cov-report=json --cov-report=xml --cov-report=html --junitxml=artifacts/junit/unit.xml
	$(PYTHON) scripts/check_coverage.py

test-compose:
	$(PYTHON) scripts/test_stack.py integration

chaos:
	$(PYTHON) scripts/test_stack.py chaos

test-all-compose:
	$(PYTHON) scripts/test_stack.py all

lint:
	$(PYTHON) -m ruff check shared services scripts tests migrations
	$(PYTHON) -m ruff format --check shared services scripts tests migrations

typecheck:
	$(PYTHON) -m mypy
