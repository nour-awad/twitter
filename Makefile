PY ?= .venv/bin/python
SERVICES := api-gateway auth-service user-service tweet-service timeline-service engagement-service \
            notification-service search-service saga-orchestrator aiops-service ai-agent-service

.PHONY: install dev demo test lint proto contracts up down logs k8s-render

install:            ## create venv + install everything for local development
	python3 -m venv .venv
	$(PY) -m pip install -U pip
	$(PY) -m pip install -r requirements-dev.txt

dev:                ## run all services in ONE process (no Docker needed)
	$(PY) scripts/devstack.py

demo:               ## walk through every course topic against a running stack
	$(PY) scripts/demo.py

test:               ## unit + end-to-end tests (boots the stack in-process)
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check libs services functions scripts tests

proto:              ## regenerate gRPC stubs from the .proto contract
	$(PY) -m grpc_tools.protoc -I proto --python_out=libs/common --pyi_out=libs/common \
	  --grpc_python_out=libs/common proto/twitter_protos/user/v1/user.proto

contracts:          ## export OpenAPI + AsyncAPI documents into contracts/
	$(PY) scripts/export_contracts.py

up:                 ## production-like stack: Kafka, Postgres, Jaeger, Prometheus, Grafana
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

k8s-render:         ## render the Kubernetes manifests (needs helm)
	helm template twitter deploy/helm/twitter --namespace twitter
