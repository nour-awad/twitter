# Architecture & design decisions

## 1. The same product through three generations (Week 1)

| | Classic SOA (≈2005) | Microservices (≈2014) | Cloud-native (today) |
|---|---|---|---|
| Unit of deployment | Large services on an app server | One small service per bounded context | Container image, scheduled by Kubernetes |
| Integration | SOAP/WSDL through a central **ESB** that routes, transforms and orchestrates | "Smart endpoints, dumb pipes": REST/gRPC + a message broker | Same, plus a **service mesh** and sidecars (Istio, Dapr) handling networking concerns |
| Data | Often one shared enterprise database | **Database per service** | Same, plus managed and event-streaming data platforms |
| Business logic in the middleware | Yes (ESB orchestration) | No | No; the mesh only handles cross-cutting concerns |
| Scaling / resilience | Vertical, manual failover | Independent horizontal scaling | Autoscaling (HPA/KEDA), self-healing probes, scale-to-zero |
| Problem it solved | Reuse across silos | Team autonomy, independent deploys | Operating hundreds of services elastically |
| Problem it created | ESB bottleneck, tight coupling | Distributed-systems complexity | Operational complexity → observability, AIOps, platform teams |

In this repo:
* `scripts/devstack.py` = microservices without infrastructure (one process, many services).
* `docker-compose.yml` = microservices with real infrastructure (broker, databases, telemetry).
* `deploy/helm` + Istio + KEDA + Knative + Dapr = cloud-native.

We deliberately did **not** build an ESB: the gateway only routes and composes, and business rules live in the
services.

## 2. Bounded contexts → services

| Context | Service | Why separate |
|---|---|---|
| Identity & access | auth-service | Security boundary; different change cadence and audit needs |
| Social graph & profiles | user-service | Read-heavy, used synchronously by others → fast gRPC API |
| Publishing | tweet-service | Write path; source of truth for tweets |
| Reading | timeline-service | Totally different access pattern (precomputed, denormalised) → CQRS |
| Engagement | engagement-service | Very high write volume, needs an audit log → event sourcing |
| Notifications | notification-service | Pure reaction to facts → choreography |
| Discovery | search-service | Disposable projection, horizontally scalable |
| Process management | saga-orchestrator | Long-running cross-service process with explicit state |
| Operations | aiops-service | Consumes telemetry, not business data |
| AI | ai-agent-service | Different runtime dependencies (LLM, MCP) and cost profile |

## 3. Communication styles and when we use which

| Style | Used for | Reason |
|---|---|---|
| REST (sync) | Client-facing APIs, saga steps | Ubiquitous, cacheable, self-describing (OpenAPI) |
| gRPC (sync) | tweet-service → user-service, gateway → user-service batch | Low latency, strict contracts, streaming |
| GraphQL | Client-shaped reads | One round trip for nested data; DataLoader avoids N+1 |
| Events (async) | Everything that is a *fact* other services may care about | Temporal decoupling: producers do not know or wait for consumers |
| SSE | Notifications to browsers | Server push over plain HTTP |
| MCP | AI agents | Self-describing tool discovery at runtime |

**Rule of thumb applied:** use synchronous calls only when the caller *cannot proceed without the answer*
(validate the author, run a saga step). Otherwise publish an event. The timeline, search and notifications can all
be down without blocking tweeting.

## 4. Anti-patterns we explicitly avoided (Week 2)

| Anti-pattern | How it would look here | What we did instead |
|---|---|---|
| Shared database | timeline-service querying tweet-service's tables | Each service has a private DB; data crosses boundaries only as events or API calls |
| Distributed monolith | Every request fans out synchronously through 5 services | Reads come from local projections; one sync hop at most on the write path |
| Chatty APIs / N+1 | GraphQL resolving each author separately | `BatchGetUsers` + DataLoader |
| Smart gateway / ESB | Business rules in the gateway | The gateway only routes, authenticates early, limits and composes |
| Shared business library | Domain models in `libs/common` | The chassis holds technical concerns only; event *contracts* are versioned schemas |
| Dual writes | Save to DB, then publish to Kafka | Transactional outbox |
| Retry storms | Retries at the app **and** mesh layers | `RESILIENCE_MODE=mesh` turns app retries off; jittered backoff; circuit breakers |
| Implicit trust inside the cluster | "Internal network = safe" | Every service verifies tokens; mTLS; deny-by-default policies |
| Nano-services | A service per table | Services follow bounded contexts |

## 5. Consistency model

* **Strong consistency** inside one service's transaction (tweet + outbox row; event store append).
* **Eventual consistency** between services: read models converge after the events are processed. The demo uses
  `eventually(...)` polling to show this honestly.
* **Idempotency everywhere:** inbox table (`processed_events`), idempotent saga steps (`user_id` chosen by the
  orchestrator), idempotent commands in the event-sourced aggregate, `ON CONFLICT DO NOTHING` fan-out.
