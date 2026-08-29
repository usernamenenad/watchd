# Roadmap

## 0. Contract and discovery

Write the precise Watch, Progress, and Resync contract; validate the problem with teams operating Postgres-backed caches or read models.

## 1. Consistent bootstrap spike

Prove snapshot-to-change-stream handoff from a single PostgreSQL source without missing committed changes.

## 2. Recoverable single-node alpha

Add transaction-aware CDC, bounded replay, an SDK projection, automatic resync, and fault-injection tests.

## 3. First user-facing integration

Ship one focused use case: a tenant-scoped Go read-model/cache SDK with observable freshness.

## 4. Cloud-native integration

Package `watchd` for Kubernetes as a standard Deployment and Service, with health/readiness endpoints, Prometheus metrics, and example application manifests. Evaluate an operator only after the normal deployment path and source lifecycle are well understood.
