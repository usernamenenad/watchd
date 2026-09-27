# Deployment

watchd runs from one container image in two shapes:

- **A single daemon**: the `watchd` binary, or Docker Compose next to PostgreSQL, in `deploy/compose/` (#59).
- **Kubernetes**: a Helm chart in `deploy/helm/watchd/` (#60), tested end to end on kind (#61).

The image itself is #58, and publishing signed release artifacts is #8.

The Kubernetes deployment model is intentionally ordinary:

- run `watchd` as a Deployment with one replica and the `Recreate` strategy: one logical replication slot has one consumer, so two pods must never stream the same source at once. High availability is #11;
- expose its watch endpoint through a ClusterIP Service;
- use readiness to indicate that its configured PostgreSQL source is safe to serve;
- expose OpenTelemetry metrics for source lag, watcher freshness, and resyncs on a separate ops port, through Prometheus or OTLP (#55, #56).

An operator is a possible later convenience layer for creating sources and managing credentials (#62). It is not required to use `watchd` and should not contain the core correctness logic.
