# Deployment

Future Docker, Kubernetes, and configuration assets belong here.

The initial Kubernetes deployment model is intentionally ordinary:

- run `watchd` as a Deployment or StatefulSet when a stable identity is needed;
- expose its watch endpoint through a ClusterIP Service;
- use readiness to indicate that its configured PostgreSQL source is safe to serve;
- export Prometheus metrics for source lag, watcher freshness, and resyncs.

An operator is a possible later convenience layer for creating sources and managing credentials. It is not required to use `watchd` and should not contain the core correctness logic.
