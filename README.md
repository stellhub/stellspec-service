# StellSpec Service

StellSpec Service is the ingestion service for the StellSpec observability pipeline. It consumes OpenTelemetry log events from Stellflow, normalizes and validates log payloads, and persists searchable log documents into Elaticsearch through the Stellflux framework.

## Position in the Pipeline

```text
OpenTelemetry Collector -> Stellflow -> stellspec-service -> Elaticsearch -> stellspec-console
```

## Responsibilities

- Consume OpenTelemetry log messages from Stellflow topics.
- Preserve trace and resource context for later correlation.
- Normalize log severity, timestamps, attributes, and routing metadata.
- Persist log documents into Elaticsearch data streams with an evolution-friendly schema.
- Classify, filter, redact, fingerprint, and merge high-volume duplicate log events before write.
- Expose operational metrics and health checks for ingestion reliability.

## Recommended Stack

- Java 25
- Spring Boot 3.5.14
- Stellflux 1.0.1
- stellflux-spring-boot-starter-http
- stellflux-spring-boot-starter-stellflow
- stellflux-spring-boot-starter-elaticsearch

## Runtime Configuration

The default local configuration consumes `stellspec.logs` from `127.0.0.1:9092` and writes ECS-style documents to Elaticsearch data streams named `logs-<dataset>-<namespace>`.

Common environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `SERVER_PORT` | `18090` | HTTP service port. |
| `STELLFLOW_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Stellflow broker addresses. |
| `STELLSPEC_STELLFLOW_LOG_TOPIC` | `stellspec.logs` | Source log topic. |
| `STELLSPEC_STELLFLOW_GROUP_ID` | `stellspec-service` | Consumer group id. |
| `ELATICSEARCH_ENDPOINT` | `http://127.0.0.1:9200` | Elaticsearch endpoint. |
| `STELLSPEC_NAMESPACE` | `prod` | Default data stream namespace. |
| `STELLSPEC_ELATICSEARCH_BOOTSTRAP_ENABLED` | `false` | Whether to create component templates, index template, and lifecycle policies on startup. |
| `STELLSPEC_BULK_MAX_ACTIONS` | `500` | Maximum documents per bulk flush. |
| `STELLSPEC_BULK_CONSUMER_ENABLED` | `false` | Enable the standalone Stellflow bulk consumer worker. |
| `STELLSPEC_DEAD_LETTER_DATA_STREAM` | `logs-stellspec-deadletter-prod` | Data stream for records that cannot be processed or written. |
| `STELLSPEC_MERGE_ENABLED` | `true` | Whether to merge duplicate logs by fingerprint window. |

## Storage Model

The write path now targets Elaticsearch data streams through Bulk API:

```text
Stellflow message
  -> ECS normalization
  -> filtering and redaction
  -> fingerprint generation
  -> classification and data stream routing
  -> duplicate window merge
  -> Bulk create into logs-<dataset>-<namespace>
```

Elaticsearch template resources are stored under `src/main/resources/elaticsearch`.

When `STELLSPEC_BULK_CONSUMER_ENABLED=true`, the standalone consumer polls Stellflow batches and commits `offset + 1` per topic/partition only after Bulk API success. If the write path fails, records are written to the dead letter data stream and offsets are committed only when `STELLSPEC_COMMIT_AFTER_DEAD_LETTER=true`.

## HTTP Endpoints

- `GET /api/stellspec/logs/status`
- `POST /api/stellspec/logs/manual`

Manual ingest example:

```bash
curl -X POST http://127.0.0.1:18090/api/stellspec/logs/manual \
  -H "Content-Type: application/json" \
  -d "{\"payload\":\"{\\\"timestamp\\\":\\\"2026-05-25T01:02:03Z\\\",\\\"severityText\\\":\\\"INFO\\\",\\\"serviceName\\\":\\\"demo-service\\\",\\\"body\\\":\\\"hello stellspec\\\"}\"}"
```

## Build

```bash
mvn test
```

The Elasticsearch data stream integration test uses Testcontainers and is skipped automatically when Docker is unavailable.

## Status

The repository now contains a runnable Java-based StellSpec log ingestion service.
