# StellSpec Service

StellSpec Service is the ingestion service for the StellSpec observability pipeline. It consumes OpenTelemetry log events from Kafka, normalizes and validates log payloads, and persists searchable log documents into Elasticsearch.

## Position in the Pipeline

```text
OpenTelemetry Collector -> Kafka -> stellspec-service -> Elasticsearch -> stellspec-console
```

## Responsibilities

- Consume OpenTelemetry log messages from Kafka topics.
- Preserve trace and resource context for later correlation.
- Normalize log severity, timestamps, attributes, and routing metadata.
- Persist log documents into Elasticsearch indices with an evolution-friendly schema.
- Expose operational metrics and health checks for ingestion reliability.

## Recommended Stack

- Java 21+
- Spring Boot
- Apache Kafka client or Spring for Apache Kafka
- Elasticsearch Java API Client
- OpenTelemetry Java instrumentation

## Status

This repository is reserved for the Java-based StellSpec log ingestion service.
