"""
OpenTelemetry tracing setup.

Instruments FastAPI, SQLAlchemy, and httpx automatically.
Exports traces to an OTLP endpoint if OTEL_EXPORTER_OTLP_ENDPOINT is set,
otherwise falls back to the console exporter (useful for local dev).

Set in .env:
  OTEL_SERVICE_NAME=llm-ops-sentinel
  OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317   # e.g. Jaeger / Honeycomb / Grafana Tempo
"""
import os
import structlog

logger = structlog.get_logger()


def setup_tracing(app):
    """Call once at startup to wire up OTel instrumentation."""
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    except ImportError:
        logger.warning("otel_not_installed", msg="pip install opentelemetry-sdk opentelemetry-instrumentation-fastapi opentelemetry-instrumentation-sqlalchemy opentelemetry-instrumentation-httpx")
        return

    service_name = os.getenv("OTEL_SERVICE_NAME", "llm-ops-sentinel")
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
            logger.info("otel_otlp_exporter_configured", endpoint=otlp_endpoint)
        except ImportError:
            logger.warning("otlp_exporter_not_installed", msg="pip install opentelemetry-exporter-otlp")
            exporter = ConsoleSpanExporter()
    else:
        exporter = ConsoleSpanExporter()
        logger.info("otel_console_exporter_active")

    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()

    logger.info("otel_tracing_configured", service=service_name)
