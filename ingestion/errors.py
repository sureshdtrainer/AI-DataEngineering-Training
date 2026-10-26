"""Exception types shared by the ingestion framework."""


class IngestError(Exception):
    """Fatal problem while reading, writing, or checkpointing data."""


class ConfigError(IngestError):
    """Configuration is missing, malformed, or fails validation."""
