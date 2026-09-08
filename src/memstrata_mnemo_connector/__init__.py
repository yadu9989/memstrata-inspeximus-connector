"""Optional, standard-library client for the MemStrata schema_v0 bridge."""

from .producer import (
    DeliveryCredentials,
    DeliveryResult,
    DurableOutbox,
    ProducerError,
    SourceConflict,
    canonical_bytes,
    freeze_record,
    remember_and_enqueue,
)

__version__ = "0.1.0"

__all__ = [
    "DeliveryCredentials",
    "DeliveryResult",
    "DurableOutbox",
    "ProducerError",
    "SourceConflict",
    "canonical_bytes",
    "freeze_record",
    "remember_and_enqueue",
]
