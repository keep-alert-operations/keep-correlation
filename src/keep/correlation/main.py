"""Correlation process entrypoint."""

from keep.correlation.consumer import correlate_record
from keep.kafka_changed import ChangedConsumer
from keep.pipeline_runtime import run


def main():
    run(
        "correlation",
        lambda db, settings, profile: ChangedConsumer(
            db,
            settings,
            profile,
            "correlation",
            correlate_record,
        ),
    )


if __name__ == "__main__":
    main()
