"""Correlation process entrypoint."""

from appchi.correlation.consumer import correlate_record
from appchi.kafka_changed import ChangedConsumer
from appchi.pipeline_runtime import run


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
