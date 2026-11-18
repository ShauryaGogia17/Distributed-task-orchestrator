"""Topic contracts shared by bootstrapping and infrastructure verification."""

from pathlib import Path


def expected_topics() -> dict[str, int]:
    """Read the topic-to-partition manifest mounted into the Kafka initializer."""
    manifest = Path(__file__).with_name("kafka-topics.txt")
    entries = (line.split() for line in manifest.read_text().splitlines())
    return {name: int(count) for name, count in entries}
