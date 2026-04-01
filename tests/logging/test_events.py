from collections.abc import Sequence
import logging

from snakekit.logging import events


def test_builtin_conversion(example_snakemake_events: Sequence[events.SnakemakeLogEvent]):
	"""Test conversion to/from builtin LogRecord type."""

	for event in example_snakemake_events:
		record = logging.makeLogRecord(event.to_extra())
		event2 = events.SnakemakeLogEvent.from_record(record)
		assert type(event2) is type(event)
		assert event2 == event
