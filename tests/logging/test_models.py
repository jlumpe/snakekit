from collections.abc import Sequence
import logging

from snakekit.logging.models import JsonLogRecord


def test_builtin_conversion(example_records: Sequence[JsonLogRecord]):
	"""Test conversion to/from builtin LogRecord type."""

	for json_record in example_records:
		# Skip meta records
		if json_record.meta is not None:
			continue

		attrs = {
			'message': json_record.message,
			'levelno': json_record.levelno,
			'created': json_record.created,
		}

		# Snakemake data
		if json_record.snakemake is not None:
			attrs |= json_record.snakemake.to_extra()


		builtin_record = logging.makeLogRecord(attrs)

		json_record2 = JsonLogRecord.from_builtin(builtin_record)
		assert isinstance(json_record2, JsonLogRecord)
		assert json_record2 == json_record
