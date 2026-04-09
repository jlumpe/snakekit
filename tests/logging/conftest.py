from typing import Sequence, TypeVar
import logging
from dataclasses import dataclass, fields

import pytest

from snakekit.logging import models, events


RANDOM_TIMESTAMP = 1759974850.185749
LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]


def make_record(**kw) -> models.LogRecord:
	kw.setdefault('message', f'Test')
	kw.setdefault('levelno', logging.INFO)
	kw.setdefault('created', RANDOM_TIMESTAMP)
	return models.LogRecord(**kw)


@dataclass
class RecordFactory:
	"""
	Cycles through some different values for attribute defaults.
	"""

	i: int = 0

	def next_time(self) -> float:
		time = RANDOM_TIMESTAMP + self.i * 5.13917
		self.i += 1
		return time

	def make_record(self, **kw) -> models.LogRecord:
		kw.setdefault('levelno', LEVELS[self.i % len(LEVELS)])
		kw.setdefault('created', self.next_time())
		return make_record(**kw)



@pytest.fixture(scope='session')
def example_snakemake_events() -> Sequence[events.SnakemakeLogEvent]:
	"""Example SnakemakeLogEvent instances, one of each type."""
	return (
		events.ErrorEvent(
			exception='some error',
			location='somewhere',
			rule='rule_name',
			file='script.py',
			# line=123,
		),
		events.WorkflowStartedEvent(
			workflow_id='f0915278-1f9d-4cc8-a2b3-f23c3649c7e4',
			snakefile='/path/to/snakefile',
		),
		events.JobInfoEvent(
			job_id=123,
			rule_name='rule_name',
			threads=4,
			input=['in/file1', 'in/file2'],
			output=['out/file3'],
			wildcards={'foo': '1'},
		),
		events.JobStartedEvent(
			job_ids=[1, 2, 3],
		),
		events.JobFinishedEvent(
			job_id=123,
		),
		events.ShellCmdEvent(
			job_id=123,
			shellcmd='echo hello',
			rule_name='some_rule',
		),
		events.JobErrorEvent(
			job_id=123,
		),
		events.GroupInfoEvent(
			group_id='123',
			jobs=[56, 78],
		),
		events.GroupErrorEvent(
			group_id='123',
			aux_logs=['one', 'two'],
			job_error_info=[{}],
		),
		events.ResourcesInfoEvent(
			# nodes=?,
			cores=10,
			# provided_resources=?,
		),
		events.DebugDagEvent(
			status='status',
			job=123,
			file='file.py',
			exception=Exception('some error'),
		),
		events.ProgressEvent(
			done=34,
			total=56,
		),
		events.RuleGraphEvent(
			rulegraph={
				'nodes': [{'rule': 'one'}, {'rule': 'two'}],
				'edges': [{'source': 0, 'target': 1, 'sourcerule': 'one', 'targetrule': 'two'}],
			},
		),
		events.RunInfoEvent(
			stats={},
		),
	)


@pytest.fixture(scope='session')
def example_records_sm(example_snakemake_events: Sequence[events.SnakemakeLogEvent]) -> Sequence[models.LogRecord]:
	"""Example log record instances with each Snakemake event type."""

	factory = RecordFactory()

	return tuple(
		factory.make_record(snakemake=event)
		for event in example_snakemake_events
	)


@pytest.fixture(scope='session')
def example_records_meta() -> Sequence[models.LogRecord]:
	"""Example log record instances with each meta event type."""

	factory = RecordFactory()

	return (
		models.LoggingStartedEvent(
			pid=1234,
			proc_started=RANDOM_TIMESTAMP,
		).record(created=factory.next_time()),
		models.LoggingFinishedEvent().record(created=factory.next_time()),
		models.FormattingErrorEvent(record_partial={'foo': 'bar'}).record(created=factory.next_time()),
	)


@pytest.fixture(scope='session')
def example_records_standard() -> Sequence[models.LogRecord]:
	"""Example standard log record instances, one of each level."""

	factory = RecordFactory()

	return tuple(
		factory.make_record(message=f'Level {level}', levelno=level)
		for level in LEVELS
	)


@pytest.fixture(scope='session')
def example_records(
	example_records_standard,
	example_records_sm,
	example_records_meta,
) -> Sequence[models.LogRecord]:
	"""Example LogRecord instances, one of each subclass."""
	return (*example_records_standard, *example_records_sm, *example_records_meta)
