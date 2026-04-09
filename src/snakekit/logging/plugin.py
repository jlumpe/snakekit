"""Logger plugin for Snakemake."""

import atexit
from typing import Optional, Any
import logging
from dataclasses import dataclass, field
from datetime import datetime
import os

from pydantic_core import to_json
from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase

from .models import (
	ExceptionInfo, LogRecord, FormattingErrorEvent, LoggingStartedEvent, LoggingFinishedEvent,
)
from snakekit.version import get_version_info


def make_logfile_path(workdir: os.PathLike | None = None, timestamp: datetime | None = None) -> str:
	"""Default log file path."""

	if timestamp is None:
		timestamp = datetime.now()

	filename = datetime.now().isoformat().replace(':', '') + '.log'

	path = os.path.join('.snakemake/snakekit/log', filename)
	if workdir is not None:
		path = os.path.join(workdir, path)

	return path


@dataclass
class SnakekitLogHandlerSettings(LogHandlerSettingsBase):
	file: Optional[str] = field(default=None, metadata={
		'help': 'File to write to (or - to use stderr).',
	})
	multiline: bool = field(default=False, metadata={
		'help': 'Write records in indented multi-line format.',
	})
	rulegraph: bool = field(default=False, metadata={
		'help': 'Output rule graph.',
	})
	validate: bool = field(default=False, metadata={
		'help': 'Validate log record attributes before writing (for testing).',
	})


@dataclass
class JsonFormatter:
	"""Log formatter emitting JSON.

	Attributes
	----------
	multiline
		Write each record over multiple lines with indentation and nice formatting. Easier for a
		human to read but harder to parse. The alternative is JSONL format.
	validate
		Validate the record's attributes.
	"""

	multiline: bool = False
	validate: bool = False

	def format(self, record: logging.LogRecord | LogRecord) -> str:
		json_record = self._get_json_record(record)
		return self._format_json_record(json_record)

	def _get_json_record(self, record: logging.LogRecord | LogRecord) -> LogRecord:
		if isinstance(record, LogRecord):
			return record

		try:
			return LogRecord.from_builtin(record)

		except Exception as exc:
			return self._make_error_record(record, exc)

	def _format_json_record(self, json_record: LogRecord) -> str:
		return json_record.model_dump_json(
			indent=2 if self.multiline else None,
			exclude_none=True,
		)

	def _make_error_record(self, record: logging.LogRecord, exc: BaseException) -> LogRecord:
		"""Make record indicating a formatting error for another record."""

		# Get partial record attributes
		partial: dict[str, Any] = dict()
		for field in ['message', 'levelno', 'created']:
			if hasattr(record, field):
				value = getattr(record, field)
				# Check if each attribute value can be serialized to JSON so we don't hit the same error again
				try:
					to_json(value)
				except Exception:
					continue
				partial[field] = value

		exc_info = ExceptionInfo.from_exception(exc)
		event = FormattingErrorEvent(record_partial=partial)
		return event.record(exc_info=exc_info)


class SnakekitLogHandler(LogHandlerBase):

	settings: SnakekitLogHandlerSettings
	baseFilename: str | None
	handler: logging.Handler
	_closed: bool

	def __post_init__(self) -> None:
		# Not called as part of LogHandlerBase.__init__() prior to 2.0
		# https://github.com/snakemake/snakemake-interface-logger-plugins/pull/34
		if not get_version_info().at_least('snakemake_interface_logger_plugins', (2, 0)):
			logging.Handler.__init__(self)

		# Shadows parent class attribute?
		self._closed = False

		if self.settings.file == '-':
			self.baseFilename = None
			self.handler = logging.StreamHandler()

		else:
			if self.settings.file:
				self.baseFilename = self.settings.file
			else:
				self.baseFilename = make_logfile_path()
				os.makedirs(os.path.dirname(self.baseFilename), exist_ok=True)

			self.handler = logging.FileHandler(self.baseFilename, mode='w')

		formatter = JsonFormatter(multiline=self.settings.multiline, validate=self.settings.validate)
		self.handler.setFormatter(formatter)  # type: ignore

		start = LoggingStartedEvent.create().record()
		self.handler.emit(start)  # type: ignore

		# Currently Snakemake doesn't close plugin handlers when stopping the logging system, make
		# sure to close the handler when the process exits.
		atexit.register(self.close)

	def emit(self, record):
		if self._closed:
			return
		self.handler.emit(record)

	def close(self):
		if self._closed:
			return

		self._closed = True
		atexit.unregister(self.close)

		# Emit logging finished event
		self.handler.emit(LoggingFinishedEvent().record())  # type: ignore

		self.handler.flush()
		self.handler.close()
		super().close()

	def flush(self):
		if self._closed:
			return
		self.handler.flush()

	@property
	def writes_to_stream(self) -> bool:
		return self.baseFilename is None

	@property
	def writes_to_file(self) -> bool:
		return self.baseFilename is not None

	@property
	def has_filter(self) -> bool:
		return True

	@property
	def has_formatter(self) -> bool:
		return True

	@property
	def needs_rulegraph(self) -> bool:
		return self.settings.rulegraph
