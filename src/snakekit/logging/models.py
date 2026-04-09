"""Classes used to represent log records."""

from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime
import logging
import os
from typing import Self, TypeVar, ClassVar, Any, Final, Literal, overload
import time

from pydantic import (
	BaseModel, Field, ConfigDict, ValidatorFunctionWrapHandler, SerializerFunctionWrapHandler,
	TypeAdapter,
	field_serializer, field_validator, model_serializer, model_validator
)
from snakemake_interface_logger_plugins.common import LogEvent

from snakekit.version import VersionInfo, get_version_info
from .events import SnakemakeLogEvent, LOG_EVENT_CLASSES


T = TypeVar('T')
_T_modeltype = TypeVar('_T_modeltype', bound=type[BaseModel])


NAMED_LEVELS = (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL)


# ------------------------------------------------------------------------------------------------ #
#                                               Utils                                              #
# ------------------------------------------------------------------------------------------------ #

class TypeAdapterCache:
	"""Caches Pydantic TypeAdapters.

	Enables using Pydantic to validate non-BaseModel types (including dataclasses), but avoids
	creating a new ``TypeAdapter`` instance each time.
	"""

	cache: dict[type, TypeAdapter]

	def __init__(self):
		self.cache = dict()

	def get(self, typ: type) -> TypeAdapter:
		if typ in self.cache:
			return self.cache[typ]
		adapter = TypeAdapter(typ)
		self.cache[typ] = adapter
		return adapter

	def validate_python(self, typ: type[T], value, **kw) -> T:
		adapter = self.get(typ)
		return adapter.validate_python(value, **kw)

	def validate_json(self, typ: type[T], data: str | bytes | bytearray, **kw) -> T:
		adapter = self.get(typ)
		return adapter.validate_json(data, **kw)

	def dump_python(self, value, astype: type | None = None, **kw) -> Any:
		if astype is None:
			astype = type(value)
		adapter = self.get(astype)
		return adapter.dump_python(value, **kw)

	def dump_json(self, value, astype: type | None = None, **kw) -> bytes:
		if astype is None:
			astype = type(value)
		adapter = self.get(astype)
		return adapter.dump_json(value, **kw)


adapter_cache = TypeAdapterCache()


def validate_snakemake_event(data: dict[str, Any], **kw) -> SnakemakeLogEvent:
	"""Validate Snakemake log event data, using type property to determine correct class."""
	event = data['event']
	cls = LOG_EVENT_CLASSES[event]
	return adapter_cache.validate_python(cls, data, **kw)


def serialize_snakemake_event(value: SnakemakeLogEvent, **kw) -> dict[str, Any]:
	"""Serialize Snakemake log event data, adding type property."""
	data = adapter_cache.dump_python(value, **kw)
	data['event'] = value.event
	return data


# ------------------------------------------------------------------------------------------------ #
#                                         Non-record models                                        #
# ------------------------------------------------------------------------------------------------ #

class ExceptionInfo(BaseModel):
	"""Information from a caught exception.

	Parameters
	----------
	message
		Exception message.
	type
		Exception type name.
	module
		Module name of exception type (or None if built-in).
	"""

	message: str
	type: str
	module: str | None

	def repr_type(self) -> str:
		return self.type if self.module is None else f'{self.module}.{self.type}'

	@staticmethod
	def from_exception(exc: BaseException) -> 'ExceptionInfo':
		typ = type(exc)
		module = typ.__module__ if typ.__module__ != 'builtins' else None
		return ExceptionInfo(message=str(exc), type=typ.__qualname__, module=module)


# ------------------------------------------------------------------------------------------------ #
#                                               Meta                                               #
# ------------------------------------------------------------------------------------------------ #

class MetaLogEvent(BaseModel):
	"""Log event containing information about the logging session itself.

	Attributes
	----------
	event
		Unique event type string (class attribute).
	"""

	event: str
	_registry: ClassVar[dict[str, type['MetaLogEvent']]] = {}
	_levelno: ClassVar[int] = logging.INFO
	_message: ClassVar[str] = ''

	def __init_subclass__(cls, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		if cls is MetaLogEvent:
			return
		assert cls.event not in cls._registry, f'Event {cls.event} already registered'
		cls._registry[cls.event] = cls

	def record(self, **kw) -> 'LogRecord':
		"""Create a log record from this event."""
		kw.setdefault('levelno', self._levelno)
		kw.setdefault('message', self._message)
		return LogRecord(meta=self, **kw)  # pyright: ignore[reportArgumentType]


class LoggingStartedEvent(MetaLogEvent):
	"""Indicates the initialization of the logging system.

	Attributes
	----------
	pid
		ID of snakemake process. Can be used to check whether the process is still running.
	proc_started
		Timestamp when the snakemake process started, if available. Can be used in addition to PID
		to avoid edge case of PID reuse.
	versions
		Installed versions of Snakekit and core Snakemake packages.
	"""

	event: Literal['logging_started'] = 'logging_started'  # pyright: ignore[reportIncompatibleVariableOverride]
	_levelno = logging.INFO
	_message = 'snakekit JSON logging plugin initialized'

	pid: int
	proc_started: float | None = None
	versions: VersionInfo

	@classmethod
	def create(cls, **kw) -> 'LoggingStartedEvent':
		"""Create with default values for ``pid`` and ``versions``.

		This is implemented as a separate function instead of assigning defaults to the Pydantic
		fields, because they should not be used when validating from JSON data.
		"""
		kw.setdefault('pid', os.getpid())
		kw.setdefault('versions', get_version_info())
		return cls(**kw)


class LoggingFinishedEvent(MetaLogEvent):
	"""Indicates that the logging system has shut down and closed successfully.
	"""

	event: Literal['logging_finished'] = 'logging_finished'  # pyright: ignore[reportIncompatibleVariableOverride]
	_levelno = logging.INFO
	_message = 'Logging concluded'


class FormattingErrorEvent(MetaLogEvent):
	"""Indicates an error formatting a log record.

	Attributes
	----------
	record_partial
		Dictionary of attributes that were successfully extracted from the log record.
	"""

	event: Literal['formatting_error'] = 'formatting_error'  # pyright: ignore[reportIncompatibleVariableOverride]
	_levelno = logging.ERROR
	_message = 'Error converting log record to JSON'

	record_partial: dict[str, Any]


type MetaLogEventSubclass = LoggingStartedEvent | LoggingFinishedEvent | FormattingErrorEvent


# ------------------------------------------------------------------------------------------------ #
#                                               Base                                               #
# ------------------------------------------------------------------------------------------------ #

class LogRecord(BaseModel):
	"""Log record with additional Snakekit-specific data that can be serialized to JSON.

	Can be constructed from builtin :class:`logging.LogRecord` instances using the
	:meth:`from_builtin` method. Afterwards should be able to be converted to/from JSON losslessly.

	Attributes
	----------
	message
		Formatted log message.
	levelno
		Numeric level.
	created
		Timestamp when log record was created.
	snakemake
		Snakemake log event data, if any.
	meta
		Structured data describing the logging session itself.
	"""

	model_config = ConfigDict(extra='forbid')

	message: str | None
	levelno: int
	# This is how the Python documentation says LogRecord.created is set, unsure how it's supposed
	# to be different than time.time()
	# https://docs.python.org/3/library/logging.html#logrecord-attributes
	created: float = Field(default_factory=lambda: time.time_ns() / 1e9)
	exc_info: ExceptionInfo | None = None
	snakemake: SnakemakeLogEvent | None = None
	meta: MetaLogEventSubclass | None = None

	@property
	def created_dt(self) -> datetime:
		"""Created timestamp as a :class:`datetime.datetime` instance."""
		return datetime.fromtimestamp(self.created)

	@property
	def levelname(self) -> str:
		"""String associated with numeric log level.

		This is always determined from :attr:`levelno`, so no need to store as an actual attribute.
		"""
		return logging.getLevelName(self.levelno)

	@classmethod
	def from_builtin(cls, record: logging.LogRecord) -> 'LogRecord':
		"""Construct a log record model from a builtin :class:`logging.LogRecord` instance.

		This recognizes records with Snakemake event data attached.

		Parameters
		----------
		record
			Log record instance from the standard logging system.
		"""
		exc_info = None
		if record.exc_info is not None:
			_typ, exc, _tb = record.exc_info
			if exc is not None:
				exc_info = ExceptionInfo.from_exception(exc)

		return cls(
			# Attibute not set if a formatter hasn't processed it yet
			message=record.message if hasattr(record, 'message') else record.getMessage(),
			levelno=record.levelno,
			created=record.created,
			exc_info=exc_info,
			snakemake=SnakemakeLogEvent.from_record(record),

		)

	@field_serializer('snakemake', mode='plain')
	def _serialize_snakemake(self, value: SnakemakeLogEvent | None) -> dict[str, Any] | None:
		if value is None:
			return None
		return serialize_snakemake_event(value)

	@field_validator('snakemake', mode='plain')
	@classmethod
	def _validate_snakemake(cls, value: SnakemakeLogEvent | dict[str, Any] | None) -> SnakemakeLogEvent | None:
		if value is None or isinstance(value, SnakemakeLogEvent):
			return value
		return validate_snakemake_event(value)

	@model_serializer(mode='wrap')
	def _serialize(self, handler: SerializerFunctionWrapHandler):
		d = handler(self)
		# Add this just for human readability
		if self.levelno in NAMED_LEVELS:
			d['levelname'] = logging.getLevelName(self.levelno)
		return d

	@model_validator(mode='wrap')
	@classmethod
	def _validate(cls, data, handler: ValidatorFunctionWrapHandler) -> Self:
		# Prevent error on extra field
		if 'levelname' in data:
			data = dict(data)
			del data['levelname']
		return handler(data)


class LogRecordList(Sequence[LogRecord]):
	"""List-like collection of log records.

	Provides basic convenience methods for filtering and grouping the records.
	"""

	_records: list[LogRecord]

	def __init__(self, records: Iterable[LogRecord]):
		self._records = list(records)

	def by_job(self) -> dict[int, list[LogRecord]]:
		"""Group records by job ID.

		Returns a dictionary mapping job IDs to lists of records. Note that a single record can be
		associated with multiple jobs.
		"""
		out = {}
		for r in self._records:
			if r.snakemake is None:
				continue
			for job_id in r.snakemake.get_jobs():
				out.setdefault(job_id, []).append(r)
		return out

	def for_job(self, job_id: int) -> list[LogRecord]:
		"""Get records with Snakemake events associated with a specific job."""
		return [r for r in self._records if r.snakemake is not None and job_id in r.snakemake.get_jobs()]

	def by_event(self) -> dict[LogEvent, list[LogRecord]]:
		"""Group records by Snakemake event type."""
		out = {}
		for r in self._records:
			if r.snakemake is None:
				continue
			out.setdefault(r.snakemake.event, []).append(r)
		return out

	def for_event(self, event: str) -> list[LogRecord]:
		"""Get records with Snakemake events of a specific type."""
		return [r for r in self._records if r.snakemake is not None and r.snakemake.event == event]

	# ------------------------------------- Special methods ------------------------------------- #

	def __len__(self) -> int:
		return len(self._records)

	@overload
	def __getitem__(self, index: int) -> LogRecord: ...

	@overload
	def __getitem__(self, index: slice) -> list[LogRecord]: ...

	def __getitem__(self, index: int | slice) -> LogRecord | list[LogRecord]:
		return self._records[index]

	def __iter__(self) -> Iterator[LogRecord]:
		return iter(self._records)

	def __contains__(self, item: Any) -> bool:
		return item in self._records

	def __repr__(self) -> str:
		return f'<LogRecordList length={len(self)}>'
