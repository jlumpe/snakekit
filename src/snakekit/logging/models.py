"""Classes used to represent log records."""

from datetime import datetime
import logging
import os
from typing import Self, TypeVar, ClassVar, Any
import time

from pydantic import (
	BaseModel, Field, ConfigDict, ValidatorFunctionWrapHandler, SerializerFunctionWrapHandler,
	TypeAdapter,
	field_serializer, field_validator, model_serializer, model_validator
)

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

	event: ClassVar[str]
	_registry: ClassVar[dict[str, type['MetaLogEvent']]] = {}
	_levelno: ClassVar[int] = logging.INFO
	_message: ClassVar[str] = ''

	def __init_subclass__(cls, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		if cls is MetaLogEvent:
			return
		assert cls.event not in cls._registry, f'Event {cls.event} already registered'
		cls._registry[cls.event] = cls

	@model_serializer(mode='wrap')
	def _serialize(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
		data = handler(self)
		data['event'] = self.event
		return data

	@model_validator(mode='wrap')
	@classmethod
	def _validate_subtype(cls, data, handler: ValidatorFunctionWrapHandler) -> Self:
		# Only dispatch on base class
		if cls is not MetaLogEvent:
			return handler(data)
		# Already an instance (still pass to handler to check right subtype based on cls?)
		if isinstance(data, MetaLogEvent):
			return handler(data)
		assert isinstance(data, dict)

		subcls = cls._registry[data['event']]
		return subcls.model_validate(data)  # type: ignore

	def record(self, **kw) -> 'JsonLogRecord':
		kw.setdefault('levelno', self._levelno)
		kw.setdefault('message', self._message)
		return JsonLogRecord(meta=self, **kw)


# @register_meta_model
class LoggingStartedEvent(MetaLogEvent):
	"""Indicates the initialization of the logging system.

	Attributes
	----------
	pid
		ID of snakemake process. Can be used to check whether the process is still running.
	proc_started
		Timestamp when the snakemake process started, if available. Can be used in addition to PID
		to avoid edge case of PID reuse.
	"""

	event = 'logging_started'
	_levelno = logging.INFO
	_message = 'snakekit JSON logging plugin initialized'

	pid: int = Field(default_factory=os.getpid)
	proc_started: float | None = None


# @register_meta_model
class LoggingFinishedEvent(MetaLogEvent):
	"""Indicates that the logging system has shut down and closed successfully.
	"""

	event = 'logging_finished'
	_levelno = logging.INFO
	_message = 'Logging concluded'


# @register_meta_model
class FormattingErrorEvent(MetaLogEvent):
	"""Indicates an error formatting a log record.

	Attributes
	----------
	record_partial
		Dictionary of attributes that were successfully extracted from the log record.
	"""

	event = 'formatting_error'
	_levelno = logging.ERROR
	_message = 'Error converting log record to JSON'

	record_partial: dict[str, Any]


# ------------------------------------------------------------------------------------------------ #
#                                               Base                                               #
# ------------------------------------------------------------------------------------------------ #

class JsonLogRecord(BaseModel):
	"""Base class for models of a JSON-formatted log records.

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
		Data associated with a Snakemake log event.
	meta
		Data associated with a meta log event.
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
	meta: MetaLogEvent | None = None

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
	def from_builtin(cls, record: logging.LogRecord) -> 'JsonLogRecord':
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
			message=record.message,
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
