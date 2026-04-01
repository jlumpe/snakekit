"""Represent event data emitted by Snakemake's logging system.

Based on the following PR, which is currently in a draft state:

https://github.com/snakemake/snakemake-interface-logger-plugins/pull/32
"""

from dataclasses import Field, dataclass, field, fields, MISSING
from logging import LogRecord
from typing import Any, ClassVar, Self, TypeVar, TypedDict, TypeAlias
from collections.abc import Mapping
from types import MappingProxyType
import uuid

from snakemake_interface_logger_plugins.common import LogEvent


_EventDataT = TypeVar("_EventDataT", bound="SnakemakeLogEvent")
StrMap: TypeAlias = Mapping[str, Any]


# ------------------------------------------------------------------------------------------------ #
#                                               Utils                                              #
# ------------------------------------------------------------------------------------------------ #


def field_has_default(field: Field[Any]) -> bool:
	"""Check whether a dataclass field has a default value."""
	return field.default is not MISSING or field.default_factory is not MISSING


def is_namedlist(obj: Any) -> bool:
	"""Check whether an object is a Snakemake NamedList.

	(Can't do an isinstance check without importing snakemake)
	"""
	return isinstance(obj, list) and hasattr(obj, "_names")


# ------------------------------------------------------------------------------------------------ #
#                                               Base                                               #
# ------------------------------------------------------------------------------------------------ #


def _from_extra_default(
	cls: type[_EventDataT], extra: StrMap, /, **kw: Any
) -> _EventDataT:
	"""Helper function to implement ``LogEventData._from_extra()``.

	Picks values from ``extra`` for all fields in dataclass ``cls`` and passes them to the ``cls``
	constructor. Behavior can be overridden for specific fields by passing their values as keyword
	arguments.
	"""
	for fld in fields(cls):
		if fld.name in kw:
			continue
		alias = fld.metadata.get("snakemake_alias")
		if fld.name in extra:
			kw[fld.name] = extra[fld.name]
		elif alias and alias in extra:
			kw[fld.name] = extra[alias]
		elif not field_has_default(fld):
			raise ValueError(f"LogRecord missing required attribute {fld.name!r}")

	return cls(**kw)


@dataclass
class SnakemakeLogEvent:
	"""Data associated with a Snakemake log event.

	In the current state of Snakemake, these attributes are added to the emitted ``LogRecord``
	instance by passing them in a dictionary as the ``extra`` parameter of the logging function.

	If fields have a ``snakemake_alias`` key in their metadata, this is used in place of the field's
	name when converting to and from the ``extra`` dictionary or a ``LogRecord`` instance.

	Attributes
	----------
	event
	    The type of log event (class attribute).
	"""

	event: ClassVar[LogEvent]

	def __init__(self) -> None:
		# Allow super().__init__() in subclasses even if it doesn't do anything
		if type(self) is SnakemakeLogEvent:
			raise TypeError(
				f"{type(self).__name__} is an abstract base class and cannot be instantiated."
			)

	@staticmethod
	def from_record(record: LogRecord) -> "SnakemakeLogEvent | None":
		"""Create an instance from a LogRecord.

		See :meth:`from_extra` for details. Returns ``None`` if record does not have an
		event attached.
		"""
		return SnakemakeLogEvent.from_extra(record.__dict__)

	@classmethod
	def from_extra(cls, extra: StrMap) -> "SnakemakeLogEvent | None":
		"""Create from dictionary of extra log record attributes.

		Selects the appropriate subclass based on the ``'event'`` key/attribute. Returns ``None`` if
		no event is present.
		"""
		event = extra.get("event", None)
		if event is None:
			return None

		# Ensure event is a LogEvent (also convert plain strings)
		try:
			event = LogEvent(event)
		except ValueError:
			return None

		cls = LOG_EVENT_CLASSES[event]
		return cls._from_extra(extra)

	@classmethod
	def _from_extra(cls, extra: StrMap) -> Self:
		"""Subclass-specific implementation of ``from_extra()``."""
		return _from_extra_default(cls, extra)

	def to_extra(self, **kw: Any) -> dict[str, Any]:
		"""Convert to dictionary of extra log record attributes.

		This is for testing.
		"""
		extra = self._to_extra()
		extra["event"] = self.event
		extra.update(kw)
		return extra

	def _to_extra(self) -> dict[str, Any]:
		"""Subclass-specific implementation of ``to_extra()``."""
		return {
			fld.metadata.get("snakemake_alias", fld.name): getattr(self, fld.name)
			for fld in fields(self)
		}


# ------------------------------------------------------------------------------------------------ #
#                                           Event classes                                          #
# ------------------------------------------------------------------------------------------------ #

@dataclass
class ErrorEvent(SnakemakeLogEvent):
	event = LogEvent.ERROR

	exception: str | None = None
	location: str | None = None
	rule: str | None = None
	traceback: str | None = None
	file: str | None = None
	line: str | None = None


@dataclass
class WorkflowStartedEvent(SnakemakeLogEvent):
	event = LogEvent.WORKFLOW_STARTED

	workflow_id: str
	snakefile: str | None

	@classmethod
	def _from_extra(cls, extra: StrMap) -> Self:
		snakefile = extra.get("snakefile", None)
		if snakefile is not None:
			# Try to convert to string - this should work for PosixPath and other path-like objects
			snakefile = str(snakefile)
		# Convert from UUID
		workflow_id = str(extra['workflow_id'])
		return _from_extra_default(cls, extra, snakefile=snakefile, workflow_id=workflow_id)

	def _to_extra(self) -> dict[str, Any]:
		extra = super()._to_extra()
		extra['workflow_id'] = uuid.UUID(extra['workflow_id'])
		return extra


@dataclass
class JobInfoEvent(SnakemakeLogEvent):
	event = LogEvent.JOB_INFO

	job_id: int = field(metadata={"snakemake_alias": "jobid"})
	rule_name: str
	threads: int
	input: list[str] | None = None
	output: list[str] | None = None
	log: list[str] | None = None
	benchmark: str | None = None
	rule_msg: str | None = None
	wildcards: dict[str, Any] | None = field(default_factory=dict)
	reason: str | None = None
	shellcmd: str | None = None
	priority: int | None = None
	resources: dict[str, Any] | None = field(default_factory=dict)
	local: bool | None = None
	is_checkpoint: bool | None = None
	is_handover: bool | None = None

	@classmethod
	def _from_extra(cls, extra: StrMap) -> Self:
		resources_obj = extra.get("resources", None)

		if resources_obj is None:
			resources = {}
		elif is_namedlist(resources_obj):
			resources = dict(resources_obj.items())
		elif isinstance(resources_obj, Mapping):
			resources = dict(resources_obj)
		else:
			raise TypeError("resources must be a Mapping, NamedList, or None")

		resources = {
			name: value
			for name, value in resources.items()
			if name not in {"_cores", "_nodes"}
		}

		return _from_extra_default(cls, extra, resources=resources)


@dataclass
class JobStartedEvent(SnakemakeLogEvent):
	event = LogEvent.JOB_STARTED

	job_ids: list[int] = field(metadata={"snakemake_alias": "jobs"})


@dataclass
class JobFinishedEvent(SnakemakeLogEvent):
	event = LogEvent.JOB_FINISHED

	job_id: int = field(metadata={"snakemake_alias": "jobid"})


@dataclass
class ShellCmdEvent(SnakemakeLogEvent):
	event = LogEvent.SHELLCMD

	job_id: int | None = field(default=None, metadata={"snakemake_alias": "jobid"})
	shellcmd: str | None = None
	rule_name: str | None = None

	@classmethod
	def _from_extra(cls, extra: StrMap) -> "ShellCmdEvent":
		# Snakemake also inconsistently uses "cmd" instead of "shellcmd" in places
		shellcmd = extra.get("shellcmd", None) or extra.get("cmd", None)
		return _from_extra_default(cls, extra, shellcmd=shellcmd)


@dataclass
class JobErrorEvent(SnakemakeLogEvent):
	event = LogEvent.JOB_ERROR

	job_id: int = field(metadata={"snakemake_alias": "jobid"})


@dataclass
class GroupInfoEvent(SnakemakeLogEvent):
	event = LogEvent.GROUP_INFO

	group_id: str = field(metadata={"snakemake_alias": "groupid"})
	jobs: list[Any] = field(default_factory=list)


@dataclass
class GroupErrorEvent(SnakemakeLogEvent):
	event = LogEvent.GROUP_ERROR

	group_id: str = field(metadata={"snakemake_alias": "groupid"})
	aux_logs: list[Any] = field(default_factory=list)
	job_error_info: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ResourcesInfoEvent(SnakemakeLogEvent):
	"""Information on resources available to workflow.

	This may be emitted multiple times at the beginning of the workflow, each time with only
	some or possible no attributes set.

	Attributes
	----------
	nodes
		Number of provided remote nodes (see :attr:`Workflow.nodes`)
	cores
		Number of provided CPU cores.
	provided_resources
		Additional resources (see :attr:`Workflow.global_resources`,
		:attr:`ResourceSettings.resources`).
	"""

	event = LogEvent.RESOURCES_INFO

	nodes: int | None = None
	cores: int | None = None
	provided_resources: dict[str, Any] | None = None


@dataclass
class DebugDagEvent(SnakemakeLogEvent):
	event = LogEvent.DEBUG_DAG

	status: str | None = None
	job: Any = None
	file: str | None = None
	exception: BaseException | None = None


@dataclass
class ProgressEvent(SnakemakeLogEvent):
	"""Progress of workflow execution.

	Attributes
	----------
	done
		Number of completed jobs.
	total
		Total number of jobs to be executed.
	"""

	event = LogEvent.PROGRESS

	done: int
	total: int


class RuleGraphNode(TypedDict):
	"""
	Attributes
	----------
	rule
		Name of rule.
	"""

	rule: str


class RuleGraphEdge(TypedDict):
	"""
	Attributes
	----------
	source
		Index of source node in list.
	target
		Index of target node in list.
	sourcerule
		Name of source rule.
	targetrule
		Name of target rule.
	"""

	source: int
	target: int
	sourcerule: str
	targetrule: str


class RuleGraphDict(TypedDict):
	"""Representation of the rule graph in ``RULEGRAPH`` event.

	This is a graph where nodes correspond to unique rules for all jobs to be executed, and an
	an edge is present from rule A to rule B if any job of rule A is a dependency of any job of rule
	B. The nodes list is sorted according to a topological sort of the job graph, using the first
	job for each rule.
	"""

	nodes: list[RuleGraphNode]
	links: list[RuleGraphEdge]


@dataclass
class RuleGraphEvent(SnakemakeLogEvent):
	"""Dependency graph of rules for all jobs to be executed.

	This is only emitted if a logging plugin specifically requests it.
	"""

	event = LogEvent.RULEGRAPH

	rulegraph: RuleGraphDict


@dataclass
class RunInfoEvent(SnakemakeLogEvent):
	"""Information on rules/jobs to be executed.

	Emitted prior to start of workflow execution or during a dry run.

	Attributes
	----------
	per_rule_job_counts
		Mapping from rule names to the number of jobs to be executed for each.
	total_job_count
		Total number of jobs to be executed.
	"""

	event = LogEvent.RUN_INFO

	per_rule_job_counts: dict[str, int]
	total_job_count: int

	def __init__(
		self,
		per_rule_job_counts: dict[str, int] | None = None,
		total_job_count: int | None = None,
		stats: dict[str, int] | None = None,
	):
		"""
		Parameters
		----------
		per_rule_job_counts
		total_job_count
		stats
			From :meth:`DAG.stats()`. Provides defaults for previous two parameters.
		"""
		if per_rule_job_counts is not None:
			self.per_rule_job_counts = per_rule_job_counts
		elif stats is not None:
			self.per_rule_job_counts = {k: v for k, v in stats.items() if k != "total"}
		else:
			self.per_rule_job_counts = {}

		if total_job_count is not None:
			self.total_job_count = total_job_count
		elif stats is not None:
			self.total_job_count = stats.get(
				"total", sum(self.per_rule_job_counts.values())
			)
		else:
			self.total_job_count = sum(self.per_rule_job_counts.values())

	@classmethod
	def _from_extra(cls, extra: StrMap) -> "RunInfoEvent":
		return cls(
			per_rule_job_counts=extra.get("per_rule_job_counts"),
			total_job_count=extra.get("total_job_count"),
			stats=extra.get("stats"),
		)

	def _to_extra(self) -> dict[str, Any]:
		extra = super()._to_extra()
		# Add "stats" key for compatibility
		stats = dict(self.per_rule_job_counts)
		stats["total"] = self.total_job_count
		extra["stats"] = stats
		return extra


#: Mapping from event types to their associated data classes.
LOG_EVENT_CLASSES: Mapping[LogEvent, type[SnakemakeLogEvent]] = MappingProxyType({
	cls.event: cls for cls in [
		ErrorEvent,
		WorkflowStartedEvent,
		JobInfoEvent,
		JobStartedEvent,
		JobFinishedEvent,
		ShellCmdEvent,
		JobErrorEvent,
		GroupInfoEvent,
		GroupErrorEvent,
		ResourcesInfoEvent,
		DebugDagEvent,
		ProgressEvent,
		RuleGraphEvent,
		RunInfoEvent,
	]
})
