"""Read log records from JSON.
"""

import json
from typing import IO, Any, Iterable, Iterator, TypeAlias, NamedTuple, get_args
from dataclasses import dataclass

from .models import LogRecord, LogRecordList
from ..util import FilePath, maybe_open


JsonData: TypeAlias = str | bytes | bytearray

JSON_DATA_TYPES: tuple[type, ...] = get_args(JsonData)


@dataclass
class JsonParseError(ValueError):

	msg: str
	data: Any = None
	start_line: int | None = None
	end_line: int | None = None

	def __post_init__(self):
		super().__init__(self.msg)


# ------------------------------------------------------------------------------------------------ #
#                                          Parse log files                                         #
# ------------------------------------------------------------------------------------------------ #

class ParsedObject(NamedTuple):
	"""Top-level JSON object parsed from file."""

	start_line: int
	end_line: int
	object: dict[str, Any]


class JsonObjectParser:
	"""Lazily parses a file containing multiple JSON objects.

	Expects either of two formats (determined automatically, and may be mixed):

	* Single-line (standard JSONL): each line contains a complete JSON object.
	* Multi-line: Each JSON object spans multiple lines. The opening and closing braces must
	  appear on their own line with no indentation. Braces of nested objects must be indented or
	  appear with other non-whitespace characters. This is what you get when concatenating output of
	  multiple calls to :func:`json.dump` with a nonzero value for ``indent``.
	"""

	current_line: int

	def __init__(self):
		self.current_line = 0
		self._current_obj: list[str] = []
		self._current_started = 0

	def process_line(self, line: str) -> ParsedObject | None:
		"""Process a single line. If it concludes an object, return it."""
		self.current_line += 1

		# Ignore trailing whitespace but not leading
		line = line.rstrip()

		# Skip blank lines
		if line.isspace():
			return None

		# Already in the middle of a multi-line object?
		if self._current_obj:
			self._current_obj.append(line)

			# Object completed?
			if line == '}':
				data = ''.join(self._current_obj)
				try:
					value = json.loads(data)
				except json.JSONDecodeError as exc:
					raise JsonParseError(
						str(exc),
						start_line=self._current_started,
						end_line=self.current_line,
					) from exc

				rval = ParsedObject(self._current_started, self.current_line, value)
				self._current_obj = []
				self._current_started = 0
				return rval

			return None

		# Starting a new multi-line object?
		if line == '{':
			self._current_obj = [line]
			self._current_started = self.current_line
			return

		# Otherwise expect complete object on single line
		if line.startswith('{'):
			try:
				value = json.loads(line)
			except json.JSONDecodeError:
				pass
			else:
				if isinstance(value, dict):
					return ParsedObject(self.current_line, self.current_line, value)

		raise JsonParseError(
			'Expected single opening brace or complete JSON object',
			start_line=self.current_line,
			end_line=self.current_line,
		)

	def process_lines(self, lines: Iterable[str], complete: bool = True) -> Iterable[ParsedObject]:
		"""Process multiple lines and yield all complete objects parsed.

		Parameters
		----------
		lines
		    Lines to process.
		complete
		    Whether to expect the final line to conclude the final object. If True and lines are
		    exhausted before current object is closed, and exception will be raised. If False, the
		    parser will be in a state to accept more lines to complete the current object (use for
		    batch processing).

		Raises
		------
		JsonParseError
			If ``complete=True`` and the an object has not been closed when the final line is
			reached.
		"""
		for line in lines:
			result = self.process_line(line)
			if result is not None:
				yield result
		if complete:
			self.complete()

	def complete(self) -> None:
		"""Signal that there are no more lines available.

		This will raise an exception if the final JSON object has not been concluded.
		"""
		if self._current_obj:
			raise JsonParseError(
				f'JSON object starting on line {self._current_started} not closed',
				start_line=self._current_started,
				end_line=self.current_line,
			)


def parse_logfile_lazy(file: FilePath | IO[str]) -> Iterator[LogRecord]:
	"""Lazily parse a JSON log file and yield log records.

	File may be in standard JSONL format or "multi-line object" format.

	Parameters
	----------
	file
		File path or open file object.
	"""
	parser = JsonObjectParser()

	with maybe_open(file) as fh:
		for l1, l2, obj in parser.process_lines(fh):
			yield LogRecord.model_validate(obj)


def parse_logfile(file: FilePath | IO[str]) -> LogRecordList:
	"""Parse a complete JSON log file.

	File may be in standard JSONL format or "multi-line object" format.

	Parameters
	----------
	file
		File path or open file object.
	"""
	return LogRecordList(parse_logfile_lazy(file))
