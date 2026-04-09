from contextlib import contextmanager
import errno
import os
from pathlib import Path
from typing import IO, Any
from collections.abc import Iterator


type FilePath = str | os.PathLike[str]


def make_oserror(typ: type[OSError], filename: FilePath) -> OSError:
	"""Properly create an instance of an :exc:`OSError` subclass.

	Not sure why Python makes this so hard.
	"""
	if not issubclass(typ, OSError):
		raise TypeError(f'Expected subclass of OSError, got {typ}')
	if typ is OSError:
		raise TypeError('Type must be a subclass of OSError')

	if typ is FileNotFoundError:
		errno_ = errno.ENOENT
	elif typ is FileExistsError:
		errno_ = errno.EEXIST
	elif typ is IsADirectoryError:
		errno_ = errno.EISDIR
	elif typ is NotADirectoryError:
		errno_ = errno.ENOTDIR
	else:
		raise NotImplementedError(f'Unsupported OSError subclass: {typ}')

	return typ(errno_, os.strerror(errno_), os.fspath(filename))


def check_path(path: FilePath, *, exists: bool | None = None, is_dir: bool | None = None) -> None:
	"""
	Check basic attributes of file path and raise the appropriate exception if the path does not
	meet the criteria.

	Parameters
	----------
	path
		The path to check.
	exists
		Whether the path should exist.
	is_dir
		Whether the path should be a directory (true) or file (false). Is not checked if the path
		does not exist.
	"""

	path = Path(path)

	if exists is not None:
		if exists and not path.exists():
			raise make_oserror(FileNotFoundError, path)
		elif not exists and path.exists():
			raise make_oserror(FileExistsError, path)

	if is_dir is not None and path.exists():
		if is_dir and not path.is_dir():
			raise make_oserror(NotADirectoryError, path)
		elif not is_dir and path.is_dir():
			raise make_oserror(IsADirectoryError, path)


@contextmanager
def maybe_open[TIo: IO[Any]](file: FilePath | TIo, **kw) -> Iterator[TIo]:
	"""Open a file if given a file path, or return argument directly if given a file object.

	Returns a context manager that yields an open file object. If the argument is path, the file is
	opened and and then closed upon exiting the context. If the argument is a file object it will
	not be closed by the context manager.

	Parameters
	----------
	file
		A file path to open or an existing file object.
	**kw
		Keyword arguments to pass to :func:`open`.
	"""
	if isinstance(file, (str, bytes, os.PathLike)):
		with open(file, **kw) as f:
			yield f
	else:
		yield file
