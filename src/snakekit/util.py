import errno
import os
from pathlib import Path


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

