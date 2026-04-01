import importlib.metadata
import functools

from packaging.version import Version, parse as parse_version


@functools.lru_cache()
def get_snakemake_version() -> Version | None:
	"""Get the installed version of Snakemake.

	Snakemake itself is not currently a required dependency of this package, so return None if it is
	not installed.
	"""
	try:
		version = importlib.metadata.version('snakemake')
	except importlib.metadata.PackageNotFoundError:
		return None
	return parse_version(version)


@functools.lru_cache()
def get_logging_interface_version() -> Version:
	"""Get the installed version of ``snakemake-interface-logger-plugins``.

	This is a dependency of the package, so it should always be present.
	"""
	return parse_version(importlib.metadata.version('snakemake-interface-logger-plugins'))
