import importlib.metadata
import functools
from typing import Annotated

from packaging.version import Version, parse as parse_version
from pydantic import BaseModel, PlainSerializer, BeforeValidator, ConfigDict


type VersionField = Annotated[
	Version,
	PlainSerializer(str),
	BeforeValidator(lambda v: v if isinstance(v, Version) else parse_version(v)),
]


def _get_version(package: str) -> Version | None:
	try:
		version_str = importlib.metadata.version(package)
	except importlib.metadata.PackageNotFoundError:
		return None
	return parse_version(version_str)


class VersionInfo(BaseModel):
	"""Installed versions of Snakekit and core Snakemake packages.

	Parameters
	----------
	snakekit
		Version of Snakekit.
	snakemake
		Version of core Snakemake package.
	snakemake_interface_common
		Version of ``snakemake-interface-common``.
	snakemake_interface_logger_plugins
		Version of ``snakemake-interface-logger-plugins``.
	"""

	model_config = ConfigDict(extra='forbid', arbitrary_types_allowed=True)

	snakekit: VersionField
	snakemake: VersionField | None
	snakemake_interface_common: VersionField | None
	snakemake_interface_logger_plugins: VersionField | None

	def _get_version(self, package: str) -> Version | None:
		if package in self.model_fields:
			return getattr(self, package)
		raise ValueError(f'Invalid package: {package}')

	def at_least(self, package: str, version: Version |  str | tuple[int, ...]) -> bool:
		"""Check whether the installed version of a package is at least a given version.

		Parameters
		----------
		package
			Model field name corresponding to the package.
		version
			Version to check against. Can be a string or ``(major, minor[, patch])`` tuple.
		"""
		installed = self._get_version(package)

		if isinstance(version, str):
			version = parse_version(version)
		elif isinstance(version, tuple):
			if not 2 <= len(version) <= 3:
				raise ValueError('Version tuple must have 2 or 3 elements')
			version = Version('.'.join(map(str, version)))
		elif not isinstance(version, Version):
			raise TypeError('version must be a string, tuple, or Version instance')

		return installed is not None and installed >= version


@functools.lru_cache()
def get_version_info() -> VersionInfo:
	"""Get information about installed software versions."""
	from snakekit import __version__ as snakekit_version

	return VersionInfo(
		snakekit=parse_version(snakekit_version),
		snakemake=_get_version('snakemake'),
		snakemake_interface_common=_get_version('snakemake-interface-common'),
		snakemake_interface_logger_plugins=_get_version('snakemake-interface-logger-plugins'),
	)
