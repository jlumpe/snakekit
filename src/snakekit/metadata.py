"""Parse job metadata in the ``.snakemake/metadata/`` directory."""

from typing import Any, Literal, cast
from base64 import b64decode
from pathlib import Path
import logging
import math

from pydantic import BaseModel, Field

from snakekit.util import FilePath, check_path


logger = logging.getLogger(__name__)


class Metadata(BaseModel):
	"""Parse job metadata in the ``.snakemake/metadata/`` directory."""

	# Depends on Snakemake version, is 6 in v9. Presumably the schema changes.
	# Don't support older versions for now.
	record_format_version: Literal[6]
	code: str | None = None
	rule: str
	input: list[str]
	log: list[str]
	params: list[Any]
	shellcmd: str | None = None
	incomplete: bool
	starttime: float | None = None
	endtime: float
	job_hash: int
	conda_env: str | None = None
	software_stack_hash: str | None = None
	container_img_url: str | None = None
	input_checksums: dict[str, str]


class MetadataJob(Metadata):
	"""Metadata combined for a single job."""

	output: list[str] = Field(default_factory=list)


def find_metadata(snakemake_dir: FilePath) -> dict[str, Path]:
	"""Find metadata files in the snakemake directory, but do not parse them.

	Returns
	-------
		Dictionary mapping decoded output file paths to metadata file paths.
	"""
	check_path(snakemake_dir, exists=True, is_dir=True)
	metadata_dir = Path(snakemake_dir) / 'metadata'
	if not metadata_dir.exists():
		return dict()
	return {
		b64decode(file.name).decode(): file
		for file in metadata_dir.iterdir()
		if file.is_file()
	}


def read_metadata(snakemake_dir: FilePath) -> dict[str, Metadata]:
	"""Read all metadata files in the snakemake directory.

	Returns
	-------
		Dictionary mapping decoded output file paths to parsed metadata.
	"""
	files = find_metadata(snakemake_dir)
	return {
		outfile: Metadata.model_validate_json(mdata_path.read_bytes())
		for outfile, mdata_path in files.items()
	}


def _check_metadata_match(mdata1: Metadata, mdata2: Metadata) -> None:
	"""Check that two metadata objects contain the same data."""

	if mdata1.job_hash != mdata2.job_hash:
		raise ValueError(f'Compared metadata instances have different job hashes: {mdata1.job_hash} != {mdata2.job_hash}')

	for field in Metadata.model_fields:
		if field in ('starttime', 'endtime'):
			continue
		v1 = getattr(mdata1, field)
		v2 = getattr(mdata2, field)
		if v1 != v2:
			raise ValueError(f'Metadata mismatch in job {mdata1.job_hash} for field {field}: {v1} != {v2}')


def combine_times(times: list[float | None], job_hash: int, attr: str) -> float | None:
	"""Combine start or end times for a single job.

	For some reason the values vary slightly between metadata files for a single job.
	"""
	if all(t is None for t in times):
		return None

	if any(t is None for t in times):
		logging.warning(f'Job {job_hash} has inconsistent missing values for {attr}')
		times = [t for t in times if t is not None]

	times2 = cast(list[float], times)

	range_ = max(times2) - min(times2)
	if range_ > 1:
		logger.warning(f'{attr} values of job {job_hash} vary by {range_:.2f} seconds')
	return sum(times2) / len(times2)


def combine_metadata(metadata: dict[str, Metadata]) -> dict[int, MetadataJob]:
	"""Combine per-file metadata into per-job metadata."""

	groups: dict[int, list[tuple[str, Metadata]]] = {}

	for outfile, mdata in metadata.items():
		groups.setdefault(mdata.job_hash, []).append((outfile, mdata))

	jobs = {}

	for job_hash, items in groups.items():
		# Check metadata attributes match
		mdata = items[0][1]
		for outfile, mdata2 in items[1:]:
			_check_metadata_match(mdata, mdata2)

		attrs = mdata.model_dump()
		attrs['output'] = [output for output, _ in items]
		attrs['starttime'] = combine_times([mdata.starttime for _, mdata in items], job_hash, 'starttime')
		attrs['endtime'] = combine_times([mdata.endtime for _, mdata in items], job_hash, 'endtime')

		jobs[job_hash] = MetadataJob(**attrs)

	return jobs
