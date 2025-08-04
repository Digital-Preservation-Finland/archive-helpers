"""Open tar/zip archives with validation."""

from __future__ import annotations

from typing import Literal, Any, get_args
from collections.abc import Generator

import os
import zipfile
import tarfile
from contextlib import contextmanager

from archive_helpers.config import CONFIG
from archive_helpers.archive_validator import TarValidator, ZipValidator
from archive_helpers.exceptions import ExtractError


TarMode = Literal[
    "r",
    "r:*",
    "r:",
    "r:gz",
    "r:bz2",
    "r:xz",
    "a",
    "a:",
    "w",
    "w:",
    "w:gz",
    "w:bz2",
    "w:xz",
    "x",
    "x:",
    "x:gz",
    "x:bz2",
    "x:xz",
    "r|*",
    "r|",
    "r|gz",
    "r|bz2",
    "r|xz",
    "w|",
    "w|gz",
    "w|bz2",
    "w|xz",
]
ZipMode = Literal["r", "w", "a", "x"]

TAR_MODES: tuple[TarMode, ...] = get_args(TarMode)
ZIP_MODES: tuple[ZipMode, ...] = get_args(ZipMode)


@contextmanager
def open_tar(
    tar_path: str | os.PathLike,
    mode: TarMode = "r:*",
    extract_path: str | os.PathLike | None = None,
    allow_overwrite: bool = False,
    max_objects: int = CONFIG.max_objects,
    max_size: int = CONFIG.max_size,
    max_ratio: int = CONFIG.max_ratio,
    **kwargs: Any,
) -> Generator[tarfile.TarFile, None, None]:
    """Context manager wrapper for tarfile objects with validation.

    Opens a tar archive and validates its contents before yielding a
    `TarFile` instance.

    If not provided, the values for `max_objects`, `max_size`, and `max_ratio`
    are loaded from `/etc/archive-helpers/archive-helpers.conf`. If the file is
    not found, default thresholds are used.

    **Usage**::

        # Iterate over members
        with open_tar("/path/to/tar.tar") as tar:
            for member in tar:
                ...

        # Extract contents
        with open_tar("/path/to/tar.tar", extract_path="/extract") as tar:
            tar.extractall("/extract", filter="fully_trusted")

    :param tar_path: Path to the tar archive.
    :param mode: Mode to open the archive (default: `"r:*"`).
    :param extract_path: Directory to extract contents to. If `None`,
        extraction path validation is disabled (default: `None`).
    :param allow_overwrite: If `True`, allows overwriting existing files when
        checking the `extract_path`. (default: `False`).
    :param max_objects: Maximum number of files allowed in the archive. `None`
        disables the check (default: 100000)
    :param max_size: Maximum total size of extracted files in bytes. `None`
        disables the check (default: 4TB).
    :param max_ratio: Maximum allowed compression ratio. `None` disables the
        check (default: 100).
    :param kwargs: Additional keyword arguments passed to `tarfile.open()`.
    :returns: A `TarFile` instance.
    """
    tarf = tarfile.open(name=tar_path, mode=mode, **kwargs)
    TarValidator(
        tarf, extract_path, allow_overwrite, max_objects, max_size, max_ratio
    ).validate_all()
    try:
        yield tarf
    finally:
        tarf.close()


@contextmanager
def open_zip(
    zip_path: str | os.PathLike,
    mode: ZipMode = "r",
    extract_path: str | os.PathLike | None = None,
    allow_overwrite: bool = False,
    max_objects: int = CONFIG.max_objects,
    max_size: int = CONFIG.max_size,
    max_ratio: int = CONFIG.max_ratio,
    **kwargs: Any,
) -> Generator[zipfile.ZipFile, None, None]:
    """Context manager wrapper for zipfile objects with validation.

    Opens a zip archive and validates its contents before yielding a `ZipFile`
    instance.

    If not provided, the values for `max_objects`, `max_size`, and `max_ratio`
    are loaded from `/etc/archive-helpers/archive-helpers.conf`. If the file is
    not found, default thresholds are used.

    **Usage**::

        # Iterate over members
        with open_zip("/path/to/zip.zip") as zip:
            for member in zip.infolist():
                ...

        # Extract contents
        with open_zip("/path/to/zip.zip", extract_path="/extract") as zip:
            zip.extractall("/extract")

    :param zip_path: Path to the zip archive.
    :param mode: Mode to open the archive (default: `"r"`).
    :param allow_overwrite: If `True`, allows overwriting existing files when
        checking the `extract_path`. (default: `False`).
    :param max_objects: Maximum number of files allowed in the archive. `None`
        disables the check (default 100000).
    :param max_size: Maximum total size of extracted files in bytes. `None`
        disables the check (default 4TB).
    :param max_ratio: Maximum allowed compression ratio. `None` disables the
        check (default 100).
    :param kwargs: Additional keyword arguments passed to `zipfile.ZipFile()`.
    :returns: A `ZipFile` instance.
    """
    zipf = zipfile.ZipFile(file=zip_path, mode=mode, **kwargs)
    ZipValidator(
        zipf, extract_path, allow_overwrite, max_objects, max_size, max_ratio
    ).validate_all()
    try:
        yield zipf
    finally:
        zipf.close()


@contextmanager
def open_archive(
    archive: str | os.PathLike,
    mode: ZipMode | TarMode | None = None,
    extract_path: str | os.PathLike | None = None,
    allow_overwrite: bool = False,
    max_objects: int = CONFIG.max_objects,
    max_size: int = CONFIG.max_size,
    max_ratio: int = CONFIG.max_ratio,
    **kwargs: Any,
) -> Generator[tarfile.TarFile | zipfile.ZipFile, None, None]:
    """Context manager wrapper for archives with validation.

    Opens a tar or zip archive and validates its contents before yielding an
    archive object intance.

    If not provided, the values for `max_objects`, `max_size`, and `max_ratio`
    are loaded from `/etc/archive-helpers/archive-helpers.conf`. If the file is
    not found, default thresholds are used.

    :param archive: Path to the archive.
    :param mode: Mode to open the archive. If `None`, uses `"r*:"` for tar
        files and `"r"` for zip files (default: `None`).
    :param extract_path: Directory to extract contents to. If `None`,
        extraction path validation is disabled (default: `None`).
    :param allow_overwrite: If `True`, allows overwriting existing files when
        checking the `extract_path`. (default: `False`).
    :param max_objects: Maximum number of files allowed in the archive. `None`
        disables the check (default: 100000)
    :param max_size: Maximum total size of extracted files in bytes. `None`
        disables the check (default: 4TB).
    :param max_ratio: Maximum allowed compression ratio. `None` disables the
        check (default: 100).
    :param kwargs: Additional keyword arguments to pass when opening the
        archive.
    :returns: A `TarFile` or `ZipFile` instance.
    """
    if tarfile.is_tarfile(archive):
        if mode is None:
            tar_mode = "r:*"
        elif mode in TAR_MODES:
            tar_mode = mode
        else:
            raise ValueError("Invalid mode for opening a tar archive.")
        try:
            with open_tar(
                archive,
                tar_mode,
                extract_path,
                allow_overwrite,
                max_objects,
                max_size,
                max_ratio,
                **kwargs,
            ) as arc:
                yield arc
        except Exception as e:
            raise ExtractError(f"Failed to open '{archive}': {e}") from e

    elif zipfile.is_zipfile(archive):
        if mode is None:
            zip_mode = "r"
        elif mode in ZIP_MODES:
            zip_mode = mode
        else:
            raise ValueError("Invalid mode for opening a zip archive.")
        try:
            with open_zip(
                archive,
                zip_mode,
                extract_path,
                allow_overwrite,
                max_objects,
                max_size,
                max_ratio,
                **kwargs,
            ) as arc:
                yield arc
        except Exception as e:
            raise ExtractError(f"Failed to open '{archive}': {e}") from e

    else:
        raise ExtractError(f"File '{archive}' is not supported")
