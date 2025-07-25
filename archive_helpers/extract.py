"""Extract/decompress various archive formats"""

from __future__ import annotations
from typing import Literal, Any, get_args
from collections.abc import Generator

import errno
import os
import tarfile
import zipfile
from contextlib import contextmanager

from archive_helpers.archive_validator import TarValidator, ZipValidator
from archive_helpers.config import CONFIG
from archive_helpers.exceptions import ExtractError


RATIO_THRESHOLD = CONFIG.max_ratio
SIZE_THRESHOLD = CONFIG.max_size
OBJECT_THRESHOLD = CONFIG.max_objects


def tarfile_extract(
    tar_path: str | os.PathLike,
    extract_path: str | os.PathLike,
    allow_overwrite: bool = False,
    precheck: bool = True,
    max_objects: int | None = OBJECT_THRESHOLD,
    max_size: int | None = SIZE_THRESHOLD,
    max_ratio: int | None = RATIO_THRESHOLD,
) -> None:
    """Decompress using tarfile module.

    :param tar_path: Path to the tar archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
        without raising an error (defaults to False)
    :param precheck: Boolean that defines whether to check to whole archive
        before extraction or not. If True, user does not need to worry about
        the cleanup. If False, archive is read only once and the members are
        extracted immediately after the check. User is responsible for the
        cleanup if member check raises an error with `precheck=False`.
    :param max_objects: Limit how many objects the tar file can have. Use
        `None` for no limit. Default limit is 1000000.
    :param max_size: Limit how large the decompressed archive can be. Use
        `None` for no limit. Default limit is 4TB.
    :param max_ratio: Limit the archive's compression ratio. This is *only*
        checked for the entire archive. Use `None` for no limit. Default limit
        is 100.
    :returns: None
    """
    if not tarfile.is_tarfile(tar_path):
        raise ExtractError("File is not a tar archive")

    # A blank tar archive with nothing in it counts as a valid tar file
    # but causes problems later on. Don't allow blank tar archives
    with tarfile.open(tar_path) as tarf:
        # next() function should be used for performance reasons instead of
        # getmembers(). In next() blank tar archive raises Invalid argument
        # OSError, catching that is interpreted as blank tar archive
        try:
            is_blank = False
            is_blank = not tarf.next()
        except OSError as exc:
            if exc.errno != errno.EINVAL:
                raise
            is_blank = True
        if is_blank:
            raise ExtractError("Blank tar archives are not supported.")

    if precheck:
        with tarfile.open(tar_path, "r|*") as tarf:
            validator = TarValidator(
                tarf=tarf,
                extract_path=extract_path,
                allow_overwrite=allow_overwrite,
                max_objects=max_objects,
                max_size=max_size,
                max_ratio=max_ratio,
            )
            validator.validate_all()
        with tarfile.open(tar_path, "r|*") as tarf:
            try:
                tarf.extractall(extract_path, filter="fully_trusted")
            except TypeError:  # 'filer' does not exist
                tarf.extractall(extract_path)
    else:
        # Read archive only once by extracting files on the fly
        extract_abs_path = os.path.abspath(extract_path)
        with tarfile.open(tar_path, "r|*") as tarf:
            validator = TarValidator(
                tarf=tarf,
                extract_path=extract_path,
                allow_overwrite=allow_overwrite,
                max_objects=max_objects,
                max_size=max_size,
                max_ratio=max_ratio,
            )

            for member in validator:
                try:
                    tarf.extract(
                        member,
                        path=extract_abs_path,
                        filter="fully_trusted",
                    )
                except TypeError:  # 'filter' does not exist
                    tarf.extract(member, path=extract_abs_path)


def zipfile_extract(
    zip_path: str | os.PathLike,
    extract_path: str | os.PathLike,
    allow_overwrite: bool = False,
    precheck: bool = True,
    max_objects: int | None = OBJECT_THRESHOLD,
    max_size: int | None = SIZE_THRESHOLD,
    max_ratio: int | None = RATIO_THRESHOLD,
) -> None:
    """Decompress using zipfile module.

    :param zip_path: Path to the zip archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
        without raising an error (defaults to False)
    :param precheck: Boolean that defines whether to check to whole archive
        before extraction or not. If True, user does not need to worry about
        the cleanup. If False, archive is read only once and the members are
        extracted immediately after the check. User is responsible for the
        cleanup if member check raises an error with `precheck=False`.
    :param max_objects: Limit how many objects the archive can have. Use
        `None` for no limit. Default limit is 100000.
    :param max_size: Limit how large the decompressed archive can be. Use
        `None` for no limit. Default limit is 4TB.
    :param max_ratio: Limit the archive's compression ratio. This is checked
        for the entire archive and for each member of the archive. Use `None`
        for no limit. Default limit is 100.
    :raises ArchiveSizeError: If the uncompressed archive is too large, or has
        too large compression ratio.
    :raises ObjectCountError: If the archive has too many objects.
    :returns: None
    """
    if not zipfile.is_zipfile(zip_path):
        raise ExtractError("File is not a zip archive")

    with zipfile.ZipFile(zip_path) as zipf:
        validator = ZipValidator(
            zipf=zipf,
            extract_path=extract_path,
            allow_overwrite=allow_overwrite,
            max_objects=max_objects,
            max_size=max_size,
            max_ratio=max_ratio,
        )
        if precheck:
            validator.validate_all()
            zipf.extractall(extract_path)
        else:
            # Read archive only once by extracting files on the fly
            for member in validator:
                zipf.extract(member, path=os.path.abspath(extract_path))


def extract(
    archive: str | os.PathLike,
    extract_path: str | os.PathLike,
    allow_overwrite: bool = False,
    precheck: bool = True,
    max_objects: int | None = OBJECT_THRESHOLD,
    max_size: int | None = SIZE_THRESHOLD,
    max_ratio: int | None = RATIO_THRESHOLD,
) -> None:
    """Extract tar or zip archives.

    If no values are provided for `max_objects`, `max_size` or `max_ratio`,
    they use values from `/etc/archive-helpers/archive-helpers.conf`.
    If reading the config fails, default values are used instead.

    :param archive: Path to the tar or zip archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
        without raising an error (defaults to False)
    :param precheck: Boolean that defines whether to check to whole archive
        before extraction or not. If True, user does not need to worry about
        the cleanup. If False, archive is read only once and the members are
        extracted immediately after the check. User is responsible for the
        cleanup if member check raises an error with precheck=False.
    :param max_objects: Limit how many objects the tar file can have. Use
        `None` for no limit. Default limit is 100000.
    :param max_size: Limit how large the decompressed archive can be. Use
        `None` for no limit. Default limit is 4TB.
    :param max_ratio: Limit the archive's compression ratio. For zip archives,
        in addition to checking the entire archive, each member is also checked
        individually. Use `None` for no limit. Default limit is 100.
    :raises ExtractError: If extracting file is not supported.
    :returns: None
    """
    if tarfile.is_tarfile(archive):
        func = tarfile_extract
    elif zipfile.is_zipfile(archive):
        func = zipfile_extract
    else:
        raise ExtractError(f"File '{archive}' is not supported")

    func(
        archive,
        extract_path=extract_path,
        allow_overwrite=allow_overwrite,
        precheck=precheck,
        max_objects=max_objects,
        max_size=max_size,
        max_ratio=max_ratio,
    )


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
TAR_MODES: tuple[TarMode, ...] = get_args(TarMode)


ZipMode = Literal["r", "w", "a", "x"]
ZIP_MODES: tuple[ZipMode, ...] = get_args(ZipMode)


@contextmanager
def open_tar(
    tar_path: str | os.PathLike,
    mode: TarMode = "r:*",
    extract_path: str | os.PathLike | None = None,
    allow_overwrite: bool = False,
    max_objects: int = OBJECT_THRESHOLD,
    max_size: int = SIZE_THRESHOLD,
    max_ratio: int = RATIO_THRESHOLD,
    **kwargs: Any,
) -> Generator[tarfile.TarFile, None, None]:
    """Context manager wrapper for tarfile objects with validation.

    Opens a tar archive and validates its contents before yielding a
    `TarFile` instance.

    If not provided, the values for `max_objects`, `max_size`, and `max_ratio`
    are loaded from `/etc/archive-helpers/archive-helpers.conf`. If the file is
    not found, default thresholds are used.

    Usage::

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
    max_objects: int = OBJECT_THRESHOLD,
    max_size: int = SIZE_THRESHOLD,
    max_ratio: int = RATIO_THRESHOLD,
    **kwargs: Any,
) -> Generator[zipfile.ZipFile, None, None]:
    """Context manager wrapper for zipfile objects with validation.

    Opens a zip archive and validates its contents before yielding a `ZipFile`
    instance.

    If not provided, the values for `max_objects`, `max_size`, and `max_ratio`
    are loaded from `/etc/archive-helpers/archive-helpers.conf`. If the file is
    not found, default thresholds are used.

    Usage::

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
    max_objects: int = OBJECT_THRESHOLD,
    max_size: int = SIZE_THRESHOLD,
    max_ratio: int = RATIO_THRESHOLD,
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
