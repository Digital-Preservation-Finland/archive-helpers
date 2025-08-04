"""Extract/decompress various archive formats"""

from __future__ import annotations


import errno
import os
import tarfile
import zipfile


from archive_helpers.archive_validator import TarValidator, ZipValidator
from archive_helpers.config import CONFIG

# TODO: Some other repos import exceptions from here, since exceptions were
# originally defined here. These unused imports should be removed when other
# repos have been updated.
from archive_helpers.exceptions import (  # noqa: F401
    ExtractError,
    ObjectCountError,
    MemberNameError,
    MemberTypeError,
    MemberOverwriteError,
    ArchiveSizeError,
)


def tarfile_extract(
    tar_path: str | os.PathLike,
    extract_path: str | os.PathLike,
    allow_overwrite: bool = False,
    precheck: bool = True,
    max_objects: int | None = CONFIG.max_objects,
    max_size: int | None = CONFIG.max_size,
    max_ratio: int | None = CONFIG.max_ratio,
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
            except TypeError:  # 'filter' does not exist
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

            # Iterating a TarValidator yields members after validating them
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
    max_objects: int | None = CONFIG.max_objects,
    max_size: int | None = CONFIG.max_size,
    max_ratio: int | None = CONFIG.max_ratio,
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
            # Read archive only once by extracting files on the fly.
            # Iterating a ZipValidator yields members after validating them
            for member in validator:
                zipf.extract(member, path=os.path.abspath(extract_path))


def extract(
    archive: str | os.PathLike,
    extract_path: str | os.PathLike,
    allow_overwrite: bool = False,
    precheck: bool = True,
    max_objects: int | None = CONFIG.max_objects,
    max_size: int | None = CONFIG.max_size,
    max_ratio: int | None = CONFIG.max_ratio,
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
        extract_func = tarfile_extract
    elif zipfile.is_zipfile(archive):
        extract_func = zipfile_extract
    else:
        raise ExtractError(f"File '{archive}' is not supported")

    extract_func(
        archive,
        extract_path=extract_path,
        allow_overwrite=allow_overwrite,
        precheck=precheck,
        max_objects=max_objects,
        max_size=max_size,
        max_ratio=max_ratio,
    )
