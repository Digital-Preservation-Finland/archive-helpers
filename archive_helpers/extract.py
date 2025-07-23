"""Extract/decompress various archive formats"""

from __future__ import annotations
from typing import TypeVar, Generic, Literal, Any, get_args
from collections.abc import Generator

import errno
import os
import stat
import tarfile
import zipfile
from contextlib import contextmanager

from archive_helpers.config import CONFIG

FILETYPES = {
    0o010000: "FIFO",
    0o020000: "CHR",
    0o040000: "DIR",
    0o060000: "BLK",
    0o100000: "REG",
    0o120000: "SYM",
    0o140000: "SOCK",
}

TAR_FILE_TYPES = {
    b"0": "REG",
    b"1": "LNK",
    b"2": "SYM",
    b"3": "CHR",
    b"4": "BLK",
    b"5": "DIR",
    b"6": "FIFO",
    b"7": "CONT",
}

SUPPORTED_ZIPFILE_COMPRESS_TYPES = {
    zipfile.ZIP_STORED,
    zipfile.ZIP_DEFLATED,
    zipfile.ZIP_BZIP2,
    zipfile.ZIP_LZMA,
}

RATIO_THRESHOLD = CONFIG.max_ratio
SIZE_THRESHOLD = CONFIG.max_size
OBJECT_THRESHOLD = CONFIG.max_objects


class ExtractError(Exception):
    """Generic archive extraction error raised when the archive is not
    supported.
    """


class ObjectCountError(Exception):
    """Generic archive extraction error raised when the archive has too many
    objects.
    """


class MemberNameError(Exception):
    """Exception raised when tar or zip files contain members with names
    pointing outside the extraction path.
    """


class MemberTypeError(Exception):
    """Exception raised when tar or zip files contain members with filetype
    other than REG or DIR.
    """


class MemberOverwriteError(Exception):
    """Exception raised when extracting the archive would overwrite files."""


class ArchiveSizeError(Exception):
    """Exception raised when an archive's compression ratio or
    uncompressed size exceeds defined thresholds.
    """


ArchiveT = TypeVar("ArchiveT", tarfile.TarFile, zipfile.ZipFile)
MemberT = TypeVar("MemberT", tarfile.TarInfo, zipfile.ZipInfo)


class _BaseArchiveValidator(Generic[ArchiveT, MemberT]):
    """Base Class for on-the-fly or full validation of zip or tar archives."""

    def __init__(
        self,
        archive: ArchiveT,
        extract_path: str | os.PathLike | None,
        allow_overwrite: bool = False,
        max_objects: int | None = OBJECT_THRESHOLD,
        max_size: int | None = SIZE_THRESHOLD,
        max_ratio: int | None = RATIO_THRESHOLD,
    ) -> None:
        """Create an archive validator instance. Use `None` to disable max
        limits.

        If not provided, max limits use values configured in
        `/etc/archive-helpers-archive-helpers-conf`. If this file is not
        available, default values are used instead.

        :param archive: Opened archive object.
        :param extract_path: Directory where the archive is extracted. Use
            `None` to disable related checks.
        :param allow_overwrite: Allow overwriting existing files
            without raising an error (default False).
        :param max_objects: Max number of objects allowed (default 100000).
        :param max_size: Max uncompressed size allowed (default 4TB).
        :param max_ratio: Max compression ratio allowed (default 100).
        """

        self.archive = archive
        self.extract_path = extract_path
        self.allow_overwrite = allow_overwrite

        self.max_objects = max_objects
        self.max_size = max_size
        self.max_ratio = max_ratio

        self.archive_path = self._get_archive_path(archive)
        self.object_count = 0
        self.uncompressed_size = 0
        self.compressed_size = os.path.getsize(self.archive_path)

    def validate_all(self) -> list[MemberT]:
        """Fully validates the archive object by iterating through it.

        :returns: List containing the archive's members.
        :raises ArchiveSizeError: If size or ratio limits are exceeded.
        :raises ObjectCountError: If object count limit is exceeded.
        """
        return list(self)

    def update(self, member: MemberT) -> None:
        """
        Update validation state with a new member for on-the-fly validation.

        :param member: A member object from the archive.
        :raises ArchiveSizeError: If size or ratio limits are exceeded.
        :raises ObjectCountError: If object count limit is exceeded.
        """
        self._validate_member(
            member=member,
            extract_path=(
                os.path.abspath(self.extract_path)
                if self.extract_path
                else None
            ),
            allow_overwrite=self.allow_overwrite,
            max_ratio=self.max_ratio,
        )

        self._update_counts(member)

        if (
            self.max_objects is not None
            and self.object_count > self.max_objects
        ):
            raise ObjectCountError(
                f"Archive '{self.archive_path}' contains more than the allowed"
                f" number of objects ({self.max_objects})."
            )

        if (
            self.max_size is not None
            and self.uncompressed_size > self.max_size
        ):
            raise ArchiveSizeError(
                f"Archive '{self.archive_path}' exceeds the allowed"
                f" uncompressed size limit of {self.max_size} bytes."
            )

        if self.max_ratio is not None and self.compressed_size > 0:
            ratio = self.uncompressed_size / self.compressed_size
            if ratio > self.max_ratio:
                raise ArchiveSizeError(
                    f"Archive '{self.archive_path}' exceeds the allowed"
                    f" compression ratio of {self.max_ratio}."
                )

    def _get_archive_path(self, archive: ArchiveT) -> str:
        if isinstance(archive, zipfile.ZipFile):
            return str(archive.filename)
        if isinstance(archive, tarfile.TarFile):
            return str(archive.name)
        raise TypeError(f"Unsupported archive type: {type(archive)}")

    def _update_counts(self, member: MemberT) -> None:
        """Update `self.object_count` and `self.uncompressed_size`

        This is implemented in child classes because `TarInfo` and `ZipInfo`
        objects have different properties.
        """
        raise NotImplementedError

    def _validate_member(
        self,
        member: MemberT,
        extract_path: str | None,
        allow_overwrite: bool = False,
        max_ratio: int | None = None,
    ) -> None:
        """Validates that there are no issues with a given member.

        :param member: ZipInfo or TarInfo member.
        :param extract_path: Directory where the archive is extracted to. Use
            `None` to disable related checks.
        :param allow_overwrite: Boolean to allow overwriting existing files
            without raising an error (defaults to False).
        :param max_ratio: Limit compression ratio for each member (only zip).
            Use `None` for no limit.
        :raises MemberNameError: is raised when filename is invalid for the
            member.
        :raises MemberTypeError: is raised when the member is of unsupported
            filetype.
        :raises MemberOverwriteError: If an existing file was discovered in the
            extract patch.
        :raises ArchiveSizeError: If any file has too large compression
            ratio.
        """

        def _tar_filetype_evaluation(
            member: tarfile.TarInfo,
        ) -> tuple[bool, str]:
            """Inner function to set the supported_type and file_type variables
            for tar files.
            :returns: Tuple of (supported_type, filetype)
            """
            return (
                member.isfile() or member.isdir(),
                TAR_FILE_TYPES[member.type],
            )

        def _zip_filetype_evaluation(
            member: zipfile.ZipInfo,
        ) -> tuple[bool, str]:
            """Inner function to set the supported_type and file_type variables
            for zip files.
            :returns: Tuple of (supported_type, filetype)
            """
            mode = member.external_attr >> 16  # Upper two bytes of ext attr'

            if mode != 0 and stat.S_IFMT(mode) not in FILETYPES:
                # Unrecognized modes are probably created by accident on
                # non-POSIX systems by legacy software.
                # The upper three bytes are non-MS-DOS external file
                # attributes (upper two by unix systems), while the lowest
                # byte is used by MS-DOS.
                # Standard MS-DOS input should set this field to zero.
                # Chapter 4.4.15 in (https://pkware.cachefly.net/webdocs/
                # casestudies/APPNOTE.TXT)
                # We'll allow files with non standard data in the external
                # attributes, but we'll mask the mode by zeroing the upper
                # two bytes used by unix systems.
                member.external_attr &= 0xFFFF
                mode = 0

            supported_type = stat.S_ISDIR(mode) or stat.S_ISREG(mode)
            # Support zip archives made with non-POSIX compliant operating
            # systems where file mode is not specified, e.g., windows.
            supported_type |= mode == 0
            filetype = (
                FILETYPES[stat.S_IFMT(mode)] if mode != 0 else "non-POSIX"
            )

            return supported_type, filetype

        # Evaluate the filetype
        supported_type, filetype = (
            _tar_filetype_evaluation(member)
            if isinstance(member, tarfile.TarInfo)
            else _zip_filetype_evaluation(member)
        )

        filename = (
            member.filename
            if isinstance(member, zipfile.ZipInfo)
            else member.name
        )
        if not supported_type:
            raise MemberTypeError(
                f"File '{filename}' has unsupported type: {filetype}"
            )

        self._validate_extract_path(extract_path, allow_overwrite, filename)

        # Check that the compression ratio does not exceed the threshold.
        # This check only applies to zip archives, as tar archives do not
        # compress individual members
        if (
            isinstance(member, zipfile.ZipInfo)
            and member.compress_size > 0
            and max_ratio is not None
        ):
            ratio = member.file_size / member.compress_size
            if ratio > max_ratio:
                raise ArchiveSizeError(
                    f"Compression ratio of file '{filename}' ({ratio:.2f})"
                    f" exceeds the allowed maximum ({max_ratio:.2f})."
                )

    def _validate_extract_path(
        self,
        extract_path: str | None,
        allow_overwrite: bool,
        filename: str,
    ) -> None:
        """Check that the extract path is valid.

        :param extract_path: Directory where the archive is extracted to. Use
            `None` to disable related checks.
        :param allow_overwrite: Boolean to allow overwriting existing files
            without raising an error (defaults to False).
        :param filename: Filename of the member.
        :raises MemberNameError: is raised when filename is invalid for the
            member.
        :raises MemberOverwriteError: If an existing file was discovered in the
            extract patch.
        """
        if extract_path is not None:
            fpath = os.path.abspath(os.path.join(extract_path, filename))
            # Check if the archive member is valid
            if os.path.commonpath([fpath, extract_path]) != os.path.abspath(
                extract_path
            ):
                raise MemberNameError(f"Invalid file path: '{filename}'")

            # Do not raise error if overwriting member files is permitted
            if not allow_overwrite and os.path.isfile(fpath):
                raise MemberOverwriteError(f"File '{filename}' already exists")

    def __iter__(self) -> Generator[MemberT]:
        """Iterate over all members in the archive, validating each one.

        :returns: Validated member object.
        :raises ArchiveSizeError: If size or ratio limits are exceeded.
        :raises ObjectCountError: If object count limit is exceeded.
        """
        # Child classes implement __iter__()
        raise NotImplementedError


class ZipValidator(_BaseArchiveValidator[zipfile.ZipFile, zipfile.ZipInfo]):
    """Class for on-the-fly or full validation of zip archives."""

    def __init__(
        self,
        zipf: zipfile.ZipFile,
        extract_path: str | os.PathLike | None,
        allow_overwrite: bool = False,
        max_objects: int | None = OBJECT_THRESHOLD,
        max_size: int | None = SIZE_THRESHOLD,
        max_ratio: int | None = RATIO_THRESHOLD,
    ) -> None:
        super().__init__(
            zipf,
            extract_path,
            allow_overwrite,
            max_objects,
            max_size,
            max_ratio,
        )

    def _update_counts(self, member: zipfile.ZipInfo) -> None:
        if not member.is_dir():
            self.object_count += 1
            self.uncompressed_size += member.file_size

    def __iter__(self) -> Generator[zipfile.ZipInfo, None, None]:
        for member in self.archive.infolist():
            comp_type = member.compress_type
            if comp_type not in SUPPORTED_ZIPFILE_COMPRESS_TYPES:
                # Rare compression types like ppmd amd deflate64 that have not
                # been implemented should raise an ExtractError
                raise ExtractError(
                    "Compression type not supported: "
                    + str(zipfile.compressor_names.get(comp_type, comp_type))
                )
            self.update(member)
            yield member


class TarValidator(_BaseArchiveValidator[tarfile.TarFile, tarfile.TarInfo]):
    """Class for on-the-fly or full validation of tar archives."""

    def __init__(
        self,
        tarf: tarfile.TarFile,
        extract_path: str | os.PathLike | None,
        allow_overwrite: bool = False,
        max_objects: int | None = OBJECT_THRESHOLD,
        max_size: int | None = SIZE_THRESHOLD,
        max_ratio: int | None = RATIO_THRESHOLD,
    ) -> None:
        super().__init__(
            tarf,
            extract_path,
            allow_overwrite,
            max_objects,
            max_size,
            max_ratio,
        )

    def _update_counts(self, member: tarfile.TarInfo) -> None:
        if member.isfile():
            self.object_count += 1
            self.uncompressed_size += member.size

    def __iter__(self) -> Generator[tarfile.TarInfo, None, None]:
        for member in self.archive:
            self.update(member)
            yield member


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
tar_modes: tuple[TarMode, ...] = get_args(TarMode)


ZipMode = Literal["r", "w", "a", "x"]
zip_modes: tuple[ZipMode, ...] = get_args(ZipMode)


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
        elif mode in tar_modes:
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
        elif mode in zip_modes:
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
