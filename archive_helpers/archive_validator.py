"""Validator classes for tar and zip archives."""


from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Generator, Generic, TypeVar

import tarfile
import zipfile
import stat
import os

from archive_helpers.config import CONFIG
from archive_helpers.exceptions import (
    ArchiveSizeError,
    ExtractError,
    MemberNameError,
    MemberOverwriteError,
    MemberTypeError,
    ObjectCountError,
)


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


COMPRESSOR_NAMES: dict[int, str] = getattr(zipfile, "compressor_names", {})


ArchiveT = TypeVar("ArchiveT", tarfile.TarFile, zipfile.ZipFile)
MemberT = TypeVar("MemberT", tarfile.TarInfo, zipfile.ZipInfo)


class _BaseArchiveValidator(Generic[ArchiveT, MemberT], metaclass=ABCMeta):
    """Base class for on-the-fly or full validation of zip or tar archives.

    This is designed to be subclassed for zip or tar archives. It provides the
    shared validation logic common to both formats.
    """

    def __init__(
        self,
        archive: ArchiveT,
        extract_path: str | os.PathLike | None,
        allow_overwrite: bool = False,
        max_objects: int | None = CONFIG.max_objects,
        max_size: int | None = CONFIG.max_size,
        max_ratio: int | None = CONFIG.max_ratio,
    ) -> None:
        """Create an archive validator instance. Use `None` to disable a max
        limit check.

        If not provided, max limits are taken from the configuration file
        `/etc/archive-helpers-archive-helpers-conf`. If this file is not
        available, default values defined in `archive_helpers.config` are used
        instead.

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
        self.compressed_size = os.path.getsize(self.archive_path)

        # Calculated during validation
        self.object_count = 0
        self.uncompressed_size = 0

    def validate_all(self) -> list[MemberT]:
        """Fully validates the archive object by iterating through all its
        members.

        This method is identical to::

            for member in validator:
                pass

        since iterating yields validated members.

        :returns: List containing the archive's validated members.
        :raises ArchiveSizeError: If the archive's uncompressed size or
            compression ratio exceeds limits.
        :raises ObjectCountError: If the archive contains more objects than
            allowed.
        :raises MemberNameError: If a member's name points outside the
            extraction path.
        :raises MemberTypeError: If a member has an unsupported file type.
        :raises MemberOverwriteError: If a member would overwrite an existing
            file during extraction.
        """
        return list(self)

    def update(self, member: MemberT) -> None:
        """
        Update internal validation state with a new archive member.

        :param member: A member object from the archive.
        :raises ArchiveSizeError: If the archive's uncompressed size or
            compression ratio exceeds limits.
        :raises ObjectCountError: If the archive contains more objects than
            allowed.
        :raises MemberNameError: If a member's name points outside the
            extraction path.
        :raises MemberTypeError: If a member has an unsupported file type.
        :raises MemberOverwriteError: If a member would overwrite an existing
            file during extraction.
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

    @abstractmethod
    def _update_counts(self, member: MemberT) -> None:
        """Update `self.object_count` and `self.uncompressed_size`."""
        # Must be implemented by subclass because `TarInfo` and `ZipInfo`
        # have different properties.

    @abstractmethod
    def __iter__(self) -> Generator[MemberT]:
        """Iterate over all members of the archive. Members are yielded after
        they are validated, so iterating over all members validates the
        archive.

        :returns: Validated member object.
        :raises ArchiveSizeError: If the archive's uncompressed size or
            compression ratio exceeds limits.
        :raises ObjectCountError: If the archive contains more objects than
            allowed.
        :raises MemberNameError: If a member's name points outside the
            extraction path.
        :raises MemberTypeError: If a member has an unsupported file type.
        :raises MemberOverwriteError: If a member would overwrite an existing
            file during extraction.
        """
        # Implemented in subclass.


class ZipValidator(_BaseArchiveValidator[zipfile.ZipFile, zipfile.ZipInfo]):
    """Validator for zip archives. Supports both incremental and full
    validation.

    This class can be used to enforce constraints on zip files:
    - Maximum number of contained files
    - Maximum total uncompressed size
    - Maximum compression ratio
    - Supported compression types

    **Usage**::

        from archive_helpers.archive_validator import ZipValidator
        from zipfile import ZipFile

        zipf = ZipFile("path/to/archive.zip")
        validator = ZipValidator(zipf)

        # Validate members one by one...
        for member in validator:
            ...

        # ...or validate the entire archive at once.
        validator.validate_all()
    """

    def __init__(
        self,
        zipf: zipfile.ZipFile,
        extract_path: str | os.PathLike | None,
        allow_overwrite: bool = False,
        max_objects: int | None = CONFIG.max_objects,
        max_size: int | None = CONFIG.max_size,
        max_ratio: int | None = CONFIG.max_ratio,
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
                # Rare compression types like ppmd and deflate64 that have not
                # been implemented should raise an ExtractError
                raise ExtractError(
                    "Compression type not supported: "
                    + str(COMPRESSOR_NAMES.get(comp_type, comp_type))
                )
            self.update(member)
            yield member


class TarValidator(_BaseArchiveValidator[tarfile.TarFile, tarfile.TarInfo]):
    """Validator for tar archives. Supports both incremental and full
    validation.

    This class can be used to enforce constraints on tar files:
    - Maximum number of contained files
    - Maximum total uncompressed size
    - Maximum compression ratio

    **Usage**::

        from archive_helpers.archive_validator import TarValidator
        from tarfile import TarFile

        tarf = TarFile.open("path/to/tar.tar")
        validator = TarValidator(tarf)

        # Validate members one by one...
        for member in validator:
            ...

        # ...or validate the entire archive at once.
        validator.validate_all()

    """

    def __init__(
        self,
        tarf: tarfile.TarFile,
        extract_path: str | os.PathLike | None,
        allow_overwrite: bool = False,
        max_objects: int | None = CONFIG.max_objects,
        max_size: int | None = CONFIG.max_size,
        max_ratio: int | None = CONFIG.max_ratio,
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
