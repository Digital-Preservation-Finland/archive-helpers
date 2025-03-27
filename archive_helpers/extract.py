"""Extract/decompress various archive formats"""
import errno
import os
import stat
import tarfile
import zipfile

FILETYPES = {
    0o010000: "FIFO",
    0o020000: "CHR",
    0o040000: "DIR",
    0o060000: "BLK",
    0o100000: "REG",
    0o120000: "SYM",
    0o140000: "SOCK"
}

TAR_FILE_TYPES = {
    b"0": "REG",
    b"1": "LNK",
    b"2": "SYM",
    b"3": "CHR",
    b"4": "BLK",
    b"5": "DIR",
    b"6": "FIFO",
    b"7": "CONT"
}


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


def tarfile_extract(tar_path,
                    extract_path,
                    allow_overwrite=False,
                    precheck=True,
                    max_size=None):
    """Decompress using tarfile module.

    :param tar_path: Path to the tar archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :param precheck: Boolean that defines whether to check to whole archive
                     before extraction or not. If True, user does not need to
                     worry about the cleanup. If False, archive is read only
                     once and the members are extracted immediately after the
                     check. User is responsible for the cleanup if member check
                     raises an error with precheck=False.
    :param max_size: Int that defines how many objects can tar file have
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
        with tarfile.open(tar_path, 'r|*') as tarf:
            _check_archive_members(
                tarf, extract_path,
                allow_overwrite=allow_overwrite,
                max_size=max_size,
            )
        with tarfile.open(tar_path, 'r|*') as tarf:
            tarf.extractall(extract_path)
    else:
        # Read archive only once by extracting files on the fly
        extract_abs_path = os.path.abspath(extract_path)
        with tarfile.open(tar_path, 'r|*') as tarf:
            archive_size = 0
            for member in tarf:
                if max_size is not None:
                    if member.isfile():
                        archive_size += 1
                    if archive_size > max_size:
                        raise ObjectCountError(
                                "Archive has too many objects -"
                                f" Max size is {max_size} objects"
                        )
                _validate_member(member,
                                 extract_path=extract_abs_path,
                                 allow_overwrite=allow_overwrite)
                tarf.extract(member, path=extract_abs_path)


def zipfile_extract(zip_path,
                    extract_path,
                    allow_overwrite=False,
                    precheck=True,
                    max_size=None):
    """Decompress using zipfile module.

    :param zip_path: Path to the zip archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :param precheck: Boolean that defines whether to check to whole archive
                     before extraction or not. If True, user does not need to
                     worry about the cleanup. If False, archive is read only
                     once and the members are extracted immediately after the
                     check. User is responsible for the cleanup if member check
                     raises an error with precheck=False.
    :param max_size: Int that defines how many objects can zip file have
    :returns: None
    """
    if not zipfile.is_zipfile(zip_path):
        raise ExtractError("File is not a zip archive")
    try:
        with zipfile.ZipFile(zip_path) as zipf:
            if precheck:
                _check_archive_members(
                    zipf.infolist(), extract_path,
                    allow_overwrite=allow_overwrite,
                    max_size=max_size,
                )
                zipf.extractall(extract_path)
            else:
                archive_size = 0
                for member in zipf.infolist():
                    if max_size is not None:
                        if not member.is_dir():
                            archive_size += 1
                        if archive_size > max_size:
                            raise ObjectCountError(
                                    "Archive has too many objects -"
                                    f" Max size is {max_size} objects"
                            )
                    # Read archive only once by extracting files on the fly
                    extract_abs_path = os.path.abspath(extract_path)
                    _validate_member(member,
                                     extract_path=extract_abs_path,
                                     allow_overwrite=allow_overwrite)
                    zipf.extract(member, path=extract_abs_path)

    # Rare compression types like ppmd amd deflate64 that have not been
    # implemented should raise an ExtractError
    except NotImplementedError as error:
        # TODO: Python 3.9 error message does not tell the compression type
        # anymore, although the information could be useful. The type could be
        # dug out with ZipInfo.compress_type, but it can't be used with the
        # ZipFile.extractall call above
        raise ExtractError("Compression type not supported.") from error


def _check_archive_members(archive, extract_path, allow_overwrite=False,
                           max_size=None):
    """Check that all files are extracted under extract_path,
    archive contains only regular files and directories, and extracting the
    archive does not overwrite anything.

    :param archive: Opened ZipFile or TarFile
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :param max_size: Int that defines how many objects can archive have
                     ("None" skip object counting)
    :returns: None
    """
    extract_path = os.path.abspath(extract_path)
    archive_size = 0

    for member in archive:
        _validate_member(member=member,
                         extract_path=extract_path,
                         allow_overwrite=allow_overwrite)
        if max_size is not None:
            if isinstance(archive, tarfile.TarFile):
                if member.isfile():
                    archive_size += 1
            else:
                if not member.is_dir():
                    archive_size += 1
            if archive_size > max_size:
                raise ObjectCountError("Archive has too many objects -"
                                       f" Max size is {max_size} objects")


def _validate_member(member, extract_path, allow_overwrite=False):
    """Validates that there are no issues with given member.

    :param member: ZipInfo or TarInfo member.
    :param extract_path: Directory where the archive is extracted to
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False).
    :raises: MemberNameError is raised when filename is invalid for the member.
    :raises: MemberTypeError is raised when the member is of unsupported
        filetype.
    :raises: MemberOverwriteError If an existing file was discovered in the
        extract patch.
    """

    def _tar_filetype_evaluation():
        """Inner function to set the supported_type and file_type variables
        for zip files.
        :returns: Tuple of (supported_type, filetype)
        """
        return member.isfile() or member.isdir(), TAR_FILE_TYPES[member.type]

    def _zip_filetype_evaluation():
        """Inner function to set the supported_type and file_type variables
        for tar files.
        :returns: Tuple of (supported_type, filetype)
        """
        mode = member.external_attr >> 16  # Upper two bytes of ext attr

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
            member.external_attr &= 0xffff
            mode = 0

        supported_type = stat.S_ISDIR(mode) or stat.S_ISREG(mode)
        # Support zip archives made with non-POSIX compliant operating
        # systems where file mode is not specified, e.g., windows.
        supported_type |= (mode == 0)
        filetype = FILETYPES[stat.S_IFMT(mode)] if mode != 0 else "non-POSIX"

        return supported_type, filetype

    _get_filename = {
        'TarInfo': lambda: member.name,
        'ZipInfo': lambda: member.filename
    }
    _evaluate_filetypes = {
        'TarInfo': _tar_filetype_evaluation,
        'ZipInfo': _zip_filetype_evaluation
    }
    member_type_instance = member.__class__.__name__

    filename = _get_filename[member_type_instance]()
    fpath = os.path.abspath(os.path.join(extract_path, filename))

    # Evaluate the filetype
    supported_type, filetype = _evaluate_filetypes[member_type_instance]()

    # Check if the archive member is valid
    if not fpath.startswith(extract_path):
        raise MemberNameError(f"Invalid file path: '{filename}'")

    if not supported_type:
        raise MemberTypeError(
            f"File '{filename}' has unsupported type: {filetype}"
        )

    # Do not raise error if overwriting member files is permitted
    if not allow_overwrite and os.path.isfile(fpath):
        raise MemberOverwriteError(f"File '{filename}' already exists")


def extract(archive, extract_path, allow_overwrite=False, precheck=True,
            max_size=None):
    """Extract tar or zip archives. Additionally, tar archives can be handled
    as stream.

    :param tar_path: Path to the tar archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :param precheck: Boolean that defines whether to check to whole archive
                     before extraction or not. If True, user does not need to
                     worry about the cleanup. If False, archive is read only
                     once and the members are extracted immediately after the
                     check. User is responsible for the cleanup if member check
                     raises an error with precheck=False.
    :returns: None
    """
    if tarfile.is_tarfile(archive):
        tarfile_extract(archive,
                        extract_path,
                        allow_overwrite=allow_overwrite,
                        precheck=precheck,
                        max_size=max_size)
    elif zipfile.is_zipfile(archive):
        zipfile_extract(archive,
                        extract_path,
                        allow_overwrite=allow_overwrite,
                        precheck=precheck,
                        max_size=max_size)
    else:
        raise ExtractError("File is not supported")
