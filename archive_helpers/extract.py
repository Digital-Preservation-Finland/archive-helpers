"""Extract/decompress various archive formats"""
from __future__ import unicode_literals

import os
import stat
import subprocess
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


def path_to_glfs(source_path, mount_path, glusterfs_host):
    """Convert local path to compatible path with glusterfs coreutils command
    line tools::

        /[mount_path]/[source_path]
        -> glfs://[hostname]/[source_path]

    Note: That glfs format includes volume name so parameter `source_path` must
    include that::

        glfs://[hostname]/[volume_name]/[source_path]

    :source_path: Path visible on local directory structure
    :mount_path: Path to mountpoint
    :glusterfs_host: GlusterFS server hostname
    :returns: Path as GlusterFS core utilsTODO

    """
    return source_path.replace(
        mount_path.rstrip("/"),
        "glfs://{}".format(glusterfs_host))


def _popen(command, **kwargs):
    """Wrap `subprocess.Popen()` with common arguments.

    :command: Commands to execute
    :**kwargs: Additional keyword arguments to Popen()
    :returns: TODO

    """
    return subprocess.Popen(command, shell=False, **kwargs)


def _tar(args, destination_path, **kwargs):
    """Extract with tar with given arguments

    :destination_path: Path to extract files to
    :input_file: Input file stream (from cat/gfcat)

    """
    tar = _popen(
        ["tar", "x", "-C", "."] + args,
        cwd=destination_path, **kwargs)
    if "stdin" in kwargs:
        kwargs["stdin"].close()
    tar.communicate()


def _tar_compression_argument(filename):
    """Determine tar compression argument from filename

    :returns: Argument for tar command

    """
    _extensions = {
        ".tar": [],
        ".gz": ["-z"],
        ".tgz": ["-z"],
        ".bz2": ["-j"]}
    return _extensions[os.path.splitext(filename)[1]]


def cat_tar_extract(source_path, destination_path, cat="cat"):
    """Decompress file using `gfcat | tar` pipe.

    This command optimizes well on GlusterFS volumes.

    :path: Path in GlusterFS URI format
    :returns: None

    """

    cat = _popen(
        [cat, source_path],
        stdout=subprocess.PIPE, cwd=destination_path)

    _tar(_tar_compression_argument(source_path),
         destination_path, stdin=cat.stdout)


def tar_extract(source_path, destination_path):
    """Decompress file using `gfcat | tar` pipe.

    This command optimizes well on GlusterFS volumes.

    :path: Path in GlusterFS URI format
    :returns: None

    """
    _tar(_tar_compression_argument(source_path) +
         ["-f", source_path], destination_path)


def zip_extract(source_path, destination_path):
    """Decompress file using `gfcat | tar` pipe.

    This command optimizes well on GlusterFS volumes.

    :path: Path in GlusterFS URI format
    :returns: None

    """
    unzip = _popen(["unzip", source_path], cwd=destination_path)
    unzip.communicate()


class ExtractError(Exception):
    """Generic archive extraction error raised when the archive is not
    supported.
    """
    pass


class MemberNameError(Exception):
    """Exception raised when tar or zip files contain members with names
    pointing outside the extraction path.
    """
    pass


class MemberTypeError(Exception):
    """Exception raised when tar or zip files contain members with filetype
    other than REG or DIR.
    """
    pass


class MemberOverwriteError(Exception):
    """Exception raised when extracting the archive would overwrite files."""
    pass


def tarfile_extract(tar_path,
                    extract_path,
                    allow_overwrite=False,
                    use_stream=False):
    """Decompress using tarfile module.

    :param tar_path: Path to the tar archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :param use_stream: Use stream read mode to increase handling speed at cost
                       of pre-validation.
    :returns: None
    """
    if not tarfile.is_tarfile(tar_path):
        raise ExtractError("File '%s' is not a tar archive" % tar_path)

    if not use_stream:
        with tarfile.open(tar_path) as tarf:
            _check_archive_members(
                tarf, extract_path, allow_overwrite=allow_overwrite)
            tarf.extractall(extract_path)
    else:
        # Reduce the number of full read required by evaluating and extracting
        # during read.
        with tarfile.open(tar_path, 'r|*') as tarf:
            extract_abs_path = os.path.abspath(extract_path)
            for member in tarf:
                _validate_member(member,
                                 extract_path=extract_abs_path,
                                 allow_overwrite=allow_overwrite)
                tarf.extract(member, path=extract_abs_path)
                # Memory caching issue during pre-python3 as mentioned here:
                # https://stackoverflow.com/a/21092098
                # The workaround is to reset the members information for the
                # archive.
                tarf.members = []


def _check_archive_members(archive, extract_path, allow_overwrite=False):
    """Check that all files are extracted under extract_path,
    archive contains only regular files and directories, and extracting the
    archive does not overwrite anything.

    :param archive: Opened ZipFile or TarFile
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :returns: None
    """
    extract_path = os.path.abspath(extract_path)

    for member in archive:
        _validate_member(member=member,
                         extract_path=extract_path,
                         allow_overwrite=allow_overwrite)


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
        raise MemberNameError(
            "Invalid file path: '%s'" % filename
        )
    elif not supported_type:
        raise MemberTypeError("File '%s' has unsupported type: %s" % (
            filename, filetype
        ))
    # Do not raise error if overwriting member files is permitted
    elif not allow_overwrite and os.path.isfile(fpath):
        raise MemberOverwriteError(
            "File '%s' already exists" % filename
        )


def zipfile_extract(zip_path, extract_path, allow_overwrite=False):
    """Decompress using zipfile module.

    :param zip_path: Path to the zip archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :returns: None
    """
    if not zipfile.is_zipfile(zip_path):
        raise ExtractError("File '%s' is not a zip archive" % zip_path)

    with zipfile.ZipFile(zip_path) as zipf:
        _check_archive_members(
            zipf.infolist(), extract_path, allow_overwrite=allow_overwrite)
        zipf.extractall(extract_path)


def extract(archive, extract_path, allow_overwrite=False, use_stream=False):
    """Extract tar or zip archives. Additionally, tar archives can be handled
    as stream.

    :param tar_path: Path to the tar archive
    :param extract_path: Directory where the archive is extracted
    :param allow_overwrite: Boolean to allow overwriting existing files
                            without raising an error (defaults to False)
    :param use_stream: Boolean for tar archives whether to handle the archive
                       as stream or not. Handling as stream increases the
                       extraction process, but files would be validated one by
                       one instead of full validation before extraction
                       (defaults to False).
    :returns: None
    """
    if tarfile.is_tarfile(archive):
        tarfile_extract(archive,
                        extract_path,
                        allow_overwrite=allow_overwrite,
                        use_stream=use_stream)
    elif zipfile.is_zipfile(archive):
        zipfile_extract(archive, extract_path, allow_overwrite=allow_overwrite)
    else:
        raise ExtractError("File '%s' is not supported" % archive)
