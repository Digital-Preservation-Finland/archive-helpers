"""Extract/decompress various archive formats"""

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
    "0": "REG",
    "1": "LNK",
    "2": "SYM",
    "3": "CHR",
    "4": "BLK",
    "5": "DIR",
    "6": "FIFO",
    "7": "CONT"
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
    """Exception raised when tar or zip files contain bad members."""
    pass


def _check_tar_members(tarf, extract_path):
    """Check that all files are extracted under extract_path,
    archive contains only regular files and directories, and extracting the
    archive does not overwrite anything.
    """
    extract_path = os.path.abspath(extract_path)

    for member in tarf:
        fpath = os.path.abspath(os.path.join(extract_path, member.name))

        if not fpath.startswith(extract_path):
            raise ExtractError(
                "Invalid file path: '%s'" % member.name
            )
        elif not member.isfile() and not member.isdir():
            raise ExtractError("File '%s' has unsupported type: %s" % (
                member.name, TAR_FILE_TYPES[member.type]
            ))
        elif os.path.isfile(fpath):
            raise ExtractError(
                "File '%s' already exists" % member.name
            )


def tarfile_extract(tar_path, extract_path):
    """Decompress using tarfile module.

    :param tar_path: Path to the tar archive
    :param extract_path: Directory where the archive is extracted
    :returns: None
    """
    if not tarfile.is_tarfile(tar_path):
        raise ExtractError("File '%s' is not a tar archive" % tar_path)

    with tarfile.open(tar_path) as tarf:
        _check_tar_members(tarf, extract_path)
        tarf.extractall(extract_path)


def _check_zip_members(zipf, extract_path):
    """Check that all files are extracted under extract_path,
    archive contains only regular files and directories, and extracting the
    archive does not overwrite anything.
    """
    extract_path = os.path.abspath(extract_path)

    for member in zipf:
        fpath = os.path.abspath(os.path.join(extract_path, member.filename))
        mode = member.external_attr >> 16 # Upper two bytes of ext attributes

        if not fpath.startswith(extract_path):
            raise ExtractError(
                "Invalid file path: '%s'" % member.filename
            )
        elif not stat.S_ISDIR(mode) and not stat.S_ISREG(mode):
            raise ExtractError("File '%s' has unsupported type: %s" % (
                member.filename, FILETYPES[stat.S_IFMT(mode)]
            ))
        elif os.path.isfile(fpath):
            raise ExtractError(
                "File '%s' already exists" % member.filename
            )


def zipfile_extract(zip_path, extract_path):
    """Decompress using zipfile module.

    :param zip_path: Path to the zip archive
    :param extract_path: Directory where the archive is extracted
    :returns: None
    """
    if not zipfile.is_zipfile(zip_path):
        raise ExtractError("File '%s' is not a zip archive" % zip_path)

    with zipfile.ZipFile(zip_path) as zipf:
        _check_zip_members(zipf.infolist(), extract_path)
        zipf.extractall(extract_path)


def extract(archive, extract_path):
    """Extract tar or zip archives.

    :param tar_path: Path to the tar archive
    :param extract_path: Directory where the archive is extracted
    :returns: None
    """
    if tarfile.is_tarfile(archive):
        tarfile_extract(archive, extract_path)
    elif zipfile.is_zipfile(archive):
        zipfile_extract(archive, extract_path)
    else:
        raise ExtractError("File '%s' is not supported" % archive)
