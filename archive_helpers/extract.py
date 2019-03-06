"""Extract/decompress various archive formats"""

import os
import subprocess


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
        ["tar", "x", "-C", destination_path] + args,
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
