"""Test extract function"""
from __future__ import unicode_literals

import getpass
import os
import socket
import subprocess
import tarfile

import six

import pytest
from archive_helpers.extract import (ExtractError, MemberNameError,
                                     MemberOverwriteError, MemberTypeError,
                                     cat_tar_extract, extract, path_to_glfs,
                                     tar_extract, zip_extract)

TAR_FILES = [
    ("source.tar", ""),
    ("source.tar.gz", "z"),
    ("source.tar.bz2", "j")
]

ARCHIVES = TAR_FILES + [("source.zip", "")]


def _tar(tmpdir, fname, dir_to_tar, compression=""):
    """Compress compress_dir to tar file"""
    subprocess.call(
        ["tar", "c%sf" % compression, fname, "-C", six.text_type(tmpdir),
         dir_to_tar],
        cwd=six.text_type(tmpdir)
    )


def _zip(tmpdir, dir_to_zip):
    """Compress compress_dir to zip file"""
    subprocess.call(
        ["zip", "-r", "source.zip", dir_to_zip, "--symlinks"],
        cwd=six.text_type(tmpdir)
    )


@pytest.fixture(scope="function", autouse=True)
def testfiles_fx(tmpdir):
    """Create test data"""

    tmpdir.join("source/file1").write("foo", ensure=True)
    file1 = tmpdir.join("symlink/file1").write("foo", ensure=True)
    tmpdir.join("symlink/link").mksymlinkto(file1)
    tmpdir.ensure("destination", dir=True)

    return tmpdir.join("source.tar")


def _glfs(*args):
    """Return path to glusterfs volume"""
    return os.path.join(
        "glfs://{}/glfs_test/".format(socket.gethostname()), *args)


# pylint: disable=redefined-outer-name
@pytest.yield_fixture(scope="function")
@pytest.mark.use_fixtures("testfiles_fx")
def glusterfs_fx(tmpdir):
    """Create GlusterFS volume for the test"""
    for fname, compression in TAR_FILES:
        _tar(tmpdir, fname, "source", compression)

    bricks = []
    for brick in range(3):
        brick_path = six.text_type(tmpdir.ensure(
            "glusterfs/brick{}".format(brick), dir=True))
        bricks.append("{}:{}".format(socket.gethostname(), brick_path))

    def _volume(command):
        """Call glusterfs client"""
        subprocess.call(
            ["sudo", "gluster", "--mode=script", "volume"] + command,
            shell=False)

    _volume(["create", "glfs_test", "replica", "3"] + bricks + ["force"])
    _volume(["start", "glfs_test"])
    _volume(["status"])

    for fname, _ in TAR_FILES:
        path = six.text_type(tmpdir.join(fname))
        subprocess.call(["gfcp", path, _glfs(fname)])
        os.unlink(path)

    yield

    _volume(["stop", "glfs_test", "force"])
    _volume(["delete", "glfs_test"])

    subprocess.call(
        [
            "sudo", "chown", "-R", "{0}:{0}".format(getpass.getuser()),
            six.text_type(tmpdir.join("glusterfs"))
        ]
    )


@pytest.mark.parametrize("archive", TAR_FILES)
def test_tar_extract(archive, tmpdir):
    """Test the tar extract functionality"""
    fname, compression = archive
    _tar(tmpdir, fname, "source", compression)

    tar_extract(
        six.text_type(tmpdir.join(fname)),
        six.text_type(tmpdir.join("destination"))
    )
    assert tmpdir.join("destination/source/file1").check()


@pytest.mark.parametrize("archive", TAR_FILES)
def test_cat_tar_extract(archive, tmpdir):
    """Test the tar extract functionality"""
    fname, compression = archive
    _tar(tmpdir, fname, "source", compression)

    cat_tar_extract(
        six.text_type(tmpdir.join(fname)),
        six.text_type(tmpdir.join("destination"))
    )
    assert tmpdir.join(fname).check()
    assert tmpdir.join("destination/source/file1").check()


@pytest.mark.sudo
@pytest.mark.gluster
@pytest.mark.usefixtures("glusterfs_fx")
@pytest.mark.parametrize("archive", TAR_FILES)
def test_gfcat_tar_extract(archive, tmpdir):
    """Test the tar extract functionality"""
    fname, _ = archive

    source_path = path_to_glfs(
        source_path=six.text_type(tmpdir.join("glfs_test/{}".format(fname))),
        mount_path=six.text_type(tmpdir),
        glusterfs_host=socket.gethostname())

    cat_tar_extract(
        source_path, six.text_type(tmpdir.join("destination")), cat="gfcat")

    assert not tmpdir.join(fname).check()
    assert tmpdir.join("destination/source/file1").check()


def test_zip_extract(tmpdir):
    """Test the zip extract functionality"""
    _zip(tmpdir, "source")

    zip_extract(
        six.text_type(tmpdir.join("source.zip")),
        six.text_type(tmpdir.join("destination")))

    assert tmpdir.join("destination/source/file1").check()


def test_extract_regular_file(tmpdir):
    """Test that trying to extract a regular file raises ExtractError"""
    with pytest.raises(ExtractError) as error:
        extract(
            six.text_type(tmpdir.join("source/file1")),
            six.text_type(tmpdir.join("destination"))
        )

    assert six.text_type(error.value).endswith("is not supported")


@pytest.mark.parametrize("archive", ARCHIVES)
def test_extract_symlink(archive, tmpdir):
    """Test that trying to extract a symlink raises MemberTypeError"""
    fname, compression = archive
    if fname.endswith(".zip"):
        _zip(tmpdir, "symlink")
    else:
        _tar(tmpdir, fname, "symlink", compression)

    with pytest.raises(MemberTypeError) as error:
        extract(
            six.text_type(tmpdir.join(fname)),
            six.text_type(tmpdir.join("destination"))
        )

    assert six.text_type(error.value) == \
           "File 'symlink/link' has unsupported type: SYM"


@pytest.mark.parametrize(("allow_overwrite"), [
    (False),
    (True)
])
@pytest.mark.parametrize("archive", ARCHIVES)
def test_extract_overwrite(archive, allow_overwrite, tmpdir):
    """Test that trying to overwrite files raises MemberOverwriteError
    if allow_overwrite is False. Else the operation should succeed.
    """
    fname, compression = archive
    if fname.endswith(".zip"):
        _zip(tmpdir, "source")
    else:
        _tar(tmpdir, fname, "source", compression)

    if not allow_overwrite:
        with pytest.raises(MemberOverwriteError) as error:
            extract(
                six.text_type(tmpdir.join(fname)),
                six.text_type(tmpdir),
                allow_overwrite=allow_overwrite
            )

        assert six.text_type(
            error.value) == "File 'source/file1' already exists"
    else:
        extract(
            six.text_type(tmpdir.join(fname)),
            six.text_type(tmpdir),
            allow_overwrite=allow_overwrite
        )
        assert tmpdir.join("source/file1").check()


@pytest.mark.parametrize("path", [
    ("../invalid", False),
    ("destination/../../invalid", False),
    ("./valid", True),
    ("../destination/valid", True)
])
def test_extract_relative_paths(path, tmpdir):
    """Test that trying to write files outside the workspace raises
    MemberNameError
    """
    path, valid_path = path

    with tarfile.open(six.text_type(tmpdir.join("test.tar")), "w") as tarf:
        tarf.add(six.text_type(tmpdir.join("source/file1")), arcname=path)

    if valid_path:
        extract(
            six.text_type(tmpdir.join("test.tar")),
            six.text_type(tmpdir.join("destination"))
        )
    else:
        with pytest.raises(MemberNameError) as error:
            extract(
                six.text_type(tmpdir.join("test.tar")),
                six.text_type(tmpdir.join("destination"))
            )
        assert six.text_type(error.value) == "Invalid file path: '%s'" % path


def _tar_absolute_path(tmpdir, fname, compression=""):
    """Create tar archives with absolute paths"""
    archive = six.text_type(tmpdir.join(fname))
    command = [
        "tar", "-c%sf" % compression, archive, "source/file1",
        "--transform", "s|source/file1|/file1|"
    ]
    subprocess.call(command, cwd=six.text_type(tmpdir))


@pytest.mark.parametrize("archive", TAR_FILES)
def test_extract_absolute_path(archive, tmpdir):
    """Test that trying to extract files with absolute paths raises
    MemberNameError.
    """
    fname, compression = archive
    _tar_absolute_path(tmpdir, fname, compression)

    with pytest.raises(MemberNameError) as error:
        extract(
            six.text_type(tmpdir.join(fname)),
            six.text_type(tmpdir.join("destination"))
        )

    assert six.text_type(error.value) == "Invalid file path: '/file1'"


@pytest.mark.parametrize('precheck', [
    True,
    False
], ids=[
    'Extract with the precheck',
    'Extract without the precheck'
])
@pytest.mark.parametrize("archive", ARCHIVES)
def test_extract_success(archive, precheck, tmpdir):
    """Test that tar and zip archives are correctly extracted."""
    fname, compression = archive
    if fname.endswith(".zip"):
        _zip(tmpdir, "source")
    else:
        _tar(tmpdir, fname, "source", compression)

    extract(
        six.text_type(tmpdir.join(fname)),
        six.text_type(tmpdir.join("destination")),
        precheck=precheck
    )

    assert len(tmpdir.join("destination").listdir()) == 1
    assert len(tmpdir.join("destination/source").listdir()) == 1
    assert tmpdir.join("destination/source/file1").check()


@pytest.mark.parametrize(
    ('archive', 'dirs', 'files'),
    [
        ("tests/data/windows_zip.zip", (
            "windows_zip",
            "windows_zip/directory"
        ), (
            "windows_zip/directory/file.txt",
            "windows_zip/directory/file2.txt"
        )),
        ("tests/data/windows_zip_symlinks.zip", (
            "windows_zip_symlinks",
            "windows_zip_symlinks/directory",
            "windows_zip_symlinks/directory_junction_link",
            "windows_zip_symlinks/soft_link"
        ), (
            "windows_zip_symlinks/file.txt",
            "windows_zip_symlinks/symlink",
            "windows_zip_symlinks/hard_link",
            "windows_zip_symlinks/directory/file.txt",
            "windows_zip_symlinks/directory/file2.txt",
            "windows_zip_symlinks/soft_link/file.txt",
            "windows_zip_symlinks/soft_link/file2.txt"
        ))
    ],
    ids=["files", "links"]
)
def test_extract_zip_windows(archive, dirs, files, tmpdir):
    """Test that zip archives made on windows are correctly extracted and only
    regular files and directories are created.
    """
    extract(
        six.text_type(archive),
        six.text_type(tmpdir)
    )

    for _dir in dirs:
        path = str(tmpdir.join(_dir))
        assert os.path.isdir(path)

    for _file in files:
        path = str(tmpdir.join(_file))
        assert os.path.isfile(path)
