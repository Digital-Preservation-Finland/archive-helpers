"""Test extract function"""

import os
import socket
import getpass
import subprocess
import tarfile

import pytest

from archive_helpers.extract import (
    path_to_glfs, cat_tar_extract, tar_extract, zip_extract, extract
)
from archive_helpers.extract import (
    ExtractError, MemberNameError, MemberTypeError, MemberOverwriteError
)


TAR_FILES = [
    ("source.tar", ""),
    ("source.tar.gz", "z"),
    ("source.tar.bz2", "j")
]

ARCHIVES = TAR_FILES + [("source.zip", "")]


def _tar(tmpdir, fname, dir_to_tar, compression=""):
    """Compress compress_dir to tar file"""
    subprocess.call(
        ["tar", "c%sf" % compression, fname, "-C", str(tmpdir), dir_to_tar],
        cwd=str(tmpdir)
    )


def _zip(tmpdir, dir_to_zip):
    """Compress compress_dir to zip file"""
    subprocess.call(
        ["zip", "-r", "source.zip", dir_to_zip, "--symlinks"], cwd=str(tmpdir)
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
        brick_path = str(tmpdir.ensure(
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
        path = str(tmpdir.join(fname))
        subprocess.call(["gfcp", path, _glfs(fname)])
        os.unlink(path)

    yield

    _volume(["stop", "glfs_test", "force"])
    _volume(["delete", "glfs_test"])

    subprocess.call(
        ["sudo", "chown", "-R", "{0}:{0}".format(getpass.getuser()),
         str(tmpdir.join("glusterfs"))])


@pytest.mark.parametrize("archive", TAR_FILES)
def test_tar_extract(archive, tmpdir):
    """Test the tar extract functionality"""
    fname, compression = archive
    _tar(tmpdir, fname, "source", compression)

    tar_extract(
        str(tmpdir.join(fname)),
        str(tmpdir.join("destination"))
    )
    assert tmpdir.join("destination/source/file1").check()


@pytest.mark.parametrize("archive", TAR_FILES)
def test_cat_tar_extract(archive, tmpdir):
    """Test the tar extract functionality"""
    fname, compression = archive
    _tar(tmpdir, fname, "source", compression)

    cat_tar_extract(
        str(tmpdir.join(fname)),
        str(tmpdir.join("destination"))
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
        source_path=str(tmpdir.join("glfs_test/{}".format(fname))),
        mount_path=str(tmpdir),
        glusterfs_host=socket.gethostname())

    cat_tar_extract(
        source_path, str(tmpdir.join("destination")), cat="gfcat")

    assert not tmpdir.join(fname).check()
    assert tmpdir.join("destination/source/file1").check()


def test_zip_extract(tmpdir):
    """Test the zip extract functionality"""
    _zip(tmpdir, "source")

    zip_extract(
        str(tmpdir.join("source.zip")),
        str(tmpdir.join("destination")))

    assert tmpdir.join("destination/source/file1").check()


def test_extract_regular_file(tmpdir):
    """Test that trying to extract a regular file raises ExtractError"""
    with pytest.raises(ExtractError) as error:
        extract(
            str(tmpdir.join("source/file1")), str(tmpdir.join("destination"))
        )

    assert str(error.value).endswith("is not supported")


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
            str(tmpdir.join(fname)), str(tmpdir.join("destination"))
        )

    assert str(error.value) == "File 'symlink/link' has unsupported type: SYM"


@pytest.mark.parametrize("archive", ARCHIVES)
def test_extract_overwrite(archive, tmpdir):
    """Test that trying to overwrite files raises MemberOverwriteError"""
    fname, compression = archive
    if fname.endswith(".zip"):
        _zip(tmpdir, "source")
    else:
        _tar(tmpdir, fname, "source", compression)

    with pytest.raises(MemberOverwriteError) as error:
        extract(
            str(tmpdir.join(fname)), str(tmpdir)
        )

    assert str(error.value) == "File 'source/file1' already exists"


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

    with tarfile.open(str(tmpdir.join("test.tar")), "w") as tarf:
        tarf.add(str(tmpdir.join("source/file1")), arcname=path)

    if valid_path:
        extract(
            str(tmpdir.join("test.tar")), str(tmpdir.join("destination"))
        )
    else:
        with pytest.raises(MemberNameError) as error:
            extract(
                str(tmpdir.join("test.tar")), str(tmpdir.join("destination"))
            )
        assert str(error.value) == "Invalid file path: '%s'" % path


def _tar_absolute_path(tmpdir, fname, compression=""):
    """Create tar archives with absolute paths"""
    archive = str(tmpdir.join(fname))
    command = [
        "tar", "-c%sf" % compression, archive, "source/file1",
        "--transform", "s|source/file1|/file1|"
    ]
    subprocess.call(command, cwd=str(tmpdir))


@pytest.mark.parametrize("archive", TAR_FILES)
def test_extract_absolute_path(archive, tmpdir):
    """Test that trying to extract files with absolute paths raises
    MemberNameError.
    """
    fname, compression = archive
    _tar_absolute_path(tmpdir, fname, compression)

    with pytest.raises(MemberNameError) as error:
        extract(
            str(tmpdir.join(fname)), str(tmpdir.join("destination"))
        )

    assert str(error.value) == "Invalid file path: '/file1'"


@pytest.mark.parametrize("archive", ARCHIVES)
def test_extract_success(archive, tmpdir):
    """Test that tar and zip archives are correctly extracted."""
    fname, compression = archive
    if fname.endswith(".zip"):
        _zip(tmpdir, "source")
    else:
        _tar(tmpdir, fname, "source", compression)

    extract(
        str(tmpdir.join(fname)), str(tmpdir.join("destination"))
    )

    assert len(tmpdir.join("destination").listdir()) == 1
    assert len(tmpdir.join("destination/source").listdir()) == 1
    assert tmpdir.join("destination/source/file1").check()