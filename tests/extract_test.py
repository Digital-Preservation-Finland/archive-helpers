"""Test extract function"""

import os
import socket
import getpass
import subprocess

import pytest

from archive_helpers.extract import (
    path_to_glfs, cat_tar_extract, tar_extract, zip_extract)


_TAR_NAMES = {
    "archive.tar": "",
    "archive.tar.gz": "z",
    "archive.tar.bz2": "j"}


@pytest.fixture(scope="function", autouse=True)
def testfiles_fx(tmpdir):
    """Create test data"""

    tmpdir.join("source/file1").write("foo", ensure=True)
    tmpdir.ensure("destination", dir=True)

    def _tar(destination_path, compression=""):
        """Compress source to tar file"""
        subprocess.call(
            ["tar", "c{}f".format(compression),
             str(destination_path), "-C", str(tmpdir), "source"],
            cwd=str(tmpdir))

    for filename, compression in _TAR_NAMES.iteritems():
        _tar(tmpdir.join(filename), compression)

    subprocess.call(
        ["zip", "-r", "archive.zip", "source"],
        cwd=str(tmpdir))

    return tmpdir.join("archive.tar")


def _glfs(*args):
    """Return path to glusterfs volume"""
    return os.path.join(
        "glfs://{}/glfs_test/".format(socket.gethostname()), *args)


# pylint: disable=redefined-outer-name
@pytest.yield_fixture(scope="function")
@pytest.mark.use_fixtures("testfiles_fx")
def glusterfs_fx(tmpdir):
    """Create GlusterFS volume for the test"""

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

    for filename in _TAR_NAMES:
        path = str(tmpdir.join(filename))
        subprocess.call(["gfcp", path, _glfs(filename)])
        os.unlink(path)

    yield

    _volume(["stop", "glfs_test", "force"])
    _volume(["delete", "glfs_test"])

    subprocess.call(
        ["sudo", "chown", "-R", "{0}:{0}".format(getpass.getuser()),
         str(tmpdir.join("glusterfs"))])


@pytest.mark.parametrize("filename", _TAR_NAMES.keys())
def test_tar_extract(filename, tmpdir):
    """Test the tar extract functionality"""

    tar_extract(
        str(tmpdir.join(filename)),
        str(tmpdir.join("destination")))

    assert tmpdir.join("destination/source/file1").check()


@pytest.mark.parametrize("filename", _TAR_NAMES.keys())
def test_cat_tar_extract(filename, tmpdir):
    """Test the tar extract functionality"""

    cat_tar_extract(
        str(tmpdir.join(filename)),
        str(tmpdir.join("destination")))

    assert tmpdir.join("archive.tar").check()
    assert tmpdir.join("destination/source/file1").check()


@pytest.mark.sudo
@pytest.mark.gluster
@pytest.mark.usefixtures("glusterfs_fx")
@pytest.mark.parametrize("filename", _TAR_NAMES.keys())
def test_gfcat_tar_extract(filename, tmpdir):
    """Test the tar extract functionality"""

    source_path = path_to_glfs(
        source_path=str(tmpdir.join("glfs_test/{}".format(filename))),
        mount_path=str(tmpdir),
        glusterfs_host=socket.gethostname())

    cat_tar_extract(
        source_path, str(tmpdir.join("destination")), cat="gfcat")

    assert not tmpdir.join("archive.tar").check()
    assert tmpdir.join("destination/source/file1").check()


def test_zip_extract(tmpdir):
    """Test the tar extract functionality"""

    zip_extract(
        str(tmpdir.join("archive.zip")),
        str(tmpdir.join("destination")))

    assert tmpdir.join("destination/source/file1").check()
