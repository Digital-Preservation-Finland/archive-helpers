"""Test extract function"""
import os
import subprocess
import tarfile

import pytest
from archive_helpers.extract import (
    ExtractError,
    ObjectCountError,
    MemberNameError,
    MemberOverwriteError,
    MemberTypeError,
    SuspiciousArchiveError,
    extract,
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
        ["tar", f"c{compression}f", fname, "-C", tmpdir, dir_to_tar],
        cwd=tmpdir
    )


def _zip(tmpdir, dir_to_zip):
    """Compress compress_dir to zip file"""
    subprocess.call(
        ["zip", "-r", "source.zip", dir_to_zip, "--symlinks"],
        cwd=tmpdir
    )


@pytest.fixture(scope="function", autouse=True)
def testfiles_fx(tmpdir):
    """Create test data"""

    tmpdir.join("source/file1").write("foo", ensure=True)
    file1 = tmpdir.join("symlink/file1").write("foo", ensure=True)
    tmpdir.join("symlink/link").mksymlinkto(file1)
    tmpdir.ensure("destination", dir=True)

    return tmpdir.join("source.tar")


def test_blank_tar_extract(tmpdir):
    """Test that extracting a blank tar archive raises ExtractError."""
    with pytest.raises(ExtractError) as error:
        extract("tests/data/blank_tar.tar", str(tmpdir))
    assert "Blank tar archives" in str(error.value)


def test_abspath_tar_extract(tmpdir):
    """Test that extracting a tar archive with absolute paths raises
    MemberNameError.
    """
    with pytest.raises(MemberNameError) as error:
        extract("tests/data/abspath.tar", str(tmpdir))
    assert "Invalid file path" in str(error.value)


def test_extract_regular_file(tmpdir):
    """Test that trying to extract a regular file raises ExtractError"""
    with pytest.raises(ExtractError) as error:
        extract(
            str(tmpdir.join("source/file1")),
            str(tmpdir.join("destination"))
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
        extract(str(tmpdir.join(fname)), str(tmpdir.join("destination")))

    assert str(error.value) == "File 'symlink/link' has unsupported type: SYM"


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
                str(tmpdir.join(fname)),
                str(tmpdir),
                allow_overwrite=allow_overwrite
            )

        assert str(error.value) == "File 'source/file1' already exists"
    else:
        extract(
            str(tmpdir.join(fname)),
            str(tmpdir),
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
    MemberNameError.
    """
    path, valid_path = path

    with tarfile.open(str(tmpdir.join("test.tar")), "w") as tarf:
        tarf.add(str(tmpdir.join("source/file1")), arcname=path)

    if valid_path:
        extract(str(tmpdir.join("test.tar")), str(tmpdir.join("destination")))
    else:
        with pytest.raises(MemberNameError) as error:
            extract(
                str(tmpdir.join("test.tar")),
                str(tmpdir.join("destination"))
            )
        assert str(error.value) == f"Invalid file path: '{path}'"


def _tar_absolute_path(tmpdir, fname, compression=""):
    """Create tar archives with absolute paths"""
    archive = str(tmpdir.join(fname))
    command = [
        "tar", f"-c{compression}f", archive, "source/file1",
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
        extract(str(tmpdir.join(fname)), str(tmpdir.join("destination")))

    assert str(error.value) == "Invalid file path: '/file1'"


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
        str(tmpdir.join(fname)),
        str(tmpdir.join("destination")),
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
    extract(archive, str(tmpdir))

    for _dir in dirs:
        path = str(tmpdir.join(_dir))
        assert os.path.isdir(path)

    for _file in files:
        path = str(tmpdir.join(_file))
        assert os.path.isfile(path)


def test_zip_unsupported_compression_type_extract(tmpdir):
    """Test that extracting a zip archive file with an unsupported
    compression type raises ExtractError.
    """
    with pytest.raises(ExtractError) as error:
        extract("tests/data/zip_ppmd_compression.zip", str(tmpdir))
    assert str(error.value) == "Compression type not supported."


def test_extract_zip_unrecognized_external_attributes(tmpdir):
    """Test that zip archives made on windows with unexpected values in
    the non-MSDOS external file attributes section can be extracted.
    """
    extract("tests/data/windows_zip_unrecognized_external_attributes.zip",
            str(tmpdir))
    assert os.path.isfile(str(tmpdir.join("windows_zip/file.txt")))


@pytest.mark.parametrize(
    ("archive", "max_objects", "precheck", "size_ok"),
    [
        ("tests/data/zip_three_files.zip", 3, True, True),
        ("tests/data/zip_three_files.zip", 2, True, False),
        ("tests/data/zip_three_files.zip", None, True, True),
        ("tests/data/zip_folder_and_three_files.zip", 3, True, True),
        ("tests/data/zip_folder_and_three_files.zip", 2, True, False),
        ("tests/data/zip_three_files.zip", 3, False, True),
        ("tests/data/zip_three_files.zip", 2, False, False),
        ("tests/data/zip_three_files.zip", None, False, True),
        ("tests/data/zip_folder_and_three_files.zip", 3, False, True),
        ("tests/data/zip_folder_and_three_files.zip", 2, False, False),
    ]
)
def test_zip_max_objects(size_ok, archive, tmp_path, precheck, max_objects):
    """Test that the max object count of the zip file is recognized correctly
    """
    if size_ok:
        extract(archive, tmp_path, True, precheck, max_objects)
    elif not size_ok:
        with pytest.raises(ObjectCountError) as error:
            extract(archive, tmp_path, True, precheck, max_objects)
        assert "Archive has too many objects" in str(error.value)


@pytest.mark.parametrize(
    ("archive", "max_objects", "precheck", "size_ok"),
    [
        ("tests/data/tar_three_files.tar", 3, True, True),
        ("tests/data/tar_three_files.tar", 2, True, False),
        ("tests/data/tar_three_files.tar", None, True, True),
        ("tests/data/tar_folder_and_three_files.tar", 3, True, True),
        ("tests/data/tar_folder_and_three_files.tar", 2, True, False),
        ("tests/data/tar_three_files.tar", 3, False, True),
        ("tests/data/tar_three_files.tar", 2, False, False),
        ("tests/data/tar_three_files.tar", None, False, True),
        ("tests/data/tar_folder_and_three_files.tar", 3, False, True),
        ("tests/data/tar_folder_and_three_files.tar", 2, False, False),
    ]
)
def test_tar_max_objects(size_ok, archive, tmp_path, precheck, max_objects):
    """Test that the max object count of the tar file is recognized correctly
    """
    if size_ok:
        extract(archive, tmp_path, True, precheck, max_objects)
    elif not size_ok:
        with pytest.raises(ObjectCountError) as error:
            extract(archive, tmp_path, True, precheck, max_objects)
        assert "Archive has too many objects" in str(error.value)


def test_zip_bomb_is_detected(tmp_path):
    """Test that zip bombs are detected"""
    with pytest.raises(SuspiciousArchiveError) as error:
        extract("tests/data/zip_bomb_220MB.zip", tmp_path, True, True, None)
    assert "suspicious compression ratio" in str(error.value)
