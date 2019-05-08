"""Test for the command line utils """

import pytest

import archive_helpers.extract


@pytest.fixture(autouse=True, scope="function")
def patch_tar_fx(monkeypatch):
    """Patch the `cat_tar_extract` function"""

    def _extract(*args, **kwargs):
        """Mockup for `cat_tar_extract` function"""
        patch_tar_fx.args = args
        patch_tar_fx.kwargs = kwargs

    monkeypatch.setattr(
        archive_helpers.extract, 'cat_tar_extract', _extract)


def test_decompress_cat(cli_fx):
    """Test decompression"""

    result = cli_fx('decompress', 'archive.tar', 'destination_path')

    assert result.exit_code == 0

    args = patch_tar_fx.args

    assert args[0].startswith('/')
    assert args[0].endswith("/archive.tar")

    assert args[1].startswith('/')
    assert args[1].endswith("/destination_path")

    assert patch_tar_fx.kwargs == {'cat': 'cat'}


@pytest.mark.gluster
def test_decompress_gfcat(cli_fx):
    """Test decompression"""

    result = cli_fx('decompress', 'glfs://archive.tar', 'destination_path')

    assert result.exit_code == 0

    args = patch_tar_fx.args
    assert args[0] == "glfs://archive.tar"
    assert args[1].endswith('/destination_path')
    assert args[1].startswith('/')

    assert patch_tar_fx.kwargs == {'cat': 'gfcat'}
