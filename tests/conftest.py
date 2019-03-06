"""Test setup"""

import pytest


@pytest.fixture(scope='function')
def cli_fx():
    """Run command line client script with given arguments::

        cli_fx('--option', 'argument', ...)

    :returns: Function for running the command

    """
    from click.testing import CliRunner
    from archive_helpers.cmdline import cli

    def _invoke(*args):
        """Return result from the executed command line script."""

        runner = CliRunner()
        args = list(args)

        result = runner.invoke(
            cli, args, catch_exceptions=False)
        return result

    return _invoke
