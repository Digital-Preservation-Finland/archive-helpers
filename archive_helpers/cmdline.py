"""Command line utility for archive library"""


import os
import click


from archive_helpers.extract import (cat_tar_extract)


@click.group()
def cli():
    """Command line utility for searching and editing contractdb database"""
    pass


@cli.command()
@click.argument("source_path")
@click.argument("destination_path")
def decompress(source_path, destination_path):
    """Add contract with contract_id, organization"""

    cat_command = "cat"

    if "glfs://" in source_path:
        cat_command = 'gfcat'
    else:
        source_path = os.path.abspath(source_path)

    cat_tar_extract(
        source_path, os.path.abspath(destination_path), cat=cat_command)


if __name__ == "__main__":
    cli()
