"""Install archive-helpers package"""
from setuptools import setup, find_packages

from archive_helpers import __version__


def main():
    """Install archive-helpers"""
    setup(
        name='archive-helpers',
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        version=__version__
    )


if __name__ == '__main__':
    main()
