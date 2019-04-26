"""Install upload-rest-api package"""
from setuptools import setup, find_packages

from version import get_version


def main():
    """Install upload-rest-api"""
    setup(
        name='upload-rest-api',
        packages=find_packages(exclude=['tests', 'tests.*']),
        version=get_version(),
        install_requires=[],
    )

if __name__ == '__main__':
    main()
