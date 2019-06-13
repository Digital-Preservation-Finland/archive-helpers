"""Install archive-helpers package"""
from setuptools import setup, find_packages

from version import get_version


def main():
    """Install archive-helpers"""
    setup(
        name='archive-helpers',
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        version=get_version(),
        install_requires=["click"],
    )


if __name__ == '__main__':
    main()
