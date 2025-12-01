"""Install archive-helpers package"""
from setuptools import setup, find_packages


def main():
    """Install archive-helpers"""
    setup(
        name='archive-helpers',
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        setup_requires=["setuptools_scm"],
        use_scm_version={
            "write_to": "archive_helpers/_version.py"
        }
    )


if __name__ == '__main__':
    main()
