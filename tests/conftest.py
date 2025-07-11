"""Configure pytest"""


def pytest_configure():
    import archive_helpers.config as config_module

    config_module.CONFIG = config_module.get_config(
        "include/rhel9/SOURCES/archive-helpers.conf"
    )
