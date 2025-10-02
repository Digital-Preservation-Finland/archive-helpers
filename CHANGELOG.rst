Changelog
=========
All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

2.0.0 - 2025-10-02
------------------

Changed
^^^^^^^

- Move all custom exceptions raised to archive-helpers.exceptions module.

1.0.0 - 2025-08-08
------------------

Added
^^^^^

- ``validator`` module for archive validation.
- ``exceptions`` module for custom error handling.
- New settings:
    - ``max_ratio``: Maximum allowed compression ratio.
    - ``max_size``: Maximum allowed uncompressed size.
- Default values for settings:
    - ``max_objects = 100000``
    - ``max_ratio = 100``
    - ``max_size = 4TB``
- ``config`` module:
    - Configuration is read from ``/etc/archive_helpers/archive_helpers.conf``.
    - If not explicitly provided, maximum settings use values defined in config.
- ``open`` module for opening archives in a context manager with validation.

Changed
^^^^^^^

- Renamed previous setting ``max_size`` to ``max_objects`` to clarify its purpose.
- Moved the project to use Keep a Changelog format and Semantic Versioning

0.15
----

- Add setting for maximum allowed objects in an archive

0.14
----

- Add missing requirements to spec

0.13
----

- Installation instructions for AlmaLinux 9 using RPM packages

0.12
----

- Add RHEL9 RPM spec file

0.11
----

- Cleanup code
- Prepare for Github publication
    - Add __version__
    - Add release notes
    - Add LICENSE
    - Add README
