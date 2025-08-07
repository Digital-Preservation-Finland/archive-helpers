Release notes
=============

Version 1.0
-----------

- Add validator module
- Add exceptions module
- Renamed setting for maximum allowed objects from ``max_size`` to ``max_objects``
- Add setting for:

    - maximum allowed compression ratio (``max_ratio``)
    - maximum allowed uncompressed size (``max_size``)

- Add default values for settings:

    - ``max_objects = 100000``
    - ``max_ratio = 100``
    - ``max_size = 4TB``

- Add config module:

    - Config is read from ``/etc/archive_helpers/archive_helpers.conf``
    - If not explicitly provided, maximum settings use values defined in config

- Add open module:

    - Allows opening archives in a context manager with validation


Version 0.15
----------

- Add setting for maximum allowed objects in an archive

Version 0.14
------------

- Add missing requirements to spec

Version 0.13
------------

- Installation instructions for AlmaLinux 9 using RPM packages

Version 0.12
------------

- Add RHEL9 RPM spec file

Version 0.11
------------

- Cleanup code
- Prepare for Github publication
    - Add __version__
    - Add release notes
    - Add LICENSE
    - Add README
