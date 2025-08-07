Archive helpers
===============

Archive helpers checks that untrusted zip or tar archives can be safely
extracted and extracts them.

Requirements
------------

Installation and usage requires Python 3.9 or newer.
The software is tested with Python 3.9 on AlmaLinux 9 release.

Installation using RPM packages (preferred)
-------------------------------------------

Installation on Linux distributions is done by using the RPM Package Manager.
See how to `configure the PAS-jakelu RPM repositories`_ to setup necessary software sources.

.. _configure the PAS-jakelu RPM repositories: https://www.digitalpreservation.fi/user_guide/installation_of_tools

After the repository has been added, the package can be installed by running the following command::

    sudo dnf install python3-archive-helpers

Usage
-----

Archive Helpers provides functions to validate and extract untrusted archives safely.

The ``archive_helpers.extract.extract`` function can be used to
check untrusted zip or tar archives and extract them. Simply run::

    >>> from archive_helpers.extract import extract
    >>> extract("<archive_path>", "<extract_path>")

The ``archive_helpers.validator.validate`` function can be used to
validate untrusted zip or tar archives without extracting them::

    >>> from archive_helpers.validator import validate
    >>> validate("<archive_path>")

By default, in addition to validation, both functions enforce thresholds for:

- maximum amount of objects
- maximum uncompressed size
- maximum compression ratio

These can be set to custom values::

    >>> extract("<archive_path>", max_objects=123, max_size=456, max_ratio=789)

Or disabled entirely::

    >>> validate("<archive_path>", max_objects=None, max_size=None, max_ratio=None)

The default threshold values can be set in the configuration file located at
``/etc/archive_helpers/archive_helpers.conf``.


Installation using Python Virtualenv for development purposes
-------------------------------------------------------------

Create a virtual environment::

    python3 -m venv venv

Run the following to activate the virtual environment::

    source venv/bin/activate

Install the required software with commands::

    pip install --upgrade pip==20.2.4 setuptools
    pip install -r requirements_github.txt
    pip install .

To deactivate the virtual environment, run ``deactivate``.
To reactivate it, run the ``source`` command above.

Copyright
---------
Copyright (C) 2023 CSC - IT Center for Science Ltd.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
details.

You should have received a copy of the GNU Lesser General Public License along
with this program. If not, see <https://www.gnu.org/licenses/>.
