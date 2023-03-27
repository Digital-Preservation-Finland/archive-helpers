Archive helpers
===============

Archive helpers checks that untrusted zip or tar archives can be safely
extracted and extracts them.

Installation
------------

Installation and usage requires Python 3.6 or newer.  The software is tested
with Python 3.6 on Centos 7.x release.

Create a virtual environment::

    python3 -m venv venv

Run the following to activate the virtual environment::

    source venv/bin/activate

Install in virtual environment::

    pip install .

To deactivate the virtual environment, run ``deactivate``. To reactivate it,
run the ``source`` command above.

Usage
-----

Archive helpers provides :code:`archive_helpers.extract.extract` function for
checking untrusted zip or tar archives and extracting them. Simply run::

    >>> from archive_helpers.extract import extract
    >>> extract("<archive_path>", "<extract_path>")

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
