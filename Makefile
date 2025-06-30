DESTDIR ?= /
PREFIX ?= /usr
ETC=${DESTDIR}/etc
PYTHON ?= python3
ARCHIVE_HELPERS_CONF_FILE=archive-helpers.conf
ARCHIVE_HELPERS_CONF_DIR=/etc/archive-helpers
ARCHIVE_HELPERS_CONF_PATH=${ARCHIVE_HELPERS_CONF_DIR}/${ARCHIVE_HELPERS_CONF_FILE}

install:
	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Use Python setuptools
	${PYTHON} ./setup.py install -O1 \
	    --prefix="${PREFIX}" \
	    --root="${DESTDIR}" \
	    --record=INSTALLED_FILES

	# Copy configuration file
	mkdir -p ${ARCHIVE_HELPERS_CONF_DIR}
	cp include/rhel9/SOURCES/${ARCHIVE_HELPERS_CONF_FILE} ${ARCHIVE_HELPERS_CONF_PATH}
	chmod 644 ${ARCHIVE_HELPERS_CONF_PATH}

test:
	${PYTHON} -m pytest tests -v \
	    --junitprefix=archives-helper --junitxml=junit.xml

coverage:
	${PYTHON} -m pytest tests \
		-svvv --cov=archive_helpers --cov-report=term-missing \
		--cov-fail-under=80
	coverage html
	coverage xml

clean: clean-rpm
	find . -iname '*.pyc' -type f -delete
	find . -iname '__pycache__' -exec rm -rf '{}' \; | true
	rm -rf coverage.xml htmlcov junit.xml .coverage

clean-rpm:
	rm -rf rpmbuild

