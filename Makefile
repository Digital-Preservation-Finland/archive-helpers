DESTDIR ?= /
PREFIX ?= /usr
ETC=${DESTDIR}/etc
PYTHON ?= python3

install:
	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Use Python setuptools
	${PYTHON} ./setup.py install -O1 \
	    --prefix="${PREFIX}" \
	    --root="${DESTDIR}" \
	    --record=INSTALLED_FILES

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

