DESTDIR ?= /
PREFIX ?= /usr
ETC=${DESTDIR}/etc

install:
	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Use Python setuptools
	python ./setup.py install -O1 \
	    --prefix="${PREFIX}" \
	    --root="${DESTDIR}" \
	    --record=INSTALLED_FILES

install3:
	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Use Python setuptools
	python3 ./setup.py install -O1 \
	    --prefix="${PREFIX}" \
	    --root="${DESTDIR}" \
	    --record=INSTALLED_FILES

test:
	py.test  tests -v -m "not gluster" \
	    --junitprefix=archives-helper --junitxml=junit.xml

coverage:
	py.test tests --cov=archive_helpers --cov-report=html
	coverage report -m
	coverage html
	coverage xml

clean: clean-rpm
	find . -iname '*.pyc' -type f -delete
	find . -iname '__pycache__' -exec rm -rf '{}' \; | true
	rm -rf coverage.xml htmlcov junit.xml .coverage

clean-rpm:
	rm -rf rpmbuild

