PYTHON_COMMANDS = \
	../barrele \
	../coral

PYTHON_LIB_FILES = $(wildcard ../pybuild/*.py ../pybarreleye/*.py ../pycoral/*.py)
PYTHON_FILES = $(PYTHON_LIB_FILES) $(PYTHON_COMMANDS)
PYTHON_CHECKS = $(PYTHON_FILES:%=%.python_checked)

CHECKS = $(PYTHON_CHECKS)

# Somehow rpmbuild does not include pip3's library to Python3's sys.path
# which will cause missing module. So add it explicitly here.
PIP3_PACKAGE_PATH = /usr/local/lib/python3.6/site-packages:/usr/local/lib64/python3.6/site-packages

%.python_checked: % ../.pylintrc
	PYLINTRC=../.pylintrc PYTHONPATH=$(PIP3_PACKAGE_PATH) python3 -m pylint --disable=I $< || exit 1; \
	PYTHONPATH=$(PIP3_PACKAGE_PATH) pep8 --ignore=E501 $< || exit 1; \
	touch $@

clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

check:
	make clean
	make

all: all-am $(CHECKS)
