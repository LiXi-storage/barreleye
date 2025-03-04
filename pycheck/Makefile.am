PYTHON_COMMANDS = \
	../cbuild

PYTHON_LIB_FILES = $(wildcard ../pycoral/*.py) \
	../pybuild/build_common.py \
	../pybuild/build_constant.py \
	../pybuild/build_doc.py \
	../pybuild/build_release_info.py \
	../pybuild/build_version.py \
	../pybuild/cache_command.py \
	../pybuild/coral_build.py \
	../pybuild/coral_command.py \
	../pybuild/coral.py \
	../pybuild/e2fsprogs_command.py \
	../pybuild/lustre_command.py \
 	../pybuild/apt_command.py \
	../pybuild/pip_command.py \
	../pybuild/yum_command.py \
	../pybuild/__init__.py

if ENABLE_BARRELE
PYTHON_COMMANDS += ../barrele
PYTHON_LIB_FILES += $(wildcard ../pybarrele/*.py) \
	../pybuild/build_barrele.py
endif

if ENABLE_CLOWNF
PYTHON_COMMANDS += ../clownf
PYTHON_LIB_FILES += $(wildcard ../pyclownf/*.py) \
	../pybuild/build_clownf.py

endif #ENABLE_CLOWNF

if ENABLE_REAF
PYTHON_COMMANDS += ../reaf
PYTHON_LIB_FILES += $(wildcard ../pyreaf/*.py)
endif #ENABLE_REAF

PYTHON_FILES = $(PYTHON_LIB_FILES) $(PYTHON_COMMANDS)
PYTHON_CHECKS = $(PYTHON_FILES:%=%.python_checked)

CHECKS = $(PYTHON_CHECKS)

# Somehow rpmbuild does not include pip3's library to Python3's sys.path
# which will cause missing module. So add it explicitly here.
PIP3_PACKAGE_PATH = /usr/local/lib/python3.6/site-packages:/usr/local/lib64/python3.6/site-packages

%.python_checked: % ../.pylintrc
	PYLINTRC=../.pylintrc PYTHONPATH=$(PIP3_PACKAGE_PATH) python3 -m pylint --disable=I $< || exit 1; \
	touch $@

clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

check:
	make clean
	make

all: all-am $(CHECKS)
