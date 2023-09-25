AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}
AM_CPPFLAGS = -D_GNU_SOURCE -include config.h
AM_CFLAGS = -Wall -Werror -g $(json_c_CFLAGS) $(json_c_LIBS) \
        -llustreapi -lpthread -Wno-error=deprecated-declarations

# Somehow rpmbuild does not include pip3's library to Python3's sys.path
# which will cause missing module. So add it explicitly here.
PIP3_PACKAGE_PATH = /usr/local/lib/python3.6/site-packages:/usr/local/lib64/python3.6/site-packages
CORAL_CMD=PYTHONPATH=$(PIP3_PACKAGE_PATH) ../coral

../pycoral/version.py: build_version.py
	$(CORAL_CMD) version save ../pycoral/version.py

SOURCES = ../pycoral/version.py
CHECKS =


clean-local:
	rm -fr $(SOURCES) $(CHECKS)

all: $(SOURCES) $(CHECKS) all-am
