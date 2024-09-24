# Somehow rpmbuild does not include pip3's library to Python3's sys.path
# which will cause missing module. So add it explicitly here.
PIP3_PACKAGE_PATH = /usr/local/lib/python3.6/site-packages:/usr/local/lib64/python3.6/site-packages
CBUILD_CMD = PYTHONPATH=$(PIP3_PACKAGE_PATH) ../cbuild
CONFIG_FILES = barreleye.conf \
	clownfish.conf


%.conf_checked: %
	$(CBUILD_CMD) config_check $< && touch $@

CHECKS = $(CONFIG_FILES:%=%.conf_checked)

clean-local:
	-rm -f $(CHECKS)

all: all-am \
	$(CHECKS)
