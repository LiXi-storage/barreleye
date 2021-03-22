# Somehow rpmbuild does not include pip3's library to Python3's sys.path
# which will cause missing module. So add it explicitly here.
PIP3_PACKAGE_PATH = /usr/local/lib/python3.6/site-packages:/usr/local/lib64/python3.6/site-packages

barrele:
	PYTHONPATH=$(PIP3_PACKAGE_PATH) ../barrele -- --completion > barrele.tmp || exit 1; \
	mv barrele.tmp barrele

COMPLETIONS = barrele

clean-local:
	rm -f $(COMPLETIONS)

all: all-am $(COMPLETIONS) 
