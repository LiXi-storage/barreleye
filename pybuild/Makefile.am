SOURCES =

../pycoral/version.py: build_version.py
	../coral version save ../pycoral/version.py

SOURCES += ../pycoral/version.py

check_clean-local:
	rm -f $(SOURCES)

all: $(SOURCES) all-am
