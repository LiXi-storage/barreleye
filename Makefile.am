SUBDIRS = pybuild
SUBDIRS += barreleye pycheck bash_completion .

build_dir = `pwd`/build
rpmbuild_opt =
ISO_PATH = `pwd`/ISO
PACKAGE_PATH = ${ISO_PATH}/Packages

# Somehow rpmbuild does not include pip3's library to Python3's sys.path
# which will cause missing module. So add it explicitly here.
PIP3_PACKAGE_PATH = /usr/local/lib/python3.6/site-packages:/usr/local/lib64/python3.6/site-packages

AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}

PYTHON_COMMANDS = \
	barrele \
	coral

EXTRA_DIST = \
	$(PYTHON_COMMANDS) \
	.pylintrc \
	coral.spec \
	detect-distro.sh \
	doc \
	example_configs/barreleye.toml \
	pybarrele/*.py \
	pybuild/*.py \
	pycoral/*.py \
	version

doc.checked: pybuild/build_doc.py
	PYTHONPATH=$(PIP3_PACKAGE_PATH) ./coral doc check || exit 1; \
	touch $@

CHECKS = doc.checked

clean-local:
	rm -f $(CHECKS)
	rm -fr ISO
	rm -f coral-*.iso
	rm -f coral-*_SHA256SUM
	rm -fr dist
	rm -fr build
	rm -f coral-*.tar.bz2
	rm -f coral-*.tar.gz
	for exe in $(EXES); do \
		rm -f $$exe.spec; \
	done
	rm -fr coral_build_*

all: all-am $(CHECKS)

# Clean up all the generated files that are ignored in the source repo
#
mrproper: maintainer-clean
	rm -f Makefile.in aclocal.m4 configure
	rm -f compile depcomp install-sh missing

BARRELEYE_RPM = build/RPMS/$(target_cpu)/coral-barreleye-$(PACKAGE_VERSION)-$(CORAL_RELEASE).$(DISTRO_SHORT)*.$(target_cpu).rpm

CORAL_RPMS =
if ENABLE_BARRELE
rpmbuild_opt += --with barrele
CORAL_RPMS += $(BARRELEYE_RPM)
else
rpmbuild_opt += --without barrele
endif

EXES = barrele

PYINSTALLER_CMD=PYTHONPATH=$(PIP3_PACKAGE_PATH) pyinstaller -F --clean

dist/barrele:
	mkdir -p dist
	$(PYINSTALLER_CMD) barrele

EXE_BINS = $(addprefix dist/, $(EXES))

exes: $(EXE_BINS)

EXTRA_OPTION =
rpms: coral.spec dist
	mkdir -p $(build_dir)/BUILD $(build_dir)/SPECS $(build_dir)/SRPMS $(build_dir)/RPMS \
		&& rpmbuild $(rpmbuild_opt) --define="_topdir $(build_dir)" \
			--define="_prefix $(prefix)" \
			$(EXTRA_OPTION) -tb $(distdir).tar.gz \
		&& echo "RPMs successfully generated in $(build_dir)/RPMS"

ISO_EXTRA =
coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu).iso: $(ISO_EXTRA) rpms
	rm $(ISO_PATH) -fr
	rm -f coral-*.iso
	rm -f coral-*_SHA256SUM
	cp -a $(ISO_CACHE_PATH) $(ISO_PATH)
	./coral release_info save $(DISTRO_SHORT) $(target_cpu) coral_release_info.yaml
	cp coral_release_info.yaml $(ISO_PATH)
	cp $(CORAL_RPMS) $(PACKAGE_PATH)
	createrepo $(PACKAGE_PATH)
	@if [ "$(DISTRO_SHORT)" = "el8" ]; then \
		repo2module -s stable $(PACKAGE_PATH) $(PACKAGE_PATH)/modules.yaml; \
		modifyrepo --mdtype=modules $(PACKAGE_PATH)/modules.yaml $(PACKAGE_PATH)/repodata; \
	fi
	mkisofs -joliet-long -R -o coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu).iso $(ISO_PATH)

coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu)_SHA256SUM: coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu).iso
	sha256sum coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu).iso \
		> coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu)_SHA256SUM

iso: coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu).iso \
	coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu)_SHA256SUM
