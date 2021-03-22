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
	example_configs/barreleye.toml \
	pybarreleye/*.py \
	pybuild/*.py \
	pycoral/*.py \
	version

CHECKS =

clean-local:
	rm -f $(CHECKS)
	rm -fr ISO
	rm -f coral-*.iso
	rm -f coral-*.md5
	rm -fr dist
	rm -fr build
	rm -f coral-*.tar.bz2
	rm -f coral-*.tar.gz
	for exec in $(EXECS); do \
		rm -f $$exec.spec; \
	done
	rm -fr coral_build_*

all: all-am $(CHECKS)

# Clean up all the generated files that are ignored in the source repo
#
mrproper: maintainer-clean
	rm -f Makefile.in aclocal.m4 configure
	rm -f compile depcomp install-sh missing

BARRELEYE_RPM = build/RPMS/$(target_cpu)/coral-barreleye-$(PACKAGE_VERSION)-$(CORAL_RELEASE).el$(DISTRO_RELEASE)*.$(target_cpu).rpm

CORAL_RPMS =
if ENABLE_BARRELE
rpmbuild_opt += --with barrele
CORAL_RPMS += $(BARRELEYE_RPM)
else
rpmbuild_opt += --without barrele
endif

EXECS = barrele

exes:
	for exec in $(EXECS); do \
		PYTHONPATH=$(PIP3_PACKAGE_PATH) pyinstaller -F $$exec --clean; \
	done

EXTRA_OPTION =
rpms: coral.spec dist
	mkdir -p $(build_dir)/BUILD $(build_dir)/SPECS $(build_dir)/SRPMS $(build_dir)/RPMS \
		&& rpmbuild $(rpmbuild_opt) --define="_topdir $(build_dir)" \
			--define="_prefix $(prefix)" \
			$(EXTRA_OPTION) -tb $(distdir).tar.gz \
		&& echo "RPMs successfully generated in $(build_dir)/RPMS"

coral-$(PACKAGE_VERSION).$(target_cpu).iso: rpms
	rm $(ISO_PATH) -fr
	rm -f coral-*.iso
	rm -f coral-*.md5
	cp -a $(ISO_CACHE_PATH) $(ISO_PATH)
	cp $(CORAL_RPMS) $(PACKAGE_PATH)
	createrepo $(PACKAGE_PATH)
	mkisofs -joliet-long -R -o coral-$(PACKAGE_VERSION).$(target_cpu).iso $(ISO_PATH)

coral-$(PACKAGE_VERSION).$(target_cpu).md5: coral-$(PACKAGE_VERSION).$(target_cpu).iso
	md5sum coral-$(PACKAGE_VERSION).$(target_cpu).iso \
		> coral-$(PACKAGE_VERSION).$(target_cpu).md5

iso: coral-$(PACKAGE_VERSION).$(target_cpu).iso coral-$(PACKAGE_VERSION).$(target_cpu).md5
