SUBDIRS = pybuild \
	@CCLOWNF_SUBDIR@ \
	@GOCORAL_SUBDIR@
DIST_SUBDIRS = pybuild \
	cclownf \
	ccoral \
	gocoral

if ENABLE_BARRELE
SUBDIRS += barreleye
endif #ENABLE_BARRELE

SUBDIRS += pycheck bash_completion .
DIST_SUBDIRS += barreleye pycheck bash_completion .

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
	clownf \
	coral

EXTRA_DIST = \
	$(PYTHON_COMMANDS) \
	.pylintrc \
	coral.spec \
	detect-distro.sh \
	doc \
	example_configs/barreleye.conf.example \
	example_configs/clownfish.conf.example \
	gocoral/* \
	man/* \
	pybarrele/*.py \
	pybuild/*.py \
	pyclownf/*.py \
	pycoral/*.py \
	systemd/* \
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
	rm -fr debian/.debhelper

all: all-am $(CHECKS)

# Clean up all the generated files that are ignored in the source repo
#
mrproper: maintainer-clean
	rm -f Makefile.in aclocal.m4 configure
	rm -f compile depcomp install-sh missing

CORAL_PACKAGES =

if ENABLE_BARRELE
rpmbuild_opt += --with barrele
CORAL_PACKAGES += barreleye
else
rpmbuild_opt += --without barrele
endif

if ENABLE_CLOWNF
rpmbuild_opt += --with clownf
CORAL_PACKAGES += clownfish
else
rpmbuild_opt += --without clownf
endif


EXES =
PYINSTALLER_CMD=PYTHONPATH=$(PIP3_PACKAGE_PATH) pyinstaller -F --clean


if ENABLE_BARRELE
EXES += barrele
dist/barrele:
	mkdir -p dist
	$(PYINSTALLER_CMD) barrele
endif

if ENABLE_CLOWNF
EXES += clownf
dist/clownf:
	mkdir -p dist
	$(PYINSTALLER_CMD) clownf
endif


EXE_BINS = @DOLLAR@(addprefix dist/, $(EXES))

exes: $(EXE_BINS)

EXTRA_OPTION =
ISO_EXTRA =


if MAKE_RPMS
rpms: coral.spec dist Makefile
	mkdir -p $(build_dir)/BUILD $(build_dir)/SPECS $(build_dir)/SRPMS $(build_dir)/RPMS \
		&& rpmbuild $(rpmbuild_opt) --define="_topdir $(build_dir)" \
			--define="_prefix $(prefix)" \
			$(EXTRA_OPTION) -tb $(distdir).tar.gz \
		&& echo "RPMs successfully generated in $(build_dir)/RPMS"
ISO_EXTRA += rpms
endif #MAKE_RPMS

if MAKE_DEBS
debs: Makefile
	VER=$$(echo @VERSION@ | tr '_' '-'); \
	changelog_version=$$(sed -ne '1s/^coral (\(.*\)).*$$/\1/p' debian/changelog); \
	if [ "$$changelog_version" != "$$VER" ]; then \
		echo -e "coral ($$VER) unstable; urgency=low\n\n  * Automated changelog.\n\n -- Coral Packaging Team <maintainer@coralfs.cn>  $$(date -R)\n" > debian/changelog; \
	fi; \
	rm -fr debs; \
	dpkg-buildpackage -us -uc -I.git  || { \
		rc=$${PIPESTATUS[0]}; \
		[ $${rc} -gt 1 ] && exit $${rc}; \
		exit 0; \
	}; \
	mkdir -p debs && \
	mv ../coral-*_$${VER}_*.deb \
		../coral_$${VER}_*.buildinfo \
		../coral_$${VER}.tar.gz \
		../coral_$${VER}.dsc \
		../coral_$${VER}_*.changes \
		debs/
ISO_EXTRA += debs
endif #MAKE_DEBS

coral-$(PACKAGE_VERSION).$(DISTRO_SHORT).$(target_cpu).iso: $(ISO_EXTRA)
	rm $(ISO_PATH) -fr
	rm -f coral-*.iso
	rm -f coral-*_SHA256SUM
	cp -a $(ISO_CACHE_PATH) $(ISO_PATH)
	./coral release_info save $(DISTRO_SHORT) $(target_cpu) coral_release_info.yaml
	cp coral_release_info.yaml $(ISO_PATH)
	mkdir -p $(PACKAGE_PATH)
	if [ "$(DISTRO_SHORT)" = "ubuntu2204" ]; then \
		DEB_PACKAGE_VERSION=$$(echo $(PACKAGE_VERSION) | tr '_' '-'); \
		for CORAL_PACKAGE in $(CORAL_PACKAGES); do \
			cp debs/coral-$${CORAL_PACKAGE}_$${DEB_PACKAGE_VERSION}_*.deb $(PACKAGE_PATH); \
		done; \
	fi
	if [ "$(DISTRO_SHORT)" = "el7" -o "$(DISTRO_SHORT)" = "el8" ]; then \
		for CORAL_PACKAGE in $(CORAL_PACKAGES); do \
			cp build/RPMS/$(target_cpu)/coral-$${CORAL_PACKAGE}-$(PACKAGE_VERSION)-$(CORAL_RELEASE).$(DISTRO_SHORT)*.$(target_cpu).rpm $(PACKAGE_PATH); \
		done; \
		createrepo $(PACKAGE_PATH); \
	fi
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
