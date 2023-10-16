AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}
if ENABLE_CLOWNF
noinst_PROGRAMS = clownf_storage
endif #ENABLE_CLOWNF

CLF_CFLAGS_ = -Wall -Werror -g -lssl -lcrypto
CLF_LDFLAGS_ =

if ZFS
CLF_CFLAGS_ += $(zfs_CFLAGS)
CLF_LDFLAGS_ += -lzfs -lzpool -lnvpair
endif

clownf_storage_SOURCES = clownf_storage.c \
	../ccoral/debug.h \
	../ccoral/debug.c
nodist_clownf_storage_SOURCES = clf_constant.h
clownf_storage_CFLAGS = $(CLF_CFLAGS_)
clownf_storage_LDFLAGS = $(CLF_LDFLAGS_)
CHECKS =


clean-local:
	rm -f $(CHECKS)

all: all-am $(CHECKS)
