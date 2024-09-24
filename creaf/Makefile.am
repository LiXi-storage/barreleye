AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}
if ENABLE_REAF
noinst_PROGRAMS = reaf_statfs
endif #ENABLE_REAF

REAF_CFLAGS_ = -Wall -Werror -g
REAF_LDFLAGS_ = -llustreapi

reaf_statfs_SOURCES = reaf_statfs.c \
	../ccoral/debug.h \
	../ccoral/debug.c
reaf_statfs_CFLAGS = $(REAF_CFLAGS_)
reaf_statfs_LDFLAGS = $(REAF_LDFLAGS_)
CHECKS =


clean-local:
	rm -f $(CHECKS)

all: all-am $(CHECKS)
