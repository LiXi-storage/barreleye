dnl Process this file with autoconf to produce a configure script.
AC_INIT([coral], [m4_esyscmd(./coral version show)], [Coralfs], [coral])
AC_CONFIG_SRCDIR(./pycoral/ssh_host.py)
AC_CANONICAL_SYSTEM
AC_CONFIG_HEADERS(config.h)

AM_INIT_AUTOMAKE([tar-pax dist-bzip2 foreign subdir-objects])
AM_EXTRA_RECURSIVE_TARGETS([check_clean])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AC_LANG(C)

AC_PREFIX_DEFAULT([/usr])

AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AM_PROG_CC_C_O
AM_CONDITIONAL(COMPILER_IS_GCC, test "x$GCC" = "xyes")

AC_DISABLE_STATIC
AC_PROG_LIBTOOL
AC_PROG_LEX
AC_PROG_YACC
PKG_PROG_PKG_CONFIG

#
# MB_CHECK_FILE
#
# Check for file existance even when cross compiling
# $1 - file to check
# $2 - do 'yes'
# $3 - do 'no'
#
AC_DEFUN([MB_CHECK_FILE], [
AS_VAR_PUSHDEF([mb_file], [mb_cv_file_$1])dnl
AC_CACHE_CHECK([for $1], mb_file, [
AS_IF([test -r "$1"],
	[AS_VAR_SET(mb_file, [yes])],
	[AS_VAR_SET(mb_file, [no])])
])
AS_IF([test AS_VAR_GET(mb_file) = yes], [$2], [$3])[]dnl
AS_VAR_POPDEF([mb_file])dnl
]) # MB_CHECK_FILE

#
# MB_ARG_REPLACE_PATH(PACKAGE, PATH)
#
AC_DEFUN([MB_ARG_REPLACE_PATH], [
new_configure_args=
eval set -- $ac_configure_args
for arg; do
	case $arg in
		--with-[$1]=*)
			arg=--with-[$1]=[$2] ;;
		*\'*)
			arg=$(printf %s\n ["$arg"] | \
				sed "s/'/'\\\\\\\\''/g") ;;
	esac
	dnl AS_VAR_APPEND([new_configure_args], [" '$arg'"])
	new_configure_args="$new_configure_args '$arg'"
done
ac_configure_args=$new_configure_args
])

#
# __MB_ARG_CANON_PATH
#
# this is the work-horse of the next function
#
AC_DEFUN([__MB_ARG_CANON_PATH], [
	[$3]=$(readlink -f $with_$2)
	MB_ARG_REPLACE_PATH([$1], $[$3])
])

#
# MB_ARG_CANON_PATH
#
# a front-end for the above function that transforms - and . in the
# PACKAGE portion of --with-PACKAGE into _ suitable for variable names
#
AC_DEFUN([MB_ARG_CANON_PATH], [
	__MB_ARG_CANON_PATH([$1], m4_translit([$1], [-.], [__]), [$2])
])

dnl We may be able to use older versions, but I have not verified that
PKG_CHECK_MODULES([ext2fs], [ext2fs >= 1.42.7 com_err >= 1.42.7])
PKG_CHECK_MODULES([json_c], [json-c >= 0.11])

# -------- check for distro version --------
AC_MSG_CHECKING([for distro version])
DISTRO=$(sh detect-distro.sh)
DISTRO_NAME=$(echo $DISTRO | awk -F '-' '{print $1}')
if [[[ "$DISTRO_NAME" != "rhel" ]]]; then
        AC_MSG_ERROR([$DISTRO_NAME is not a supported distro.])
fi
DISTRO_RELEASE=$(echo $DISTRO | awk -F 'rhel-' '{print $2}' | awk -F '.' '{print $1}')
DISTRO_SHORT=el$DISTRO_RELEASE
AC_MSG_RESULT([$DISTRO_SHORT])
AC_SUBST(DISTRO_SHORT)

# ------- check for target_cpu --------
AC_SUBST(target_cpu)

# ------- check for python3 --------
AC_CHECK_PROGS([PYTHON3], [python3], [])
if test "x$PYTHON3" = "x"; then
	AC_MSG_ERROR([Python3 is needed. Install python3 please.])
fi

# -------- check whether enable zfs support --------
AC_MSG_CHECKING([whether enable zfs support])
AC_ARG_ENABLE([zfs],
	AC_HELP_STRING([--enable-zfs],
                       [enable zfs support]),
        [], [enable_zfs="no"])
AC_MSG_RESULT([$enable_zfs])
AM_CONDITIONAL([ZFS], [test x$enable_zfs = xyes])

AS_IF([test "x$enable_zfs" = xyes ], [
	PKG_CHECK_MODULES([zfs], [libzfs], AC_DEFINE(HAVE_ZFS, 1, [enable ZFS support]))

	saved_libs=$LIBS
	LIBS="-lzfs -lzpool -I/usr/include/libspl -I/usr/include/libzfs"

	AC_MSG_CHECKING([libzutil.h exists])
	AC_LINK_IFELSE([AC_LANG_SOURCE([
		#ifndef _GNU_SOURCE
		#define _GNU_SOURCE
		#endif
		#include <libzfs/libzutil.h>
		int main(void) {
		}
	])],[have_libzfs_libzutil_h=yes
	], [have_libzfs_libzutil_h="no"])
	AC_MSG_RESULT([$have_libzfs_libzutil_h])
	AS_IF([test "x$have_libzfs_libzutil_h" = xyes ],
	      [AC_DEFINE(HAVE_LIBZFS_LIBZUTIL_H, 1, [have the <libzfs/libzutil.h> header file])])

	AC_MSG_CHECKING([ZFS has special kthread_t])
	AC_LINK_IFELSE([AC_LANG_SOURCE([
		#ifndef _GNU_SOURCE
		#define _GNU_SOURCE
		#endif
		#include <sys/zfs_context.h>

		int main(void) {
			kthread_t kt;
			kt.t_func = NULL;
		}
	])],[have_zfs_special_kthread_t=yes
	], [have_zfs_special_kthread_t="no"])
	AC_MSG_RESULT([$have_zfs_special_kthread_t])
	AS_IF([test "x$have_zfs_special_kthread_t" = xyes ],
	      [AC_DEFINE(HAVE_ZFS_SPECIAL_KTHREAD_T, 1, [ZFS has special kthread_t])])

	AC_MSG_CHECKING([ZFS has ZPL_PROJID])
	AC_LINK_IFELSE([AC_LANG_SOURCE([
		#ifndef _GNU_SOURCE
		#define _GNU_SOURCE
		#endif
		#include <sys/sa.h>
		#include <sys/zfs_acl.h>
		#include <sys/zfs_sa.h>

		int main(void) {
			int attr = ZPL_PROJID;
		}
	])],[have_zfs_zpl_projid=yes
	], [have_zfs_zpl_projid="no"])
	AC_MSG_RESULT([$have_zfs_zpl_projid])
	AS_IF([test "x$have_zfs_zpl_projid" = xyes ],
	      [AC_DEFINE(HAVE_ZFS_ZPL_PROJID, 1, [ZFS has ZPL_PROJID])])

	LIBS=$saved_libs
])

# -------- whether enable Barreleye plugin --------
AC_MSG_CHECKING([whether enable Barreleye plugin])
AC_ARG_ENABLE([barrele],
    AC_HELP_STRING([--disable-barrele],
                   [disable Barreleye plugin]),
              [], [enable_barrele="yes"])
AC_MSG_RESULT([$enable_barrele])
AC_SUBST(ENABLE_BARRELE, $enable_barrele)
AM_CONDITIONAL(ENABLE_BARRELE, test "$enable_barrele" = "yes")


# -------- check for ISO cache dir --------
AC_MSG_CHECKING([for ISO cache dir])
ISO_CACHE_PATH="/var/log/coral/build_cache/release/iso_cache"
AS_IF([test "$enable_devel" != "no"],
	[ISO_CACHE_PATH="/var/log/coral/build_cache/devel/iso_cache"])
AC_ARG_WITH([iso-cache],
	AC_HELP_STRING([--with-iso-cache=path],
		       [set path of ISO cache dir]),
	[MB_ARG_CANON_PATH([iso-cache], [ISO_CACHE_PATH])],
	[])
AC_MSG_RESULT([$ISO_CACHE_PATH])
AC_SUBST(ISO_CACHE_PATH)

AS_IF([test "x$ISO_CACHE_PATH" != "x"],
      [MB_CHECK_FILE([$ISO_CACHE_PATH], [],
                     [AC_MSG_ERROR([ISO cache dir $ISO_CACHE_PATH could not be found.])])])


dnl Pull the needed libraries into LIBS (needed for the AC_LINK_IFELSE below)
dnl These should never fail if the PKG_CHECK above passes
AC_SEARCH_LIBS([com_err], [com_err])
AC_SEARCH_LIBS([ext2fs_open2], [ext2fs])

CORAL_RELEASE="1"
AC_DEFINE_UNQUOTED(RELEASE, "$CORAL_RELEASE", [release info] )
AC_SUBST(CORAL_RELEASE)

# for exporting to spec file
AC_SUBST(ac_configure_args)
AC_CONFIG_FILES([Makefile
                 coral.spec
                 barreleye/Makefile
                 pybuild/Makefile
                 pycheck/Makefile
                 bash_completion/Makefile])
AC_OUTPUT
