"""
Constants used in Coral common library
"""
# DO NOT import any library that needs extra python package,
# since this might cause failure of commands that uses this
# library to install python packages.


# Exit command used when checking whether a device is mountable or not
CSM_MOUNTABLE = 23
CSM_FORCE_REQUIRED = 24
CSM_EINVAL = 25
CSM_FATAL = 26
CSM_UNSUPPORTED = 27
CSM_AGAIN = 28
CSM_OCCUPIED = 29

MSG_ALREADY_MOUNTED_NO_NEWLINE = "Already mounted"
MSG_ALREADY_MOUNTED = MSG_ALREADY_MOUNTED_NO_NEWLINE + "\n"

ETC_CORAL_DIR = "/etc/coral"

# This fname should be consistent with ISO_PATH in Mafile.am
SOURCE_ISO_FNAME = "ISO"
BUILD_PACKAGES = "Packages"
SOURCE_ISO_PACKAGES_PATH = SOURCE_ISO_FNAME + "/" + BUILD_PACKAGES
BUILD_PIP = "pip"
CORAL_DIR = "/var/lib/coral"

CORAL_LOG_DIR = "/var/log/coral"
# Dir to save build cache. It includes subdirs for devel and release.
CORAL_BUILD_CACHE = CORAL_LOG_DIR + "/build_cache"
# Dir to save ISO cache. The files under it will be put into ISO of Coral.
ISO_CACHE_FNAME = "iso_cache"
# File name to save build cache.
CORAL_BUILD_CACHE_TYPE_OPEN = "open"
# File name to save development cache.
CORAL_BUILD_CACHE_TYPE_DEVEL = "devel"
# File name to save release cache.
CORAL_BUILD_CACHE_TYPE_RELEASE = "release"
# Dir to save development cache.
CORAL_BUILD_CACHE_DEVEL_DIR = (CORAL_BUILD_CACHE + "/" +
                               CORAL_BUILD_CACHE_TYPE_DEVEL)
# Dir to save open cache.
CORAL_BUILD_CACHE_OPEN_DIR = (CORAL_BUILD_CACHE + "/" +
                              CORAL_BUILD_CACHE_TYPE_OPEN)
# Dir to save release cache.
CORAL_BUILD_CACHE_RELEASE_DIR = (CORAL_BUILD_CACHE + "/" +
                                 CORAL_BUILD_CACHE_TYPE_RELEASE)
# Dir to save pip packages needed when coral command bootstrap from Internet
CORAL_BUILD_CACHE_PIP_DIR = CORAL_BUILD_CACHE_DEVEL_DIR + "/" + BUILD_PIP

ISO_FNAME = "iso"
# ISO dir
CORAL_ISO_DIR = CORAL_LOG_DIR + "/" + ISO_FNAME
CORAL_BUILD_LOG_DIR_BASENAME = "coral_build"
LUSTRE_RPM_DIR_BASENAME = "lustre"
# Lustre dir under the ISO dir
CORAL_ISO_LUSTRE_DIR = (CORAL_ISO_DIR +
                        "/" + LUSTRE_RPM_DIR_BASENAME)
# Basename of SRPMS
SRPMS_DIR_BASENAME = "SRPMS"
# Basename of RPMS
RPMS_DIR_BASENAME = "RPMS"
LUSTRE_DIR_BASENAMES = [SRPMS_DIR_BASENAME, RPMS_DIR_BASENAME]
E2FSPROGS_DIR_BASENAMES = [SRPMS_DIR_BASENAME, RPMS_DIR_BASENAME]
# Lustre SRPM dir
CORAL_ISO_LUSTRE_SRPM_DIR = (CORAL_ISO_LUSTRE_DIR +
                             "/" + SRPMS_DIR_BASENAME)
E2FSPROGS_RPM_DIR_BASENAME = "e2fsprogs"
CORAL_ISO_E2FSPROGS_DIR = (CORAL_ISO_DIR +
                           "/" + E2FSPROGS_RPM_DIR_BASENAME)

#
# RPM needed by almost all use cases of Coral.
#
CORAL_DEPENDENT_RPMS = ["git",  # For build anything from Git repository
                        "libyaml",  # For loading config, needed by lctl too.
                        "rsync",  # For syncing files
                        "bash-completion",  # For bash completion
                        "unzip",  # For unpacking tarballs
                        "attr",  # Needed by Lustre test RPM
                        "bc",  # Needed by Lustre test RPM
                        "dbench",  # Needed by Lustre test RPM
                        "bzip2",
                        "kexec-tools",  # In case of kernel crash
                        "linux-firmware",  # Needed by Lustre Linux kernel.
                        "lsof",  # Needed by Lustre test scriot.
                        "net-snmp-libs",  # needed by Lustre RPM
                        "net-snmp-agent-libs",  # needed by Lustre RPM
                        "nfs-utils",
                        "pciutils",
                        "pdsh",  # Lustre test need to run remote commands
                        "perl-File-Path",  # Needed by lustre-iokit.
                        "psmisc",  # For fuser/killall command
                        "selinux-policy-targeted",  # Needed by Lustre Linux kernel.
                        "sg3_utils",
                        "sysstat",
                        "yum-utils"]  # Yum commands.

# The pip needed by almost all Coral use cases.
# No package is needed since they should have been packaged by pyinstaller.
CORAL_DEPENDENT_PIPS = []

# The message to use in commands
CMD_MSG_ERROR = "<error>"
CMD_MSG_NONE = "<none>"
CMD_MSG_UNKNOWN = "<unknown>"

TITLE_CURRENT_RELEASE = "Current Release"

CORAL_ISO_VERSION_FNAME = "VERSION.yaml"
CORAL_RELEASE_INFO_FNAME = "release_info.yaml"

# The constant in VERSION of iso
CORAL_STR_RELEASE_NAME = "release_name"
# The constant in VERSION of iso
CORAL_STR_TARGET_CPU = "target_cpu"
# The constant in VERSION of iso
CORAL_STR_DISTRO_SHORT = "distro_short"
# The constant in VERSION of iso
CORAL_STR_RELEASE_DATE = "release_date"
