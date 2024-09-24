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
CORAL_DIR = "/var/lib/coral"

CORAL_LOG_DIR = "/var/log/coral"
# Dir to save build cache. It includes subdirs for devel and release.
CORAL_BUILD_CACHE = CORAL_LOG_DIR + "/build_cache"
# Dir to save ISO cache. The files under it will be put into ISO of Coral.
ISO_CACHE_FNAME = "iso_cache"

# File name to save build cache.
CORAL_BUILD_CACHE_TYPE_OPEN = "open"
# Dir to save open cache.
CORAL_BUILD_CACHE_OPEN_DIR = (CORAL_BUILD_CACHE + "/" +
                              CORAL_BUILD_CACHE_TYPE_OPEN)
CORAL_BUILD_CACHE_TYPES = [CORAL_BUILD_CACHE_TYPE_OPEN]


ISO_FNAME = "iso"
# ISO dir
CORAL_ISO_DIR = CORAL_LOG_DIR + "/" + ISO_FNAME
CORAL_BUILD_LOG_DIR_BASENAME = "coral_build"
CORAL_LUSTRE_RELEASE_BASENAME = "lustre_release"
# Lustre dir under the ISO dir
CORAL_ISO_LUSTRE_DIR = (CORAL_ISO_DIR +
                        "/" + CORAL_LUSTRE_RELEASE_BASENAME)
# Basename of SRPMS
SRPMS_DIR_BASENAME = "SRPMS"
# Basename of RPMS
RPMS_DIR_BASENAME = "RPMS"
LUSTRE_RELEASE_INFO_FNAME = "lustre_release_info.yaml"
E2FSPROGS_RELEASE_INFO_FNAME = "e2fsprogs_release_info.yaml"
# Lustre SRPM dir
CORAL_ISO_LUSTRE_SRPM_DIR = (CORAL_ISO_LUSTRE_DIR +
                             "/" + SRPMS_DIR_BASENAME)
# Lustre RPM dir
CORAL_ISO_LUSTRE_RPM_DIR = (CORAL_ISO_LUSTRE_DIR +
                            "/" + RPMS_DIR_BASENAME)
CORAL_E2FSPROGS_RELEASE_BASENAME = "e2fsprogs_release"
CORAL_ISO_E2FSPROGS_DIR = (CORAL_ISO_DIR + "/" + CORAL_E2FSPROGS_RELEASE_BASENAME)

#
# RPM needed by almost all use cases of Coral.
#
CORAL_DEPENDENT_RPMS = ["attr",  # Needed by Lustre test RPM
                        "bash-completion",  # For bash completion
                        "bc",  # Needed by Lustre test RPM
                        "bzip2",
                        "dbench",  # Needed by Lustre test RPM
                        "git",  # For build anything from Git repository
                        "kexec-tools",  # In case of kernel crash
                        "libyaml",  # For loading config, needed by lctl too.
                        "linux-firmware",  # Needed by Lustre Linux kernel.
                        "lsof",  # Needed by Lustre test scriot.
                        "net-snmp-libs",  # needed by Lustre RPM
                        "net-snmp-agent-libs",  # needed by Lustre RPM
                        "net-tools",  # netstat is needed by Lustre test RPM
                        "nfs-utils",
                        "p7zip",  # 7z command for extracting ISO
                        "p7zip-plugins",  # 7z command for extracting ISO
                        "pciutils",
                        "pdsh",  # Lustre test need to run remote commands
                        "perl-File-Path",  # Needed by lustre-iokit.
                        "psmisc",  # For fuser/killall command
                        "rsync",  # For syncing files
                        "selinux-policy-targeted",  # Needed by Lustre Linux kernel.
                        "sg3_utils",
                        "sysstat",
                        "yum-utils",  # Yum commands.
                        "unzip"]  # For unpacking tarballs

# The message to use in commands
CMD_MSG_ERROR = "@error@"
CMD_MSG_NONE = "@none@"
CMD_MSG_UNKNOWN = "@unknown@"

TITLE_CURRENT_RELEASE = "Current Release"

CORAL_RELEASE_INFO_FNAME = "coral_release_info.yaml"

# The constant in VERSION of iso
CORAL_STR_RELEASE_NAME = "release_name"
# The constant in VERSION of iso
CORAL_STR_TARGET_CPU = "target_cpu"
# The constant in VERSION of iso
# The constant in VERSION of iso
CORAL_STR_RELEASE_DATE = "release_date"

CORAL_LANGUAGES = ["zh", "en"]

# The plain tag of Coral artifact
CORAL_TAG_PLAIN = "plain"
CORAL_ISO_PREFIX = "coral-"
CORAL_ISO_SUFFIX = ".iso"

# The info fname of artifact
CORAL_ARTIFACT_INFO_FNAME = "artifact_info.yaml"

# The path to save coral build artifacts
CORAL_BUILD_ARTIFACTS = CORAL_LOG_DIR + "/coral_build_artifacts"
# The path to save coral build jobs
CORAL_BUILD_JOBS = CORAL_BUILD_ARTIFACTS + "/jobs"

# The shortest time that a reboot could finish. It is used to check whether
# a host has actually rebooted or not.
SHORTEST_TIME_REBOOT = 10
# The logest time that a reboot wil takes
LONGEST_TIME_REBOOT = 240
# The longest time that a simple command should finish
LONGEST_SIMPLE_COMMAND_TIME = 600
# Yum install is slow, so use a larger timeout value
LONGEST_TIME_YUM_INSTALL = LONGEST_SIMPLE_COMMAND_TIME * 3
# RPM install is slow, so use a larger timeout value
LONGEST_TIME_RPM_INSTALL = LONGEST_SIMPLE_COMMAND_TIME * 2
# The longest time that a issue reboot would stop the SSH server
LONGEST_TIME_ISSUE_REBOOT = 10

CORAL_REAF_DIR_NAME = "reaf"
LUSTRE_VERSION_DEFINITION_DIR_NAME = "lustre_version_definitions"
CORAL_REAF_DIR = CORAL_DIR + "/" + CORAL_REAF_DIR_NAME
LUSTRE_VERSION_DEFINITION_DIR = CORAL_REAF_DIR + "/" + LUSTRE_VERSION_DEFINITION_DIR_NAME
LUSTRE_VERSION_DEFINITION_SOURCE_PATH = "coral_reaf/" + LUSTRE_VERSION_DEFINITION_DIR_NAME

CORAL_PUBLIC_KEY_FNAME = "public_key.gpg"

LUSTRE_STR_ERROR = "error"
LUSTRE_STR_HEALTHY = "healthy"
LUSTRE_STR_UNHEALTHY = "UNHEALTHY"
LUSTRE_STR_LBUG = "LBUG"


LUSTRE_RELEASES_DIRNAME = "lustre_releases"
LUSTRE_RELEASES_DIR = CORAL_LOG_DIR + "/" + LUSTRE_RELEASES_DIRNAME
E2FSPROGS_RELEASES_DIRNAME = "e2fsprogs_releases"
E2FSPROGS_RELEASES_DIR = CORAL_LOG_DIR + "/" + E2FSPROGS_RELEASES_DIRNAME
