"""
Library for Lustre version
"""
import re

RPM_KERNEL = "kernel"
RPM_KERNEL_FIRMWARE = "kernel-firmware"
RPM_LUSTRE = "lustre"
RPM_LUSTRE_CLIENT = "lustre_client"
RPM_IOKIT = "iokit"
RPM_KMOD = "kmod"
RPM_KMOD_LUSTRE_CLENT = "kmod_lustre_client"
RPM_OSD_LDISKFS = "osd_ldiskfs"
RPM_OSD_LDISKFS_MOUNT = "osd_ldiskfs_mount"
RPM_OSD_ZFS = "osd_zfs"
RPM_OSD_ZFS_MOUNT = "osd_zfs_mount"
RPM_TESTS = "tests"
RPM_TESTS_KMOD = "tests_kmod"
RPM_MLNX_OFA = "mlnx_ofa"
RPM_MLNX_KMOD = "mlnx_ofa_modules"

# The order should be proper for the dependency of RPMs
LUSTRE_RPM_TYPES = [RPM_KMOD, RPM_OSD_LDISKFS_MOUNT, RPM_OSD_LDISKFS,
                    RPM_OSD_ZFS_MOUNT, RPM_OSD_ZFS,
                    RPM_LUSTRE, RPM_IOKIT, RPM_TESTS_KMOD, RPM_TESTS]
# Version dectection RPM types.
LUSTRE_VERSION_DETECTION_RPM_TYPES = [RPM_KERNEL, RPM_KMOD, RPM_OSD_LDISKFS_MOUNT,
                                      RPM_OSD_LDISKFS, RPM_LUSTRE, RPM_IOKIT]


LUSTRE_TEST_RPM_TYPES = [RPM_TESTS_KMOD, RPM_TESTS]

# A Lustre version need to have these patterns set, otherwise tools would
# fail.
LUSTRE_REQUIRED_RPM_TYPES = (LUSTRE_VERSION_DETECTION_RPM_TYPES +
                             LUSTRE_TEST_RPM_TYPES)
LUSTRE_CLIENT_REQUIRED_RPM_TYPES = (RPM_LUSTRE_CLIENT, RPM_KMOD_LUSTRE_CLENT)


class LustreVersion():
    """
    RPM version of Lustre
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, name, rpm_patterns, priority):
        # pylint: disable=too-few-public-methods
        self.lv_name = name
        # Key: RPM_*, value: regular expression to match the RPM fname.
        self.lv_rpm_pattern_dict = rpm_patterns
        # If two versions are matched, the one with higher priority will
        # be used.
        self.lv_priority = priority
        assert priority >= 0
        for required_rpm in LUSTRE_REQUIRED_RPM_TYPES:
            if required_rpm not in rpm_patterns:
                reason = "no RPM pattern for [%s]" % required_rpm
                raise Exception(reason)


# Key is version name, value is LustreVersion
LUSTRE_VERSION_DICT = {}

B2_12_PATTERNS = {
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.12.+\.rpm)$",
    RPM_IOKIT: r"^(lustre-iokit-2.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.12.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.12.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.12.+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.12.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.12.+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
}
LUSTRE_VERSION_NAME_2_12 = "2.12"
LUSTRE_VERSION_2_12 = LustreVersion(LUSTRE_VERSION_NAME_2_12,
                                    B2_12_PATTERNS,
                                    0 # Priority
                                    )
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_2_12] = LUSTRE_VERSION_2_12

ES5_1_PATTERNS = {
    RPM_IOKIT: r"^(lustre-iokit-2\.12\.3.+\.rpm)$",
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.12\.3_ddn.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.12\.3_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.12\.3_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.12\.3.+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.12\.3_ddn.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.12\.3.+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
}
LUSTRE_VERSION_NAME_ES5_1 = "es5.1"
LUSTRE_VERSION_ES5_1 = LustreVersion(LUSTRE_VERSION_NAME_ES5_1,
                                     ES5_1_PATTERNS,
                                     1 # Priority
                                     )
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_ES5_1] = LUSTRE_VERSION_ES5_1


ES5_2_PATTERNS = {
    RPM_IOKIT: r"^(lustre-iokit-2\.12\.[567].+\.rpm)$",
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.12\.[567]_ddn.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.12\.[567]_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.12\.[567]_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.12\.[567].+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.12\.[567]_ddn.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.12\.[567].+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
}
LUSTRE_VERSION_NAME_ES5_2 = "es5.2"
LUSTRE_VERSION_ES5_2 = LustreVersion(LUSTRE_VERSION_NAME_ES5_2,
                                     ES5_2_PATTERNS,
                                     1 # Priority
                                     )
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_ES5_2] = LUSTRE_VERSION_ES5_2


# Since 2.14.0-ddn87 (include), the following patch is included and the
# format of proc entires has been changed dramatically. Thus, consider the
# versions between 2.14.0-ddn0 and 2.14.0-ddn86 as the ES6.0 and the
# 2.14.0-ddn87+ as ES6.1.
#
# LU-15642 obdclass: use consistent stats units
#
ES6_0_PATTERNS = {
    RPM_IOKIT: r"^(lustre-iokit-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
    RPM_LUSTRE_CLIENT: r"^(lustre-client-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
    RPM_KMOD_LUSTRE_CLENT: r"^(kmod-lustre-client-2\.14\.[0]_ddn([0-9]|[1-7][0-9]|8[0-6])\D.+\.rpm)$",
}
LUSTRE_VERSION_NAME_ES6_0 = "es6.0"
LUSTRE_VERSION_ES6_0 = LustreVersion(LUSTRE_VERSION_NAME_ES6_0,
                                     ES6_0_PATTERNS,
                                     1 # Priority
                                     )
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_ES6_0] = LUSTRE_VERSION_ES6_0


ES6_1_PATTERNS = {
    RPM_IOKIT: r"^(lustre-iokit-2\.14\.[0].+\.rpm)$",
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.14\.[0]_ddn.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.14\.[0]_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.14\.[0]_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.14\.[0].+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.14\.[0]_ddn.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.14\.[0].+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
    RPM_LUSTRE_CLIENT: r"^(lustre-client-2\.14\.[0]_ddn.+\.rpm)$",
    RPM_KMOD_LUSTRE_CLENT: r"^(kmod-lustre-client-2\.14\.[0]_ddn.+\.rpm)$",
}
LUSTRE_VERSION_NAME_ES6_1 = "es6.1"
LUSTRE_VERSION_ES6_1 = LustreVersion(LUSTRE_VERSION_NAME_ES6_1,
                                     ES6_1_PATTERNS,
                                     0 # Priority should be lower than ES6.0
                                     )
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_ES6_1] = LUSTRE_VERSION_ES6_1


B2_15_PATTERNS = {
    RPM_IOKIT: r"^(lustre-iokit-2\.15.+\.rpm)$",
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.15.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.15.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.15.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.15.+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.15.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.15.+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
}
LUSTRE_VERSION_NAME_2_15 = "2.15"
LUSTRE_VERSION_2_15 = LustreVersion(LUSTRE_VERSION_NAME_2_15,
                                    B2_15_PATTERNS,
                                    0 # Priority
                                    )
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_2_15] = LUSTRE_VERSION_2_15


def match_lustre_version_from_rpms(log, rpm_fnames, skip_kernel=False,
                                   skip_test=False, client=False):
    """
    Match the Lustre version from RPM names
    """
    # pylint: disable=too-many-locals,too-many-branches
    # pylint: disable=too-many-statements
    rpm_types = LUSTRE_REQUIRED_RPM_TYPES
    definition_message = "server "
    if client:
        rpm_types = LUSTRE_CLIENT_REQUIRED_RPM_TYPES
        definition_message = "client "
    # Key is version name, type is matched_rpm_type_dict
    matched_version_dict = {}
    for version in LUSTRE_VERSION_DICT.values():
        # Key is RPM type, value is RPM fname
        matched_rpm_type_dict = {}
        # Key is RPM fname, value is RPM type
        used_rpm_fname_dict = {}
        version_matched = True

        if client:
            # Check whether client support has been added.
            supported = True
            for rpm_type in rpm_types:
                if rpm_type not in version.lv_rpm_pattern_dict:
                    supported = False
                    break
            if not supported:
                continue

        for rpm_type in rpm_types:
            if rpm_type == RPM_KERNEL and skip_kernel:
                continue
            if rpm_type in LUSTRE_TEST_RPM_TYPES and skip_test:
                continue
            if rpm_type not in version.lv_rpm_pattern_dict:
                log.cl_error("Lustre %sversion [%s] does not have required RPM "
                             "pattern for type [%s]",
                             definition_message, version.lv_name,
                             rpm_type)
                return None, None
            pattern = version.lv_rpm_pattern_dict[rpm_type]

            matched = False
            for rpm_fname in rpm_fnames:
                match = re.search(pattern, rpm_fname)
                if match is None:
                    continue

                matched = True
                if rpm_type in matched_rpm_type_dict:
                    log.cl_error("both RPM [%s] and [%s] can be matched to "
                                 "type [%s] of Lustre %sversion [%s]",
                                 rpm_fname,
                                 matched_rpm_type_dict[rpm_type],
                                 rpm_type,
                                 definition_message,
                                 version.lv_name)
                    return None, None

                if rpm_fname in used_rpm_fname_dict:
                    log.cl_error("RPM [%s] can be matched to both type [%s] "
                                 "and [%s] of Lustre %sversion [%s]",
                                 rpm_fname,
                                 used_rpm_fname_dict[rpm_fname],
                                 rpm_type,
                                 definition_message,
                                 version.lv_name)
                    return None, None
                used_rpm_fname_dict[rpm_fname] = rpm_type
                matched_rpm_type_dict[rpm_type] = rpm_fname

            if not matched:
                log.cl_debug("unmatched Lustre %sversion "
                             "[%s] due to unmatched RPM type [%s]",
                             definition_message, version.lv_name,
                             rpm_type)
                version_matched = False
                break
        if version_matched:
            matched_version_dict[version.lv_name] = matched_rpm_type_dict

    if len(matched_version_dict) == 0:
        log.cl_debug("no Lustre %sversion is matched by RPMs %s",
                     definition_message, rpm_fnames)
        return None, None

    highest_priority = 0
    for version_name in matched_version_dict:
        version = LUSTRE_VERSION_DICT[version_name]
        if version.lv_priority > highest_priority:
            highest_priority = version.lv_priority

    matched_versions = []
    matched_rpm_type_dicts = []
    version_string = ""
    for version_name, rpm_type_dict in matched_version_dict.items():
        version = LUSTRE_VERSION_DICT[version_name]
        if version.lv_priority == highest_priority:
            matched_versions.append(version)
            matched_rpm_type_dicts.append(rpm_type_dict)
            if version_string == "":
                version_string = version.lv_name
            else:
                version_string += ", " + version.lv_name

    if len(matched_versions) > 1:
        log.cl_error("multiple Lustre %sversions [%s] are matched by RPMs",
                     definition_message, version_string)
        return None, None
    return matched_versions[0], matched_rpm_type_dicts[0]

# See the commend of ES6_0_PATTERNS for the difference between es6.0
# and es6.1.
DEB_ES6_0_PATTERN = r"^(2\.14\.[0]-ddn([0-9]|[1-7][0-9]|8[0-6])\D.+)$"
DEB_ES6_1_PATTERN = r"^(2\.14\.[0]-ddn.+)$"
DEB_2_15_PATTERN = r"^(2\.15\..+)$"

def match_lustre_version_from_deb(log, deb_version):
    """
    The version of deb package usually comes from command
    apt list --installed | grep lustre-client-modules
    or
    apt show lustre-client-modules-5.15.0-69-generic | grep Version
    Example:
    2.15.2-70-gb74560d-1
    """
    match = re.search(DEB_ES6_0_PATTERN, deb_version)
    if match is not None:
        return LUSTRE_VERSION_ES6_0

    match = re.search(DEB_ES6_1_PATTERN, deb_version)
    if match is not None:
        return LUSTRE_VERSION_ES6_1

    match = re.search(DEB_2_15_PATTERN, deb_version)
    if match is not None:
        return LUSTRE_VERSION_2_15

    log.cl_error("unsupported Lustre version [%s]", deb_version)
    return None
