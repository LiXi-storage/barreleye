"""
Library for Lustre version
"""
import re
import traceback
import yaml
from pycoral import utils
from pycoral import cmd_general

# LVD: Lustre Version Definition
LVD_RPM_PATTERNS = "rpm_patterns"
LVD_LUSTRE_VERSION = "lustre_version"
LVD_PRIORITY = "priority"
LVD_DEB_PATTERN = "deb_pattern"
LVD_COLLECTD_DEFINITION_FILE = "collectd_definition_file"
# Kernel RPM. Required in a Lustre version. Used in version detection.
LVD_RPM_KERNEL = "kernel"
# Kernel Core RPM. Not required in a Lustre version. Not used in version
# detection. Will be installed during Lustre installation if defined. Will
# not be installed if not.
LVD_RPM_KERNEL_CORE = "kernel_core"
# Kernel modules RPM. Not required in a Lustre version. Not used in version
# detection. Will be installed during Lustre installation if defined. Will
# not be installed if not.
LVD_RPM_KERNEL_MODULES = "kernel_modules"
# Kernel formware RPM. Not required in a Lustre version. Not used in version
# detection. Will be installed during Lustre installation if defined. Will
# not be installed if not.
LVD_RPM_KERNEL_FIRMWARE = "kernel_firmware"
# Lustre RPM. Required in a Lustre version. Used in version detection.
LVD_LUSTRE = "lustre"
# Lustre devel RPM. Not required in a Lustre version. Not used in version
# detection. Will be installed during Lustre installation if defined. Wi`ll
# not be installed if not.
LVD_LUSTRE_DEVEL = "lustre_devel"
# Lustre iokit RPM. Required in a Lustre version. Used in version detection.
LVD_LUSTRE_IOKIT = "lustre_iokit"
# Lustre kmod RPM. Required in a Lustre version. Used in version detection.
LVD_LUSTRE_KMOD = "lustre_kmod"
# Lustre osd ldiskfs RPM. Required in a Lustre version. Used in version detection.
LVD_LUSTRE_OSD_LDISKFS = "lustre_osd_ldiskfs"
# Lustre ldiskfs mount RPM. Required in a Lustre version. Used in version detection.
LVD_LUSTRE_OSD_LDISKFS_MOUNT = "lustre_osd_ldiskfs_mount"
# Lustre test RPM.  Required in a Lustre version. Not Used in version detection.
LVD_LUSTRE_TESTS = "lustre_tests"
# Lustre test kmod RPM.  Required in a Lustre version. Not Used in version detection.
LVD_LUSTRE_TESTS_KMOD = "lustre_tests_kmod"

# Lustre osd zfs RPM. Not required in a Lustre version. Not used in version
# detection. If not defined, ZFS support is disabled.
LVD_LUSTRE_OSD_ZFS = "lustre_osd_zfs"
# Lustre osd zfs mount RPM. Not required in a Lustre version. Not used in version
# detection. If not defined, ZFS support is disabled.
LVD_LUSTRE_OSD_ZFS_MOUNT = "lustre_osd_zfs_mount"

# Lustre client RPM. Not required in a Lustre version. If defeined, it
# will be used to detect version of Lustre client.
LVD_LUSTRE_KMOD_LUSTRE_CLENT = "lustre_kmod_client"
# Lustre client RPM. Not required in a Lustre version. If defeined, it
# will be used to detect version of Lustre client.
LVD_LUSTRE_CLIENT = "lustre_client"

LUSTRE_RPM_TYPES = [LVD_LUSTRE_KMOD, LVD_LUSTRE_OSD_LDISKFS_MOUNT,
                    LVD_LUSTRE_OSD_LDISKFS, LVD_LUSTRE_OSD_ZFS_MOUNT,
                    LVD_LUSTRE_OSD_ZFS, LVD_LUSTRE, LVD_LUSTRE_IOKIT,
                    LVD_LUSTRE_TESTS_KMOD, LVD_LUSTRE_TESTS,
                    LVD_LUSTRE_DEVEL]
# Version dectection RPM types.
LUSTRE_VERSION_DETECTION_RPM_TYPES = [LVD_RPM_KERNEL, LVD_LUSTRE_KMOD,
                                      LVD_LUSTRE_OSD_LDISKFS_MOUNT,
                                      LVD_LUSTRE_OSD_LDISKFS,
                                      LVD_LUSTRE, LVD_LUSTRE_IOKIT]


LUSTRE_TEST_RPM_TYPES = [LVD_LUSTRE_TESTS_KMOD, LVD_LUSTRE_TESTS]

# A Lustre version need to have these patterns set, otherwise tools would
# fail.
LUSTRE_REQUIRED_RPM_TYPES = (LUSTRE_VERSION_DETECTION_RPM_TYPES +
                             LUSTRE_TEST_RPM_TYPES)
LUSTRE_CLIENT_REQUIRED_RPM_TYPES = (LVD_LUSTRE_CLIENT,
                                    LVD_LUSTRE_KMOD_LUSTRE_CLENT)


class LustreVersion():
    """
    RPM version of Lustre
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, name, rpm_patterns, priority, deb_pattern,
                 collectd_definition_file):
        # pylint: disable=too-few-public-methods
        self.lv_version_name = name
        # Key: RPM_*, value: regular expression to match the RPM fname.
        self.lv_rpm_pattern_dict = rpm_patterns
        # If two versions are matched, the one with higher priority will
        # be used.
        self.lv_priority = priority
        # The pattern of deb package. Can be None.
        self.lv_deb_pattern = deb_pattern
        # The file name of Collectd definition
        self.lv_collectd_definition_file = collectd_definition_file


class LustreVersionDatabase():
    """
    The database that keep information about Lustre RPM versions
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        # Key is version name, value is LustreVersion
        self.lvd_version_dict = {}

    def _lvd_match_version_from_rpms(self, log, rpm_fnames, skip_kernel=False,
                                     skip_test=False, client=False):
        """
        Match the Lustre version from RPM fnames
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
        for version in self.lvd_version_dict.values():
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
                if rpm_type == LVD_RPM_KERNEL and skip_kernel:
                    continue
                if rpm_type in LUSTRE_TEST_RPM_TYPES and skip_test:
                    continue
                if rpm_type not in version.lv_rpm_pattern_dict:
                    log.cl_error("Lustre %sversion [%s] does not have required RPM "
                                 "pattern for type [%s]",
                                 definition_message, version.lv_version_name,
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
                                     version.lv_version_name)
                        return None, None

                    if rpm_fname in used_rpm_fname_dict:
                        log.cl_error("RPM [%s] can be matched to both type [%s] "
                                     "and [%s] of Lustre %sversion [%s]",
                                     rpm_fname,
                                     used_rpm_fname_dict[rpm_fname],
                                     rpm_type,
                                     definition_message,
                                     version.lv_version_name)
                        return None, None
                    used_rpm_fname_dict[rpm_fname] = rpm_type
                    matched_rpm_type_dict[rpm_type] = rpm_fname

                if not matched:
                    log.cl_debug("unmatched Lustre %sversion "
                                 "[%s] due to unmatched RPM type [%s]",
                                 definition_message, version.lv_version_name,
                                 rpm_type)
                    version_matched = False
                    break
            if version_matched:
                matched_version_dict[version.lv_version_name] = matched_rpm_type_dict

        if len(matched_version_dict) == 0:
            log.cl_debug("no Lustre %sversion is matched by RPMs %s",
                         definition_message, rpm_fnames)
            return None, None

        highest_priority = 0
        for version_name in matched_version_dict:
            version = self.lvd_version_dict[version_name]
            if version.lv_priority > highest_priority:
                highest_priority = version.lv_priority

        matched_versions = []
        matched_rpm_type_dicts = []
        version_string = ""
        for version_name, rpm_type_dict in matched_version_dict.items():
            version = self.lvd_version_dict[version_name]
            if version.lv_priority == highest_priority:
                matched_versions.append(version)
                matched_rpm_type_dicts.append(rpm_type_dict)
                if version_string == "":
                    version_string = version.lv_version_name
                else:
                    version_string += ", " + version.lv_version_name

        if len(matched_versions) > 1:
            log.cl_error("multiple Lustre %sversions [%s] are matched by RPMs",
                         definition_message, version_string)
            return None, None
        return matched_versions[0], matched_rpm_type_dicts[0]

    def lvd_match_version_from_rpms(self, log, rpm_fnames, skip_kernel=False,
                                    skip_test=False, client=False):
        """
        Match the Lustre version from RPM names.
        """
        definition_message = "server "
        if client:
            definition_message = "client "

        matched_version, matched_rpm_type_dict = \
            self._lvd_match_version_from_rpms(log, rpm_fnames, skip_kernel=skip_kernel,
                                              skip_test=skip_test, client=client)
        if matched_version is None or matched_rpm_type_dict is None:
            return None, None

        # Try to find all RPMs that is not in LUSTRE_REQUIRED_RPM_TYPES or
        # LUSTRE_CLIENT_REQUIRED_RPM_TYPES
        for rpm_type in matched_version.lv_rpm_pattern_dict:
            if rpm_type in matched_rpm_type_dict:
                continue
            pattern = matched_version.lv_rpm_pattern_dict[rpm_type]
            for rpm_fname in rpm_fnames:
                match = re.search(pattern, rpm_fname)
                if match is None:
                    continue
                for tmp_rpm_type, tmp_rpm_fname in matched_rpm_type_dict.items():
                    if rpm_fname == tmp_rpm_fname:
                        log.cl_error("RPM [%s] can be matched to both type [%s] "
                                     "and [%s] of Lustre %sversion [%s]",
                                     rpm_fname,
                                     tmp_rpm_type,
                                     rpm_type,
                                     definition_message,
                                     matched_version.lv_version_name)
                        return None, None
                if rpm_type in matched_rpm_type_dict:
                    log.cl_error("both RPM [%s] and [%s] can match type [%s] "
                                 "of Lustre %sversion [%s]",
                                 rpm_fname,
                                 matched_rpm_type_dict[rpm_type],
                                 rpm_type,
                                 definition_message,
                                 matched_version.lv_version_name)
                    return None, None
                matched_rpm_type_dict[rpm_type] = rpm_fname
        return matched_version, matched_rpm_type_dict

    def lvd_match_version_from_deb(self, log, deb_version):
        """
        The version of deb package usually comes from command
        apt list --installed | grep lustre-client-modules
        or
        apt show lustre-client-modules-5.15.0-69-generic | grep Version
        Example:
        2.15.2-70-gb74560d-1
        """
        matched_versions = []
        for version in self.lvd_version_dict.values():
            deb_pattern = version.lv_deb_pattern
            if deb_pattern is None:
                continue
            match = re.search(deb_pattern, deb_version)
            if match is not None:
                continue
            matched_versions.append(version)

        max_priority = -1
        for matched_version in matched_versions:
            if matched_version.lv_priority > max_priority:
                max_priority = matched_version.lv_priority

        highest_versions = []
        version_names = []
        for matched_version in matched_versions:
            if matched_version.lv_priority == max_priority:
                highest_versions.append(matched_version)
                version_names.append(matched_version.lv_version_name)
        if len(highest_versions) > 1:
            log.cl_error("multiple matches of Lustre versions [%s] with deb [%s]",
                         utils.list2string(version_names),
                         deb_version)
            return None
        if len(highest_versions) == 0:
            log.cl_error("no match of Lustre version with deb [%s]",
                         deb_version)
            return None
        return highest_versions[0]


def load_lustre_version_definition(log, fpath):
    """
    Load the YAML file and return LustreVersion
    """
    # pylint: disable=too-many-branches
    try:
        with open(fpath, 'r', encoding='utf-8') as config_fd:
            data = yaml.load(config_fd, Loader=yaml.FullLoader)
    except:
        log.cl_error("failed to load file [%s] using YAML format: %s",
                     fpath, traceback.format_exc())
        return None

    if LVD_LUSTRE_VERSION not in data:
        log.cl_error("missing [%s] in defintion of Lustre version [%s]",
                     LVD_LUSTRE_VERSION, fpath)
        return None
    version_name = str(data[LVD_LUSTRE_VERSION])
    ret = cmd_general.check_name_is_valid(version_name)
    if ret:
        log.cl_error("invalid [%s] as [%s] in defintion of Lustre version [%s]",
                     version_name, LVD_LUSTRE_VERSION, fpath)
        return None

    if LVD_PRIORITY not in data:
        log.cl_error("missing [%s] in defintion of Lustre version [%s]",
                     LVD_PRIORITY, fpath)
        return None
    priority_str = data[LVD_PRIORITY]
    try:
        priority = int(priority_str)
    except:
        log.cl_error("invalid [%s] of [%s] in defintion of Lustre version [%s]",
                     priority_str, LVD_PRIORITY, fpath)
        return None
    if priority < 0:
        log.cl_error("negative [%s] of [%s] in defintion of Lustre version [%s]",
                     priority_str, LVD_PRIORITY, fpath)
        return None

    if LVD_RPM_PATTERNS not in data:
        log.cl_error("missing [%s] in defintion of Lustre version [%s]",
                     LVD_RPM_PATTERNS, fpath)
        return None
    rpm_patterns = data[LVD_RPM_PATTERNS]
    if not isinstance(rpm_patterns, dict):
        log.cl_error("invalid [%s] in defintion of Lustre version [%s]",
                     rpm_patterns, fpath)
        return None

    for required_rpm in LUSTRE_REQUIRED_RPM_TYPES:
        if required_rpm not in rpm_patterns:
            log.cl_error("missing RPM pattern for [%s] in Lustre version [%s]",
                         required_rpm, fpath)
            return None

    if LVD_DEB_PATTERN not in data:
        log.cl_debug("missing [%s] in defintion of Lustre version [%s]",
                     LVD_DEB_PATTERN, fpath)
        deb_version = None
    else:
        deb_version = data[LVD_DEB_PATTERN]

    if LVD_COLLECTD_DEFINITION_FILE not in data:
        log.cl_error("missing [%s] in Lustre version [%s]",
                     LVD_COLLECTD_DEFINITION_FILE, fpath)
        return None
    collectd_definition_file = data[LVD_COLLECTD_DEFINITION_FILE]

    version = LustreVersion(version_name, rpm_patterns, priority,
                            deb_version, collectd_definition_file)
    return version

def load_lustre_version_database(log, local_host, directory):
    """
    Load the database files under a directory and return LustreVersionDatabase
    """
    fnames = local_host.sh_get_dir_fnames(log, directory)
    if fnames is None:
        log.cl_error("failed to list dir [%s] on host [%s]",
                     directory, local_host.sh_hostname)
        return None

    database = LustreVersionDatabase()
    for fname in fnames:
        fpath = directory + "/" + fname
        version = load_lustre_version_definition(log, fpath)
        if version is None:
            log.cl_error("failed to load Lustre version from [%s]",
                         fpath)
            return None
        if version.lv_version_name in database.lvd_version_dict:
            log.cl_error("duplicated Lustre version [%s]",
                         version.lv_version_name)
            return None

        database.lvd_version_dict[version.lv_version_name] = version

    return database
