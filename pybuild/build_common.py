"""
Common library for building
"""
import os
import logging
import sys
import filelock
from pycoral import constant
from pycoral import utils
from pycoral import time_util


# Key is plugin name, value is CoralPluginType()
CORAL_PLUGIN_DICT = {}
# Key is plugin name, value is CoralPluginType()
CORAL_RELEASE_PLUGIN_DICT = {}
# Key is plugin name, value is CoralPluginType()
CORAL_DEVEL_PLUGIN_DICT = {}
# Lock to prevent parallel RPM check/install
RPM_INSTALL_LOCK = constant.CORAL_LOG_DIR + "/rpm_install"


class CoralCommand():
    """
    The command line utility for building Coral.
    :param debug: Whether to dump debug logs into files, default: False
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, debug=False):
        # pylint: disable=protected-access
        self._cc_log_to_file = debug
        for attr_name in dir(self):
            # My own attrs, ignore.
            if attr_name.startswith("_"):
                continue
            attr = getattr(self, attr_name)
            # Registered commands, do not need to init
            if callable(attr):
                continue
            attr._init(debug)


def coral_command_register(command_name, obj):
    """
    Register a new subcommand to coral command
    """
    if hasattr(CoralCommand, command_name):
        logging.error("command [%s] has already been registered",
                      command_name)
        sys.exit(1)
    setattr(CoralCommand, command_name, obj)


class CoralPluginType():
    """
    Each resource has this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, plugin_name,
                 build_dependent_pips=None, is_devel=True,
                 need_lustre_rpms=False, need_collectd=False,
                 install_lustre=False):
        # The name of the plugin
        self.cpt_plugin_name = plugin_name
        # Whether the plugin is only for devel
        self.cpt_is_devel = is_devel
        # Whether Lustre/E2fsprogs RPMs are needed in the ISO
        self.cpt_need_lustre_rpms = need_lustre_rpms
        # Whether Collectd RPMs are needed
        self.cpt_need_collectd = need_collectd
        if build_dependent_pips is None:
            build_dependent_pips = []
        # The pip packages to install before building
        self.cpt_build_dependent_pips = build_dependent_pips
        # Whether install Lustre library RPM for build
        self.cpt_install_lustre = install_lustre

    def cpt_build_dependent_rpms(self, log, distro):
        """
        Return the RPMs needed to install before building
        """
        # pylint: disable=unused-argument,no-self-use
        return []

    def cpt_install_build_dependency(self, log, workspace, host,
                                     target_cpu, type_cache):
        """
        Install dependency before build
        """
        # pylint: disable=unused-argument,no-self-use
        return 0

    def cpt_build(self, log, workspace, local_host, source_dir, target_cpu,
                  type_cache, iso_cache, packages_dir, extra_iso_fnames,
                  extra_package_fnames, extra_rpm_names, collectd):
        """
        Build the plugin
        """
        # pylint: disable=unused-argument,no-self-use
        return 0


def coral_plugin_register(plugin):
    """
    Register a new plugin
    """
    plugin_name = plugin.cpt_plugin_name
    if plugin_name in CORAL_PLUGIN_DICT:
        logging.error("plugin [%s] has already been registered",
                      plugin_name)
        sys.exit(1)
    if plugin.cpt_is_devel:
        CORAL_DEVEL_PLUGIN_DICT[plugin_name] = plugin
    else:
        CORAL_RELEASE_PLUGIN_DICT[plugin_name] = plugin
    CORAL_PLUGIN_DICT[plugin_name] = plugin


def install_pip3_package_from_file(log, host, type_cache, tarball_url,
                                   expected_sha1sum, tsinghua_mirror=False):
    """
    Install pip3 package and cache it for future usage
    """
    tarball_fname = os.path.basename(tarball_url)
    tarball_fpath = type_cache + "/" + tarball_fname

    ret = host.sh_download_file(log, tarball_url, tarball_fpath,
                                expected_sha1sum)
    if ret:
        log.cl_error("failed to download Pyinstaller")
        return -1

    command = "pip3 install %s" % (tarball_fpath)
    if tsinghua_mirror:
        command += " -i https://pypi.tuna.tsinghua.edu.cn/simple"
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def reinstall_rpm(log, host, packages_dir, rpm_name_prefix, rpm_fname):
    """
    Reinstall RPM
    """
    rpm_name = rpm_fname[:-4]
    ret = host.sh_rpm_query(log, rpm_name)
    if ret == 0:
        log.cl_debug("%s is already installed on host [%s], skipping",
                     rpm_name, host.sh_hostname)
        return 0

    ret = host.sh_rpm_find_and_uninstall(log, "grep %s" % rpm_name_prefix)
    if ret:
        log.cl_error("failed to uninstall RPM [%s] on host [%s]",
                     rpm_name_prefix, host.sh_hostname)
        return -1

    command = "rpm -ivh %s/%s" % (packages_dir, rpm_fname)
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return 0


def install_generated_rpm(log, host, packages_dir, extra_package_fnames,
                          rpm_name_prefix):
    """
    Install the RPM if not installed
    """
    rpm_fname = None
    for fname in extra_package_fnames:
        if fname.startswith(rpm_name_prefix):
            rpm_fname = fname
            break
    if rpm_fname is None:
        log.cl_error("no [%s] RPM generated under dir [%s] of host [%s]",
                     rpm_name_prefix, packages_dir, host.sh_hostname)
        return -1

    lock_file = RPM_INSTALL_LOCK + "-" + rpm_name_prefix + ".lock"
    lock = filelock.FileLock(lock_file)
    ret = 0
    # Need to avoid confict of "rpm -ivh":
    # package %s is already installed
    try:
        with lock.acquire(timeout=600):
            ret = reinstall_rpm(log, host, packages_dir, rpm_name_prefix,
                                rpm_fname)
            lock.release()
    except filelock.Timeout:
        ret = -1
        log.cl_error("someone else is holding lock of file [%s] for more "
                     "than 10 minutes, aborting",
                     lock_file)
    if ret:
        log.cl_error("failed to reinstall RPM [%s] on host [%s]",
                     rpm_name_prefix, host.sh_hostname)
        return -1
    return 0


def packages_check_rpms(log, host, packages_dir, rpm_fnames):
    """
    Check whether RPMs exists and their integrity
    """
    for rpm_fname in rpm_fnames:
        rpm_fpath = packages_dir + "/" + rpm_fname
        ret = host.sh_path_exists(log, rpm_fpath)
        if ret < 0 or ret == 0:
            log.cl_info("RPM [%s] does not exist, need to build",
                        rpm_fpath)
            return -1

        ret = host.sh_rpm_checksig(log, rpm_fpath)
        if ret:
            log.cl_info("RPM [%s] is broken, need to rebuild",
                        rpm_fpath)
            return -1
    return 0


def packages_add_rpms(log, host, rpm_dir, packages_dir, expected_rpm_fnames):
    """
    Copy the generated RPMs to the package dir
    """
    fnames = host.sh_get_dir_fnames(log, rpm_dir)
    if fnames is None:
        log.cl_error("failed to get the fnames under dir [%s] on host [%s]",
                     rpm_dir, host.sh_hostname)
        return -1

    for expected_fname in expected_rpm_fnames:
        if expected_fname not in fnames:
            log.cl_error("failed to find expected RPM [%s] under dir [%s] "
                         "on host [%s]",
                         expected_fname, rpm_dir, host.sh_hostname)
            return -1

    for fname in fnames:
        if fname not in expected_rpm_fnames:
            log.cl_error("extra RPM [%s] is generated under dir [%s] "
                         "on host [%s]",
                         fname, rpm_dir, host.sh_hostname)
            return -1

        package_fpath = "%s/%s" % (packages_dir, fname)
        command = "rm -f %s" % package_fpath
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        generated_fpath = "%s/%s" % (rpm_dir, fname)
        command = ("/usr/bin/cp -f %s %s" %
                   (generated_fpath, packages_dir))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
    return 0


def get_build_path():
    """
    Return the random build path
    """
    return ("coral_build_" +
            time_util.local_strftime(time_util.utcnow(),
                                     "%Y-%m-%d-%H_%M_%S-") +
            utils.random_word(8))
