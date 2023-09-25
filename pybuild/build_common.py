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
from pybuild import build_constant


# Key is plugin name, value is CoralPluginType()
CORAL_PLUGIN_DICT = {}
# Key is plugin name, value is CoralPluginType()
CORAL_RELEASE_PLUGIN_DICT = {}
# Key is plugin name, value is CoralPluginType()
CORAL_DEVEL_PLUGIN_DICT = {}
# Lock to prevent parallel RPM check/install
RPM_INSTALL_LOCK = constant.CORAL_LOG_DIR + "/rpm_install"
# Key is the name of the package under ISO, value is CoralPackageBuild()
CORAL_PACKAGE_DICT = {}


class CoralFileDownload():
    """
    Download of packages
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, url, sha1sum, fname=None, no_check_certificate=False):
        # URL of the file
        self.cfd_url = url
        # Sha1sum of the file
        self.cfd_sha1sum = sha1sum
        # filename
        if fname is None:
            self.cfd_fname = os.path.basename(url)
        else:
            self.cfd_fname = fname
        # Whether need to ignore certificate when downloading
        self.cfd_no_check_certificate = no_check_certificate

    def cfd_download(self, log, host, dirpath):
        """
        Download file to dir
        """
        fpath = dirpath + "/" + self.cfd_fname
        ret = host.sh_download_file(log, self.cfd_url, fpath,
                                    self.cfd_sha1sum,
                                    no_check_certificate=self.cfd_no_check_certificate)
        if ret:
            log.cl_error("failed to download Nettle tarball")
            return -1
        return 0


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
    Each Coral plugin has this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, plugin_name,
                 build_dependent_pips=None, is_devel=True,
                 need_lustre_rpms=False, need_collectd=False,
                 install_lustre=False,
                 packages=None,
                 plugins=None):
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
        # Package names that this plugin depend on
        if packages is None:
            self.cpt_packages = []
        else:
            self.cpt_packages = packages
        # Plugin names this plugin depend on
        if plugins is None:
            self.cpt_plugins = []
        else:
            self.cpt_plugins = plugins

    def cpt_build_dependent_packages(self, distro):
        """
        Return the RPMs needed to install before building.
        Return None on failure.
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
                  extra_package_fnames, extra_package_names, option_dict):
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


class CoralPackageBuild():
    """
    Each package has this build type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, package_name, depend_package_names=None):
        # The name of the package
        self.cpb_package_name = package_name
        # The packages to build before building this package. This is
        # useful when need to install other newly buildt packages.
        self.cpb_depend_package_names = depend_package_names

    def cpb_build_dependent_packages(self, distro):
        """
        Return the RPMs needed to install before building
        """
        # pylint: disable=unused-argument,no-self-use
        return []

    def cpb_build(self, log, workspace, local_host, source_dir, target_cpu,
                  type_cache, iso_cache, packages_dir, extra_iso_fnames,
                  extra_package_fnames, extra_rpm_names, option_dict):
        """
        Build the package
        """
        # pylint: disable=unused-argument,no-self-use
        return 0


def coral_package_register(package):
    """
    Register a new package
    """
    package_name = package.cpb_package_name
    if package_name in CORAL_PACKAGE_DICT:
        logging.error("package [%s] has already been registered",
                      package_name)
        sys.exit(1)
    CORAL_PACKAGE_DICT[package_name] = package


def install_pip3_package_from_file(log, host, type_cache, tarball_url,
                                   expected_sha1sum, tarball_fname=None, tsinghua_mirror=False):
    """
    Install pip3 package and cache it for future usage
    """
    if tarball_fname is None:
        tarball_fname = os.path.basename(tarball_url)
    tarball_fpath = type_cache + "/" + tarball_fname

    ret = host.sh_download_file(log, tarball_url, tarball_fpath,
                                expected_sha1sum,
                                output_fname=tarball_fname)
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
    # pylint: disable=abstract-class-instantiated
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



def get_shared_build_cache_locked(log, host, workspace,
                                  shared_cache):
    """
    Get the shared build cache
    """
    command = ("mkdir -p %s && cp -a %s %s" %
               (shared_cache, shared_cache, workspace))
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


def get_shared_build_cache(log, host, workspace, shared_cache):
    """
    Get the shared build cache
    """
    # pylint: disable=abstract-class-instantiated
    log.cl_info("copying shared cache from [%s] to [%s] on host [%s]",
                shared_cache, workspace, host.sh_hostname)
    lock_file = build_constant.CORAL_BUILD_CACHE_LOCK
    lock = filelock.FileLock(lock_file)
    ret = 0
    try:
        with lock.acquire(timeout=600):
            ret = get_shared_build_cache_locked(log, host, workspace,
                                                shared_cache)
    except filelock.Timeout:
        ret = -1
        log.cl_error("someone else is holding lock of file [%s] for more "
                     "than 10 minutes, aborting",
                     lock_file)
    return ret


def apply_patches(log, host, target_source_dir, patch_dir):
    """
    Apply a series of patches. The patches shall be in patch_dir. And they
    shall have sorted file names like:
    0000-fname.patch
    0001-fname.patch
    ...
    """
    patch_fnames = host.sh_get_dir_fnames(log, patch_dir)
    if patch_fnames is None:
        log.cl_error("failed to get patches of Mpifileutils under dir [%s] "
                     "on host [%s]",
                     patch_dir, host.sh_hostname)
        return -1

    patch_fnames.sort()

    for patch_fname in patch_fnames:
        if not patch_fname.endswith(".patch"):
            continue
        patch_fpath = patch_dir + "/" + patch_fname
        command = "cd %s && patch -p1 < %s" % (target_source_dir, patch_fpath)
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
