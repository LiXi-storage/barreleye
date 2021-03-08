"""
Library for building Coral
"""
import os
import re

# pylint: disable=unused-import
# Local libs
from pycoral import ssh_host
from pycoral import constant
from pycoral import lustre as lustre_lib
from pycoral import install_common
from pycoral import cmd_general
from pybuild import build_constant
from pybuild import build_barrele
from pybuild import build_common
from pybuild import build_version
import filelock

# The url of pyinstaller tarball. Need to update together with
# PYINSTALLER_TARBALL_SHA1SUM
PYINSTALLER_TARBALL_URL = "https://github.com/pyinstaller/pyinstaller/releases/download/v4.2/PyInstaller-4.2.tar.gz"
# The sha1sum of pyinstaller tarball. Need to update together with
# PYINSTALLER_TARBALL_URL
PYINSTALLER_TARBALL_SHA1SUM = "bac8d46737876468d7be607a44b90debd60422b5"


def merge_list(list_x, list_y):
    """
    Merge two list and remove the duplicated items
    """
    merged_list = []
    for item in list_x:
        if item in merged_list:
            continue
        merged_list.append(item)
    for item in list_y:
        if item in merged_list:
            continue
        merged_list.append(item)
    return merged_list


def download_dependent_rpms(log, host, packages_dir,
                            filter_rpm_fnames, extra_rpm_names):
    """
    Download dependent RPMs
    """
    # pylint: disable=too-many-locals,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    # The yumdb might be broken, so sync
    log.cl_info("downloading dependency RPMs")
    command = "yumdb sync"
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

    existing_rpm_fnames = host.sh_get_dir_fnames(log, packages_dir)
    if existing_rpm_fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                     packages_dir, host.sh_hostname)
        return -1

    dependent_rpms = merge_list(constant.CORAL_DEPENDENT_RPMS,
                                extra_rpm_names)

    target_cpu = host.sh_target_cpu(log)
    if target_cpu is None:
        log.cl_error("failed to get target cpu on host [%s]",
                     host.sh_hostname)
        return -1

    command = "repotrack -a %s -p %s" % (target_cpu, packages_dir)
    for rpm_name in dependent_rpms:
        command += " " + rpm_name

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

    exist_pattern = (r"^%s/(?P<rpm_fname>\S+) already exists and appears to be "
                     "complete$" % (packages_dir))
    exist_regular = re.compile(exist_pattern)
    download_pattern = (r"^Downloading (?P<rpm_fname>\S+)$")
    download_regular = re.compile(download_pattern)
    lines = retval.cr_stdout.splitlines()
    for line in lines:
        match = exist_regular.match(line)
        if match:
            rpm_fname = match.group("rpm_fname")
        else:
            match = download_regular.match(line)
            if match:
                rpm_fname = match.group("rpm_fname")
            else:
                log.cl_error("unknown output [%s] of repotrack on host "
                             "[%s], stdout = [%s]",
                             line, host.sh_hostname, retval.cr_stdout)
                return -1
        if rpm_fname in existing_rpm_fnames:
            existing_rpm_fnames.remove(rpm_fname)

    # RPMs saved in the other downloading steps
    for rpm_fname in filter_rpm_fnames:
        if rpm_fname in existing_rpm_fnames:
            existing_rpm_fnames.remove(rpm_fname)

    for fname in existing_rpm_fnames:
        fpath = packages_dir + "/" + fname
        log.cl_debug("found unnecessary file [%s] under directory [%s], "
                     "removing it", fname, packages_dir)
        ret = host.sh_remove_file(log, fpath)
        if ret:
            return -1

    # Check the wanted RPMs are all downloaded
    new_rpm_fnames = host.sh_get_dir_fnames(log, packages_dir)
    if new_rpm_fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                     packages_dir, host.sh_hostname)
        return -1
    for rpm_name in dependent_rpms:
        rpm_pattern = (r"^%s.*\.rpm$" % rpm_name)
        rpm_regular = re.compile(rpm_pattern)
        match = False
        for new_rpm_fname in new_rpm_fnames:
            match = rpm_regular.match(new_rpm_fname)
            if match:
                break
        if not match:
            log.cl_error("RPM [%s] is needed but not downloaded in directory [%s]",
                         rpm_name, packages_dir)
            return -1
    return 0


def install_pyinstaller(log, host, type_cache):
    """
    Install pyinstaller
    """
    log.cl_info("installing pyinstaller")
    if host.sh_has_command(log, "pyinstaller"):
        return 0

    tarball_url = PYINSTALLER_TARBALL_URL
    expected_sha1sum = PYINSTALLER_TARBALL_SHA1SUM
    ret = build_common.install_pip3_package_from_file(log, host, type_cache,
                                                      tarball_url,
                                                      expected_sha1sum)
    if ret:
        log.cl_error("failed to install pip package of pyinstaller")
        return -1
    return 0


def install_dependency(log, workspace, host, target_cpu, type_cache, plugins,
                       pip_dir, origin_mirror=False):
    """
    Install the dependency of building Coral
    """
    # pylint: disable=too-many-arguments
    if not origin_mirror:
        # "epel-release" should have been installed
        command = """sed -e 's!^metalink=!#metalink=!g' \
-e 's!^#baseurl=!baseurl=!g' \
-e 's!//download\.fedoraproject\.org/pub!//mirrors.tuna.tsinghua.edu.cn!g' \
-e 's!http://mirrors\.tuna!https://mirrors.tuna!g' \
-i /etc/yum.repos.d/epel.repo /etc/yum.repos.d/epel-testing.repo"""
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

    command = 'yum -y groupinstall "Development Tools"'
    log.cl_info("running command [%s] on host [%s]",
                command, host.sh_hostname)
    retval = host.sh_watched_run(log, command, None, None,
                                 return_stdout=False,
                                 return_stderr=False)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s]",
                     command, host.sh_hostname)
        return -1

    dependent_rpms = ["createrepo",  # To create the repo in ISO
                      "e2fsprogs-devel",  # Needed for ./configure
                      "genisoimage",  # Generate the ISO image
                      "git",  # Needed by building anything from Git repository.
                      "libtool-ltdl-devel",  # Otherwise, `COPYING.LIB' not found
                      "python36-pylint",  # Needed for Python codes check
                      "python-pep8",  # Needed for Python codes check
                      "python36-psutil",  # Used by Python codes
                      "redhat-lsb-core",  # Needed by detect-distro.sh for lsb_release
                      "wget"]  # Needed by downloading from web
    dependent_pips = []
    for plugin in plugins:
        dependent_rpms += plugin.cpt_build_dependent_rpms

    # We need all Python depdendency for all plugins since no matter they
    # are needed or not, the Python codes will be checked.
    for plugin in build_common.CORAL_RELEASE_PLUGIN_DICT.values():
        dependent_pips += plugin.cpt_build_dependent_pips

    ret = install_common.bootstrap_from_internet(log, host, dependent_rpms,
                                                 dependent_pips,
                                                 pip_dir=pip_dir)
    if ret:
        log.cl_error("failed to install missing packages on host [%s]",
                     host.sh_hostname)
        return -1

    for plugin in plugins:
        ret = plugin.cpt_install_build_dependency(log, workspace, host,
                                                  target_cpu, type_cache)
        if ret:
            log.cl_error("failed to install dependency for plugin [%s]",
                         plugin.cpt_plugin_name)
            return -1

    ret = install_pyinstaller(log, host, type_cache)
    if ret:
        log.cl_error("failed to install pyinstaller on host [%s]",
                     host.sh_hostname)
        return -1
    return 0


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
    log.cl_info("copying shared cache from [%s] to [%s] on host [%s]",
                shared_cache, workspace, host.sh_hostname)
    lock_file = build_constant.CORAL_BUILD_CACHE_LOCK
    lock = filelock.FileLock(lock_file)
    ret = 0
    try:
        with lock.acquire(timeout=600):
            ret = get_shared_build_cache_locked(log, host, workspace,
                                                shared_cache)
            lock.release()
    except filelock.Timeout:
        ret = -1
        log.cl_error("someone else is holding lock of file [%s] for more "
                     "than 10 minutes, aborting",
                     lock_file)
    return ret


def sync_shared_build_cache(log, host, private_cache, shared_parent):
    """
    Sync from the local cache to shared cache
    """
    log.cl_info("syncing [%s] to shared cache [%s]", private_cache,
                shared_parent)
    lock_file = build_constant.CORAL_BUILD_CACHE_LOCK
    lock = filelock.FileLock(lock_file)
    ret = 0
    try:
        with lock.acquire(timeout=600):
            ret = host.sh_sync_two_dirs(log, private_cache, shared_parent)
            lock.release()
    except filelock.Timeout:
        ret = -1
        log.cl_error("someone else is holding lock of file [%s] for more "
                     "than 10 minutes, aborting",
                     lock_file)

    return ret


def build(log, cache=constant.CORAL_BUILD_CACHE,
          lustre_rpms_dir=None,
          e2fsprogs_rpms_dir=None,
          collectd=None,
          enable_zfs=False,
          enable_devel=False,
          disable_plugin=None,
          origin_mirror=False):
    """
    Build the Coral ISO.
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    # pylint: disable=too-many-statements
    if disable_plugin is None:
        disabled_plugins = []
    else:
        disabled_plugins = cmd_general.parse_list_string(log, disable_plugin)
        if disabled_plugins is None:
            log.cl_error("invalid option [%s] of --disable_plugin",
                         disable_plugin)
            return -1

    plugins = list(build_common.CORAL_RELEASE_PLUGIN_DICT.values())
    if enable_devel:
        plugins += list(build_common.CORAL_DEVEL_PLUGIN_DICT.values())

    sync_cache_back = True
    disable_plugins_str = ""
    for plugin_name in disabled_plugins:
        if plugin_name not in build_common.CORAL_PLUGIN_DICT:
            log.cl_error("unknown plugin [%s] of --disable_plugin",
                         plugin_name)
            log.cl_error("possible plugins are %s",
                         list(build_common.CORAL_PLUGIN_DICT.keys()))
            return -1

        if ((not enable_devel) and
                (plugin_name not in build_common.CORAL_RELEASE_PLUGIN_DICT)):
            log.cl_info("plugin [%s] will not be included in release "
                        "ISO anyway", plugin_name)
            continue

        sync_cache_back = False
        plugin = build_common.CORAL_PLUGIN_DICT[plugin_name]
        if plugin in plugins:
            plugins.remove(plugin)
        disable_plugins_str += " --disable-%s" % plugin_name

    if len(plugins) == 0:
        log.cl_error("everything has been disabled, nothing to build")
        return -1

    need_lustre_rpms = False
    need_collectd = False
    enabled_plugin_str = ""
    for plugin in plugins:
        if enabled_plugin_str == "":
            enabled_plugin_str = plugin.cpt_plugin_name
        else:
            enabled_plugin_str += ", " + plugin.cpt_plugin_name
        if plugin.cpt_need_lustre_rpms:
            need_lustre_rpms = True
        if plugin.cpt_need_collectd:
            need_collectd = True

    type_fname = constant.CORAL_BUILD_CACHE_TYPE_OPEN
    if type_fname == constant.CORAL_BUILD_CACHE_TYPE_OPEN:
        log.cl_info("building ISO with %s", enabled_plugin_str)

    local_host = ssh_host.get_local_host(ssh=False)
    distro = local_host.sh_distro(log)
    if distro != ssh_host.DISTRO_RHEL7:
        log.cl_error("build on distro [%s] is not supported yet, only "
                     "support RHEL7/CentOS7", distro)
        return -1

    shared_cache = cache.rstrip("/")
    # Shared cache for this build type
    shared_type_cache = shared_cache + "/" + type_fname
    # Extra RPMs to download
    extra_rpm_names = []
    # Extra RPM file names under package directory
    extra_package_fnames = []
    # Extra file names under ISO directory
    extra_iso_fnames = []

    enable_zfs_string = ""
    if enable_zfs:
        enable_zfs_string = ", ZFS support disabled"

    source_dir = os.getcwd()
    workspace = source_dir + "/" + build_common.get_build_path()
    type_cache = workspace + "/" + type_fname
    build_pip_dir = type_cache + "/" + constant.BUILD_PIP
    iso_cache = type_cache + "/" + constant.ISO_CACHE_FNAME
    # Directory path of package under ISO cache
    packages_dir = iso_cache + "/" + constant.BUILD_PACKAGES
    default_lustre_rpms_dir = (iso_cache + "/" +
                               constant.LUSTRE_RPM_DIR_BASENAME)
    default_e2fsprogs_rpms_dir = (iso_cache + "/" +
                                  constant.E2FSPROGS_RPM_DIR_BASENAME)

    if not need_collectd:
        if collectd is not None:
            log.cl_warning("option [--collectd %s] has been ignored since "
                           "no need to have Collectd RPMs",
                           collectd)
    elif collectd is not None:
        sync_cache_back = False

    if not need_lustre_rpms:
        if lustre_rpms_dir is not None:
            log.cl_warning("option [--lustre %s] has been ignored since "
                           "no need to have Lustre RPMs",
                           lustre_rpms_dir)
        if e2fsprogs_rpms_dir is not None:
            log.cl_warning("option [--e2fsprogs %s] has been ignored since "
                           "no need to have Lustre RPMs",
                           e2fsprogs_rpms_dir)
    else:
        if lustre_rpms_dir is None:
            lustre_rpms_dir = default_lustre_rpms_dir
        if e2fsprogs_rpms_dir is None:
            e2fsprogs_rpms_dir = default_e2fsprogs_rpms_dir

    command = ("mkdir -p %s" % workspace)
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    ret = get_shared_build_cache(log, local_host, workspace,
                                 shared_type_cache)
    if ret:
        log.cl_error("failed to get shared build cache")
        return -1

    target_cpu = local_host.sh_target_cpu(log)
    if target_cpu is None:
        log.cl_error("failed to get the target cpu on host [%s]",
                     local_host.sh_hostname)
        return -1

    ret = install_dependency(log, workspace, local_host, target_cpu,
                             type_cache, plugins, build_pip_dir,
                             origin_mirror=origin_mirror)
    if ret:
        log.cl_error("failed to install dependency for building")
        return -1

    command = ("mkdir -p %s" % (packages_dir))
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    for plugin in plugins:
        ret = plugin.cpt_build(log, workspace, local_host, source_dir,
                               target_cpu, type_cache, iso_cache,
                               packages_dir, extra_iso_fnames,
                               extra_package_fnames, extra_rpm_names,
                               collectd)
        if ret:
            log.cl_error("failed to build plugin [%s]",
                         plugin.cpt_plugin_name)
            return -1

    ret = download_dependent_rpms(log, local_host, packages_dir,
                                  extra_package_fnames,
                                  extra_rpm_names)
    if ret:
        log.cl_error("failed to download dependent rpms")
        return -1

    pip_dir = iso_cache + "/" + constant.BUILD_PIP
    ret = install_common.download_pip3_packages(log, local_host, pip_dir,
                                                constant.CORAL_DEPENDENT_PIPS)
    if ret:
        log.cl_error("failed to download pip3 packages")
        return -1

    if need_lustre_rpms:
        lustre_distribution = lustre_lib.get_lustre_dist(log, local_host,
                                                         lustre_rpms_dir,
                                                         e2fsprogs_rpms_dir)
        if lustre_distribution is None:
            log.cl_error("invalid Lustre RPMs [%s] or e2fsprogs RPMs [%s]",
                         lustre_rpms_dir, e2fsprogs_rpms_dir)
            return -1

        if lustre_rpms_dir != default_lustre_rpms_dir:
            ret = local_host.sh_sync_two_dirs(log, lustre_rpms_dir, iso_cache)
            if ret:
                log.cl_error("failed to sync [%s] to a subdir under dir [%s] "
                             "on host [%s]", lustre_rpms_dir, iso_cache,
                             local_host.sh_hostname)
                return -1

        if e2fsprogs_rpms_dir != default_e2fsprogs_rpms_dir:
            ret = local_host.sh_sync_two_dirs(log, e2fsprogs_rpms_dir,
                                              iso_cache)
            if ret:
                log.cl_error("failed to sync [%s] to a subdir under dir [%s] "
                             "on host [%s]", e2fsprogs_rpms_dir, iso_cache,
                             local_host.sh_hostname)
                return -1

        extra_iso_fnames.append(constant.LUSTRE_RPM_DIR_BASENAME)
        extra_iso_fnames.append(constant.E2FSPROGS_RPM_DIR_BASENAME)

    contents = ([constant.BUILD_PACKAGES, constant.BUILD_PIP] +
                extra_iso_fnames)
    ret = local_host.sh_check_dir_content(log, iso_cache, contents,
                                          cleanup=True)
    if ret:
        log.cl_error("directory [%s] does not have expected content",
                     iso_cache)
        return -1

    log.cl_info("generating Coral ISO")

    enable_zfs_string = ""
    if enable_zfs:
        enable_zfs_string = " --enable-zfs"
    enable_devel_string = ""
    if enable_devel:
        enable_devel_string = " --enable-devel"
    command = ("cd %s && rm coral-*.tar.bz2 coral-*.tar.gz -f && "
               "sh autogen.sh && "
               "./configure --with-iso-cache=%s%s%s%s && "
               "make -j8 && "
               "make iso" %
               (source_dir, iso_cache, enable_zfs_string, enable_devel_string,
                disable_plugins_str))
    retval = local_host.sh_watched_run(log, command, None, None,
                                       return_stdout=False,
                                       return_stderr=False)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s]",
                     command,
                     local_host.sh_hostname)
        return -1

    # If there is any plugin disabled or Collectd is special, the local cache
    # might have some things missing thus should not be used by other build.
    if sync_cache_back:
        ret = sync_shared_build_cache(log, local_host, type_cache,
                                      shared_cache)
        if ret:
            log.cl_error("failed to sync to shared build cache")
            return -1

    log.cl_info("Built Coral ISO successfully")
    return 0
