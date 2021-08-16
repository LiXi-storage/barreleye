"""
Library for building Coral
"""
import os
import re
import filelock
# pylint: disable=unused-import
# Local libs
from pycoral import ssh_host
from pycoral import constant
from pycoral import lustre_version
from pycoral import install_common
from pycoral import cmd_general
from pybuild import build_constant
from pybuild import build_barrele
from pybuild import build_common
from pybuild import build_version
from pybuild import build_doc

# The url of pyinstaller tarball. Need to update together with
# PYINSTALLER_TARBALL_SHA1SUM
PYINSTALLER_TARBALL_URL = "https://github.com/pyinstaller/pyinstaller/releases/download/v4.3/pyinstaller-4.3.tar.gz"
# The sha1sum of pyinstaller tarball. Need to update together with
# PYINSTALLER_TARBALL_URL
PYINSTALLER_TARBALL_SHA1SUM = "972f24ef11cf69875daa2ebd4804c5f505c0fec8"
# The url of pdsh tarball. Need to update together with
# PDSH_TARBALL_SHA1SUM
PDSH_TARBALL_URL = "https://github.com/chaos/pdsh/releases/download/pdsh-2.34/pdsh-2.34.tar.gz"
# The sha1sum of pdsh tarball. Need to update together with
# PDSH_TARBALL_URL
PDSH_TARBALL_SHA1SUM = "c7bdd20c5ba211b0ae80339c671c602d6a3a5a66"


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


def download_dependent_rpms_rhel7(log, host, target_cpu, packages_dir,
                                  dependent_rpms, extra_package_fnames):
    """
    Download dependent RPMs for RHEL7
    """
    # pylint: disable=too-many-locals
    command = "repotrack -a %s -p %s" % (target_cpu, packages_dir)
    for rpm_name in dependent_rpms:
        command += " " + rpm_name

    log.cl_info("running command [%s] on host [%s]", command, host.sh_hostname)
    retval = host.sh_watched_run(log, command, None, None,
                                 return_stdout=True,
                                 return_stderr=False)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], ret = [%d]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status)
        return -1

    exist_pattern = (r"^%s/(?P<rpm_fname>\S+) already exists and appears to be "
                     "complete$" % (packages_dir))
    download_pattern = (r"^Downloading (?P<rpm_fname>\S+)$")
    exist_regular = re.compile(exist_pattern)
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
                log.cl_error("unknown stdout line [%s] of command [%s] on host "
                             "[%s], stdout = [%s]",
                             line, host.sh_hostname, command,
                             retval.cr_stdout)
                return -1
        extra_package_fnames.append(rpm_fname)
    return 0


def build_pdsh(log, workspace, host, target_cpu, type_cache,
               packages_dir, extra_package_fnames):
    """
    Build pdsh since RHEL8 does not have pdsh in EPEL.

    Building process is quick, so no need to cache the RPMs.
    """
    # pylint: disable=too-many-locals
    tarball_url = PDSH_TARBALL_URL
    tarball_fname = os.path.basename(tarball_url)
    package_dirname = tarball_fname[:-7]
    tarball_fpath = type_cache + "/" + tarball_fname
    src_dir = workspace + "/" + package_dirname
    ret = host.sh_download_file(log, tarball_url, tarball_fpath,
                                PDSH_TARBALL_SHA1SUM)
    if ret:
        log.cl_error("failed to download PDSH sourcecode tarball")
        return -1

    # pdsh-2.34.tar.gz has two problems:
    #
    # 1. File README.QsNet is missing.
    # 2. The file name should be rebaned to pdsh-2.34-1.tar.gz from
    #    pdsh-2.34.tar.gz. Otherwise rpmbuild will fail.
    #
    # Not sure whether other versions have the same problem.
    spec_file = src_dir + "/pdsh.spec"
    build_tarball_fpath = src_dir + "-1.tar.gz"
    build_dir = workspace + "/pdsh_build"
    cmds = ["rm -f %s/pdsh-*" % packages_dir,
            "cd %s && tar xzf %s" % (workspace, tarball_fpath),
            "sed -i 's/ README.QsNet//g' %s" % spec_file,
            "cd %s && tar czf %s %s" %
            (workspace, build_tarball_fpath, package_dirname),
            "mkdir -p %s" % build_dir,
            'rpmbuild -ta %s --define="_topdir %s"' %
            (build_tarball_fpath, build_dir)]
    for command in cmds:
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

    rpm_dir = build_dir + "/RPMS/" + target_cpu
    rpm_fnames = host.sh_get_dir_fnames(log, rpm_dir)
    if rpm_fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                     rpm_dir,
                     host.sh_hostname)
        return -1

    for rpm_fname in rpm_fnames:
        rpm_fpath = rpm_dir + "/" + rpm_fname
        command = "cp -a %s %s" % (rpm_fpath, packages_dir)
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
    extra_package_fnames += rpm_fnames

    return 0


def download_dependent_rpms_rhel8(log, workspace, host, target_cpu,
                                  packages_dir, type_cache, dependent_rpms,
                                  extra_package_fnames):
    """
    Download dependent RPMs for RHEL8
    """
    # pylint: disable=too-many-locals
    ret = build_pdsh(log, workspace, host, target_cpu, type_cache,
                     packages_dir, extra_package_fnames)
    if ret:
        log.cl_error("failed to build PDSH")
        return -1

    command = ("dnf download --resolve --alldeps --destdir %s" %
               (packages_dir))

    for rpm_name in dependent_rpms:
        if rpm_name == "pdsh":
            continue
        command += " " + rpm_name

    log.cl_info("running command [%s] on host [%s]", command, host.sh_hostname)
    retval = host.sh_watched_run(log, command, None, None,
                                 return_stdout=False,
                                 return_stderr=False)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], ret = [%d]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status)
        return -1

    # Run twice. The first time might download some RPMs, and the
    # output looks like:
    #
    # (256/400): net-snmp-agent-libs-5.8-18.el8_3.1.x 2.6 MB/s | 747 kB     00:00
    #
    # As we can see part of the file name is omitted.
    #
    # In the second time, all the packages are already downloaded, and the output
    # looks like:
    #
    # [SKIPPED] net-snmp-libs-5.8-18.el8_3.1.x86_64.rpm: Already downloaded
    #
    # It always has full file name.
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

    exist_pattern = r"^\[SKIPPED\] (?P<rpm_fname>\S+): Already downloaded"
    exist_regular = re.compile(exist_pattern)
    lines = retval.cr_stdout.splitlines()
    if len(lines) == 0:
        log.cl_error("no line of command [%s] on host [%s], stdout = [%s]",
                     host.sh_hostname, command,
                     retval.cr_stdout)
        return -1
    first_line = lines[0]
    expected_prefix = "Last metadata expiration check:"
    if not first_line.startswith(expected_prefix):
        log.cl_error("unexpected first line [%s] of command [%s] on host "
                     "[%s], stdout = [%s], expected prefix [%s]",
                     first_line, host.sh_hostname, command,
                     retval.cr_stdout,
                     expected_prefix)
        return -1
    lines = lines[1:]
    for line in lines:
        match = exist_regular.match(line)
        if match:
            rpm_fname = match.group("rpm_fname")
        else:
            log.cl_error("unknown stdout line [%s] of command [%s] on host "
                         "[%s], stdout = [%s]",
                         line, host.sh_hostname, command,
                         retval.cr_stdout)
            return -1
        extra_package_fnames.append(rpm_fname)
    return 0


def check_package_rpms(log, host, packages_dir, dependent_rpms,
                       extra_package_fnames):
    """
    Check the package dir has nessary RPMs and does not have any buggage.
    """
    # pylint: disable=too-many-locals
    existing_rpm_fnames = host.sh_get_dir_fnames(log, packages_dir)
    if existing_rpm_fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                     packages_dir, host.sh_hostname)
        return -1

    useless_rpm_fnames = existing_rpm_fnames[:]
    # RPMs saved in the building or downloading steps
    for rpm_fname in extra_package_fnames:
        if rpm_fname in useless_rpm_fnames:
            useless_rpm_fnames.remove(rpm_fname)

    for fname in useless_rpm_fnames:
        fpath = packages_dir + "/" + fname
        log.cl_info("found unnecessary file [%s] under directory [%s], "
                    "removing it", fname, packages_dir)
        ret = host.sh_remove_file(log, fpath)
        if ret:
            log.cl_error("failed to remove useless file [%s] on host [%s]",
                         fpath, host.sh_hostname)
            return -1

    for rpm_name in dependent_rpms:
        rpm_pattern = (r"^%s.*\.rpm$" % rpm_name)
        rpm_regular = re.compile(rpm_pattern)
        match = False
        for rpm_fname in existing_rpm_fnames:
            match = rpm_regular.match(rpm_fname)
            if match:
                break
        if not match:
            log.cl_error("RPM [%s] is needed but not downloaded in directory [%s]",
                         rpm_name, packages_dir)
            return -1

    for fname in extra_package_fnames:
        if fname not in existing_rpm_fnames:
            log.cl_error("RPM [%s] is recorded as extra file, but not found in "
                         "directory [%s] of host [%s]",
                         fname, packages_dir, host.sh_hostname)
            return -1
    return 0


def download_dependent_rpms(log, workspace, host, distro, target_cpu,
                            packages_dir, type_cache, extra_package_fnames,
                            extra_rpm_names):
    """
    Download dependent RPMs
    """
    # The yumdb might be broken, so sync
    log.cl_info("downloading dependency RPMs")
    # yumdb has been removed for RHEL8
    if distro == ssh_host.DISTRO_RHEL7:
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

    dependent_rpms = merge_list(constant.CORAL_DEPENDENT_RPMS,
                                extra_rpm_names)

    if distro == ssh_host.DISTRO_RHEL7:
        ret = download_dependent_rpms_rhel7(log, host, target_cpu,
                                            packages_dir, dependent_rpms,
                                            extra_package_fnames)
    elif distro == ssh_host.DISTRO_RHEL8:
        ret = download_dependent_rpms_rhel8(log, workspace, host, target_cpu,
                                            packages_dir, type_cache,
                                            dependent_rpms,
                                            extra_package_fnames)
    if ret:
        log.cl_error("failed to download dependent RPMs on host [%s]",
                     host.sh_hostname)
        return -1

    ret = check_package_rpms(log, host, packages_dir, dependent_rpms,
                             extra_package_fnames)
    if ret:
        log.cl_error("unexpected files in package dir [%s]",
                     packages_dir)
        return -1
    return 0


def install_pyinstaller(log, host, type_cache, tsinghua_mirror=False):
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
                                                      expected_sha1sum,
                                                      tsinghua_mirror=tsinghua_mirror)
    if ret:
        log.cl_error("failed to install pip package of pyinstaller")
        return -1
    return 0


def prepare_install_modulemd_tools(log, host):
    """
    modulemd-tools is needed as tools of modulemd YAML files for YUM
    repository, especially repo2module.
    """
    command = 'dnf copr enable frostyx/modulemd-tools-epel -y'
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None

    return ["modulemd-tools"]


def install_build_dependency(log, workspace, host, distro, target_cpu,
                             type_cache, plugins, pip_dir,
                             tsinghua_mirror=False):
    """
    Install the dependency of building Coral
    """
    # pylint: disable=too-many-locals
    if tsinghua_mirror:
        ret = install_common.yum_replace_to_tsinghua(log, host)
        if ret:
            log.cl_error("failed to replace YUM mirrors to Tsinghua University")
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

    dependent_pips = []
    dependent_rpms = ["e2fsprogs-devel",  # Needed for ./configure
                      "genisoimage",  # Generate the ISO image
                      "git",  # Needed by building anything from Git repository.
                      "libtool-ltdl-devel",  # Otherwise, `COPYING.LIB' not found
                      "libyaml-devel",  # yaml C functions.
                      "json-c-devel",  # Needed by json C functions
                      "redhat-lsb-core",  # Needed by detect-distro.sh for lsb_release
                      "wget"]  # Needed by downloading from web

    if distro == ssh_host.DISTRO_RHEL7:
        dependent_pips += ["pylint"]  # Needed for Python codes check
        dependent_rpms += ["createrepo",  # To create the repo in ISO
                           "python-pep8",  # Needed for Python codes check
                           "python36-psutil"]  # Used by Python codes
    else:
        assert distro == ssh_host.DISTRO_RHEL8
        dependent_rpms += ["createrepo_c",  # To create the repo in ISO
                           "python3-pylint",  # Needed for Python codes check
                           "python3-psutil"]  # Used by Python codes
        dependent_pips += ["pep8"]  # Needed for Python codes check

        rpms = prepare_install_modulemd_tools(log, host)
        if rpms is None:
            log.cl_error("failed to install modulemd-tools on host [%s]",
                         host.sh_hostname)
            return -1
        dependent_rpms += rpms

    # We need all depdendency for all plugins since no matter they
    # are needed or not, the Python codes will be checked, and the Python
    # codes might depend on the RPMs.
    for plugin in build_common.CORAL_PLUGIN_DICT.values():
        rpms = plugin.cpt_build_dependent_rpms(log, distro)
        if rpms is None:
            log.cl_error("failed to get build dependent rpms of plugin [%s] on host [%s]",
                         plugin.cpt_plugin_name, host.sh_hostname)
            return -1
        dependent_rpms += rpms
        dependent_pips += plugin.cpt_build_dependent_pips

    ret = install_common.bootstrap_from_internet(log, host, dependent_rpms,
                                                 dependent_pips,
                                                 pip_dir,
                                                 tsinghua_mirror=tsinghua_mirror)
    if ret:
        log.cl_error("failed to install missing packages on host [%s] "
                     "from Internet", host.sh_hostname)
        return -1

    for plugin in plugins:
        ret = plugin.cpt_install_build_dependency(log, workspace, host,
                                                  target_cpu, type_cache)
        if ret:
            log.cl_error("failed to install dependency for plugin [%s]",
                         plugin.cpt_plugin_name)
            return -1

    ret = install_pyinstaller(log, host, type_cache,
                              tsinghua_mirror=tsinghua_mirror)
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


def install_lustre_util_rpm(log, local_host, lustre_dist):
    """
    Install Lustre until on local host
    """
    log.cl_info("installing Lustre util RPM for building")
    command = "rpm -qi %s" % lustre_version.RPM_LUSTRE
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status == 0:
        return 0

    rpm_name = lustre_dist.ldis_lustre_rpm_dict[lustre_version.RPM_LUSTRE]
    rpm_fpath = "%s/%s" % (lustre_dist.ldis_lustre_rpm_dir, rpm_name)
    command = "rpm -ivh %s --nodeps" % rpm_fpath
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
    return 0


def install_e2fsprogs_rpm(log, local_host, lustre_dist):
    """
    Install E2fsprogs until on local host
    """
    log.cl_info("installing E2fsprogs RPM for building")

    need_install = False
    retval = local_host.sh_run(log,
                               "rpm -q e2fsprogs --queryformat '%{version}'")
    if retval.cr_exit_status:
        need_install = True
    else:
        current_version = retval.cr_stdout
        if lustre_dist.ldis_e2fsprogs_version != current_version:
            need_install = True

    if not need_install:
        log.cl_info("the same e2fsprogs RPMs are already installed on "
                    "host [%s]", local_host.sh_hostname)
        return 0

    command = "rpm -Uvh %s/*.rpm" % lustre_dist.ldis_e2fsprogs_rpm_dir
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
    return 0


def build(log, source_dir, workspace,
          cache=constant.CORAL_BUILD_CACHE,
          lustre_rpms_dir=None,
          e2fsprogs_rpms_dir=None,
          collectd=None,
          enable_zfs=False,
          enable_devel=False,
          disable_plugin=None,
          tsinghua_mirror=False):
    """
    Build the Coral ISO.
    """
    # pylint: disable=too-many-locals,too-many-branches
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
    install_lustre = False
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
        if plugin.cpt_install_lustre:
            install_lustre = True

    type_fname = constant.CORAL_BUILD_CACHE_TYPE_OPEN
    if type_fname == constant.CORAL_BUILD_CACHE_TYPE_OPEN:
        log.cl_info("building ISO with %s", enabled_plugin_str)

    local_host = ssh_host.get_local_host(ssh=False)
    distro = local_host.sh_distro(log)
    if distro not in (ssh_host.DISTRO_RHEL7, ssh_host.DISTRO_RHEL8):
        log.cl_error("build on distro [%s] is not supported yet", distro)
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

    if not need_lustre_rpms and not install_lustre:
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

    ret = install_build_dependency(log, workspace, local_host, distro,
                                   target_cpu, type_cache, plugins,
                                   build_pip_dir,
                                   tsinghua_mirror=tsinghua_mirror)
    if ret:
        log.cl_error("failed to install dependency for building")
        return -1

    command = ("mkdir -p %s" % (packages_dir))
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

    ret = download_dependent_rpms(log, workspace, local_host, distro,
                                  target_cpu, packages_dir, type_cache,
                                  extra_package_fnames, extra_rpm_names)
    if ret:
        log.cl_error("failed to download dependent rpms")
        return -1

    pip_dir = iso_cache + "/" + constant.BUILD_PIP
    command = ("mkdir -p %s" % (pip_dir))
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

    ret = install_common.download_pip3_packages(log, local_host, pip_dir,
                                                constant.CORAL_DEPENDENT_PIPS,
                                                tsinghua_mirror=tsinghua_mirror)
    if ret:
        log.cl_error("failed to download pip3 packages")
        return -1

    lustre_distribution = None
    if need_lustre_rpms or install_lustre:
        if lustre_distribution is None:
            log.cl_error("Lustre distribution is needed unexpectedly")
            return -1

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
    extra_str = ""
    command = ("cd %s && rm coral-*.tar.bz2 coral-*.tar.gz -f && "
               "sh autogen.sh && "
               "./configure --with-iso-cache=%s%s%s%s%s && "
               "make -j8 && "
               "make iso" %
               (source_dir, iso_cache, enable_zfs_string, enable_devel_string,
                disable_plugins_str, extra_str))
    log.cl_info("running command [%s] on host [%s]", command,
                local_host.sh_hostname)
    retval = local_host.sh_watched_run(log, command, None, None,
                                       return_stdout=False,
                                       return_stderr=False)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s]",
                     command, local_host.sh_hostname)
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
