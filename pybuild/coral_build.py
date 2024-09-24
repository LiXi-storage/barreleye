"""
Library for building Coral
"""
import re
import filelock
# pylint: disable=unused-import,too-many-lines
# Local libs
from pycoral import utils
from pycoral import os_distro
from pycoral import ssh_host
from pycoral import constant
from pycoral import lustre_version
from pycoral import lustre as lustre_lib
from pycoral import cmd_general
from pycoral import install_common
from pybuild import cache_command
from pybuild import e2fsprogs_command
from pybuild import lustre_command
from pybuild import apt_command
from pybuild import yum_command
from pybuild import pip_command
from pybuild import build_barrele
from pybuild import build_clownf
from pybuild import build_common
from pybuild import build_constant
from pybuild import build_doc
from pybuild import build_release_info
from pybuild import build_reaf
from pybuild import build_version

# The url of pyinstaller tarball. Need to update together with
# PYINSTALLER_TARBALL_SHA1SUM
PYINSTALLER_TARBALL_URL = "https://github.com/pyinstaller/pyinstaller/archive/refs/tags/v4.10.tar.gz"
# The sha1sum of pyinstaller tarball. Need to update together with
# PYINSTALLER_TARBALL_URL
PYINSTALLER_TARBALL_SHA1SUM = "60c595f5cbe66223d33c6edf1bb731ab9f02c3de"
# "v4.10.tar.gz" is not a good name, specify the fname to save.
PYINSTALLER_TABALL_FNAME = "pyinstaller-4.10.tar.gz"
REPLACE_DEB_DICT = {}
REPLACE_DEB_DICT["debconf-2.0"] = "debconf"
# Only have fonts-freefont-otf and fonts-freefont-ttf
# Looks unncessary to install
REPLACE_DEB_DICT["fonts-freefont"] = None

# Common dependency needed by building Coral on Ubuntu
BUILD_DEPENDENT_DEBS_UBUNTU = ["libjson-c-dev", "apt-rdepends",
                               "debhelper"]
# Common pips needed by building Coral on Ubuntu
BUILD_DEPENDENT_PIPS_UBUNTU = ["PyInstaller==5.13.2", "tinyaes", "pycryptodome"]


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

    log.cl_info("downloading RPMs [%s]", utils.list2string(dependent_rpms))
    retval = host.sh_run(log, command, timeout=None)
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
                log.cl_debug("unknown stdout line [%s] of command [%s] on host "
                             "[%s], stdout = [%s]",
                             line, host.sh_hostname, command,
                             retval.cr_stdout)
                continue
        # We don't care about i686 RPMs
        if rpm_fname.endswith(".i686.rpm"):
            continue

        extra_package_fnames.append(rpm_fname)
    return 0


def download_dependent_rpms_rhel8(log, host, packages_dir,
                                  dependent_rpms, extra_package_fnames):
    """
    Download dependent RPMs for RHEL8
    """
    # pylint: disable=too-many-locals
    if len(dependent_rpms) == 0:
        return 0
    command = ("dnf download --resolve --alldeps --destdir %s" %
               (packages_dir))
    for rpm_name in dependent_rpms:
        command += " " + rpm_name

    log.cl_info("downloading RPMs [%s]", utils.list2string(dependent_rpms))
    retval = host.sh_run(log, command, timeout=None)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
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
    # The first line either starts with "Last metadata expiration check:"
    # for RHEL7 or "Copr repo for modulemd-tools-epel owned by fros" for
    # RHEL8. Skip it.
    for line in lines:
        match = exist_regular.match(line)
        if match:
            rpm_fname = match.group("rpm_fname")
        else:
            log.cl_debug("unknown stdout line [%s] of command [%s] on host "
                         "[%s], stdout = [%s]",
                         line, host.sh_hostname, command,
                         retval.cr_stdout)
            continue

        # We don't care about i686 RPMs
        if rpm_fname.endswith(".i686.rpm"):
            continue

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
        log.cl_debug("removing unnecessary file [%s/%s]", packages_dir, fname)
        ret = host.sh_run(log, "rm -f %s" % (fpath))
        if ret.cr_exit_status:
            log.cl_error("failed to remove file [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         fpath, host.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
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


def download_dependent_rpms(log, host, distro, target_cpu,
                            packages_dir, extra_package_fnames,
                            extra_package_names):
    """
    Download dependent RPMs
    """
    # The yumdb might be broken, so sync
    # yumdb has been removed for RHEL8
    if distro == os_distro.DISTRO_RHEL7:
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
                                extra_package_names)

    if distro == os_distro.DISTRO_RHEL7:
        ret = download_dependent_rpms_rhel7(log, host, target_cpu,
                                            packages_dir, dependent_rpms,
                                            extra_package_fnames)
    elif distro == os_distro.DISTRO_RHEL8:
        ret = download_dependent_rpms_rhel8(log, host, packages_dir,
                                            dependent_rpms,
                                            extra_package_fnames)
    else:
        log.cl_error("unsupported OS distro [%s] when downloading dependent RPMs",
                     distro)
        return -1
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


def apt_read_passphrase(log, host, passphrase_fpath):
    """
    Read passphrase
    """
    command = "cat " + passphrase_fpath
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
    lines = retval.cr_stdout.strip().splitlines()
    if len(lines) != 1:
        log.cl_info("unexpected line number of passphrase")
        return None
    return lines[0]


def apt_private_key_import(log, host, gpg_dir, passphrase):
    """
    Import the private key.
    """
    command = ("gpg --import --pinentry-mode loopback --batch "
               "--passphrase %s %s/private_key.asc" %
               (passphrase, gpg_dir))
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


def apt_repositry_signate(log, host, source_dir, packages_dir,
                          extra_package_fnames):
    """
    Signate the Release.
    """
    gpg_dir = source_dir + "/gpg"
    passphrase_fpath = gpg_dir + "/passphrase"
    passphrase = apt_read_passphrase(log, host, passphrase_fpath)
    if passphrase is None:
        log.cl_error("failed to read passphrase")
        return -1

    ret = apt_private_key_import(log, host, gpg_dir, passphrase)
    if ret:
        log.cl_error("failed to import the private key")
        return -1

    extra_package_fnames.append("InRelease")
    command = ("cd %s && rm -f InRelease && "
               "gpg --pinentry-mode loopback --batch "
               "--passphrase %s --clearsign -o InRelease Release" %
               (packages_dir, passphrase))
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

    extra_package_fnames.append(constant.CORAL_PUBLIC_KEY_FNAME)
    command = ("cp %s/%s %s" %
               (gpg_dir, constant.CORAL_PUBLIC_KEY_FNAME, packages_dir))
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


def apt_repository_generate(log, host, source_dir, packages_dir,
                            extra_package_fnames):
    """
    Pack the source of Coral
    """
    extra_package_fnames.append("Packages")
    command = ("cd %s && apt-ftparchive packages . > Packages" %
               (packages_dir))
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

    extra_package_fnames.append("Release")
    command = ("cd %s && apt-ftparchive release . > Release" %
               (packages_dir))
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

    ret = apt_repositry_signate(log, host, source_dir, packages_dir,
                                extra_package_fnames)
    if ret:
        log.cl_error("failed to signate repository")
        return -1

    extra_package_fnames.append("Release.gpg")
    command = ("cd %s && rm -f Release.gpg && "
               "gpg --pinentry-mode loopback --batch "
               "-abs -o Release.gpg Release" %
               (packages_dir))
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


def download_dependent_debs(log, host, source_dir, packages_dir,
                            extra_package_fnames, extra_package_names):
    """
    Download dependent debs
    """
    # pylint: disable=consider-using-get
    if len(extra_package_names) == 0:
        return 0
    log.cl_info("downloading dependency debs [%s]", utils.list2string(extra_package_names))
    command = 'apt-rdepends'
    for extra_package_name in extra_package_names:
        command += " " + extra_package_name
    command = command + ' | grep -v "^ "'
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
    packages = retval.cr_stdout.splitlines()

    downloading_dir = packages_dir + "/downloading." + cmd_general.get_identity()
    command = "mkdir %s" % downloading_dir
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

    for package in packages:
        if package in REPLACE_DEB_DICT:
            package = REPLACE_DEB_DICT[package]
        if package is None:
            continue
        command = ("cd %s && apt download %s" %
                   (downloading_dir, package))
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

    downloaded_fnames = host.sh_get_dir_fnames(log, downloading_dir)
    if downloaded_fnames is None:
        log.cl_error("failed to get the fnames under [%s]",
                     downloading_dir)
        return -1
    extra_package_fnames += downloaded_fnames

    command = ("mv %s/* %s && rmdir %s" %
               (downloading_dir, packages_dir, downloading_dir))
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

    ret = apt_repository_generate(log, host, source_dir, packages_dir,
                                  extra_package_fnames)
    if ret:
        log.cl_error("failed to generate apt repository")
        return -1

    return 0


def download_dependent_packages(log, host, source_dir, distro, target_cpu,
                                packages_dir, extra_package_fnames,
                                extra_package_names):
    """
    Download packages for Barreleye.
    """
    if distro in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8):
        return download_dependent_rpms(log, host, distro, target_cpu,
                                       packages_dir, extra_package_fnames,
                                       extra_package_names)
    if distro in (os_distro.DISTRO_UBUNTU2004,
                  os_distro.DISTRO_UBUNTU2204):
        return download_dependent_debs(log, host, source_dir,
                                       packages_dir, extra_package_fnames,
                                       extra_package_names)
    log.cl_error("unsupported distro [%s] when downloading dependent packages",
                 distro)
    return -1


def install_pyinstaller(log, host, type_cache):
    """
    Install pyinstaller
    """
    if host.sh_has_command(log, "pyinstaller"):
        log.cl_info("pyinstaller already installed")
        return 0

    log.cl_info("installing pyinstaller")
    tarball_url = PYINSTALLER_TARBALL_URL
    tarball_fname = PYINSTALLER_TABALL_FNAME
    expected_sha1sum = PYINSTALLER_TARBALL_SHA1SUM
    ret = build_common.install_pip3_package_from_file(log, host, type_cache,
                                                      tarball_url,
                                                      expected_sha1sum,
                                                      tarball_fname=tarball_fname)
    if ret:
        log.cl_error("failed to install pyinstaller from pip tarball [%s]",
                     tarball_fname)
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


def get_build_dependent_packages(log, host, distro, plugins, package_dict):
    """
    Return a list of dependent packages (RPMs/debs) and pips
    """
    ret = install_common.check_yum_repo_epel(log, host, distro)
    if ret:
        log.cl_error("EPEL repo is not configured properly on host [%s]",
                     host.sh_hostname)
        return None, None

    dependent_packages = []
    dependent_pips = []
    for plugin in plugins:
        packages = plugin.cpt_build_dependent_packages(log, distro)
        if packages is None:
            log.cl_error("failed to get the dependet packages for building [%s]",
                         plugin.cpt_plugin_name)
            return None, None
        dependent_packages += packages
        dependent_pips += plugin.cpt_build_dependent_pips

    for package in package_dict.values():
        packages, pips = package.cpb_build_dependent_packages(log, host, distro)
        if packages is None or pips is None:
            log.cl_error("failed to get the dependet packages for building [%s]",
                         package.cpb_package_name)
            return None, None
        dependent_packages += packages
        dependent_pips += pips
    return dependent_packages, dependent_pips


def _install_build_dependency_rhel(log, workspace, host, distro, target_cpu,
                                   type_cache, plugins, package_dict):
    """
    Install the dependency of building Coral
    """
    # pylint: disable=too-many-locals,too-many-branches
    log.cl_info("installing RPM group of Development Tools")
    if distro == os_distro.DISTRO_RHEL7:
        command = 'yum -y groupinstall "Development Tools"'
    elif distro == os_distro.DISTRO_RHEL8:
        command = 'dnf groupinstall -y --nobest "Development Tools"'
    else:
        return -1
    retval = host.sh_run(log, command, timeout=None)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    dependent_pips = ["wheel"]  # Needed for pyinstaller
    dependent_rpms = ["e2fsprogs-devel",  # Needed for ./configure
                      "genisoimage",  # Generate the ISO image
                      "git",  # Needed by building anything from Git repository.
                      "json-c-devel",  # Needed by json C functions
                      "libtool-ltdl-devel",  # Otherwise, `COPYING.LIB' not found
                      "json-c-devel",  # Needed by json C functions
                      "redhat-lsb-core",  # Needed by detect-distro.sh for lsb_release
                      "wget",  # Needed by downloading from web
                      "yum-utils",  # Commnad like "yumdb sync"
                      "p7zip",  # 7z command for extracting ISO
                      "p7zip-plugins",  # 7z command for extracting ISO
                      ]

    if distro == os_distro.DISTRO_RHEL7:
        dependent_pips += ["pylint"]  # Needed for Python codes check
        dependent_rpms += ["createrepo",  # To create the repo in ISO
                           "python36-psutil",  # Used by Python codes
                           "python3-devel",  # Needed by building Python libraries
                           ]
    else:
        assert distro == os_distro.DISTRO_RHEL8
        dependent_pips += ["pylint"]  # Needed for Python codes check
        dependent_rpms += ["createrepo_c",  # To create the repo in ISO
                           "python3-psutil",  # Used by Python codes
                           "python36-devel",  # Needed by building Python libraries
                           ]

        rpms = prepare_install_modulemd_tools(log, host)
        if rpms is None:
            log.cl_error("failed to install modulemd-tools on host [%s]",
                         host.sh_hostname)
            return -1
        dependent_rpms += rpms

    # Upgrade pip3 first since python packages might need new pip3 to install
    log.cl_info("upgrading pip")
    command = "pip3 install --upgrade pip"
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

    rpms, pips = get_build_dependent_packages(log, host, distro, plugins, package_dict)
    if rpms is None or pips is None:
        return -1
    dependent_rpms += rpms
    dependent_pips += pips

    ret = install_common.install_package_and_pip(log, host, dependent_rpms,
                                                 dependent_pips)
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


def _install_build_dependency_ubuntu(log, workspace, host, distro,
                                     target_cpu, type_cache, plugins,
                                     package_dict):
    """
    Install the dependency of building Coral for Ubuntu
    """
    # pylint: disable=unused-argument,too-many-locals
    dependent_debs = BUILD_DEPENDENT_DEBS_UBUNTU
    dependent_pips = BUILD_DEPENDENT_PIPS_UBUNTU

    debs, pips = get_build_dependent_packages(log, host, distro, plugins,
                                              package_dict)
    if debs is None or pips is None:
        return -1
    dependent_pips += pips
    dependent_debs += debs

    ret = install_common.install_package_and_pip(log, host,
                                                 dependent_debs,
                                                 dependent_pips)
    if ret:
        log.cl_error("failed to install missing packages on host [%s]",
                     host.sh_hostname)
        return -1
    return 0


def _install_build_dependency(log, workspace, host, distro, target_cpu,
                              type_cache, plugins, package_dict,
                              china=False):
    """
    Install the dependency of building Coral
    """
    if china:
        ret = install_common.china_mirrors_configure(log, host, workspace)
        if ret:
            log.cl_error("failed to configure mirrors on host [%s]",
                         host.sh_hostname)
            return -1
    if distro in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8, os_distro.DISTRO_RHEL9):
        return _install_build_dependency_rhel(log, workspace, host, distro,
                                              target_cpu, type_cache, plugins,
                                              package_dict)

    if distro in (os_distro.DISTRO_UBUNTU2004,
                  os_distro.DISTRO_UBUNTU2204):
        return _install_build_dependency_ubuntu(log, workspace, host, distro,
                                                target_cpu, type_cache, plugins,
                                                package_dict)
    log.cl_error("unsupported OS distro [%s] when installing build dependency",
                 distro)
    return -1


def sync_shared_build_cache(log, host, private_cache, shared_parent):
    """
    Sync from the local cache to shared cache
    """
    # pylint: disable=abstract-class-instantiated
    log.cl_info("syncing [%s] to shared cache [%s]", private_cache,
                shared_parent)
    lock_file = build_constant.CORAL_BUILD_CACHE_LOCK
    lock = filelock.FileLock(lock_file)
    ret = 0
    try:
        with lock.acquire(timeout=600):
            ret = host.sh_sync_two_dirs(log, private_cache, shared_parent)
    except filelock.Timeout:
        ret = -1
        log.cl_error("someone else is holding lock of file [%s] for more "
                     "than 10 minutes, aborting",
                     lock_file)

    return ret


def _handle_lustre_e2fsprogs_rpms(log, local_host, workspace,
                                  source_dir, iso_cache,
                                  plugins_need_pack_lustre,
                                  plugins_need_install_lustre,
                                  lustre_dir, e2fsprogs_dir,
                                  extra_iso_fnames):
    """
    Handling Lustre/E2fsprogs RPMs
    """
    # pylint: disable=too-many-locals,too-many-branches,unused-argument
    iso_lustre_dir = iso_cache + "/" + constant.CORAL_LUSTRE_RELEASE_BASENAME
    iso_e2fsprogs_dir = iso_cache + "/" + constant.CORAL_E2FSPROGS_RELEASE_BASENAME
    if (len(plugins_need_pack_lustre) == 0 and
            len(plugins_need_install_lustre) == 0):
        if lustre_dir is not None:
            log.cl_warning("option [--lustre %s] has been ignored since "
                           "no need to have Lustre RPMs",
                           lustre_dir)
        if e2fsprogs_dir is not None:
            log.cl_warning("option [--e2fsprogs %s] has been ignored since "
                           "no need to have Lustre RPMs",
                           e2fsprogs_dir)
    else:
        if lustre_dir is None:
            lustre_dir = iso_lustre_dir
        if e2fsprogs_dir is None:
            e2fsprogs_dir = iso_e2fsprogs_dir

    lustre_distribution = None
    if (len(plugins_need_pack_lustre) != 0 or
            len(plugins_need_install_lustre) != 0):
        definition_dir = source_dir + "/" + constant.LUSTRE_VERSION_DEFINITION_SOURCE_PATH
        lustre_distribution = lustre_lib.get_lustre_dist(log, local_host,
                                                         workspace,
                                                         lustre_dir,
                                                         e2fsprogs_dir,
                                                         definition_dir=definition_dir)
        if lustre_distribution is None:
            log.cl_error("invalid Lustre RPMs [%s] or e2fsprogs RPMs [%s]",
                         lustre_dir, e2fsprogs_dir)
            log.cl_error("Lustre RPMs are needed by plugins [%s]",
                         get_plugin_str(plugins_need_pack_lustre +
                                        plugins_need_install_lustre))
            return -1

        if len(plugins_need_pack_lustre) != 0:
            if lustre_dir != iso_lustre_dir:
                rinfo = lustre_distribution.ldis_lustre_rinfo
                log.cl_info("syncing Lustre release from [%s] to [%s]",
                            lustre_dir, iso_lustre_dir)
                ret = rinfo.rli_files_send(log, local_host,
                                           lustre_dir,
                                           local_host, iso_lustre_dir)
                if ret:
                    log.cl_error("failed to sync Lustre release from [%s] "
                                 "to [%s] on local host [%s]",
                                 lustre_dir, iso_e2fsprogs_dir,
                                 local_host.sh_hostname)
                    return -1

            if e2fsprogs_dir != iso_e2fsprogs_dir:
                rinfo = lustre_distribution.ldis_e2fsprogs_rinfo
                log.cl_info("syncing E2fsprogs release from [%s] to [%s]",
                            e2fsprogs_dir, iso_e2fsprogs_dir)
                ret = rinfo.rli_files_send(log, local_host,
                                           e2fsprogs_dir,
                                           local_host, iso_e2fsprogs_dir)
                if ret:
                    log.cl_error("failed to sync E2fsprogs release from [%s] "
                                 "to [%s] on local host [%s]",
                                 lustre_dir, iso_e2fsprogs_dir,
                                 local_host.sh_hostname)
                    return -1

            extra_iso_fnames.append(constant.CORAL_LUSTRE_RELEASE_BASENAME)
            extra_iso_fnames.append(constant.CORAL_E2FSPROGS_RELEASE_BASENAME)

        if len(plugins_need_install_lustre) != 0:
            ret = lustre_distribution.ldis_install_lustre_devel_rpms(log, local_host)
            if ret:
                log.cl_error("failed to install Lustre RPMs for build")
                return -1

            ret = lustre_distribution.ldis_install_e2fsprogs_rpms(log)
            if ret:
                log.cl_error("failed to install E2fsprogs RPMs")
                return -1

        if lustre_distribution is None:
            log.cl_error("Lustre distribution is needed unexpectedly")
            return -1
    return 0


def resolve_package_build_order(log, package_dict):
    """
    Put the package into a ordered list according to the dependency.
    """
    # pylint: disable=too-many-branches
    ordered_packages = []
    former_included = -1
    while len(ordered_packages) != former_included:
        former_included = len(ordered_packages)
        if former_included == len(package_dict):
            break

        for package in package_dict.values():
            if package in ordered_packages:
                continue
            if package.cpb_depend_package_names is None:
                ordered_packages.append(package)
                continue
            depend_included = True
            for package_name in package.cpb_depend_package_names:
                if package_name not in package_dict:
                    log.cl_error("unknown depend package [%s]",
                                 package_name)
                depend = package_dict[package_name]
                if depend not in ordered_packages:
                    depend_included = False
                    break
            if depend_included:
                ordered_packages.append(package)

    if former_included != len(package_dict):
        resolved = ""
        not_resolved = ""
        for package in package_dict.values():
            if package in ordered_packages:
                if resolved != "":
                    resolved += ","
                resolved += package.cpb_package_name
                continue
            if not_resolved != "":
                not_resolved += ","
            not_resolved += package.cpb_package_name
        log.cl_error("failed to resolve the build dependency of "
                     "packages [%s], resolved [%s]",
                     not_resolved, resolved)
        return None
    return ordered_packages


def get_needed_packages(log, plugins):
    """
    Return a dict of packages (type CoralPackageBuild) needed to build.
    """
    # pylint: disable=too-many-locals
    package_dict = {}
    for plugin in plugins:
        plugin_name = plugin.cpt_plugin_name
        for package_name in plugin.cpt_packages:
            if package_name not in build_common.CORAL_PACKAGE_DICT:
                log.cl_error("package [%s] required by plugin [%s] is "
                             "not defined",
                             package_name, plugin_name)
                return None
            package = build_common.CORAL_PACKAGE_DICT[package_name]
            if package_name not in package_dict:
                package_dict[package_name] = package

    while True:
        added = False
        packages = list(package_dict.values())
        for package in packages:
            if package.cpb_depend_package_names is None:
                continue
            for package_name in package.cpb_depend_package_names:
                if package_name in package_dict:
                    continue
                if package_name not in build_common.CORAL_PACKAGE_DICT:
                    log.cl_error("unknown package [%s]", package_name)
                    return None
                package_dict[package_name] = \
                    build_common.CORAL_PACKAGE_DICT[package_name]
                added = True
        if not added:
            break

    return package_dict


def build_packages(log, workspace, local_host, source_dir, target_cpu,
                   type_cache, iso_cache, packages_dir, extra_iso_fnames,
                   extra_package_fnames, extra_package_names, option_dict,
                   package_dict):
    """
    Build the packages in cpt_packages.
    """
    # pylint: disable=too-many-locals
    ordered_packages = resolve_package_build_order(log, package_dict)
    if ordered_packages is None:
        return -1

    if len(ordered_packages) == 0:
        log.cl_info("no Coral package needs to be built")
        return 0

    for package in ordered_packages:
        package_name = package.cpb_package_name
        log.cl_info("building Coral package [%s]", package_name)
        ret = package.cpb_build(log, workspace, local_host, source_dir,
                                target_cpu, type_cache, iso_cache,
                                packages_dir, extra_iso_fnames,
                                extra_package_fnames, extra_package_names,
                                option_dict)
        if ret:
            log.cl_error("failed to build package [%s]",
                         package_name)
            return -1
    return 0


def get_plugin_str(plugins):
    """
    Return a string of plugins
    """
    plugin_names = []
    for plugin in plugins:
        plugin_names.append(plugin.cpt_plugin_name)
    return utils.list2string(plugin_names)


def cleanup_dir_content(log, host, directory, contents):
    """
    Cleanup unexpected content under a directory
    """
    existing_fnames = host.sh_get_dir_fnames(log, directory)
    if existing_fnames is None:
        log.cl_error("failed to get the file names under [%s] of "
                     "host [%s]",
                     directory, host.sh_hostname)
        return -1

    for fname in contents:
        if fname not in existing_fnames:
            log.cl_error("failed to find necessary content [%s] under "
                         "directory [%s] of host [%s]", fname, directory,
                         host.sh_hostname)
            return -1
        existing_fnames.remove(fname)

    for fname in existing_fnames:
        fpath = directory + "/" + fname
        log.cl_debug("found unnecessary content [%s] under directory "
                     "[%s] of host [%s], removing it", fname, directory,
                     host.sh_hostname)
        command = "rm -fr %s" % (fpath)
        ret = host.sh_run(log, command)
        if ret.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
    return 0


def build(log, source_dir, workspace,
          cache=constant.CORAL_BUILD_CACHE,
          lustre_dir=None,
          e2fsprogs_dir=None,
          collectd=None,
          enable_zfs=False,
          enable_devel=False,
          disable_creaf=False,
          disable_plugin=None,
          only_plugin=None,
          china=False):
    """
    Build the Coral ISO
    """
    # pylint: disable=too-many-locals,too-many-branches
    # pylint: disable=too-many-statements
    if disable_plugin is not None and only_plugin is not None:
        log.cl_error("--disable_plugin and --only_plugin can not be "
                     "specified together")
        return -1

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

    all_plugin_names = list(build_common.CORAL_PLUGIN_DICT.keys())
    all_plugin_names_str = utils.list2string(all_plugin_names)
    sync_cache_back = True
    disable_plugins_str = ""
    for plugin_name in disabled_plugins:
        if plugin_name not in build_common.CORAL_PLUGIN_DICT:
            log.cl_error("unknown plugin [%s] of --disable_plugin",
                         plugin_name)
            log.cl_error("possible plugins are [%s]",
                         all_plugin_names_str)
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

    if only_plugin:
        sync_cache_back = False
        plugin_names = cmd_general.parse_list_string(log, only_plugin)
        if plugin_names is None:
            log.cl_error("invalid option [%s] of --only_plugin",
                         only_plugin)
            return -1
        plugins = []
        for plugin_name in plugin_names:
            if plugin_name not in build_common.CORAL_PLUGIN_DICT:
                log.cl_error("unknown plugin [%s] of --only_plugin",
                             plugin_name)
                log.cl_error("possible plugins are [%s]",
                             all_plugin_names_str)
                return -1
            if plugin_name in build_common.CORAL_DEVEL_PLUGIN_DICT:
                enable_devel = True
            plugin = build_common.CORAL_PLUGIN_DICT[plugin_name]
            if plugin in plugins:
                continue
            plugins.append(plugin)

        disable_plugins_str = ""
        for plugin_name in build_common.CORAL_PLUGIN_DICT:
            if plugin_name in plugin_names:
                continue
            disable_plugins_str += " --disable-%s" % plugin_name

    if len(plugins) == 0:
        log.cl_error("everything has been disabled, nothing to build")
        return -1

    plugin_dict = {}
    for plugin in plugins:
        plugin_dict[plugin.cpt_plugin_name] = plugin
    for plugin in plugins:
        for plugin_name in plugin.cpt_plugins:
            if plugin_name not in plugin_dict:
                log.cl_error("plugin [%s] depends on plugin [%s], "
                             "but the later is disabled",
                             plugin.cpt_plugin_name, plugin_name)
                return -1

    plugins_need_pack_lustre = []
    plugins_need_collectd = []
    plugins_need_install_lustre = []
    enabled_plugin_str = get_plugin_str(plugins)
    for plugin in plugins:
        if plugin.cpt_need_pack_lustre:
            plugins_need_pack_lustre.append(plugin)
        if plugin.cpt_need_collectd:
            plugins_need_collectd.append(plugin)
        if (disable_creaf and
                plugin.cpt_plugin_name == build_reaf.REAF_PLUGIN_NAME):
            continue
        if plugin.cpt_need_lustre_for_build:
            plugins_need_install_lustre.append(plugin)

    type_fname = constant.CORAL_BUILD_CACHE_TYPE_OPEN
    if type_fname == constant.CORAL_BUILD_CACHE_TYPE_OPEN:
        log.cl_info("building ISO with plugins [%s]", enabled_plugin_str)

    local_host = ssh_host.get_local_host(ssh=False)
    distro = local_host.sh_distro(log)
    if distro not in (os_distro.DISTRO_RHEL7,
                      os_distro.DISTRO_RHEL8,
                      os_distro.DISTRO_RHEL9,
                      os_distro.DISTRO_UBUNTU2004,
                      os_distro.DISTRO_UBUNTU2204):
        log.cl_error("unsupported distro [%s] when building Coral", distro)
        return -1

    shared_cache = cache.rstrip("/")
    # Shared cache for this build type
    shared_type_cache = shared_cache + "/" + type_fname
    # Extra RPMs to download
    extra_package_names = []
    # Extra RPM file names under package directory
    extra_package_fnames = []
    # Extra file names under ISO directory
    extra_iso_fnames = []

    type_cache = workspace + "/" + type_fname
    iso_cache = type_cache + "/" + constant.ISO_CACHE_FNAME
    # Directory path of package under ISO cache
    packages_dir = iso_cache + "/" + constant.BUILD_PACKAGES

    if len(plugins_need_collectd) == 0:
        if collectd is not None:
            log.cl_warning("option [--collectd %s] has been ignored since "
                           "no need to have Collectd RPMs",
                           collectd)
    elif collectd is not None:
        sync_cache_back = False

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

    ret = build_common.get_shared_build_cache(log, local_host, workspace,
                                              shared_type_cache)
    if ret:
        log.cl_error("failed to get shared build cache")
        return -1

    # Do this after copying ISO cache, and before cpt_build when Lustre rpms
    # should have been installed if necessary.
    ret = _handle_lustre_e2fsprogs_rpms(log, local_host, workspace,
                                        source_dir, iso_cache,
                                        plugins_need_pack_lustre,
                                        plugins_need_install_lustre,
                                        lustre_dir, e2fsprogs_dir,
                                        extra_iso_fnames)
    if ret:
        log.cl_error("failed on Lustre/e2fsprogs RPMs")
        return -1

    target_cpu = local_host.sh_target_cpu(log)
    if target_cpu is None:
        log.cl_error("failed to get the target cpu on host [%s]",
                     local_host.sh_hostname)
        return -1

    package_dict = get_needed_packages(log, plugins)
    if package_dict is None:
        log.cl_error("failed to get the needed packages")
        return -1

    ret = _install_build_dependency(log, workspace, local_host, distro,
                                    target_cpu, type_cache, plugins,
                                    package_dict,
                                    china=china)
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

    option_dict = {}
    option_dict["collectd"] = collectd

    ret = build_packages(log, workspace, local_host, source_dir, target_cpu,
                         type_cache, iso_cache, packages_dir, extra_iso_fnames,
                         extra_package_fnames, extra_package_names, option_dict,
                         package_dict)
    if ret:
        log.cl_error("failed to build packages")
        return -1

    for plugin in plugins:
        plugin_name = plugin.cpt_plugin_name
        ret = plugin.cpt_build(log, workspace, local_host, source_dir,
                               target_cpu, type_cache, iso_cache,
                               packages_dir, extra_iso_fnames,
                               extra_package_fnames, extra_package_names,
                               option_dict)
        if ret:
            log.cl_error("failed to build Coral plugin [%s]",
                         plugin_name)
            return -1

    ret = download_dependent_packages(log, local_host, source_dir,
                                      distro,
                                      target_cpu, packages_dir,
                                      extra_package_fnames,
                                      extra_package_names)
    if ret:
        log.cl_error("failed to download dependent packages")
        return -1

    contents = ([constant.BUILD_PACKAGES] +
                extra_iso_fnames)
    ret = cleanup_dir_content(log, local_host, iso_cache, contents)
    if ret:
        log.cl_error("failed to cleanup content under [%s] on host [%s]",
                     iso_cache, local_host.sh_hostname)
        return -1

    ret = cleanup_dir_content(log, local_host, packages_dir, extra_package_fnames)
    if ret:
        log.cl_error("failed to cleanup content under [%s] on host [%s]",
                     packages_dir, local_host.sh_hostname)
        return -1

    enable_zfs_string = ""
    if enable_zfs:
        enable_zfs_string = " --enable-zfs"
    enable_devel_string = ""
    if enable_devel:
        enable_devel_string = " --enable-devel"
    disable_creaf_string = ""
    if disable_creaf:
        sync_cache_back = False
        disable_creaf_string = " --disable-creaf"
    extra_str = ""
    command = ("cd %s && rm coral-*.tar.bz2 coral-*.tar.gz -f && "
               "sh autogen.sh && "
               "./configure --with-iso-cache=%s%s%s%s%s%s && "
               "make -j8 && "
               "make iso" %
               (source_dir, iso_cache, enable_zfs_string, enable_devel_string,
                disable_creaf_string, disable_plugins_str, extra_str))
    log.cl_info("generating ISO using command [%s]", command)
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
