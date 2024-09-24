"""
Entrance of Coral command
"""
# DO NOT import any library that needs extra python package,
# since this might cause failure of commands that uses this
# library to install python packages.
# coral_command_bootstrap() should install them if there is any.
import sys
import os
from pycoral import install_common
from pycoral import ssh_host
from pycoral import clog
from pycoral import os_distro


def command_missing_packages_rhel8():
    """
    Add the missing RPMs and pip packages for RHEL8
    """
    # pylint: disable=unused-import,bad-option-value,import-outside-toplevel
    # pylint: disable=unused-variable
    missing_rpms = []
    missing_pips = []
    # This RPM will improve the interactivity
    list_add(missing_rpms, "bash-completion")
    try:
        import yaml
    except ImportError:
        list_add(missing_pips, "PyYAML")

    try:
        import dateutil
    except ImportError:
        list_add(missing_rpms, "python3-dateutil")

    try:
        import prettytable
    except ImportError:
        list_add(missing_rpms, "python3-prettytable")

    try:
        import toml
    except ImportError:
        list_add(missing_pips, "toml")

    try:
        import psutil
    except ImportError:
        list_add(missing_rpms, "python3-psutil")

    try:
        import fire
    except ImportError:
        list_add(missing_pips, "fire")

    try:
        import filelock
    except ImportError:
        list_add(missing_pips, "filelock")
    return missing_rpms, missing_pips


def list_add(alist, name):
    """
    If not exist, append.
    """
    if name not in alist:
        alist.append(name)


def command_missing_packages_rhel7():
    """
    Add the missing RPMs and pip packages for RHEL7
    """
    # pylint: disable=unused-import,bad-option-value,import-outside-toplevel
    # pylint: disable=unused-variable
    missing_rpms = []
    missing_pips = []
    # This RPM will improve the interactivity
    list_add(missing_rpms, "bash-completion")
    try:
        import yaml
    except ImportError:
        list_add(missing_pips, "PyYAML")

    try:
        import dateutil
    except ImportError:
        list_add(missing_rpms, "python36-dateutil")

    try:
        import prettytable
    except ImportError:
        list_add(missing_rpms, "python36-prettytable")

    try:
        import toml
    except ImportError:
        list_add(missing_pips, "toml")

    try:
        import psutil
    except ImportError:
        list_add(missing_rpms, "python36-psutil")

    try:
        import fire
    except ImportError:
        list_add(missing_pips, "fire")

    try:
        import filelock
    except ImportError:
        list_add(missing_pips, "filelock")
    return missing_rpms, missing_pips


def command_missing_packages_ubuntu():
    """
    Add the missing debs and pip packages for Ubuntu (20.04/22.04)
    """
    # pylint: disable=unused-import,bad-option-value,import-outside-toplevel
    # pylint: disable=unused-variable
    missing_debs = []
    try:
        import fire
    except ImportError:
        missing_debs.append("python3-fire")

    try:
        import prettytable
    except ImportError:
        missing_debs.append("python3-prettytable")

    try:
        import toml
    except ImportError:
        missing_debs.append("python3-toml")

    try:
        import dateutil
    except ImportError:
        missing_debs.append("python3-dateutil")

    try:
        import filelock
    except ImportError:
        missing_debs.append("python3-filelock")

    try:
        import psutil
    except ImportError:
        missing_debs.append("python3-psutil")

    missing_pips = []
    return missing_debs, missing_pips


def command_missing_packages(distro):
    """
    Add the missing RPMs/debs and pip packages
    """
    if distro == os_distro.DISTRO_RHEL7:
        return command_missing_packages_rhel7()
    if distro == os_distro.DISTRO_RHEL8:
        return command_missing_packages_rhel8()
    if distro == os_distro.DISTRO_RHEL9:
        return command_missing_packages_rhel8()
    if distro in (os_distro.DISTRO_UBUNTU2004,
                  os_distro.DISTRO_UBUNTU2204):
        return command_missing_packages_ubuntu()
    return None, None


def coral_command_bootstrap(china=False):
    """
    Bootstrap the command by installing the dependencies.
    """
    # pylint: disable=unused-variable
    local_host = ssh_host.get_local_host(ssh=False)
    log = clog.get_log(console_format=clog.FMT_NORMAL)
    distro = local_host.sh_distro(log)
    if distro is None:
        log.cl_error("failed to get distro of host [%s]",
                     local_host.sh_hostname)
        sys.exit(-1)

    missing_packages, missing_pips = command_missing_packages(distro)
    if missing_packages is None or missing_pips is None:
        log.cl_error("failed to get the missing packages")
        sys.exit(-1)

    if china:
        workspace = os.getcwd()
        ret = install_common.china_mirrors_configure(log, local_host, workspace)
        if ret:
            log.cl_error("failed to configure local mirrors on host [%s]",
                         local_host.sh_hostname)
            sys.exit(-1)

    ret = install_common.install_package_and_pip(log, local_host,
                                                 missing_packages,
                                                 missing_pips)
    if ret:
        log.cl_error("failed to install missing packages")
        sys.exit(ret)


def main():
    """
    main routine
    """
    # pylint: disable=bad-option-value,import-outside-toplevel
    china_options = ["--china"]
    china = False
    for china_option in china_options:
        if china_option in sys.argv:
            china = True
            break

    coral_command_bootstrap(china=china)

    from pybuild import coral_command
    coral_command.main()
