"""
Entrance of Coral command
"""
# DO NOT import any library that needs extra python package,
# since this might cause failure of commands that uses this
# library to install python packages.
# coral_command_bootstrap() should install them if there is any.
import sys
from pycoral import install_common
from pycoral import ssh_host
from pycoral import clog
from pycoral import constant


def coral_command_bootstrap(tsinghua_mirror=False):
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

    missing_rpms, missing_pips = \
        install_common.command_missing_packages(distro)
    if missing_rpms is None:
        log.cl_error("failed to get the missing packages of host [%s]",
                     local_host.sh_hostname)
        sys.exit(-1)

    command = "mkdir -p %s" % constant.CORAL_BUILD_CACHE_PIP_DIR
    retval = local_host.sh_run(log, command, timeout=None)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = %d, stdout = [%s], stderr = [%s]",
                     command, local_host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        sys.exit(-1)

    ret = install_common.bootstrap_from_internet(log, local_host, missing_rpms,
                                                 missing_pips,
                                                 constant.CORAL_BUILD_CACHE_PIP_DIR,
                                                 tsinghua_mirror=tsinghua_mirror)
    if ret:
        log.cl_error("failed to bootstrap the coral command from Internet")
        sys.exit(ret)


def main():
    """
    main routine
    """
    # pylint: disable=bad-option-value,import-outside-toplevel
    tsinghua_mirror_options = ["--tsinghua-mirror", "--tsinghua_mirror"]
    tsinghua_mirror = False
    for tsinghua_mirror_option in tsinghua_mirror_options:
        if tsinghua_mirror_option in sys.argv:
            tsinghua_mirror = True
            break
    coral_command_bootstrap(tsinghua_mirror=tsinghua_mirror)

    from pybuild import coral_command
    coral_command.main()
