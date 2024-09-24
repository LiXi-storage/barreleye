"""
Commands to manage yum repository.
"""
from pycoral import clog
from pycoral import cmd_general
from pycoral import ssh_host
from pycoral import yum_mirror
from pycoral import constant
from pycoral import install_common
from pybuild import build_common


def check_yum_repos(log, host):
    """
    Check the status of yum repos
    """
    distro = host.sh_distro(log)
    if distro is None:
        log.cl_error("failed to detect OS distro on host [%s]",
                     host.sh_hostname)
        return -1
    ret = install_common.check_yum_repo_powertools(log, host, distro)
    if ret:
        return -1
    ret = install_common.check_yum_repo_epel(log, host, distro)
    if ret:
        return -1
    return 0


class CoralYumCommand():
    """
    Commands to manage the yum repostory on the build host.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._cyc_log_to_file = log_to_file

    def status(self):
        """
        Print the status of yum configuration
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        local_host = ssh_host.get_local_host(ssh=False)
        rc = 0
        configured_mirror = None
        for mirror_name, mirror in yum_mirror.YUM_MIRROR_DICT.items():
            ret = mirror.ym_is_configured(log, local_host)
            if ret < 0:
                log.cl_error("failed to check whether mirror [%s] "
                             "is configured", mirror_name)
                rc = -1
                continue
            if ret:
                configured_mirror = mirror_name
                break
        mirror_string = clog.colorful_message(clog.COLOR_YELLOW,
                                              constant.CMD_MSG_NONE)
        if configured_mirror is not None:
            mirror_string = clog.colorful_message(clog.COLOR_GREEN,
                                                  configured_mirror)
        cmd_general.print_field(log, "Mirror", mirror_string)
        ret = check_yum_repos(log, local_host)
        if ret:
            rc = -1
            repo_status = clog.colorful_message(clog.COLOR_RED,
                                                "bad")
        else:
            repo_status = clog.colorful_message(clog.COLOR_GREEN,
                                                "good")
        cmd_general.print_field(log, "Status", repo_status)

        cmd_general.cmd_exit(log, rc)

    def mirrors(self):
        """
        List all the mirror names.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        for mirror_name in yum_mirror.YUM_MIRROR_DICT:
            log.cl_stdout("%s", mirror_name)
        cmd_general.cmd_exit(log, 0)

    def config(self, mirror=None,
               force=False):
        """
        Configure the mirror of yum repositories.
        :param mirror: The name of the mirror. By default: None.
        :param force: Configure again even the mirror has been configured.
            By default: False.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        if mirror is not None:
            mirror = cmd_general.check_argument_str(log, "mirror",
                                                    mirror)
        cmd_general.check_argument_bool(log, "force", force)
        local_host = ssh_host.get_local_host(ssh=False)
        if mirror is not None:
            rc = yum_mirror.yum_mirror_configure(log, local_host,
                                                 mirror_name=mirror,
                                                 force=force)
            if rc:
                cmd_general.cmd_exit(log, rc)
        rc = install_common.enable_yum_repo_powertools(log, local_host)
        cmd_general.cmd_exit(log, rc)


build_common.coral_command_register("yum", CoralYumCommand())
