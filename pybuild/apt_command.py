"""
Commands to manage resource list of apt.
"""
from pycoral import clog
from pycoral import cmd_general
from pycoral import ssh_host
from pycoral import apt_mirror
from pybuild import build_common


class CoralAptCommand():
    """
    Commands to manage the apt resouce list on the build host.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._cac_log_to_file = log_to_file

    def status(self):
        """
        Print the status of yum configuration
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        local_host = ssh_host.get_local_host(ssh=False)
        rc = 0
        for mirror_name, mirror in apt_mirror.APT_MIRROR_DICT.items():
            ret = mirror.am_is_configured(log, local_host)
            if ret < 0:
                log.cl_error("failed to check whether mirror [%s] "
                             "is configured", mirror_name)
                rc = -1
                continue
            if ret:
                log.cl_stdout("%s: %s",
                              mirror_name,
                              clog.colorful_message(clog.COLOR_GREEN,
                                                    "configured"))
            else:
                log.cl_stdout("%s: %s",
                              mirror_name,
                              clog.colorful_message(clog.COLOR_YELLOW,
                                                    "unconfigured"))
        cmd_general.cmd_exit(log, rc)

    def ls(self):
        """
        List all the mirror names.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        for mirror_name in apt_mirror.APT_MIRROR_DICT:
            log.cl_stdout("%s", mirror_name)
        cmd_general.cmd_exit(log, 0)

    def config(self, mirror=apt_mirror.APT_MIRROR_TSINGHUA,
               force=False):
        """
        Configure the mirror of apt resource list.
        :param mirror: The name of the mirror. By default: tsinghua.
        :param force: Configure again even the mirror has been configured.
            By default: False.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        mirror = cmd_general.check_argument_str(log, "mirror",
                                                mirror)
        cmd_general.check_argument_bool(log, "force", force)
        local_host = ssh_host.get_local_host(ssh=False)
        rc = apt_mirror.apt_mirror_configure(log, local_host,
                                             mirror_name=apt_mirror.APT_MIRROR_TSINGHUA,
                                             force=force)
        cmd_general.cmd_exit(log, rc)


build_common.coral_command_register("apt", CoralAptCommand())
