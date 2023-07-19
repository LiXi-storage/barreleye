"""
Release info of Corals
"""
import os
from pycoral import clog
from pycoral import release_info
from pycoral import cmd_general
from pycoral import coral_version
from pybuild import build_common


class CoralReleaseInfoCommand():
    """
    Commands to for managing release info.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._cric_log_to_file = log_to_file

    def save(self, distro, arch, path):
        """
        Save release info to a file.
        :param distro: Distro version, e.g. el7 or el8.
        :param arch: CPU arch type, e.g. x86_64.
        :param path: The file to save the release info.
        """
        # pylint: disable=no-self-use,protected-access
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        source_dir = os.getcwd()
        version_string = coral_version.coral_get_version_string(log, source_dir)
        if version_string is None:
            log.cl_error("failed to get version of source [%s]", source_dir)
            cmd_general.cmd_exit(log, -1)
        rinfo = release_info.ReleaseInfo()
        rinfo.rli_version = version_string
        rinfo.rli_distro_short = distro
        rinfo.rli_target_cpu = arch
        ret = rinfo.rli_save_to_file(log, path)
        if ret:
            log.cl_error("failed to save release info to file [%s]", path)
            cmd_general.cmd_exit(log, -1)
        cmd_general.cmd_exit(log, 0)


build_common.coral_command_register("release_info", CoralReleaseInfoCommand())
