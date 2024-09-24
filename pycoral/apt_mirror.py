"""
Library to manage apt mirror.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
from pycoral import os_distro

APT_BACKUP_REFIX = "coral."
APT_MIRROR_TSINGHUA = "tsinghua"
APT_CONFIG_DIR = "/etc/apt"
SOURCE_LIST_FPATH = APT_CONFIG_DIR + "/sources.list"
APT_MIRROR_DICT = {}

class AptMirror():
    """
    Each mirror has an object of this type.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, mirror_name):
        # Name of the mirror.
        self.am_mirror_name = mirror_name

    def _am_ubuntu2004_is_configured(self, log, host):
        """
        Check whether this mirror is being used for Ubuntu-2004.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        # pylint: disable=unused-argument
        log.cl_error("ubuntu2004_is_configured not implemented for "
                     "mirror [%s]", self.am_mirror_name)
        return -1

    def _am_ubuntu2004_configure(self, log, host):
        """
        Configure the apt mirror for Ubuntu-2004.
        """
        # pylint: disable=unused-argument
        log.cl_error("ubuntu2204_configure not implemented for "
                     "mirror [%s]", self.am_mirror_name)
        return -1

    def _am_ubuntu2204_is_configured(self, log, host):
        """
        Check whether this mirror is being used for Ubuntu-2204.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        # pylint: disable=unused-argument
        log.cl_error("ubuntu2204_is_configured not implemented for "
                     "mirror [%s]", self.am_mirror_name)
        return -1

    def _am_ubuntu2204_configure(self, log, host):
        """
        Configure the apt mirror for Ubuntu-2204.
        """
        # pylint: disable=unused-argument
        log.cl_error("ubuntu2204_configure not implemented for "
                     "mirror [%s]", self.am_mirror_name)
        return -1

    def _am_update(self, log, host):
        """
        Update the list of available packages.
        """
        # pylint: disable=no-self-use
        command = 'apt update'
        log.cl_debug("running command [%s] on host [%s]",
                     command, host.sh_hostname)
        retval = host.sh_watched_run(log, command, None, None,
                                     return_stdout=False,
                                     return_stderr=False)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, host.sh_hostname)
            return -1
        return 0

    def am_configure(self, log, host):
        """
        Configure the mirror for apt.
        """
        distro = host.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get OS distro on host [%s]",
                         host.sh_hostname)
            return -1

        if distro not in (os_distro.DISTRO_UBUNTU2004,
                          os_distro.DISTRO_UBUNTU2204):
            log.cl_error("OS distro [%s] is not supported",
                         distro)
            return -1

        ret = apt_source_list_backup(log, host)
        if ret:
            log.cl_error("failed to backup resource list of apt")
            return -1

        if distro == os_distro.DISTRO_UBUNTU2004:
            ret = self._am_ubuntu2004_configure(log, host)
        elif distro == os_distro.DISTRO_UBUNTU2204:
            ret = self._am_ubuntu2204_configure(log, host)
        else:
            log.cl_error("OS distro [%s] is not supported",
                         distro)
            return -1
        if ret:
            return -1

        ret = self._am_update(log, host)
        if ret:
            log.cl_error("failed to update apt")
            return -1
        return 0

    def am_is_configured(self, log, host):
        """
        Whether apt has been configured to use the mirror.
        """
        distro = host.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get OS distro on host [%s]",
                         host.sh_hostname)
            return -1

        if distro == os_distro.DISTRO_UBUNTU2004:
            return self._am_ubuntu2004_is_configured(log, host)
        if distro == os_distro.DISTRO_UBUNTU2204:
            return self._am_ubuntu2204_is_configured(log, host)
        log.cl_error("OS distro [%s] on host [%s] is not supported",
                     distro, host.sh_hostname)
        return -1


class AptTinghuaMirror(AptMirror):
    """
    Deb mirror of Tinghua University.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        super().__init__(APT_MIRROR_TSINGHUA)
        self.atm_ubuntu2004_config = \
"""deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-updates main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-backports main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu/ focal-security main restricted universe multiverse"""
        self.atm_ubuntu2204_config = \
"""deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ jammy main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ jammy-updates main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ jammy-backports main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu/ jammy-security main restricted universe multiverse"""

    def _atm_is_configured(self, log, host):
        """
        Check whether this mirror is being used for Ubuntu-2204.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        # pylint: disable=no-self-use
        is_reg = host.sh_path_isreg(log, SOURCE_LIST_FPATH)
        if is_reg < 0:
            log.cl_error("failed to check whether path [%s] is regular file on host [%s]",
                         SOURCE_LIST_FPATH,
                         host.sh_hostname)
            return -1

        if not is_reg:
            log.cl_error("path [%s] is not regular file on host [%s]",
                         SOURCE_LIST_FPATH, host.sh_hostname)
            return -1

        command = "grep tsinghua %s" % SOURCE_LIST_FPATH
        retval = host.sh_run(log, command)
        if retval.cr_exit_status == 0:
            return 1
        return 0

    def _atm_configure(self, log, host, config):
        """
        Replace apt mirror.
        """
        # pylint: disable=no-self-use
        # See https://mirror.tuna.tsinghua.edu.cn/help/ubuntu/ for more
        # information.
        command = "echo '# Configured by Coral' > %s" % SOURCE_LIST_FPATH
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

        for line in config.splitlines():
            command = "sed -i '$a\%s' %s" % (line, SOURCE_LIST_FPATH)
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

    def _am_ubuntu2004_is_configured(self, log, host):
        """
        Check whether this mirror is being used for Ubuntu-2204.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        return self._atm_is_configured(log, host)

    def _am_ubuntu2004_configure(self, log, host):
        """
        Configure the apt mirror for Ubuntu-2204.
        """
        return self._atm_configure(log, host,
                                   self.atm_ubuntu2004_config)

    def _am_ubuntu2204_is_configured(self, log, host):
        """
        Check whether this mirror is being used for Ubuntu-2204.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        return self._atm_is_configured(log, host)

    def _am_ubuntu2204_configure(self, log, host):
        """
        Configure the apt mirror for Ubuntu-2204.
        """
        return self._atm_configure(log, host,
                                   self.atm_ubuntu2204_config)


APT_MIRROR_DICT[APT_MIRROR_TSINGHUA] = AptTinghuaMirror()


def apt_source_list_backup(log, host):
    """
    Backup the source list of apt
    """
    tmp_identity = host.sh_get_identity(log)
    if tmp_identity is None:
        return -1

    identity = APT_BACKUP_REFIX + tmp_identity
    backup_dir = APT_CONFIG_DIR + "/" + identity

    is_reg = host.sh_path_isreg(log, SOURCE_LIST_FPATH)
    if is_reg < 0:
        log.cl_error("failed to check whether path [%s] is regular file on host [%s]",
                     SOURCE_LIST_FPATH,
                     host.sh_hostname)
        return -1

    if not is_reg:
        log.cl_error("path [%s] is not regular file on host [%s]",
                     SOURCE_LIST_FPATH, host.sh_hostname)
        return -1

    command = "mkdir -p %s" % backup_dir
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
    command = "mv %s %s" % (SOURCE_LIST_FPATH, backup_dir)
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


def apt_mirror_configure(log, host, mirror_name=APT_MIRROR_TSINGHUA,
                         force=False):
    """
    Configure the apt mirror.
    """
    if mirror_name not in APT_MIRROR_DICT:
        log.cl_error("unsupported apt mirror [%s]", mirror_name)
        return -1
    mirror = APT_MIRROR_DICT[mirror_name]

    if not force:
        ret = mirror.am_is_configured(log, host)
        if ret < 0:
            log.cl_error("failed to check whether apt mirror [%s] is configured",
                         mirror_name)
            return -1
        if ret:
            log.cl_info("apt mirror [%s] has already been configured "
                        "on host [%s], skipping",
                        mirror_name, host.sh_hostname)
            return 0

    log.cl_info("configuring apt mirror [%s] on host [%s]",
                mirror_name, host.sh_hostname)
    ret = mirror.am_configure(log, host)
    if ret:
        log.cl_error("failed to configure resource list of apt")
        return -1

    ret = mirror.am_is_configured(log, host)
    if ret < 0:
        log.cl_error("failed to check whether apt mirror [%s] is configured",
                     mirror_name)
        return -1
    if ret:
        return 0
    log.cl_error("attemption to configure apt mirror [%s] on host [%s] failed",
                 mirror_name, host.sh_hostname)
    return -1
