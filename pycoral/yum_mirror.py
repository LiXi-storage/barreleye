"""
Library to manage yum repository.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
from pycoral import os_distro

YUM_MIRROR_ALIYUN = "aliyun"
YUM_MIRROR_DEFAULT = "default"
YUM_MIRROR_DICT = {}
YUM_BACKUP_REFIX = "coral."
YUM_REPOSITORY_DIR = "/etc/yum.repos.d"


def yum_backup_repository(log, host):
    """
    Backup the current repository config
    """
    tmp_identity = host.sh_get_identity(log)
    if tmp_identity is None:
        return -1
    identity = YUM_BACKUP_REFIX + tmp_identity
    backup_dir = YUM_REPOSITORY_DIR + "/" + identity

    fnames = host.sh_get_dir_fnames(log, YUM_REPOSITORY_DIR)
    if fnames is None:
        log.cl_error("failed to get fnames under [%s]",
                     YUM_REPOSITORY_DIR)
        return -1
    nondir_fpaths = []
    for fname in fnames:
        fpath = YUM_REPOSITORY_DIR + "/" + fname
        ret = host.sh_path_isdir(log, fpath)
        if ret < 0:
            log.cl_error("failed to check whether file is a dir",
                         fpath)
            return -1
        if ret:
            continue
        nondir_fpaths.append(fpath)

    if len(nondir_fpaths) == 0:
        return 0

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
    for fpath in nondir_fpaths:
        command = "mv %s %s" % (fpath, backup_dir)
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


class YumMirror():
    """
    Each mirror has an object of this type.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, mirror_name):
        # Name of the mirror.
        self.ym_mirror_name = mirror_name

    def _ym_centos7_base_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL7 base repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        # pylint: disable=unused-argument
        log.cl_error("centos7_base_is_configured not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_centos7_base_configure(self, log, host):
        """
        Configure the mirror for RHEL7 base repository.
        """
        # pylint: disable=unused-argument
        log.cl_error("centos7_base_configure not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_epel7_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL7 EPEL repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        # pylint: disable=unused-argument
        log.cl_error("rhel7_epel_is_configured not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_epel7_configure(self, log, host):
        """
        Configure the mirror for RHEL7 epel repository.
        """
        # pylint: disable=unused-argument
        log.cl_error("rhel7_epel_configure not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_centos7_configure(self, log, host):
        """
        Configure the mirror for RHEL7 epel/base repository.
        """
        ret = self._ym_centos7_base_configure(log, host)
        if ret:
            log.cl_error("failed to configure base repository")
            return -1
        ret = self._ym_epel7_configure(log, host)
        if ret:
            log.cl_error("failed to configure epel repository")
        return 0

    def _ym_centos7_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL7 base/epel repository.
        """
        ret = self._ym_centos7_base_is_configured(log, host)
        if ret < 0:
            log.cl_error("failed to check whether base repository is configured")
            return -1
        if ret == 0:
            return 0
        ret = self._ym_epel7_is_configured(log, host)
        if ret < 0:
            log.cl_error("failed to check whether epel repository is configured")
        if ret == 0:
            return 0
        return 1

    def _ym_centos8_base_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL8 base repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        # pylint: disable=unused-argument
        log.cl_error("centos8_base_is_configured not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_centos8_base_configure(self, log, host):
        """
        Configure the mirror for RHEL8 base repository.
        """
        # pylint: disable=unused-argument
        log.cl_error("centos8_base_configure not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_epel8_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL8 EPEL repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        # pylint: disable=unused-argument
        log.cl_error("rhel8_epel_is_configured not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_epel8_configure(self, log, host):
        """
        Configure the mirror for RHEL8 epel repository.
        """
        # pylint: disable=unused-argument
        log.cl_error("rhel8_epel_configure not implemented for "
                     "mirror [%s]", self.ym_mirror_name)
        return -1

    def _ym_centos8_configure(self, log, host):
        """
        Configure the mirror for RHEL8 epel/base repository.
        """
        ret = self._ym_centos8_base_configure(log, host)
        if ret:
            log.cl_error("failed to configure base repository")
            return -1
        ret = self._ym_epel8_configure(log, host)
        if ret:
            log.cl_error("failed to configure epel repository")
        return 0

    def _ym_centos8_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL8 base/epel repository.
        """
        ret = self._ym_centos8_base_is_configured(log, host)
        if ret < 0:
            log.cl_error("failed to check whether base repository is configured")
            return -1
        if ret == 0:
            return 0
        ret = self._ym_epel8_is_configured(log, host)
        if ret < 0:
            log.cl_error("failed to check whether epel repository is configured")
        if ret == 0:
            return 0
        return 1

    def _ym_yum_reset(self, log, host):
        """
        Cleanup the yum repository, otherwise, might hit problem like:

        https://mirrors.xxx/epel/7/x86_64/repodata/xxx-filelists.sqlite.bz2:
        [Errno 14] HTTPS Error 404 - Not Found
        Trying other mirror.
        """
        # pylint: disable=no-self-use
        cmds = ["yum clean all",
                "rpm --rebuilddb"]
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
        return 0

    def ym_configure(self, log, host):
        """
        Configure the mirror for epel/base repository.
        """
        # pylint: disable=too-many-branches
        distro = host.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get OS distro on host [%s]",
                         host.sh_hostname)
            return -1

        if distro not in (os_distro.DISTRO_RHEL7,
                          os_distro.DISTRO_RHEL8):
            log.cl_error("OS distro [%s] is not supported",
                         distro)
            return -1

        os_distributor = host.sh_os_distributor(log)
        if os_distributor not in (os_distro.OS_DISTRIBUTOR_CENTOS,
                                  os_distro.OS_DISTRIBUTOR_ROCKY):
            log.cl_error("OS distributor [%s] is not supported",
                         os_distributor)
            return -1

        if distro == os_distro.DISTRO_RHEL7:
            ret = yum_backup_repository(log, host)
            if ret:
                log.cl_error("failed to backup repository")
                return -1
            ret = self._ym_centos7_configure(log, host)
        elif distro == os_distro.DISTRO_RHEL8:
            if os_distributor == os_distro.OS_DISTRIBUTOR_CENTOS:
                ret = yum_backup_repository(log, host)
                if ret:
                    log.cl_error("failed to backup repository")
                    return -1
                ret = self._ym_centos8_configure(log, host)
            elif os_distributor == os_distro.OS_DISTRIBUTOR_ROCKY:
                # Do not backup other respository, only EPEL
                ret = self._ym_epel8_configure(log, host)
            else:
                log.cl_error("OS distributor [%s] is not supported for distro [%s]",
                             os_distributor, distro)
                return -1
        else:
            log.cl_error("OS distro [%s] is not supported",
                         distro)
            return -1
        if ret:
            return -1
        ret = self._ym_yum_reset(log, host)
        if ret:
            log.cl_error("failed to reset yum")
            return -1
        return 0

    def ym_is_configured(self, log, host):
        """
        Whether epel/base repository has been configured to the mirror.
        """
        distro = host.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get OS distro on host [%s]",
                         host.sh_hostname)
            return -1

        os_distributor = host.sh_os_distributor(log)
        if os_distributor not in (os_distro.OS_DISTRIBUTOR_CENTOS,
                                  os_distro.OS_DISTRIBUTOR_ROCKY):
            log.cl_error("OS distributor [%s] is not supported",
                         os_distributor)
            return -1

        if distro == os_distro.DISTRO_RHEL7:
            return self._ym_centos7_is_configured(log, host)
        if distro == os_distro.DISTRO_RHEL8:
            if os_distributor == os_distro.OS_DISTRIBUTOR_CENTOS:
                return self._ym_centos8_is_configured(log, host)
            if os_distributor == os_distro.OS_DISTRIBUTOR_ROCKY:
                return self._ym_epel8_is_configured(log, host)
            log.cl_error("OS distributor [%s] is not supported for distro [%s]",
                         os_distributor, distro)
            return -1
        log.cl_error("OS distro [%s] on host [%s] is not supported",
                     distro, host.sh_hostname)
        return -1


class YumAliyunMirror(YumMirror):
    """
    Yum mirror of Aliyun.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        super().__init__(YUM_MIRROR_ALIYUN)
        # key word of repo
        self.yam_repo_keyword = ".coral.aliyun"
        # Fname of Centos Base repo.
        self.yam_centos_base_repo_fname = "CentOS-Base" + self.yam_repo_keyword + ".repo"
        # Fpath of Centos Base repo.
        self.yam_centos_base_repo_fpath = (YUM_REPOSITORY_DIR + "/" +
                                           self.yam_centos_base_repo_fname)
        # Fname of epel repo.
        self.yam_epel_repo_fname = "epel" + self.yam_repo_keyword + ".repo"
        # Fpath of Centos Base repo.
        self.yam_epel_repo_fpath = (YUM_REPOSITORY_DIR + "/" +
                                    self.yam_epel_repo_fname)

    def _ym_centos7_base_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL7 base repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        fnames = host.sh_get_dir_fnames(log, YUM_REPOSITORY_DIR)
        if fnames is None:
            log.cl_error("failed to get fnames under [%s]",
                         YUM_REPOSITORY_DIR)
            return -1
        configured = bool(self.yam_centos_base_repo_fname in fnames)
        return configured

    def _ym_centos7_base_configure(self, log, host):
        """
        Set the mirror for RHEL7 base repository.
        """
        url = "https://mirrors.aliyun.com/repo/Centos-7.repo"
        expected_checksum = None
        output_fname = self.yam_centos_base_repo_fname
        fpath = self.yam_centos_base_repo_fpath
        ret = host.sh_download_file(log, url, fpath,
                                    expected_checksum=expected_checksum,
                                    use_curl=True,
                                    output_fname=output_fname,
                                    download_anyway=True)
        if ret:
            log.cl_error("failed to download repository file")
            return -1
        return 0

    def _ym_epel7_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL7 epel repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        fnames = host.sh_get_dir_fnames(log, YUM_REPOSITORY_DIR)
        if fnames is None:
            log.cl_error("failed to get fnames under [%s]",
                         YUM_REPOSITORY_DIR)
            return -1
        configured = bool(self.yam_epel_repo_fname in fnames)
        return configured

    def _ym_epel7_configure(self, log, host):
        """
        Configure the mirror for RHEL7 epel repository.
        """
        url = "https://mirrors.aliyun.com/repo/epel-7.repo"
        expected_checksum = None
        output_fname = self.yam_epel_repo_fname
        fpath = self.yam_epel_repo_fpath
        ret = host.sh_download_file(log, url, fpath,
                                    expected_checksum=expected_checksum,
                                    use_curl=True,
                                    download_anyway=True,
                                    output_fname=output_fname)
        if ret:
            log.cl_error("failed to download repository file")
            return -1
        return 0

    def _ym_centos8_base_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL8 base repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        return self._ym_centos7_base_is_configured(log, host)

    def _ym_centos8_base_configure(self, log, host):
        """
        Set the mirror for RHEL8 base repository.
        """
        suffix = ".tmp"
        tmp_fpath = self.yam_centos_base_repo_fpath + suffix
        url = "https://mirrors.aliyun.com/repo/Centos-vault-8.5.2111.repo"
        expected_checksum = None
        output_fname = self.yam_centos_base_repo_fname + suffix
        ret = host.sh_download_file(log, url, tmp_fpath,
                                    expected_checksum=expected_checksum,
                                    use_curl=True,
                                    output_fname=output_fname,
                                    download_anyway=True)
        if ret:
            log.cl_error("failed to download repository file")
            return -1

        command = ("sed -i 's|^enabled=0|enabled=1|' %s" % tmp_fpath)
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

        command = ("mv %s %s" %
                   (tmp_fpath, self.yam_centos_base_repo_fpath))
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

    def _ym_epel8_is_configured(self, log, host):
        """
        Check whether this mirror is being used for RHEL8 EPEL repository.
        Return -1 on error. Return 1 if yes, 0 if no.
        """
        return self._ym_epel7_is_configured(log, host)

    def _ym_epel8_configure(self, log, host):
        """
        Set the mirror for RHEL8 epel repository.
        """
        # pylint: disable=too-many-locals
        command = "rpm -e epel-release"
        host.sh_run(log, command)
        # ignore the error since the RPM might not exists

        retval = host.sh_run(log, command)
        rpm = "https://mirrors.aliyun.com/epel/epel-release-latest-8.noarch.rpm"
        ret = host.sh_yum_install_nocheck(log, [rpm])
        if ret:
            return -1

        command = "rpm -ql epel-release"
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
        fpaths = retval.cr_stdout.splitlines()
        repository_names = []
        prefix = YUM_REPOSITORY_DIR + "/"
        prefix_length = len(prefix)
        suffix = ".repo"
        suffix_length = len(suffix)
        for fpath in fpaths:
            if not fpath.startswith(prefix):
                continue
            if not fpath.endswith(suffix):
                continue
            if len(fpath) <= prefix_length + suffix_length:
                continue
            repository_name = fpath[prefix_length:-suffix_length]
            repository_names.append(repository_name)

        for repository_name in repository_names:
            source = (YUM_REPOSITORY_DIR + "/" +
                      repository_name + ".repo")
            dest = (YUM_REPOSITORY_DIR + "/" +
                    repository_name +
                    self.yam_repo_keyword + ".repo")
            tmp = dest + ".tmp"
            command = "cp %s %s" % (source, tmp)
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

            command = ("sed -i 's|^#baseurl=https://download.example/pub|"
                       "baseurl=https://mirrors.aliyun.com|' "
                       "/etc/yum.repos.d/epel*")
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

            command = ("sed -i 's|^metalink|#metalink|' "
                       "/etc/yum.repos.d/epel*")
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

            command = "mv %s %s" % (tmp, dest)
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

        command = "rpm -e epel-release"
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


YUM_MIRROR_DICT[YUM_MIRROR_ALIYUN] = YumAliyunMirror()


def yum_mirror_configure(log, host, mirror_name=YUM_MIRROR_ALIYUN,
                         force=False):
    """
    Configure the yum mirror.
    """
    if mirror_name not in YUM_MIRROR_DICT:
        log.cl_error("unsupported yum mirror [%s]", mirror_name)
        return -1
    mirror = YUM_MIRROR_DICT[mirror_name]

    if not force:
        ret = mirror.ym_is_configured(log, host)
        if ret < 0:
            log.cl_error("failed to check whether yum mirror [%s] is configured",
                         mirror_name)
            return -1
        if ret:
            if ret == 1:
                log.cl_info("yum mirror [%s] has already been configured "
                            "on host [%s], skipping",
                            mirror_name, host.sh_hostname)
            return 0

    log.cl_info("configuring yum mirror [%s] on host [%s]",
                mirror_name, host.sh_hostname)
    ret = mirror.ym_configure(log, host)
    if ret:
        log.cl_error("failed to configure repositories")
        return -1

    ret = mirror.ym_is_configured(log, host)
    if ret < 0:
        log.cl_error("failed to check whether yum mirror [%s] is configured",
                     mirror_name)
        return -1
    if ret:
        return 0
    log.cl_error("attemption to configure yum mirror [%s] on host [%s] failed",
                 mirror_name, host.sh_hostname)
    return -1
