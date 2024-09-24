"""
Library for detecting OS distribution through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""

from pycoral import os_distro

class SSHHostDistroMixin():
    """
    Mixin class of SSHHost for distribution detection.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostCopyMixin)
    """
    def sh_os_distributor(self, log):
        """
        Return the os distributor, like CentOS or Rocky
        """
        if hasattr(self, "_shdm_cached_os_distributor"):
            return self._shdm_cached_os_distributor
        distro = self.sh_distro(log)
        if distro is None:
            log.cl_error("failed to detect OS distro on host [%s]",
                         self.sh_hostname)
            return None
        return self._shdm_cached_os_distributor

    def sh_distro(self, log):
        """
        Return the distro of this host
        """
        # pylint: disable=too-many-return-statements,too-many-branches,too-many-statements
        if hasattr(self, "_shdm_cached_distro"):
            return self._shdm_cached_distro

        no_lsb = False
        retval = self.sh_run(log, "which lsb_release")
        if retval.cr_exit_status != 0:
            log.cl_debug("lsb_release is needed on host [%s] for accurate "
                         "distro identification", self.sh_hostname)
            no_lsb = True

        if no_lsb:
            command = "cat /etc/redhat-release"
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return None
            output = retval.cr_stdout
            if output.startswith(os_distro.OS_DISTRIBUTOR_CENTOS):
                self._shdm_cached_os_distributor = os_distro.OS_DISTRIBUTOR_CENTOS
            elif output.startswith(os_distro.OS_DISTRIBUTOR_ROCKY):
                self._shdm_cached_os_distributor = os_distro.OS_DISTRIBUTOR_ROCKY
            else:
                # Add more support here
                self._shdm_cached_os_distributor = None

            if (output.startswith("CentOS Linux release 7.") or
                    output.startswith("Red Hat Enterprise Linux Server release 7.")):
                self._shdm_cached_distro = os_distro.DISTRO_RHEL7
                return os_distro.DISTRO_RHEL7
            if (output.startswith("CentOS Linux release 8.") or
                    output.startswith("Red Hat Enterprise Linux Server release 8.") or
                    output.startswith("Red Hat Enterprise Linux release 8.") or
                    output.startswith("Rocky Linux release 8.")):
                self._shdm_cached_distro = os_distro.DISTRO_RHEL8
                return os_distro.DISTRO_RHEL8
            if (output.startswith("Rocky Linux release 9.")):
                self._shdm_cached_distro = os_distro.DISTRO_RHEL9
                return os_distro.DISTRO_RHEL9
            if (output.startswith("CentOS Linux release 6.") or
                    output.startswith("Red Hat Enterprise Linux Server release 6.")):
                self._shdm_cached_distro = os_distro.DISTRO_RHEL6
                return os_distro.DISTRO_RHEL6
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        command = "lsb_release -s -i"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        distributor = retval.cr_stdout.strip('\n')
        self._shdm_cached_os_distributor = distributor

        command = "lsb_release -s -r"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        version = retval.cr_stdout.strip('\n')

        if (distributor in ("RedHatEnterpriseServer", "ScientificSL", "CentOS", "Rocky")):
            if version.startswith("7"):
                self._shdm_cached_distro = os_distro.DISTRO_RHEL7
                return os_distro.DISTRO_RHEL7
            if version.startswith("6"):
                self._shdm_cached_distro = os_distro.DISTRO_RHEL6
                return os_distro.DISTRO_RHEL6
            if version.startswith("8"):
                self._shdm_cached_distro = os_distro.DISTRO_RHEL8
                return os_distro.DISTRO_RHEL8
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "rhel", self.sh_hostname)
            return None
        if distributor == "EnterpriseEnterpriseServer":
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "oel", self.sh_hostname)
            return None
        if distributor == "SUSE LINUX":
            # PATCHLEVEL=$(sed -n -e 's/^PATCHLEVEL = //p' /etc/SuSE-release)
            # version="${version}.$PATCHLEVEL"
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "sles", self.sh_hostname)
            return None
        if distributor == "Fedora":
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "fc", self.sh_hostname)
            return None
        if distributor == "Ubuntu":
            if version == "20.04":
                return os_distro.DISTRO_UBUNTU2004
            if version == "22.04":
                return os_distro.DISTRO_UBUNTU2204
        log.cl_error("unsupported version [%s] of distributor [%s] on host [%s]",
                     version, distributor, self.sh_hostname)
        return None

    def sh_distro_short(self, log):
        """
        Return the short distro of this host
        """
        distro = self.sh_distro(log)
        if distro is None:
            return None
        return os_distro.distro2short(log, distro)
