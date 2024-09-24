"""
Library for Selinux management on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
class SSHHostSelinuxMixin():
    """
    Mixin class of SSHHost for Selinux management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostSelinuxMixin)
    """
    def sh_selinux_status(self, log):
        """
        Check the current status of SELinux
        """
        command = "getenforce"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout.strip()

    def sh_disable_selinux(self, log):
        """
        Disable SELinux permanently
        """
        command = "sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        status = self.sh_selinux_status(log)
        if status in ("Disabled", "Permissive"):
            log.cl_debug("SELinux is already [%s] on host [%s]",
                         status, self.sh_hostname)
            return 0

        command = "setenforce 0"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        status = self.sh_selinux_status(log)
        if status not in ("Disabled", "Permissive"):
            log.cl_error("SELinux still has [%s] status on host [%s] after "
                         "command running [%s]",
                         status, self.sh_hostname, command)
            return -1
        return 0
