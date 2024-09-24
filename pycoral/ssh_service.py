"""
Library for service management on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""

class SSHHostServiceMixin():
    """
    Mixin class of SSHHost for service management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostServiceMixin)
    """
    def sh_service_is_active(self, log, service_name):
        """
        If is active, return 1. If inactive, return 0. Return -1 on error.
        """
        command = "systemctl is-active %s" % service_name
        retval = self.sh_run(log, command)
        # Unknown service means this service might have not been configured
        if retval.cr_stdout == "unknown\n":
            return 0

        if retval.cr_stdout == "inactive\n":
            return 0

        if retval.cr_stdout == "failed\n":
            return 0

        if retval.cr_stdout in ["active\n", "activating\n"]:
            return 1

        log.cl_error("unexpecteed result of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_stop(self, log, service_name):
        """
        Stop the service if it is running
        """
        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        # Unknown service means this service might have not been configured
        if retval.cr_stdout == "unknown\n":
            return 0

        if retval.cr_stdout == "inactive\n":
            return 0

        if retval.cr_stdout == "failed\n":
            return 0

        if retval.cr_stdout in ["active\n", "activating\n"]:
            command = ("systemctl stop %s" % (service_name))
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            command = ("systemctl is-active %s" % (service_name))
            retval = self.sh_run(log, command)
            if retval.cr_stdout == "unknown\n":
                return 0

            if retval.cr_stdout == "inactive\n":
                return 0

            if retval.cr_stdout == "failed\n":
                return 0
            log.cl_error("failed to stop service [%s] on host [%s], "
                         "command = [%s], ret = [%d], stdout = [%s], stderr = [%s]",
                         service_name,
                         self.sh_hostname,
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_start(self, log, service_name):
        """
        Start the service if it is not running
        """
        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 0

        command = ("systemctl start %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 0

        log.cl_error("failed to start service [%s] on host [%s], "
                     "command = [%s], ret = [%d], stdout = [%s], "
                     "stderr = [%s]",
                     service_name,
                     self.sh_hostname,
                     command,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_restart(self, log, service_name):
        """
        Restart the service if it is not running
        """
        command = ("systemctl restart %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 0

        log.cl_error("failed to restart service [%s] after checking it "
                     "using command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     service_name,
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_is_enabled(self, log, service_name):
        """
        Check whether service is enabled.
        """
        command = "systemctl is-enabled %s" % service_name
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "disabled\n":
            return 0

        if retval.cr_stdout == "enabled\n":
            return 1

        log.cl_error("unexpected command of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_disable(self, log, service_name):
        """
        Disable the service from starting automatically
        """
        command = ("systemctl disable %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status and retval.cr_stderr != "":
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_service_check_exists(self, log, service_name):
        """
        Check whether service exist.
        """
        command = ("systemctl cat -- %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            return -1
        return 0

    def sh_service_enable(self, log, service_name):
        """
        Disable the service from starting automatically
        """
        command = ("systemctl enable %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        return 0

    def sh_service_start_enable(self, log, service_name, restart=False):
        """
        Start and enable a service
        """
        skip_wait = False
        if restart:
            ret = self.sh_service_restart(log, service_name)
            command = "restart"
        else:
            command = ("systemctl is-active %s" % (service_name))
            retval = self.sh_run(log, command)
            if retval.cr_stdout == "active\n":
                skip_wait = True
                ret = 0
            else:
                ret = self.sh_service_start(log, service_name)
            command = "start"
        if ret:
            log.cl_error("failed to %s service [%s] on host [%s]",
                         command, service_name, self.sh_hostname)
            return -1

        ret = self.sh_service_enable(log, service_name)
        if ret:
            log.cl_error("failed to enable service [%s] on host [%s]",
                         service_name, self.sh_hostname)
            return -1

        if skip_wait:
            return 0

        command = ("systemctl status %s" % service_name)
        ret = self.sh_wait_update(log, command, diff_exit_status=0,
                                  timeout=5, quiet=True)
        if ret:
            # Still active after timeout
            return 0
        log.cl_error("service [%s] is not active on host [%s]",
                     service_name, self.sh_hostname)
        return -1
