"""
Library for rebooting host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import time
from pycoral import constant

class SSHHostRebootMixin():
    """
    Mixin class of SSHHost for distribution detection.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostRebootMixin)
    """
    def sh_is_up(self, log, timeout=60):
        """
        Whether this host is up now
        """
        ret = self.sh_run(log, "true", timeout=timeout)
        if ret.cr_exit_status != 0:
            return False
        return True

    def sh_wait_up(self, log, timeout=constant.LONGEST_TIME_REBOOT):
        """
        Wait until the host is up
        """
        return self.sh_wait_update(log, "true", expect_exit_status=0,
                                   timeout=timeout)

    def sh_get_uptime(self, log, quiet=False):
        """
        Get the uptime of the host
        """
        command = ("expr $(date +%s) - $(cat /proc/uptime | "
                   "awk -F . '{print $1}')")
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            if not quiet:
                log.cl_error("can't get uptime on host [%s], command = [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             self.sh_hostname, command,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return int(retval.cr_stdout)

    def sh_reboot_issue(self, log, force=False):
        """
        Issuing the reboot command on host
        """
        log.cl_debug("issuing rebooting of host [%s]",
                     self.sh_hostname)

        # Check whether the host is up first, to avoid unncessary wait
        if not self.sh_is_up(log):
            log.cl_error("unable to issue reboot of host [%s] because it is "
                         "not up", self.sh_hostname)
            return -1

        if force:
            retval = self.sh_run(log, "echo b > /proc/sysrq-trigger &")
        else:
            retval = self.sh_run(log, "reboot &")

        # We can't really trust the return value or output, sometimes the
        # reboot is so quick that the pipe might be broken and print
        # "Write failed: Broken pipe", so do not check the return value or
        # outputs at all, just wait and check after running the command.
        log.cl_debug("issued reboot on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return 0


    def sh_rebooted(self, log, uptime_before_reboot):
        """
        Check whether the host rebooted
        """
        if uptime_before_reboot == 0:
            log.cl_debug("host [%s] does not even start rebooting",
                         self.sh_hostname)
            return False

        if not self.sh_is_up(log):
            log.cl_debug("host [%s] is not up yet",
                         self.sh_hostname)
            return False

        uptime = self.sh_get_uptime(log, quiet=True)
        if uptime < 0:
            return False

        if (uptime_before_reboot + constant.SHORTEST_TIME_REBOOT >
                uptime):
            log.cl_debug("the uptime of host [%s] doesn't look "
                         "like rebooted, uptime now: [%d], uptime "
                         "before reboot: [%d], keep on waiting",
                         self.sh_hostname, uptime,
                         uptime_before_reboot)
            return False
        return True

    def sh_wait_reboot(self, log, uptime_before_rebooted):
        """
        Wait until host rebooted
        """
        log.cl_info("waiting until host [%s] rebooted", self.sh_hostname)
        time_start = time.time()
        sleep_interval = 1
        while not self.sh_rebooted(log, uptime_before_rebooted):
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed < constant.LONGEST_TIME_REBOOT:
                time.sleep(sleep_interval)
                continue
            log.cl_error("booting of host [%s] timeouts after [%f] seconds",
                         self.sh_hostname, elapsed)
            return -1
        return 0

    def sh_reboot(self, log):
        """
        Reboot the host
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        log.cl_info("rebooting host [%s]", self.sh_hostname)

        # Do not try to reboot myself!
        if self.sh_is_localhost():
            log.cl_error("will not reboot host [%s], because it is local host",
                         self.sh_hostname)
            return -1

        none_force_issued = False
        uptime = self.sh_get_uptime(log)
        if uptime < 0:
            log.cl_error("failed to get uptime on host [%s]",
                         self.sh_hostname)
            return -1

        force = False
        ret = self.sh_run(log, "sync", timeout=120)
        if ret.cr_exit_status:
            log.cl_error("failed to sync on host [%s], force reboot",
                         self.sh_hostname)
            force = True

        if not force:
            ret = self.sh_reboot_issue(log, force=False)
            if ret:
                log.cl_error("failed to issue none-force reboot on host [%s]",
                             self.sh_hostname)
                return -1

            none_force_issued = True
            if self.sh_wait_reboot(log, uptime):
                log.cl_info("none-force reboot of host [%s] failed, trying "
                            "force reboot", self.sh_hostname)
            else:
                log.cl_info("none-force reboot of host [%s] finished",
                            self.sh_hostname)
                return 0

        ret = self.sh_reboot_issue(log, force=True)
        if ret:
            if not none_force_issued:
                log.cl_error("failed to issue force reboot on host [%s]",
                             self.sh_hostname)
                return -1
            log.cl_info("failed to issue force reboot on host [%s], but "
                        "none-force reboot was issued successfully, "
                        "waiting again in case reboot takes longer than "
                        "expected",
                        self.sh_hostname)

        if self.sh_wait_reboot(log, uptime):
            log.cl_error("reboot of host [%s] failed",
                         self.sh_hostname)
            return -1

        log.cl_info("reboot of host [%s] finished",
                    self.sh_hostname)
        return 0

    def sh_poweroff_issue(self, log, force=False):
        """
        Issuing the poweroff command on host
        """
        log.cl_debug("issuing poweroff of host [%s]",
                     self.sh_hostname)

        # Check whether the host is up first, to avoid unncessary wait
        if not self.sh_is_up(log):
            log.cl_error("unable to issue poweroff of host [%s] because it is "
                         "not up", self.sh_hostname)
            return -1

        if force:
            retval = self.sh_run(log, "echo o > /proc/sysrq-trigger &")
        else:
            retval = self.sh_run(log, "poweroff &")

        # We can't really trust the return value or output, sometimes the
        # shutdown is so quick that the pipe might be broken and print
        # "Write failed: Broken pipe", so do not check the return value or
        # outputs at all, just wait and check after running the command.
        log.cl_debug("issued shutdown on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return 0
