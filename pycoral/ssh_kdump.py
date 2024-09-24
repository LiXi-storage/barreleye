"""
Use SSH to manage kdump on a remote host.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import os
import socket
from pycoral import constant

class SSHHostKdumpMixin():
    """
    Mixin class of SSHHost for distribution detection.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostKdumpMixin)
    """
    def sh_kdump_init(self, log):
        """
        Init kdump in case of kernel crash
        """
        command = "grep ^path /etc/kdump.conf | awk '{print $2}'"
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

        if retval.cr_stdout != "":
            # The dir that saves crash dump of kdump
            self._shkm_kdump_dir_path = retval.cr_stdout.strip()
            log.cl_debug("kdump path on host [%s] is configured as [%s]",
                         self.sh_hostname, self._shkm_kdump_dir_path)
        else:
            self._shkm_kdump_dir_path = "/var/crash"
            log.cl_info("kdump path on host [%s] is not configured, using "
                        "default value [%s]",
                        self.sh_hostname, self._shkm_kdump_dir_path)

        command = "ls %s" % self._shkm_kdump_dir_path
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
        self._shkm_kdump_subdirs = retval.cr_stdout.strip().split("\n")
        return 0

    def sh_kdump_get(self, log, local_kdump_path,
                     check_timeout=constant.LONGEST_SIMPLE_COMMAND_TIME):
        """
        Return the new kdump number since the last copy
        If local_kdump_path is None, do not copy.
        """
        local_hostname = socket.gethostname()
        command = "ls %s" % self._shkm_kdump_dir_path
        retval = self.sh_run(log, command, timeout=check_timeout)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        kdump_subdirs = retval.cr_stdout.strip().split("\n")

        ret = 0
        for subdir in kdump_subdirs:
            if subdir in self._shkm_kdump_subdirs:
                continue
            kdump_dir = ("%s/%s" % (self._shkm_kdump_dir_path, subdir))
            if local_kdump_path is None:
                log.cl_info("found a new crash dump [%s] on host [%s]",
                            kdump_dir, self.sh_hostname)
            else:
                log.cl_info("found a new crash dump [%s] on host [%s], "
                            "copying to [%s] on local host [%s]",
                            kdump_dir, self.sh_hostname,
                            local_kdump_path, local_hostname)

                if not os.path.exists(local_kdump_path):
                    os.makedirs(local_kdump_path)

                rc = self.sh_get_file(log, kdump_dir, local_kdump_path)
                if rc:
                    log.cl_error("failed to get file [%s] from host [%s] to "
                                 "path [%s] on local host [%s]",
                                 kdump_dir, self.sh_hostname,
                                 local_kdump_path, local_hostname)
                    return -1
                self._shkm_kdump_subdirs.append(subdir)
            ret += 1
        return ret
