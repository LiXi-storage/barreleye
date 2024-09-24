"""
Library for filesystem operations on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import re

class SSHHostFilesystemMixin():
    """
    Mixin class of SSHHost for filesystem operations.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostFilesystemMixin)
    """
    def sh_filesystem_mounted(self, log, device, fstype=None, mount_point=None):
        """
        Check whether the device is mounted

        Return -1 on error. Return 1 if mounted. Return 0 if not mounted.
        """
        if device.startswith("/"):
            # Device could be symbol link.
            command = "realpath %s" % device
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            lines = retval.cr_stdout.strip().splitlines()
            if len(lines) != 1:
                log.cl_error("unexpected output of realpath: [%s]",
                             retval.cr_stdout)
                return -1
            real_device = lines[0]
        else:
            real_device = device

        command = "cat /proc/mounts"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status not in [0, 1]:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            fields = line.split()
            if fields[0] not in [device, real_device]:
                continue
            tmp_mount_point = fields[1]
            tmp_fstype = fields[2]

            if mount_point and tmp_mount_point != mount_point:
                log.cl_error("device [%s] is mounted to [%s] on host "
                             "[%s], not expected mount point [%s]",
                             device, tmp_mount_point,
                             self.sh_hostname,
                             mount_point)
                return -1

            if fstype and tmp_fstype != fstype:
                log.cl_error("device [%s] is mounted to [%s] on host "
                             "[%s] with type [%s], not [%s]", device,
                             tmp_mount_point, self.sh_hostname,
                             tmp_fstype, fstype)
                return -1
            return 1
        return 0

    def sh_filesystem_mount(self, log, device, fstype, mount_point,
                            options=None, check_device=True):
        """
        Mount file system
        """
        # pylint: disable=too-many-return-statements
        if check_device:
            retval = self.sh_run(log, "test -b %s" % device)
            if retval.cr_exit_status != 0:
                log.cl_error("device [%s] is not a device", device)
                return -1

        retval = self.sh_run(log, "test -e %s" % mount_point)
        if retval.cr_exit_status != 0:
            retval = self.sh_run(log, "mkdir -p %s" % mount_point)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to create directory [%s]", mount_point)
                return -1

        retval = self.sh_run(log, "test -d %s" % mount_point)
        if retval.cr_exit_status != 0:
            log.cl_error("mount point [%s] is not a directory",
                         mount_point)
            return -1

        ret = self.sh_filesystem_mounted(log, device, fstype, mount_point)
        if ret == 1:
            return 0
        if ret < 0:
            return -1

        option_string = ""
        if options:
            option_string = ("-o %s" % options)
        command = ("mount %s -t %s %s %s" %
                   (option_string, fstype, device, mount_point))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_filesystem_umount(self, log, mount_point):
        """
        Mount file system
        """
        command = ("umount %s" % (mount_point))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_filesystem_type(self, log, path):
        """
        Mount file system
        """
        fstype = None
        command = ("df --output=fstype %s" % (path))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, fstype
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 2:
            log.cl_error("command [%s] has unexpected output, "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, fstype
        fstype = lines[1]
        return 0, fstype

    def sh_filesystem_df(self, log, directory):
        """
        Return the space usage of a file system.
        """
        total = 0
        used = 0
        available = 0
        command = ("df %s" % (directory))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, total, used, available
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 2:
            log.cl_error("command [%s] has unexpected line number on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, total, used, available
        df_pattern = (r"^(?P<device>\S+) +(?P<total>\d+) +(?P<used>\d+) "
                      r"+(?P<available>\d+) +(?P<percentage>\S+) +\S+")
        df_regular = re.compile(df_pattern)
        line = lines[1]
        match = df_regular.match(line)
        if not match:
            log.cl_error("command [%s] has unexpected format on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, total, used, available
        total = int(match.group("total"))
        used = int(match.group("used"))
        available = int(match.group("available"))

        return 0, total, used, available

    def sh_same_filesystem(self, log, file0, file1):
        """
        Check whether two files/dirs are on the same file system.
        If same, return 1. If not same, return 0. -1 on error.
        """
        stat0 = self.sh_stat(log, file0)
        if stat0 is None:
            log.cl_error("failed to stat file [%s] on host [%s]",
                         file0, self.sh_hostname)
            return -1

        stat1 = self.sh_stat(log, file1)
        if stat1 is None:
            log.cl_error("failed to stat file [%s] on host [%s]",
                         file1, self.sh_hostname)
            return -1

        if stat0.st_dev == stat1.st_dev:
            return 1
        return 0
