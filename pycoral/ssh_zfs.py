"""
Library for zfs on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
class SSHHostZFSMixin():
    """
    Mixin class of SSHHost for ZFS management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostZFSMixin)
    """
    def sh_zfspool_list(self, log):
        """
        Return the ZFS pools
        """
        command = "lsmod | grep ^zfs"
        retval = self.sh_run(log, command)
        if (retval.cr_exit_status == 1 and retval.cr_stdout == ""):
            return []

        ret = self.sh_has_command(log, "zpool")
        if not ret:
            return []

        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        command = "zpool list -o name"
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

        lines = retval.cr_stdout.splitlines()
        if (len(lines) < 1 or
                (lines[0] != "NAME" and lines[0] != "no pools available")):
            log.cl_error("unexpected command result [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return lines[1:]

    def sh_destroy_zfs_pools(self, log):
        """
        Destroy all ZFS pools
        """
        zfs_pools = self.sh_zfspool_list(log)
        if zfs_pools is None:
            log.cl_error("failed to list ZFS pools on host [%s]",
                         self.sh_hostname)
            return -1

        if len(zfs_pools) == 0:
            return 0

        for zfs_pool in zfs_pools:
            command = "zpool destroy %s" % (zfs_pool)
            retval = self.sh_run(log, command, timeout=10)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        zfs_pools = self.sh_zfspool_list(log)
        if zfs_pools is None:
            log.cl_error("failed to list ZFS pools on host [%s]",
                         self.sh_hostname)
            return -1

        if len(zfs_pools) > 0:
            log.cl_error("failed to destroy all ZFS pools on host [%s], "
                         "still has pools %s",
                         self.sh_hostname, zfs_pools)
            return -1
        return 0

    def sh_zfs_get_srvname(self, log, device):
        """
        Get lustre:svname property of ZFS
        """
        property_name = "lustre:svname"
        command = ("zfs get -H %s %s" % (property_name, device))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        fields = retval.cr_stdout.split('\t')
        if len(fields) != 4:
            log.cl_error("invalid output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command, self.sh_hostname,
                         retval.cr_stdout)
            return None

        if fields[0] != device or fields[1] != property_name:
            log.cl_error("invalid output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command, self.sh_hostname,
                         retval.cr_stdout)
            return None

        if fields[2] == "-" or fields[3] == "-":
            log.cl_error("no property [%s] of device [%s] on host [%s], "
                         "stdout = [%s]",
                         property_name, device, self.sh_hostname,
                         retval.cr_stdout)
            return None

        return fields[2]
