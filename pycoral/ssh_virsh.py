"""
Library for running virsh commands on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import re

class SSHHostVirshMixin():
    """
    Mixin class of SSHHost for virsh management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostVirshMixin)
    """
    def sh_virsh_volume_path_dict(self, log, pool_name):
        """
        Return a dict of name/path got from "virsh vol-list --pool $pool"
        """
        command = "virsh vol-list --pool %s" % pool_name
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None

        lines = retval.cr_stdout.strip().splitlines()
        if len(lines) < 2:
            log.cl_error("two few lines [%s] in output of the command [%s] "
                         "on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         len(lines),
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        data_lines = lines[2:]
        path_dict = {}
        for line in data_lines:
            fields = line.split()
            if len(fields) != 2:
                log.cl_error("unexpected field number [%s] in output line "
                             "[%s] of command [%s] on host [%s]",
                             len(fields), line,
                             command,
                             self.sh_hostname)
                return None
            volume_name = fields[0]
            volume_path = fields[1]
            path_dict[volume_name] = volume_path
        return path_dict

    def sh_virsh_volume_delete(self, log, pool_name, volume_name):
        """
        Delete a volume from virsh
        """
        command = "virsh vol-delete --pool %s %s" % (pool_name, volume_name)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_virsh_dominfo(self, log, hostname, quiet=False):
        """
        Get the virsh dominfo of a domain
        """
        command = ("virsh dominfo %s" % hostname)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            if quiet:
                log.cl_debug("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            else:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        output_pattern = (r"^(?P<key>.+): +(?P<value>.+)$")
        output_regular = re.compile(output_pattern)
        infos = {}
        for line in lines:
            match = output_regular.match(line)
            if match:
                log.cl_debug("matched pattern [%s] with line [%s]",
                             output_pattern, line)
                key = match.group("key")
                value = match.group("value")
                infos[key] = value

        return infos

    def sh_virsh_dominfo_state(self, log, hostname, quiet=False):
        """
        Get the state of a hostname
        """
        dominfos = self.sh_virsh_dominfo(log, hostname, quiet=quiet)
        if dominfos is None:
            if quiet:
                log.cl_debug("failed to get dominfo of [%s] on host [%s]",
                             hostname, self.sh_hostname)
            else:
                log.cl_error("failed to get dominfo of [%s] on host [%s]",
                             hostname, self.sh_hostname)
            return None

        if "State" not in dominfos:
            if quiet:
                log.cl_debug("no [State] in dominfo of [%s] on host [%s]",
                             hostname, self.sh_hostname)
            else:
                log.cl_error("no [State] in dominfo of [%s] on host [%s]",
                             hostname, self.sh_hostname)
            return None
        return dominfos["State"]

    def sh_virsh_detach_domblks(self, log, vm_hostname, filter_string):
        """
        Detach the disk from the host
        """
        command = ("virsh domblklist --inactive --details %s" %
                   (vm_hostname))
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

        for line in retval.cr_stdout.splitlines():
            fields = line.split()
            if (len(fields) != 4 or
                    (fields[0] == "Type" and fields[1] == "Device")):
                continue
            device_type = fields[1]
            target_name = fields[2]
            source = fields[3]
            if device_type != "disk":
                continue

            # Don't detach the disks that can't be detached
            if target_name.startswith("hd"):
                continue

            # Filter the disks with image path
            if source.find(filter_string) == -1:
                continue

            log.cl_info("detaching disk [%s] of VM [%s]",
                        target_name, vm_hostname)

            command = ("virsh detach-disk %s %s --persistent" %
                       (vm_hostname, target_name))
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

            command = "rm -f %s" % source
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
