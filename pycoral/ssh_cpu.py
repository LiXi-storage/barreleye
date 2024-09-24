"""
Library for cpu information on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import re

class SSHHostCPUMixin():
    """
    Mixin class of SSHHost for network information.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostCPUMixin)
    """
    def sh_lscpu_dict(self, log):
        """
        Return the key/value pair of "lscpu". Key is the name of the field
        before ":" in "lscpu" output. Value is the string after ":" with
        all leading spaces removed.
        """
        if self.sh_cached_lscpu_dict is not None:
            return self.sh_cached_lscpu_dict
        command = "lscpu"
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

        lscpu_dict = {}
        pair_pattern = r"^(?P<key>\S.*): +(?P<value>\S.*)$"
        pair_regular = re.compile(pair_pattern)
        for line in lines:
            match = pair_regular.match(line)
            if not match:
                log.cl_error("command [%s] has unexpected line [%s] on host [%s]",
                             command,
                             line,
                             self.sh_hostname)
                return None
            key = match.group("key")
            value = match.group("value")
            if key in lscpu_dict:
                log.cl_error("duplicated key [%s] of command [%s] on host [%s]",
                             key, command, self.sh_hostname)
                return None
            lscpu_dict[key] = value
        self.sh_cached_lscpu_dict = lscpu_dict
        return lscpu_dict

    def sh_lscpu_field_number(self, log, key):
        """
        Return Number of a key from lscpu
        """
        lscpu_dict = self.sh_lscpu_dict(log)
        if lscpu_dict is None:
            log.cl_error("failed to get info of [lscpu]")
            return -1
        if key not in lscpu_dict:
            log.cl_error("no value for key [%s] in info of [lscpu]",
                         key)
            return -1
        value = lscpu_dict[key]

        try:
            value_number = int(value, 10)
        except ValueError:
            log.cl_error("unexpected value [%s] for key [%s] in info of [lscpu]",
                         value, key)
            return -1
        return value_number

    def sh_socket_number(self, log):
        """
        Return number of physical processor sockets/chips.
        """
        return self.sh_lscpu_field_number(log, "Socket(s)")

    def sh_cores_per_socket(self, log):
        """
        Return number of cores in a single physical processor socket.
        """
        return self.sh_lscpu_field_number(log, "Core(s) per socket")

    def sh_cpus(self, log):
        """
        Return number of logical processors.
        """
        return self.sh_lscpu_field_number(log, "CPU(s)")

    def sh_threads_per_core(self, log):
        """
        Return number of logical threads in a single physical core.
        """
        return self.sh_lscpu_field_number(log, "Thread(s) per core")
