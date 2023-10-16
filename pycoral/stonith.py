# pylint: disable=too-many-lines
"""
Shoot The Other Node In The Head

Use Libvirt, IPMI or other ways to accomplish Stonith
"""
import time
import socket

# The logest time that a shutdown will takes
LONGEST_TIME_POWER_OFF = 60
# The logest time that a power on will takes
LONGEST_TIME_POWER_ON = 60


class StonithHost():
    """
    General class of stonith for each host
    """
    SH_POWER_ERROR = "error"
    SH_POWER_ON = "on"
    SH_POWER_OFF = "off"
    SH_POWER_IN_SHUTDOWN = "in shutdown"
    SH_POWER_PAUSED = "paused"

    def __init__(self, host):
        self.sth_host = host

    def sth_start(self, log):
        """
        Start the host if it is off
        """
        log.cl_error("start method is not implemented for host [%s]",
                     self.sth_host.sh_hostname)
        return -1

    def sth_raw_stop(self, log):
        """
        Stop the host with a low-level way

        If this command fails, we have no further way to stop the host
        """
        log.cl_error("raw_stop method is not implemented for host [%s]",
                     self.sth_host.sh_hostname)
        return -1

    def sth_power_status(self, log):
        """
        The low-level way to determine the power status of the host

        Return SH_POWER_*
        If a host is power on, it is still possible that we can't use
        it or ssh to it. If a host is poweroff, we definitely can't use
        it or ssh to it.
        """
        log.cl_error("power_status method is not implemented for host [%s]",
                     self.sth_host.sh_hostname)
        return StonithHost.SH_POWER_ERROR

    def sth_wait_power(self, log, timeout, power_on=True, check_interval=1):
        """
        Wait until the lower-level way says this host is power off or on
        """
        if power_on:
            operation = "power on"
        else:
            operation = "power off"
        time_start = time.time()
        while True:
            status = self.sth_power_status(log)
            if ((power_on and status == StonithHost.SH_POWER_ON) or
                    (not power_on and status == StonithHost.SH_POWER_OFF)):
                return 0
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed < timeout:
                time.sleep(check_interval)
                continue
            log.cl_error("%s of host [%s] timeouts after [%f] seconds",
                         operation, self.sth_host.sh_hostname, elapsed)
            return -1
        return 0

    def sth_wait_power_off(self, log, check_interval=1, timeout=LONGEST_TIME_POWER_OFF):
        """
        Wait until the lower-level way says this host is power off
        """
        return self.sth_wait_power(log, timeout, power_on=False, check_interval=check_interval)

    def sth_wait_power_on(self, log, check_interval=1, timeout=LONGEST_TIME_POWER_ON):
        """
        Wait until the lower-level way says this host is power on
        """
        return self.sth_wait_power(log, timeout, power_on=True, check_interval=check_interval)

    def sth_stop(self, log):
        """
        Stop the host with gentle ways first, then forcefully ways.

        First try with gentle shutdown from ssh.
        Then try with foce shutdown from ssh.
        Then try with the raw stop.
        """
        host = self.sth_host
        hostname = host.sh_hostname
        local_hostname = socket.gethostname()
        if local_hostname == hostname:
            log.cl_error("will not stop host [%s], because it is localhost",
                         hostname)
            return -1

        force = False
        ret = host.sh_run(log, "sync", timeout=60)
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to sync on host [%s], force shutdown",
                           hostname)
            force = True

        if not force:
            ret = host.sh_poweroff_issue(log, force=False)
            if ret:
                log.cl_warning("failed to issue none-force shutdown on host [%s], trying foce shutdown",
                               hostname)
            else:
                ret = self.sth_wait_power_off(log, timeout=60)
                if ret == 0:
                    log.cl_info("none-force shutdown of host [%s] finished",
                                hostname)
                    return 0

        ret = host.sh_poweroff_issue(log, force=True)
        if ret:
            log.cl_warning("failed to issue force shutdown on host [%s], trying raw shutdown",
                           hostname)
        else:
            ret = self.sth_wait_power_off(log, timeout=30)
            if ret == 0:
                log.cl_info("force shutdown of host [%s] finished",
                            hostname)
                return 0

        ret = self.sth_raw_stop(log)
        if ret:
            log.cl_error("failed to stop host [%s]",
                         hostname)
            return -1
        log.cl_info("host [%s] is down now", hostname)
        return 0


class LibvirtStonithHost(StonithHost):
    """
    Stonith for each host based on Libvirt.
    """
    def __init__(self, server_host, guest_host, domain_name=None):
        super().__init__(guest_host)
        # Type: ssh_host.SSHHost
        self.lvsh_server_host = server_host
        if domain_name is None:
            # The KVM domain name seen in libvirt
            self.lvsh_domain_name = self.sth_host.sh_hostname
        else:
            self.lvsh_domain_name = domain_name

    def sth_power_status(self, log):
        """
        The low-level way to determine the power status of the host

        Return SH_POWER_*
        If a host is power on, it is still possible that we can't use
        it or ssh to it. If a host is poweroff, we definitely can't use
        it or ssh to it.
        """
        server_host = self.lvsh_server_host
        guest_hostname = self.sth_host.sh_hostname
        state = server_host.sh_virsh_dominfo_state(log, self.lvsh_domain_name)
        if state == "running":
            return StonithHost.SH_POWER_ON
        if state == "shut off":
            return StonithHost.SH_POWER_OFF
        if state == "in shutdown":
            return StonithHost.SH_POWER_IN_SHUTDOWN
        if state == "paused":
            return StonithHost.SH_POWER_PAUSED
        if state is None:
            log.cl_error("failed to get power status of KVM guest [%s]",
                         guest_hostname)
            return StonithHost.SH_POWER_ERROR
        log.cl_error("unknown power status [%s] of KVM guest [%s]", state,
                     guest_hostname)
        return StonithHost.SH_POWER_ERROR

    def sth_start(self, log):
        """
        Start the host if it is off. If the host is down, poweroff first
        and then power on.
        """
        # pylint: disable=too-many-branches
        server_host = self.lvsh_server_host
        host = self.sth_host
        guest_hostname = host.sh_hostname

        need_power_off = True
        log.cl_debug("checking whether host [%s] is power on", guest_hostname)
        status = self.sth_power_status(log)
        if status == StonithHost.SH_POWER_ERROR:
            log.cl_warning("failed to check the power status of host [%s], "
                           "will power off first",
                           guest_hostname)
        elif status == StonithHost.SH_POWER_ON:
            if host.sh_is_up(log):
                return 0

            # If the host is power on but not up, it is unlikely that it is
            # booting and will boot successfully, because the boot time is
            # short and it is unlikely we are so *luckily* to hit the window.
            # So, do not waste time waitting for booting.
            log.cl_info("host [%s] is not up even it is power on, will "
                        "power off first",
                        guest_hostname)
        elif status == StonithHost.SH_POWER_PAUSED:
            log.cl_info("host [%s] is paused, waiting until host up",
                        guest_hostname)
            ret = host.sh_wait_up(log)
            if ret:
                log.cl_info("host [%s] is paused for a long time, "
                            "will power off", guest_hostname)
            else:
                return 0
        elif status == StonithHost.SH_POWER_IN_SHUTDOWN:
            ret = self.sth_wait_power_off(log, timeout=20)
            if ret == 0:
                log.cl_error("host [%s] cannot goto power off status from "
                             "in shutdown status",
                             guest_hostname)
                return -1
            need_power_off = False
        elif status == StonithHost.SH_POWER_OFF:
            need_power_off = False
        else:
            log.cl_warning("unsupported power status [%s] of host [%s]",
                           status, guest_hostname)

        if need_power_off:
            log.cl_info("stopping host [%s] using \"virsh destroy\"",
                        guest_hostname)
            ret = self.sth_raw_stop(log)
            if ret:
                log.cl_error("failed to raw stop host [%s]",
                             guest_hostname)
                return -1

        log.cl_info("starting host [%s] using \"virsh start\"",
                    guest_hostname)
        command = ("virsh start %s" % (self.lvsh_domain_name))
        retval = server_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         server_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_info("waiting until host [%s] power on", guest_hostname)
        ret = self.sth_wait_power_on(log, timeout=10)
        if ret:
            log.cl_error("failed to power on KVM guest [%s]",
                         guest_hostname)
            return -1

        log.cl_info("waiting until host [%s] up", guest_hostname)
        ret = host.sh_wait_up(log)
        if ret:
            log.cl_error("host [%s] is not up after power on",
                         guest_hostname)
            return -1

        return 0

    def sth_raw_stop(self, log):
        """
        Stop the VM using virsh.
        """
        server_host = self.lvsh_server_host
        host = self.sth_host
        guest_hostname = host.sh_hostname

        command = ("virsh destroy %s" % (self.lvsh_domain_name))
        retval = server_host.sh_run(log, command)
        # The command could fail if the host has been shutdown,
        # so do not check the result, just wait the host to
        # power off later.
        if retval.cr_exit_status:
            log.cl_debug("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         server_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)

        ret = self.sth_wait_power_off(log, timeout=10)
        if ret:
            log.cl_error("KVM guest [%s] is still not powered off after raw stopping",
                         guest_hostname)
            return -1
        return 0


class IMPIStonithHost(StonithHost):
    """
    Stonith for each host based on IPMI.
    """
    def __init__(self, ipmi_interface, ipmi_username, ipmi_password,
                 ipmi_hostname, ipmi_port, host, command_host):
        super().__init__(host)
        # Remote host name for LAN interface.
        self.ipmish_hostname = ipmi_hostname
        # Remote RMCP port.
        self.ipmish_port = ipmi_port
        # Remote session username.
        self.ipmish_username = ipmi_username
        # Remote session password.
        self.ipmish_password = ipmi_password
        # Interface to use, e.g. lanplus
        self.ipmish_interface = ipmi_interface
        # Prefix of ipmi commands
        self.ipmish_command_prefix = ("ipmitool -I %s -U %s -P %s -H %s -p %s" %
                                      (self.ipmish_interface,
                                       self.ipmish_username,
                                       self.ipmish_password,
                                       self.ipmish_hostname,
                                       self.ipmish_port))
        # Host to run IPMI commands
        self.ipmish_command_host = command_host

    def sth_power_status(self, log):
        """
        The low-level way to determine the power status of the host

        Return SH_POWER_*
        """
        command_host = self.ipmish_command_host
        command = self.ipmish_command_prefix + " power status"
        retval = command_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         command_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return StonithHost.SH_POWER_ERROR
        if retval.cr_stdout == "Chassis Power is on\n":
            return StonithHost.SH_POWER_ON
        if retval.cr_stdout == "Chassis Power is off\n":
            return StonithHost.SH_POWER_OFF

        log.cl_error("unknown stdout [%s] of command [%s] on host [%s]",
                     retval.cr_stdout, command, command_host.sh_hostname)
        return StonithHost.SH_POWER_ERROR

    def sth_start(self, log):
        """
        Start the host if it is off. If the host is down, poweroff first
        and then power on.
        """
        # pylint: disable=too-many-branches
        command_host = self.ipmish_command_host
        host = self.sth_host
        guest_hostname = host.sh_hostname

        need_power_off = True
        log.cl_debug("checking whether host [%s] is power on", guest_hostname)
        status = self.sth_power_status(log)
        if status == StonithHost.SH_POWER_ERROR:
            log.cl_warning("failed to check the power status of host [%s], "
                           "will power off first",
                           guest_hostname)
        elif status == StonithHost.SH_POWER_ON:
            if host.sh_is_up(log):
                return 0

            # If the host is power on but not up, it is unlikely that it is
            # booting and will boot successfully, because the boot time is
            # short and it is unlikely we are so *luckily* to hit the window.
            # So, do not waste time waitting for booting.
            log.cl_info("host [%s] is not up even it is power on, will "
                        "power off first",
                        guest_hostname)
        elif status == StonithHost.SH_POWER_OFF:
            need_power_off = False
        else:
            log.cl_warning("unsupported power status [%s] of host [%s]",
                           status, guest_hostname)

        if need_power_off:
            log.cl_info("stopping host [%s] using IPMI commands",
                        guest_hostname)
            ret = self.sth_raw_stop(log)
            if ret:
                log.cl_error("failed to raw stop host [%s]",
                             guest_hostname)
                return -1

        log.cl_info("starting host [%s] using IPMI commands",
                    guest_hostname)
        command = self.ipmish_command_prefix + " power on"
        retval = command_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         command_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_info("waiting until host [%s] power on", guest_hostname)
        ret = self.sth_wait_power_on(log, timeout=30)
        if ret:
            log.cl_error("failed to power on host [%s]",
                         guest_hostname)
            return -1

        log.cl_info("waiting until host [%s] up", guest_hostname)
        ret = host.sh_wait_up(log)
        if ret:
            log.cl_error("host [%s] is not up after power on",
                         guest_hostname)
            return -1

        return 0

    def sth_raw_stop(self, log):
        """
        Stop the host using IPMI commands.
        """
        command_host = self.ipmish_command_host
        host = self.sth_host
        guest_hostname = host.sh_hostname

        command = self.ipmish_command_prefix + " power off"
        retval = command_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         command_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret = self.sth_wait_power_off(log, timeout=30)
        if ret:
            log.cl_error("host [%s] is still not powered off after raw stopping",
                         guest_hostname)
            return -1
        return 0
