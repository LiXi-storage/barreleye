"""
Library for network management on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
from pycoral import utils

class SSHHostNetworkMixin():
    """
    Mixin class of SSHHost for network management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostNetworkMixin)
    """
    def sh_disable_dns(self, log):
        """
        Disable DNS
        """
        command = "> /etc/resolv.conf"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_enable_dns(self, log, dns):
        """
        Disable DNS
        """
        command = "echo 'nameserver %s' > /etc/resolv.conf" % dns
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_check_network_connection(self, log, remote_host, quiet=False):
        """
        Check whether the Internet connection works well
        """
        command = "ping -c 1 %s" % remote_host
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return 0

    def sh_check_internet(self, log):
        """
        Check whether the Internet connection works well
        """
        ret = self.sh_check_network_connection(log, "www.bing.com", quiet=True)
        if ret == 0:
            return 0

        return self.sh_check_network_connection(log, "www.baidu.com")


    def sh_ip_addresses(self, log):
        """
        Return the IPs of the host.
        """
        command = "hostname --all-ip-addresses"
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

        return retval.cr_stdout.split()


    def sh_ip_subnet2interface(self, log, subnet):
        """
        Return the network dev for a subnet.
        subnet: format like "10.0.2.11/22"
        Return interface name like "eth1"
        """
        command = "ip route list %s" % subnet
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected line number of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None
        line = lines[0]
        fields = line.split()
        # Formats:
        # 10.0.0.0/22 dev eth1 proto kernel scope link src 10.0.2.33 metric 10
        # 10.0.0.0/22 dev eth1 proto kernel scope link src 10.0.2.40
        if len(fields) != 11 and len(fields) != 9:
            log.cl_error("unexpected field number of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None
        interface = fields[2]
        return interface

    def sh_ip2mac(self, log, interface, ip):
        """
        !!! Please note arping command is not able to find the MAC if
        the IP belongs to local host.
        Return the mac address from an IP through running arping.
        subnet: format like "10.0.2.11"
        If error, return (-1, None).
        Return interface name like "52:54:00:AE:E3:41"
        """
        command = "hostname -I"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        ips = retval.cr_stdout.split()
        if ip in ips:
            # The IP is on host, arping won't be able to get the MAC.
            # Return the MAC of the interface.
            host_mac = self.sh_interface2mac(log, interface)
            if host_mac is None:
                log.cl_error("failed to get the MAC of interface [%s] "
                             "on host [%s]",
                             interface, self.sh_hostname)
                return -1, None
            # sh_interface2mac returns a MAC with non-capitalized characters
            return 0, str.upper(host_mac)

        command = "arping -I %s %s -c 1" % (interface, ip)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status and len(retval.cr_stdout) == 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None

        lines = retval.cr_stdout.splitlines()
        mac_line = None
        for line in lines:
            if line.startswith("Unicast reply from"):
                mac_line = line
                break

        # Probably no mac with that IP
        if mac_line is None:
            log.cl_debug("no MAC line in stdout of command [%s] "
                         "on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return 0, None

        mac = ""
        started = False
        for char in mac_line:
            if started:
                if char == "]":
                    break
                mac += char
            elif char == "[":
                started = True
        ret = utils.check_mac(mac)
        if ret:
            log.cl_error("invalid MAC [%s] found in stdout of command [%s] "
                         "on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         mac, command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        return 0, mac

    def sh_interface2mac(self, log, interface):
        """
        Return the mac address from an interface.
        interface: format like "eth0"
        Return MAC like "52:54:00:ae:e3:41"
        """
        path = "/sys/class/net/%s/address" % interface
        command = "cat %s" % path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected line number in stdout of command [%s] "
                         "on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None

        mac = lines[0].strip()
        ret = utils.check_mac(mac, capital_letters=False)
        if ret:
            log.cl_error("invalid mac [%s] found in stdout of command [%s] "
                         "on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         mac, command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return mac

    def sh_ip_delete(self, log, ip_address, cidr):
        """
        Delete an IP from the host
        """
        subnet = ip_address + "/" + str(cidr)
        interface = self.sh_ip_subnet2interface(log, subnet)
        if interface is None:
            log.cl_error("failed to get the interface of subnet [%s] "
                         "on host [%s]", subnet, self.sh_hostname)
            return -1

        command = ("ip address delete %s dev %s" %
                   (subnet, interface))
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

        ip_addresses = self.sh_ip_addresses(log)
        if ip_addresses is None:
            log.cl_error("failed to get IP addresses of host [%s]",
                         self.sh_hostname)
            return -1
        if ip_address in ip_addresses:
            log.cl_error("host [%s] still has IP [%s] after removal",
                         self.sh_hostname, ip_address)
            return -1
        return 0

    def sh_ip_add(self, log, ip_address, cidr):
        """
        Add IP to a host.
        # A number, CIDR from network mask, e.g. 22
        """
        subnet = ip_address + "/" + str(cidr)
        interface = self.sh_ip_subnet2interface(log, subnet)
        if interface is None:
            log.cl_error("failed to get the interface of subnet [%s] "
                         "on host [%s]", subnet, self.sh_hostname)
            return -1

        command = ("ip address add %s dev %s" %
                   (subnet, interface))
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

        ip_addresses = self.sh_ip_addresses(log)
        if ip_addresses is None:
            log.cl_error("failed to get IP addresses of host [%s]",
                         self.sh_hostname)
            return -1
        if ip_address not in ip_addresses:
            log.cl_error("host [%s] still does not have IP [%s] after "
                         "adding it",
                         self.sh_hostname, ip_address)
            return -1
        return 0
