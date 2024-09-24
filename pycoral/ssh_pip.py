"""
Library for pip management on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
class SSHHostPipMixin():
    """
    Mixin class of SSHHost for pip management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostPipMixin)
    """
    def sh_install_pip3_packages(self, log, pip_packages,
                                 pip_dir=None, quiet=False):
        """
        Install pip3 packages on a host
        """
        if len(pip_packages) == 0:
            return 0
        command = "pip3 install"
        if pip_dir is not None:
            command += " --no-index --find-links %s" % pip_dir
        for pip_package in pip_packages:
            command += " " + pip_package

        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return 0

    def sh_download_pip3_packages(self, log, pip_dir, pip_packages):
        """
        Download pip3 packages to a dir.
        """
        if len(pip_packages) == 0:
            return 0
        log.cl_info("downloading pip3 packages %s to dir [%s] on host [%s]",
                    pip_packages, pip_dir, self.sh_hostname)
        command = ("cd %s && pip3 download" % (pip_dir))
        for pip_package in pip_packages:
            command += " " + pip_package
        retval = self.sh_run(log, command, timeout=None)
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

    def sh_show_pip3_package(self, log, pip_package, quiet=False):
        """
        Get information of pip3 package on a host.
        Return a dict
        """
        command = ("pip3 show %s" % pip_package)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None

        info_dict = {}
        lines = retval.cr_stdout.splitlines()
        seperator = ": "
        for line in lines:
            seperator_index = line.find(seperator)
            if seperator_index <= 0:
                if not quiet:
                    log.cl_error("unexpected output of command [%s] on host [%s], "
                                 "ret = [%d], stdout = [%s], stderr = [%s]",
                                 command,
                                 self.sh_hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                return None
            key = line[:seperator_index]
            value = line[seperator_index + 2:]
            info_dict[key] = value
        return info_dict

    def sh_pip3_package_location(self, log, pip_package):
        """
        Return the location of a pip3 package.
        """
        package_info_dict = self.sh_show_pip3_package(log, pip_package)
        if package_info_dict is None:
            log.cl_error("failed to get info about pip3 package [%s] on "
                         "host [%s]",
                         pip_package, self.sh_hostname)
            return None
        location_key = "Location"
        if location_key not in package_info_dict:
            log.cl_error("info of pip3 packages [%s] does not have [%s] on "
                         "host [%s]",
                         pip_package, location_key, self.sh_hostname)
            return None
        return package_info_dict[location_key]
