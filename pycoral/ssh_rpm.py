"""
Library for RPM/YUM management on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import re
import datetime
import traceback
from pycoral import os_distro
from pycoral import utils


def rpm_name2version(log, rpm_name):
    """
    From RPM name to version.
    collectd-5.12.0.barreleye0-1.el7.x86_64 -> 5.12.0.barreleye0-1.el7.x86_64
    """
    minus_index = -1
    for current_index in range(len(rpm_name) - 1):
        if (rpm_name[current_index] == "-" and
                rpm_name[current_index + 1].isdigit()):
            minus_index = current_index
            break
    if minus_index == -1:
        log.cl_error("RPM [%s] on host [%s] does not have expected format",
                     rpm_name)
        return None
    rpm_version = rpm_name[minus_index + 1:]
    return rpm_version


class SSHHostRPMMixin():
    """
    Mixin class of SSHHost for RPM/YUM management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostRPMMixin)
    """
    def sh_yum_install_nocheck(self, log, rpms, extra_option=None):
        """
        Yum install RPMs without checking
        """
        distro = self.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get distro of host [%s]",
                         self.sh_hostname)
            return -1
        if distro not in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8):
            log.cl_error("unsupported distro [%s] of host [%s]",
                         distro, self.sh_hostname)
            return -1
        if distro == os_distro.DISTRO_RHEL8:
            command = "dnf install -y --nobest"
        else:
            command = "yum install -y"
        for rpm in rpms:
            command += " %s" % rpm
        if extra_option is not None:
            if not extra_option.startswith(" "):
                command += " "
            command += extra_option

        log.cl_info("installing RPMs [%s] on host [%s] through yum",
                    utils.list2string(rpms),
                    self.sh_hostname)
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

    def sh_yum_install(self, log, rpms, extra_option=None):
        """
        Yum install RPMs
        """
        # pylint: disable=too-many-branches
        missing_rpms = []
        for rpm in rpms:
            ret = self.sh_rpm_query(log, rpm)
            if ret != 0:
                missing_rpms.append(rpm)

        if len(missing_rpms) == 0:
            return 0

        ret = self.sh_yum_install_nocheck(log, missing_rpms,
                                          extra_option=extra_option)
        if ret:
            return -1

        missing_rpms = []
        for rpm in rpms:
            ret = self.sh_rpm_query(log, rpm)
            if ret != 0:
                missing_rpms.append(rpm)

        if len(missing_rpms) > 0:
            log.cl_error("rpms [%s] are still missing after yum install",
                         utils.list2string(missing_rpms))
            return -1
        return 0

    def sh_rpm_version(self, log, rpm_keyword):
        """
        Return the RPM version. The rpm_keyword will be used to filter the
        installed RPMs. And a series of RPMs could be matched. The series of
        RPMs should share the same version. And the version number should start
        with a number. And the number should follows a "-".

        collectd-filedata-5.12.0.barreleye0-1.el7.x86_64
        collectd-5.12.0.barreleye0-1.el7.x86_64

        kmod-lustre-client-2.12.6_874_g59b0328-1.el7.x86_64
        lustre-client-2.12.6_874_g59b0328-1.el7.x86_64
        """
        command = "rpm -qa | grep %s" % rpm_keyword
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 1 and retval.cr_stdout == "":
            return 0, None
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        rpm_names = retval.cr_stdout.splitlines()
        if len(rpm_names) == 0:
            log.cl_error("unexpected stdout of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        version = None
        for rpm_name in rpm_names:
            rpm_version = rpm_name2version(log, rpm_name)
            if rpm_version is None:
                log.cl_error("failed to get version of RPM [%s] on host [%s]",
                             rpm_name, self.sh_hostname)
                return -1, None

            if version is None:
                version = rpm_version
            elif rpm_version != version:
                log.cl_error("RPM [%s] on host [%s] has unexpected version "
                             "[%s],  expected [%s]", rpm_name, rpm_version,
                             version)
                return -1, None
        return 0, version

    def sh_has_rpm(self, log, rpm_name):
        """
        Check whether rpm is installed in the system.
        """
        command = "rpm -qi %s" % rpm_name
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            return False
        return True

    def sh_kernel_has_rpm(self, log):
        """
        Check whether the current running kernel has RPM installed, if not,
        means the RPM has been uninstalled.
        """
        kernel_version = self.sh_get_kernel_ver(log)
        if kernel_version is None:
            return False

        rpm_name = "kernel-" + kernel_version
        return self.sh_has_rpm(log, rpm_name)

    def sh_rpm_find_and_uninstall(self, log, find_command, option=""):
        """
        Find and uninstall RPM on the host
        """
        command = "rpm -qa | %s" % find_command
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            for rpm in retval.cr_stdout.splitlines():
                log.cl_debug("uninstalling RPM [%s] on host [%s]",
                             rpm, self.sh_hostname)
                retval = self.sh_run(log, "rpm -e %s --nodeps %s" % (rpm, option))
                if retval.cr_exit_status != 0:
                    log.cl_error("failed to uninstall RPM [%s] on host [%s], "
                                 "ret = %d, stdout = [%s], stderr = [%s]",
                                 rpm, self.sh_hostname,
                                 retval.cr_exit_status, retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1
        elif (retval.cr_exit_status == 1 and
              len(retval.cr_stdout) == 0):
            log.cl_debug("no rpm can be find by command [%s] on host [%s], "
                         "no need to uninstall",
                         command, self.sh_hostname)
        else:
            log.cl_error("unexpected result of command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_check_rpm_file_integrity(self, log, fpath, quiet=False):
        """
        Check the integrity of a RPM file
        """
        command = "rpm -K --nosignature %s" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return 0

    def sh_rpm_query(self, log, rpm_name):
        """
        Find RPM on the host
        """
        command = "rpm -q %s" % rpm_name
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            return -1
        return 0

    def sh_rpm_install_time(self, log, rpm_name, quiet=False):
        """
        Return the installation time of a RPM
        """
        command = "rpm -q --queryformat '%%{installtime:day}' %s" % rpm_name
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
        try:
            install_time = datetime.datetime.strptime(retval.cr_stdout,
                                                      "%a %b %d %Y")
        except:
            if not quiet:
                log.cl_error("invalid output [%s] of command [%s] on host [%s] "
                             "for date: %s",
                             retval.cr_stdout, command,
                             self.sh_hostname,
                             traceback.format_exc())
            return None
        return install_time

    def sh_rpm_checksig(self, log, rpm_fpath):
        """
        Run "rpm --checksig" on a RPM
        """
        command = "rpm --checksig %s" % rpm_fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            return -1
        return 0

    def sh_yumdb_info(self, log, rpm_name):
        """
        Get the key/value pairs of a RPM from yumdb
        """
        command = "yumdb info %s" % rpm_name
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
        output_pattern = (r"^ +(?P<key>\S+) = (?P<value>.+)$")
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

    def sh_yumdb_sha256(self, log, rpm_name):
        """
        Get the SHA256 checksum of a RPM from yumdb
        """
        rpm_infos = self.sh_yumdb_info(log, rpm_name)
        if rpm_infos is None:
            log.cl_error("failed to get YUM info of [%s] on host [%s]",
                         rpm_name, self.sh_hostname)
            return None

        if ("checksum_data" not in rpm_infos or
                "checksum_type" not in rpm_infos):
            log.cl_error("failed to get YUM info of [%s] on host [%s]",
                         rpm_name, self.sh_hostname)
            return None

        if rpm_infos["checksum_type"] != "sha256":
            log.cl_error("unexpected checksum type of RPM [%s] on host [%s], "
                         "expected [sha256], got [%s]",
                         rpm_name, self.sh_hostname,
                         rpm_infos["checksum_type"])
            return None

        return rpm_infos["checksum_data"]

    def sh_unpack_rpm(self, log, rpm_fpath, target_dir):
        """
        Unpack the RPM to the target dir
        """
        command = ("mkdir -p %s && cd %s && rpm2cpio %s | cpio -idmv" %
                   (target_dir, target_dir, rpm_fpath))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_yum_repo_ids(self, log):
        """
        Get the repo IDs of yum
        """
        command = "yum repolist"
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

        # RHEL7 has some garbage lines before the column of "repo id". We need
        # to filter out these garbage.
        lines = retval.cr_stdout.splitlines()
        found_column = False
        repo_lines = []
        for line in lines:
            if line.startswith("repo id"):
                found_column = True
            if found_column:
                repo_lines.append(line)
        if not found_column:
            log.cl_error("unexpected stdout of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        repo_ids = []
        for line in repo_lines:
            fields = line.split()
            repo_field = fields[0]
            # RHEL7 has repo IDs like "base/7/x86_64", only get the prefix here.
            # Roky8 has repo IDs like "baseos".
            repo_fields = repo_field.split("/")
            repo_id = repo_fields[0]
            repo_ids.append(repo_id)
        return repo_ids
