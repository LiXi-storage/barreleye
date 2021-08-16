"""
Library for installing a tool from ISO

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
# pylint: disable=too-many-lines
import socket
import os
import sys
from pycoral import utils
from pycoral import ssh_host
from pycoral import constant
from pycoral import parallel


def find_iso_path_in_cwd(log, host, iso_path_pattern):
    """
    Find iso path in current work directory
    """
    command = ("ls %s" % (iso_path_pattern))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None

    current_dir = os.getcwd()
    iso_names = retval.cr_stdout.split()
    if len(iso_names) != 1:
        log.cl_error("found unexpected ISOs %s under currect directory [%s]",
                     iso_names, current_dir)
        return None

    iso_name = iso_names[0]

    if iso_name.startswith("/"):
        return iso_name
    return current_dir + "/" + iso_name


def generate_repo_file(repo_fpath, packages_dir, package_name):
    """
    Prepare the local repo config file
    """
    repo_config = ("""# %s packages
[%s]
name=%s Packages
baseurl=file://%s
priority=1
gpgcheck=0
enabled=1
gpgkey=
""" % (package_name, package_name, package_name, packages_dir))
    with open(repo_fpath, 'w', encoding='utf-8') as config_fd:
        config_fd.write(repo_config)


def yum_repo_install(log, host, repo_fpath, rpms):
    """
    Install RPMs using YUM
    """
    if len(rpms) == 0:
        return 0

    repo_ids = host.sh_yum_repo_ids(log)
    if repo_ids is None:
        log.cl_error("failed to get the yum repo IDs on host [%s]",
                     host.sh_hostname)
        return -1

    command = "yum -c %s" % repo_fpath
    for repo_id in repo_ids:
        command += " --disablerepo %s" % repo_id

    command += " install -y"
    for rpm in rpms:
        command += " %s" % rpm

    retval = host.sh_run(log, command, timeout=ssh_host.LONGEST_TIME_YUM_INSTALL)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    missing_rpms = []
    for rpm in rpms:
        ret = host.sh_rpm_query(log, rpm)
        if ret != 0:
            missing_rpms.append(rpm)
    if len(missing_rpms) != 0:
        log.cl_error("rpms %s are still missing on host [%s] after yum install",
                     missing_rpms, host.sh_hostname)
        return -1
    return 0


def sync_iso_dir(log, workspace, host, iso_pattern, dest_iso_dir,
                 use_cp=False):
    """
    Sync the files in the iso to the directory
    """
    # pylint: disable=too-many-branches
    # Just to check ssh to local host works well
    retval = host.sh_run(log, "true", timeout=60)
    if retval.cr_exit_status:
        log.cl_error("failed to ssh to local host [%s] using user [%s], "
                     "stdout = [%s], stderr = [%s]",
                     host.sh_hostname,
                     host.sh_login_name,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    if not iso_pattern.startswith("/"):
        iso_pattern = os.getcwd() + "/" + iso_pattern
    fnames = host.sh_resolve_path(log, iso_pattern, quiet=True)
    if fnames is None or len(fnames) == 0:
        log.cl_error("failed to find ISO with pattern [%s] on local host [%s]",
                     iso_pattern, host.sh_hostname)
        return -1
    if len(fnames) > 1:
        log.cl_info("multiple ISOs found by pattern [%s] on local host [%s]",
                    iso_pattern, host.sh_hostname)
        return -1
    iso_path = fnames[0]

    is_dir = host.sh_path_isdir(log, iso_path)
    if is_dir < 0:
        log.cl_error("failed to check whether path [%s] is dir or not on host [%s]",
                     iso_path, host.sh_hostname)
        return -1
    if is_dir == 0:
        iso_dir = workspace + "/mnt/" + utils.random_word(8)
        command = ("mkdir -p %s && mount -o loop %s %s" %
                   (iso_dir, iso_path, iso_dir))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
    else:
        iso_dir = iso_path

    ret = 0
    log.cl_info("syncing ISO to [%s] on host [%s]",
                dest_iso_dir, host.sh_hostname)
    cmds = ["mkdir -p %s" % dest_iso_dir]
    if use_cp:
        cmds.append("rm -fr %s/*" % (dest_iso_dir))
        cmds.append("cp -a %s/* %s" % (iso_dir, dest_iso_dir))
    else:
        cmds.append("rsync --delete -a %s/ %s" % (iso_dir, dest_iso_dir))
    for command in cmds:
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            ret = -1
            break

    if iso_dir != iso_path:
        cmds = ["umount %s" % iso_dir,
                "rmdir %s" % iso_dir]
        for command in cmds:
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                ret = -1
    return ret


def install_pip3_packages_from_cache(log, local_host, pip_packages,
                                     pip_dir, quiet=False):
    """
    Install pip3 packages on local host and make sure it can be imported later.
    """
    ret = local_host.sh_install_pip3_packages(log, pip_packages,
                                              pip_dir, quiet=quiet)
    if ret:
        if not quiet:
            log.cl_error("failed to install pip3 packages %s in dir "
                         "[%s] on localhost [%s]",
                         pip_packages, pip_dir, local_host.sh_hostname)
        return -1

    for pip_package in pip_packages:
        location = local_host.sh_pip3_package_location(log, pip_package)
        if location is None:
            log.cl_error("failed to get the location of pip3 packages [%s] on "
                         "host [%s]",
                         pip_package, local_host.sh_hostname)
            return -1

        if location not in sys.path:
            sys.path.append(location)
    return 0


def install_dependency_from_iso(log, workspace, local_host, iso_dir,
                                missing_rpms, missing_pips, name):
    """
    Install the dependent RPMs and pip packages after ISO is synced
    """
    repo_config_fpath = ("%s/%s.repo" % (workspace, name))
    packages_dir = iso_dir + "/" + constant.BUILD_PACKAGES

    command = ("mkdir -p %s" % (workspace))
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on localhost [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    generate_repo_file(repo_config_fpath, packages_dir, name)
    ret = yum_repo_install(log, local_host, repo_config_fpath, missing_rpms)
    if ret:
        log.cl_error("failed to install RPMs %s on local host [%s]",
                     missing_rpms, local_host.sh_hostname)
        return -1

    pip_dir = iso_dir + "/" + constant.BUILD_PIP
    ret = install_pip3_packages_from_cache(log, local_host, missing_pips,
                                           pip_dir)
    if ret:
        log.cl_error("failed to install missing pip packages %s in dir "
                     "[%s] on localhost [%s]",
                     missing_pips, pip_dir, local_host.sh_hostname)
        return -1

    return 0


def coral_rpm_reinstall(log, host, iso_path):
    """
    Reinstall the Coral RPMs
    """
    log.cl_info("reinstalling Coral RPMs on host [%s]", host.sh_hostname)
    package_dir = iso_path + "/" + constant.BUILD_PACKAGES

    # Use RPM upgrade so that the running services will be restarted by RPM
    # scripts.
    command = ("rpm -Uvh %s/coral-*.rpm --force --nodeps" % (package_dir))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return 0


class CoralInstallationHost():
    """
    Each host that needs installation has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, workspace, host, is_local, iso_dir, pip_libs,
                 dependent_rpms, send_fpath_dict, need_backup_fpaths,
                 coral_reinstall=True, tsinghua_mirror=False):
        self.cih_workspace = workspace
        # ISO dir that configured in repo file
        self.cih_iso_dir = iso_dir
        # pip dir under ISO dir
        self.cih_pip_dir = iso_dir + "/" + constant.BUILD_PIP
        # Host
        self.cih_host = host
        # Whether this host is localhost
        self.cih_is_local = is_local
        # The Pip packages to install
        self.cih_pip_libs = pip_libs
        # The RPMs to install
        self.cih_dependent_rpms = dependent_rpms
        # Key is the the source path on localhost, value is the target
        # path on this host
        self.cih_send_fpath_dict = send_fpath_dict
        # File paths to backup before reinstalling and restore after
        # reinstalling
        self.cih_need_backup_fpaths = need_backup_fpaths
        # Reinstall Coral RPMs
        self.cih_coral_reinstall = coral_reinstall
        # A list of Coral service names to preserve. If the service is
        # running before reinstalling Coral RPMs, it should be restarted
        # after reinstalling Coral RPMs. Usually, these services should
        # already been restarted in the RPM post-installation script, but the
        # restarting process could fail in some circumstances. So the services
        # will be started for several times until timeout.
        #
        # Note this list will be shrinked when services are up running.
        self.cih_preserve_services = []
        # Whether to use YUM and pip mirror from Tsinghua University
        self.cih_tsinghua_mirror = tsinghua_mirror

    def _cih_send_iso_dir(self, log):
        """
        Send the dir of ISO to a host
        """
        host = self.cih_host
        if self.cih_is_local:
            return 0

        log.cl_info("syncing ISO dir from local host [%s] "
                    "to host [%s]", socket.gethostname(),
                    host.sh_hostname)
        target_dirname = os.path.dirname(self.cih_iso_dir)
        command = ("mkdir -p %s" % (target_dirname))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret = host.sh_send_file(log, self.cih_iso_dir, target_dirname,
                                delete_dest=True)
        if ret:
            log.cl_error("failed to send dir [%s] on local host to "
                         "directory [%s] on host [%s]",
                         self.cih_iso_dir, target_dirname,
                         host.sh_hostname)
            return -1

        return 0

    def _cih_backup_file(self, log, fpath):
        """
        Backup files to workspace
        """
        host = self.cih_host
        dirname = os.path.dirname(fpath)
        backup_dirname = self.cih_workspace + dirname
        command = ("mkdir -p %s" % (backup_dirname))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        command = ("cp -a %s %s" % (fpath, backup_dirname))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _cih_restore_file(self, log, fpath):
        """
        Backup files to workspace
        """
        random_words = utils.random_word(8)
        host = self.cih_host
        backup_fpath = self.cih_workspace + "/" + fpath
        command = ("mv %s %s_backup_%s" % (fpath, fpath, random_words))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        dirname = os.path.dirname(fpath)
        command = ("cp -a %s %s" % (backup_fpath, dirname))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _cih_config_rsyslog(self, log):
        """
        Configure the rsyslog so that SSH won't cause too many messages
        """
        host = self.cih_host
        log.cl_info("updating [/etc/rsyslog.conf] on host [%s] to avoid "
                    "flood of messages",
                    host.sh_hostname)
        target_rule = "daemon.none;auth.none;mail.none;authpriv.none;cron.none"
        command = "grep \"%s\" /etc/rsyslog.conf" % target_rule
        retval = host.sh_run(log, command)
        if retval.cr_exit_status == 0:
            # Already changed before
            pass
        elif retval.cr_exit_status != 1 or retval.cr_stdout != "":
            # Error
            log.cl_error("unexpected result of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        else:
            # Not configured
            origin_rule = "mail.none;authpriv.none;cron.none"
            command = ("sed -i 's/\*.info;%s/\*.info;%s/' /etc/rsyslog.conf" % (origin_rule, target_rule))
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        command = ("systemctl restart rsyslog")
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _cih_disable_selinux(self, log):
        """
        Disable the SELinux on the host
        """
        host = self.cih_host
        log.cl_info("disabling SELinux on host [%s]",
                    host.sh_hostname)
        ret = host.sh_disable_selinux(log)
        if ret:
            log.cl_error("failed to disable SELinux on host [%s]",
                         host.sh_hostname)
            return -1
        return 0

    def _cih_disable_firewalld(self, log):
        """
        Disable the firewalld on the host
        """
        host = self.cih_host
        log.cl_info("disabling firewalld on host [%s]",
                    host.sh_hostname)
        service_name = "firewalld"
        ret = host.sh_service_stop(log, service_name)
        if ret:
            log.cl_error("failed to stop service [%s] on host [%s]",
                         service_name, host.sh_hostname)
            return -1

        ret = host.sh_service_disable(log, service_name)
        if ret:
            log.cl_error("failed to disable service [%s] on host [%s]",
                         service_name, host.sh_hostname)
            return -1
        return 0

    def _cih_services_pre_preserve(self, log):
        """
        Check the service status before reinstallation
        """
        host = self.cih_host
        command = "rpm -qa | grep coral-"
        retval = host.sh_run(log, command)
        if retval.cr_exit_status == 0:
            rpm_names = retval.cr_stdout.splitlines()
        elif (retval.cr_exit_status == 1 and
              len(retval.cr_stdout) == 0):
            log.cl_debug("no rpm can be find by command [%s] on host [%s], "
                         "no need to uninstall",
                         command, host.sh_hostname)
            rpm_names = []
        else:
            log.cl_error("unexpected result of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        service_prefix = "/usr/lib/systemd/system/"
        installed_services = []
        for rpm_name in rpm_names:
            command = "rpm -ql %s | grep %s" % (rpm_name, service_prefix)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status == 0:
                systemd_files = retval.cr_stdout.splitlines()
            elif (retval.cr_exit_status == 1 and
                  len(retval.cr_stdout) == 0):
                log.cl_debug("no files can be find by command [%s] on host [%s], "
                             "no need to uninstall",
                             command, host.sh_hostname)
                systemd_files = []
            else:
                log.cl_error("unexpected result of command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
            for systemd_file in systemd_files:
                if not systemd_file.startswith(service_prefix):
                    log.cl_error("unexpected file name [%s] in RPM [%s] on "
                                 "host [%s]", systemd_file, rpm_name,
                                 host.hostname)
                    return -1
                # This service name has ".service" tail. But that is fine.
                service_name = systemd_file[len(service_prefix):]
                installed_services.append(service_name)

        for service_name in installed_services:
            command = ("systemctl is-active %s" % (service_name))
            retval = host.sh_run(log, command)
            if retval.cr_stdout == "active\n":
                self.cih_preserve_services.append(service_name)
        return 0

    def _cih_services_try_preserve(self, log):
        """
        Try start the services after reinstallation
        """
        host = self.cih_host
        rc = 0
        for service_name in self.cih_preserve_services[:]:
            ret = host.sh_service_start(log, service_name)
            if ret:
                log.cl_warning("failed to start service [%s] on host [%s]",
                               service_name, host.sh_hostname)
                rc = -1
            else:
                self.cih_preserve_services.remove(service_name)
        return rc

    def _cih_services_preserve(self, log, timeout=90):
        """
        Start the services after reinstallation with a timeout.
        """
        if len(self.cih_preserve_services) == 0:
            return 0

        service_str = ""
        for service in self.cih_preserve_services:
            if service_str == "":
                service_str = service
            else:
                service_str += ", " + service
        if service_str != "":
            log.cl_info("preserving services [%s] after reinstalling Coral RPMs "
                        "on host [%s]",
                        service_str, self.cih_host.sh_hostname)
        ret = utils.wait_condition(log, self._cih_services_try_preserve,
                                   (), timeout=timeout)
        if ret:
            service_string = ""
            for service_name in self.cih_preserve_services:
                if service_string == "":
                    service_string += service_name
                else:
                    service_string += ", " + service_name
            log.cl_error("failed to start services [%s] on host [%s] after "
                         "trying for [%s] seconds",
                         service_string, self.cih_host.sh_hostname,
                         timeout)
        return ret

    def cih_install(self, log, repo_config_fpath, disable_selinux=True,
                    disable_firewalld=True):
        """
        Install the RPMs and pip packages on a host, and send the files
        :param send_fpath_dict: key is the fpath on localhost, value is the
        fpath on remote host.
        """
        # pylint: disable=too-many-statements,too-many-branches,too-many-locals
        pip_libs = self.cih_pip_libs
        dependent_rpms = self.cih_dependent_rpms
        send_fpath_dict = self.cih_send_fpath_dict
        need_backup_fpaths = self.cih_need_backup_fpaths
        coral_reinstall = self.cih_coral_reinstall
        host = self.cih_host
        hostname = host.sh_hostname
        # only support RHEL7 series
        distro = host.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get distro of host [%s]",
                         hostname)
            return -1
        if distro not in [ssh_host.DISTRO_RHEL7, ssh_host.DISTRO_RHEL8]:
            log.cl_error("unsupported distro [%s] of host [%s]",
                         distro, hostname)
            return -1

        ret = self._cih_config_rsyslog(log)
        if ret:
            log.cl_error("failed to configure rsyslog on host [%s]",
                         hostname)
            return -1

        if disable_selinux:
            ret = self._cih_disable_selinux(log)
            if ret:
                log.cl_error("failed to disable SELinux on host [%s]",
                             hostname)
                return -1

        if disable_firewalld:
            ret = self._cih_disable_firewalld(log)
            if ret:
                log.cl_error("failed to disable firewalld on host [%s]",
                             hostname)
                return -1

        ret = self._cih_send_iso_dir(log)
        if ret:
            log.cl_error("failed to syncing ISO dir from local host [%s] "
                         "to host [%s]", socket.gethostname(), hostname)
            return -1
        log.cl_info("installing dependent RPMs on host [%s]",
                    hostname)
        log.cl_debug("installing dependent RPMs %s and pip package %s on "
                     "host [%s]",
                     dependent_rpms, pip_libs, hostname)
        if not self.cih_is_local:
            command = ("mkdir -p %s" % (self.cih_workspace))
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            ret = host.sh_send_file(log, repo_config_fpath, self.cih_workspace)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "directory [%s] on host [%s]",
                             repo_config_fpath, self.cih_workspace,
                             hostname)
                return -1

        # If local file, backup the send files in case overwritten by RPMs/pip
        if self.cih_is_local:
            for fpath in need_backup_fpaths:
                self._cih_backup_file(log, fpath)

        if self.cih_tsinghua_mirror:
            ret = yum_replace_to_tsinghua(log, host)
            if ret:
                log.cl_error("failed to replace YUM mirrors to Tsinghua "
                             "University on host [%s]",
                             hostname)
                return -1

        ret = yum_repo_install(log, host, repo_config_fpath,
                               dependent_rpms)
        if ret:
            log.cl_error("failed to install dependent RPMs on host "
                         "[%s]", hostname)
            return -1

        ret = host.sh_install_pip3_packages(log, pip_libs,
                                            self.cih_pip_dir)
        if ret:
            log.cl_error("failed to install missing pip packages %s in "
                         "dir [%s] on host [%s]",
                         pip_libs, self.cih_pip_dir, host.sh_hostname)
            return -1

        if coral_reinstall:
            ret = self._cih_services_pre_preserve(log)
            if ret:
                log.cl_error("failed to prepare for preserving Coral "
                             "services on host [%s]",
                             hostname)
                return -1

            ret = coral_rpm_reinstall(log, host, self.cih_iso_dir)
            if ret:
                log.cl_error("failed to install Coral RPMs on host [%s]",
                             hostname)
                return -1
            ret = self._cih_services_preserve(log)
            if ret:
                log.cl_error("failed to preserve Coral services on host [%s]",
                             hostname)
                return -1

        # If local file, restore the send files in case overwritten by RPMs/pip
        if self.cih_is_local:
            for fpath in need_backup_fpaths:
                self._cih_restore_file(log, fpath)

        # Send the files
        for local_fpath, remote_fpath in send_fpath_dict.items():
            parent = os.path.dirname(remote_fpath)
            command = "mkdir -p %s" % parent
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            # sh_send_file should be able to handle this, but do not take
            # the risk/time
            if self.cih_is_local and local_fpath == remote_fpath:
                continue

            ret = host.sh_send_file(log, local_fpath, remote_fpath)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "dir [%s] on host [%s]",
                             local_fpath, parent,
                             hostname)
                return -1
        return 0


def cluster_host_install(log, workspace, installation_host, repo_config_fpath):
    """
    Install on a host of CoralInstallationCluster
    """
    # pylint: disable=unused-argument
    ret = installation_host.cih_install(log, repo_config_fpath)
    if ret:
        log.cl_error("failed to install on host [%s]",
                     installation_host.cih_host.sh_hostname)
        return -1
    return 0


class CoralInstallationCluster():
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    Installation cluster config.
    :param workspace: The workspace on local and remote host
    :param hosts: Hosts to install
    :param iso_path: ISO file path on localhost
    """
    def __init__(self, workspace, local_host, iso_dir):
        # A list of CoralInstallationHost
        self.cic_installation_hosts = []
        # The workspace on local and remote host
        self.cic_workspace = workspace
        # The ISO dir. This dir will be sent to remote host to the same
        # place
        self.cic_iso_dir = iso_dir
        # The generated repo config file under workspace on local host
        self.cic_repo_config_fpath = workspace + "/coral.repo"
        # Local host to run command
        self.cic_local_host = local_host

    def cic_add_hosts(self, hosts, pip_libs, dependent_rpms,
                      send_fpath_dict, need_backup_fpaths,
                      coral_reinstall=True,
                      tsinghua_mirror=False):
        """
        Add installation host.
        """
        for host in hosts:
            is_local = host.sh_hostname == self.cic_local_host.sh_hostname
            installation_host = CoralInstallationHost(self.cic_workspace,
                                                      host, is_local,
                                                      self.cic_iso_dir,
                                                      pip_libs, dependent_rpms,
                                                      send_fpath_dict,
                                                      need_backup_fpaths,
                                                      coral_reinstall=coral_reinstall,
                                                      tsinghua_mirror=tsinghua_mirror)
            self.cic_installation_hosts.append(installation_host)

    def cic_install(self, log, parallelism=10):
        """
        Install RPMs, PIP libs and send config files
        :param send_fpath_dict: key is the fpath on localhost, value is the
        fpath on remote host.
        """
        # pylint: disable=too-many-locals
        # Prepare the local repo config file
        local_host = self.cic_local_host
        command = ("mkdir -p %s" % (self.cic_workspace))
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        packages_dir = self.cic_iso_dir + "/" + constant.BUILD_PACKAGES
        generate_repo_file(self.cic_repo_config_fpath, packages_dir,
                           "Coral")

        args_array = []
        thread_ids = []
        for installation_host in self.cic_installation_hosts:
            args = (installation_host, self.cic_repo_config_fpath)
            args_array.append(args)
            thread_id = "host_install_%s" % installation_host.cih_host.sh_hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(log,
                                                    self.cic_workspace,
                                                    "install",
                                                    cluster_host_install,
                                                    args_array,
                                                    thread_ids=thread_ids,
                                                    parallelism=parallelism)
        ret = parallel_execute.pe_run()
        if ret:
            log.cl_error("failed to install hosts in parallel")
            return -1
        return 0


def yum_install_rpm_from_internet(log, host, rpms, tsinghua_mirror=False):
    """
    Check whether a RPM installed or not. If not, use yum to install
    """
    # pylint: disable=too-many-branches
    if tsinghua_mirror:
        ret = yum_replace_to_tsinghua(log, host)
        if ret:
            log.cl_error("failed to replace YUM mirrors to Tsinghua "
                         "University on host [%s]", host.sh_hostname)
            return -1

    missing_rpms = []
    for rpm in rpms:
        ret = host.sh_rpm_query(log, rpm)
        if ret != 0:
            missing_rpms.append(rpm)

    if len(missing_rpms) == 0:
        return 0

    # Cleanup, otherwise, might hit problem like:
    #
    # https://mirrors.xxx/epel/7/x86_64/repodata/xxx-filelists.sqlite.bz2:
    # [Errno 14] HTTPS Error 404 - Not Found
    # Trying other mirror.
    #
    cmds = ["yum clean all",
            "rpm --rebuilddb"]
    for command in cmds:
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

    command = "yum install -y"
    if host.sh_distro(log) == ssh_host.DISTRO_RHEL8:
        command += " --enablerepo powertools"
    for rpm in rpms:
        command += " %s" % rpm

    log.cl_info("running command [%s] on host [%s]",
                command, host.sh_hostname)
    retval = host.sh_watched_run(log, command, None, None,
                                 return_stdout=False,
                                 return_stderr=False)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s]",
                     command, host.sh_hostname)
        return -1

    new_missing_rpms = []
    for rpm in rpms:
        ret = host.sh_rpm_query(log, rpm)
        if ret != 0:
            new_missing_rpms.append(rpm)
    if len(new_missing_rpms) != 0:
        if "epel-release" in new_missing_rpms:
            log.cl_error("rpms %s is missing after yum install",
                         new_missing_rpms)
            return -1
        if "epel-release" in missing_rpms:
            # Replace the yum too if installed epel-release. So pass down
            # tsinghua_yum.
            ret = yum_install_rpm_from_internet(log, host, new_missing_rpms,
                                                tsinghua_mirror=tsinghua_mirror)
            return ret
    return 0


def yum_replace_to_tsinghua(log, host):
    """
    Replace the YUM to Tsinghua University
    """
    log.cl_info("replaceing CentOS and epel YUM to use mirrors from "
                "Tsinghua University")
    config_fpath = "/etc/yum.repos.d/epel.repo"
    ret = host.sh_path_exists(log, config_fpath)
    if ret < 0:
        log.cl_error("failed to check whether file [%s] exists on host [%s]",
                     config_fpath, host.sh_hostname)
        return -1
    if ret:
        command = """sed -e 's!^metalink=!#metalink=!g' \
    -e 's!^#baseurl=!baseurl=!g' \
    -e 's!//download\.fedoraproject\.org/pub!//mirrors.tuna.tsinghua.edu.cn!g' \
    -e 's!http://mirrors\.tuna!https://mirrors.tuna!g' \
    -i /etc/yum.repos.d/epel*.repo"""
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

    # Centos-base should have been installed
    command = """sed -e 's|^mirrorlist=|#mirrorlist=|g' \
-e 's|^#baseurl=http://mirror.centos.org|baseurl=https://mirrors.tuna.tsinghua.edu.cn|g' \
-i.bak \
/etc/yum.repos.d/CentOS-*.repo
"""
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return 0


def bootstrap_from_internet(log, host, rpms, pip_packages, pip_dir,
                            tsinghua_mirror=False):
    """
    Install the dependent RPMs and pip packages from Internet
    """
    ret = yum_install_rpm_from_internet(log, host, rpms,
                                        tsinghua_mirror=tsinghua_mirror)
    if ret:
        log.cl_error("failed to install missing RPMs on host [%s]",
                     host.sh_hostname)
        return -1

    if pip_dir is not None:
        ret = install_pip3_packages_from_cache(log, host, pip_packages,
                                               pip_dir, quiet=True)
        if ret == 0:
            return 0

        # Download if failed to install from cache
        ret = download_pip3_packages(log, host, pip_dir, pip_packages,
                                     tsinghua_mirror=tsinghua_mirror)
        if ret:
            log.cl_error("failed to download pip packages on host [%s]",
                         host.sh_hostname)
            return -1

    ret = install_pip3_packages_from_cache(log, host, pip_packages,
                                           pip_dir)
    if ret:
        log.cl_error("failed to install pip3 packages %s on "
                     "host [%s]",
                     pip_packages, host.sh_hostname)
        return -1
    return 0


def command_missing_packages_rhel8():
    """
    Add the missing RPMs and pip packages for RHEL9
    """
    # pylint: disable=unused-import,bad-option-value,import-outside-toplevel
    # pylint: disable=unused-variable
    missing_rpms = []
    missing_pips = []
    # This RPM will improve the interactivity
    list_add(missing_rpms, "bash-completion")
    try:
        import yaml
    except ImportError:
        list_add(missing_rpms, "python3-pyyaml")

    try:
        import dateutil
    except ImportError:
        list_add(missing_rpms, "python3-dateutil")
        list_add(missing_rpms, "epel-release")

    try:
        import prettytable
    except ImportError:
        list_add(missing_rpms, "python3-prettytable")

    try:
        import toml
    except ImportError:
        list_add(missing_rpms, "python3-toml")
        list_add(missing_rpms, "epel-release")

    try:
        import psutil
    except ImportError:
        list_add(missing_rpms, "python3-psutil")

    try:
        import fire
    except ImportError:
        list_add(missing_rpms, "python3-pip")
        list_add(missing_pips, "fire")

    try:
        import filelock
    except ImportError:
        list_add(missing_rpms, "python3-filelock")
    return missing_rpms, missing_pips


def list_add(alist, name):
    """
    If not exist, append.
    """
    if name not in alist:
        alist.append(name)


def command_missing_packages_rhel7():
    """
    Add the missing RPMs and pip packages for RHEL7
    """
    # pylint: disable=unused-import,bad-option-value,import-outside-toplevel
    # pylint: disable=unused-variable
    missing_rpms = []
    missing_pips = []
    # This RPM will improve the interactivity
    list_add(missing_rpms, "bash-completion")
    try:
        import yaml
    except ImportError:
        list_add(missing_rpms, "python36-PyYAML")
        list_add(missing_rpms, "epel-release")

    try:
        import dateutil
    except ImportError:
        list_add(missing_rpms, "python36-dateutil")
        list_add(missing_rpms, "epel-release")

    try:
        import prettytable
    except ImportError:
        list_add(missing_rpms, "python36-prettytable")
        list_add(missing_rpms, "epel-release")

    try:
        import toml
    except ImportError:
        list_add(missing_rpms, "epel-release")
        list_add(missing_pips, "toml")

    try:
        import psutil
    except ImportError:
        list_add(missing_rpms, "python36-psutil")
        list_add(missing_rpms, "epel-release")

    try:
        import fire
    except ImportError:
        list_add(missing_rpms, "python3-pip")
        list_add(missing_pips, "fire")

    try:
        import filelock
    except ImportError:
        list_add(missing_pips, "filelock")
    return missing_rpms, missing_pips


def command_missing_packages(distro):
    """
    Add the missing RPMs and pip packages
    """
    if distro == ssh_host.DISTRO_RHEL7:
        return command_missing_packages_rhel7()
    if distro == ssh_host.DISTRO_RHEL8:
        return command_missing_packages_rhel8()
    return None, None


def download_pip3_packages(log, host, pip_dir, pip_packages,
                           tsinghua_mirror=False):
    """
    Download pip3 packages
    """
    if len(pip_packages) == 0:
        return 0
    if tsinghua_mirror:
        message = " from Tsinghua University mirror"
    else:
        message = ""
    log.cl_info("downloading pip3 packages %s to dir [%s] on host [%s]%s",
                pip_packages, pip_dir, host.sh_hostname, message)
    ret = yum_install_rpm_from_internet(log, host, ["python3-pip"],
                                        tsinghua_mirror=tsinghua_mirror)
    if ret:
        log.cl_error("failed to install [python3-pip] RPM")
        return -1

    command = ("mkdir -p %s && cd %s && pip3 download" % (pip_dir, pip_dir))
    if tsinghua_mirror:
        command += " -i https://pypi.tuna.tsinghua.edu.cn/simple"
    for pip_package in pip_packages:
        command += " " + pip_package
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return 0


def install_lustre_and_coral(log, workspace, local_host, iso_dir, host,
                             lazy_prepare=True, tsinghua_mirror=False):
    """
    Install Lustre and Coral packages on a host
    """
    install_cluster = CoralInstallationCluster(workspace, local_host,
                                               iso_dir)
    send_fpath_dict = {}
    need_backup_fpaths = []
    install_cluster.cic_add_hosts([host], constant.CORAL_DEPENDENT_PIPS,
                                  constant.CORAL_DEPENDENT_RPMS,
                                  send_fpath_dict,
                                  need_backup_fpaths,
                                  coral_reinstall=True,
                                  tsinghua_mirror=tsinghua_mirror)
    ret = install_cluster.cic_install(log)
    if ret:
        log.cl_error("failed to install dependent RPMs on host [%s]",
                     host.sh_hostname)
        return -1

    ret = host.lsh_lustre_prepare(log, workspace,
                                  lazy_prepare=lazy_prepare)
    if ret:
        log.cl_error("failed to prepare host [%s] to run Lustre service",
                     host.sh_hostname)
        return -1
    return 0
