"""
Library for installing a tool from ISO

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
# pylint: disable=too-many-lines
import os
import sys
import socket
from pycoral import utils
from pycoral import ssh_host
from pycoral import os_distro
from pycoral import constant
from pycoral import parallel
from pycoral import yum_mirror
from pycoral import apt_mirror
from pycoral import pip_mirror


def _generate_repo_file(repo_fpath, packages_dir, package_name):
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


def check_fpath_pattern(fpath_pattern):
    """
    Do not allow ".."
    """
    fields = fpath_pattern.split("/")
    for field in fields:
        if field == "..":
            return -1
    return 0


def _yum_repo_install(log, host, repo_fpath, rpms):
    """
    Install RPMs using YUM
    """
    extra_option = "-c " + repo_fpath
    return host.sh_yum_install(log, rpms, extra_option=extra_option)


def sync_iso_dir(log, workspace, host, iso_pattern, dest_iso_dir,
                 use_cp=False):
    """
    Sync the files in the iso to the directory
    """
    # pylint: disable=too-many-branches
    # Just to check ssh to host works well
    retval = host.sh_run(log, "true", timeout=60)
    if retval.cr_exit_status:
        log.cl_error("failed to ssh to host [%s] using user [%s], "
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
        log.cl_error("failed to find ISO with pattern [%s] on host [%s]",
                     iso_pattern, host.sh_hostname)
        return -1
    if len(fnames) > 1:
        log.cl_info("multiple ISOs found by pattern [%s] on host [%s]",
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


def _install_pip3_packages(log, local_host, pip_packages,
                           pip_dir=None, quiet=False):
    """
    Install pip3 packages on local host and make sure it can be imported later.

    pip package can have the format like "PyInstaller==5.13.2".
    """
    ret = local_host.sh_install_pip3_packages(log, pip_packages,
                                              pip_dir=pip_dir, quiet=quiet)
    if ret:
        if not quiet:
            log.cl_error("failed to install pip3 packages %s on localhost [%s]",
                         pip_packages, local_host.sh_hostname)
        return -1

    pakage_names = []
    for pip_package in pip_packages:
        if "==" in pip_package:
            fields = pip_package.split("==")
            if len(fields) != 2:
                log.cl_error("unexpected name of pip package [%s]",
                             pip_package)
                return -1
            pakage_name = fields[0]
        else:
            pakage_name = pip_package
        pakage_names.append(pakage_name)

    for pakage_name in pakage_names:
        location = local_host.sh_pip3_package_location(log, pakage_name)
        if location is None:
            log.cl_error("failed to get the location of pip3 packages [%s] on "
                         "host [%s]",
                         pakage_name, local_host.sh_hostname)
            return -1

        if location not in sys.path:
            sys.path.append(location)
    return 0


def install_rpms_from_iso(log, workspace, local_host, iso_dir,
                          missing_rpms, name):
    """
    Install the dependent RPMs after ISO is synced
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

    _generate_repo_file(repo_config_fpath, packages_dir, name)
    ret = _yum_repo_install(log, local_host, repo_config_fpath, missing_rpms)
    if ret:
        log.cl_error("failed to install RPMs %s on local host [%s]",
                     missing_rpms, local_host.sh_hostname)
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
    retval = host.sh_run(log, command, timeout=None)
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


class CoralInstallHost():
    """
    Each host that needs installation has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, cluster, host,
                 pip_names=None, package_names=None,
                 send_fpath_dict=None, fpaths_need_backup=None,
                 coral_reinstall=True, china_mirror=False,
                 disable_selinux=True, disable_firewalld=True,
                 change_sshd_max_startups=True, config_rsyslog=True,
                 send_iso_dir=True, send_relative_path_patterns=None,
                 uninstall_package_names=None,
                 install_iso_package_patterns=None):
        # pylint: disable=too-many-locals
        # CoralInstallCluster
        self.cih_cluster = cluster
        # Packages dir under ISO dir
        self.cih_packages_dir = cluster.cic_iso_dir + "/" + constant.BUILD_PACKAGES
        # Host
        self.cih_host = host
        # Whether this host is localhost
        self.cih_is_local = (host.sh_hostname == cluster.cic_local_host.sh_hostname)
        # The Pip packages to install
        if pip_names is None:
            self.cih_pip_names = []
        else:
            self.cih_pip_names = pip_names
        # The RPM/deb names to install
        if package_names is None:
            self.cih_package_names = []
        else:
            self.cih_package_names = package_names
        # Key is the the source path on localhost, value is the target
        # path on this host
        if send_fpath_dict is None:
            self.cih_send_fpath_dict = {}
        else:
            self.cih_send_fpath_dict = send_fpath_dict
        # File paths to backup before reinstalling and restore after
        # reinstalling
        if fpaths_need_backup is None:
            self.cih_fpaths_need_backup = []
        else:
            self.cih_fpaths_need_backup = fpaths_need_backup
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
        # Whether to use local yum/apt and pip mirrors
        self.cih_china_mirror = china_mirror
        # Whether disable SELinux on the host
        self.cih_disable_selinux = disable_selinux
        # Whether disable Firewalld on the host
        self.cih_disable_firewalld = disable_firewalld
        # Whether change sshd max startups on the host
        self.cih_change_sshd_max_startups = change_sshd_max_startups
        # Whether to change rsyslog to avoid flood of login
        self.cih_config_rsyslog = config_rsyslog
        # Whether to send ISO dir from local host
        self.cih_send_iso_dir = send_iso_dir
        # The relative file path patterns to send from ISO dir of local host. None if not send.
        self.cih_send_relative_path_patterns = send_relative_path_patterns
        # The RPM names to uninstall first. By putting a same RPM in
        # cih_uninstall_package_names and cih_install_iso_package_patterns, we are able to
        # reinstall the specific RPM/deb.
        self.cih_uninstall_package_names = uninstall_package_names
        # The package patterns to install from ISO dir.
        self.cih_install_iso_package_patterns = install_iso_package_patterns

    def _cih_send_iso_dir(self, log):
        """
        Send the dir of ISO to a host
        """
        cluster = self.cih_cluster
        iso_dir = cluster.cic_iso_dir
        host = self.cih_host
        local_host = self.cih_cluster.cic_local_host
        if self.cih_is_local:
            return 0

        log.cl_info("syncing ISO dir from local host [%s] "
                    "to host [%s]",
                    local_host.sh_hostname,
                    host.sh_hostname)
        target_dirname = os.path.dirname(iso_dir)
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

        ret = host.sh_send_file(log, iso_dir, target_dirname,
                                delete_dest=True)
        if ret:
            log.cl_error("failed to send dir [%s] on local host [%s] to "
                         "directory [%s] on host [%s]",
                         iso_dir, socket.gethostname(), target_dirname,
                         host.sh_hostname)
            return -1

        return 0

    def _cih_find_and_remove_pattern(self, log, file_pattern):
        """
        Find and remove the file that matches the pattern.
        """
        host = host = self.cih_host
        fpaths = host.sh_resolve_path(log, file_pattern, quiet=True)
        if fpaths is None:
            log.cl_error("failed to find file with pattern [%s] on host [%s]",
                         file_pattern, host.sh_hostname)
            return -1
        if len(fpaths) > 1:
            log.cl_error("multiple files found by pattern [%s] on host [%s]",
                         file_pattern, host.sh_hostname)
            return -1
        if len(fpaths) == 0:
            return 0
        fpath = fpaths[0]
        command = "rm -f %s" % fpath
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

    def _cih_send_iso_files(self, log, relative_patterns):
        """
        Send the dir of ISO to a host
        """
        # pylint: disable=too-many-locals
        host = self.cih_host
        cluster = self.cih_cluster
        iso_dir = cluster.cic_iso_dir
        local_host = cluster.cic_local_host
        if self.cih_is_local:
            return 0

        log.cl_info("syncing ISO files from local host [%s] "
                    "to host [%s]", local_host.sh_hostname,
                    host.sh_hostname)
        command = ("mkdir -p %s" % (self.cih_packages_dir))
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

        for relative_pattern in relative_patterns:
            ret = check_fpath_pattern(relative_pattern)
            if ret:
                log.cl_error("invalid pattern [%s]", relative_pattern)
                return -1
            file_pattern = iso_dir + "/" + relative_pattern

            ret = self._cih_find_and_remove_pattern(log, file_pattern)
            if ret:
                return -1

            fpaths = local_host.sh_resolve_path(log, file_pattern, quiet=True)
            if fpaths is None or len(fpaths) == 0:
                log.cl_error("failed to find file with pattern [%s] on local host [%s]",
                             file_pattern, local_host.sh_hostname)
                return -1
            if len(fpaths) > 1:
                log.cl_error("multiple files found by pattern [%s] on local host [%s]",
                             file_pattern, local_host.sh_hostname)
                return -1
            fpath = fpaths[0]

            prefix = iso_dir + "/"
            if not fpath.startswith(prefix):
                log.cl_error("file [%s] with pattern [%s] is not under ISO dir [%s]",
                             fpath, file_pattern, iso_dir)
                return -1

            relative_fpath = fpath[len(prefix):]
            relative_dir = os.path.dirname(relative_fpath)
            target_dir = iso_dir + "/" + relative_dir

            command = ("mkdir -p %s" % (target_dir))
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

            ret = host.sh_send_file(log, fpath, target_dir, delete_dest=True)
            if ret:
                log.cl_error("failed to send file [%s] on local host [%s] to "
                             "directory [%s] on host [%s]",
                             fpath, socket.gethostname(), target_dir,
                             host.sh_hostname)
                return -1
        return 0

    def _cih_backup_file(self, log, fpath):
        """
        Backup files to workspace
        """
        host = self.cih_host
        dirname = os.path.dirname(fpath)
        workspace = self.cih_cluster.cic_workspace
        backup_dirname = workspace + dirname
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
        workspace = self.cih_cluster.cic_workspace
        backup_fpath = workspace + "/" + fpath
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
        # pylint: disable=no-self-use
        log.cl_error("disable selinux not implemented")
        return -1

    def _cih_disable_firewalld(self, log):
        """
        Disable the firewalld on the host
        """
        # pylint: disable=no-self-use
        log.cl_error("disable firewalld not implemented")
        return -1

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

    def _cih_change_sshd_max_startups(self, log):
        """
        Parallel ssh commands might got following error if the config is small:
        ssh_exchange_identification: read: Connection reset by peer
        """
        host = self.cih_host
        config = "MaxStartups 30:40:100"
        retval = host.sh_run(log,
                             r"grep MaxStartups /etc/ssh/sshd_config "
                             r"| grep -v \#")
        if retval.cr_exit_status != 0:
            log.cl_info("adding [%s] of sshd config on host [%s]",
                        config, host.sh_hostname)
            retval = host.sh_run(log, "echo '%s' >> /etc/ssh/sshd_config" %
                                 config)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to change sshd config on host [%s]",
                             host.sh_hostname)
                return -1

            service_name = "sshd"
            log.cl_info("restarting [%s] service on host [%s]",
                        service_name, host.sh_hostname)
            ret = host.sh_service_restart(log, service_name)
            if ret:
                log.cl_error("failed to restart service [%s] on host [%s]",
                             service_name, host.sh_hostname)
                return -1
        else:
            line = retval.cr_stdout.strip()
            if line != config:
                log.cl_error("unexpected sshd config on "
                             "host [%s], expected [%s], got [%s]",
                             host.sh_hostname, config,
                             line)
                return -1

        return 0

    def _cih_uninstall_packages(self, log, package_names):
        """
        Install packages on the host.
        """
        # pylint: disable=unused-argument,no-self-use
        log.cl_error("package uninstall not implemented")
        return -1

    def cih_packages_install(self, log, package_names):
        """
        Install packages on the host.
        """
        # pylint: disable=unused-argument,no-self-use
        log.cl_error("package install not implemented")
        return -1

    def _cih_install_package_patterns(self, log, package_patterns):
        """
        Install package patterns from the ISO.
        """
        # pylint: disable=unused-argument,no-self-use
        log.cl_error("package pattern install not implemented")
        return -1

    def cih_coral_packages_reinstall(self, log):
        """
        Reinstall Coral RPMs or debs.
        """
        # pylint: disable=no-self-use
        log.cl_error("package packages install not implemented")
        return -1

    def cih_install(self, log):
        """
        Install the RPMs and pip packages on a host, and send the files
        :param send_fpath_dict: key is the fpath on localhost, value is the
        fpath on remote host.
        """
        # pylint: disable=too-many-statements,too-many-branches,too-many-locals
        cluster = self.cih_cluster
        disable_selinux = self.cih_disable_selinux
        disable_firewalld = self.cih_disable_firewalld
        change_sshd_max_startups = self.cih_change_sshd_max_startups
        pip_names = self.cih_pip_names
        package_names = self.cih_package_names
        send_fpath_dict = self.cih_send_fpath_dict
        fpaths_need_backup = self.cih_fpaths_need_backup
        coral_reinstall = self.cih_coral_reinstall
        config_rsyslog = self.cih_config_rsyslog
        send_iso_dir = self.cih_send_iso_dir
        send_relative_path_patterns = self.cih_send_relative_path_patterns
        uninstall_package_names = self.cih_uninstall_package_names
        install_iso_package_patterns = self.cih_install_iso_package_patterns
        host = self.cih_host
        hostname = host.sh_hostname
        cluster = self.cih_cluster
        workspace = cluster.cic_workspace
        distro = host.sh_distro(log)
        local_host = cluster.cic_local_host
        if distro is None:
            log.cl_error("failed to get distro of host [%s]",
                         hostname)
            return -1
        if distro != cluster.cic_distro:
            log.cl_error("distro [%s] of host [%s] is different with [%s] "
                         "of local host [%s]",
                         distro, hostname, cluster.cic_distro,
                         local_host.sh_hostname)
            return -1

        if distro not in [os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8,
                          os_distro.DISTRO_UBUNTU2004, os_distro.DISTRO_UBUNTU2204]:
            log.cl_error("unsupported distro [%s] of host [%s]",
                         distro, hostname)
            return -1

        if config_rsyslog:
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

        if change_sshd_max_startups:
            ret = self._cih_change_sshd_max_startups(log)
            if ret:
                log.cl_error("failed to change MaxStartups of sshd on host [%s]",
                             hostname)
                return -1

        if send_iso_dir:
            ret = self._cih_send_iso_dir(log)
            if ret:
                log.cl_error("failed to send ISO dir from local host [%s] "
                             "to host [%s]",
                             local_host.sh_hostname, hostname)
                return -1

        if send_relative_path_patterns is not None:
            ret = self._cih_send_iso_files(log, send_relative_path_patterns)
            if ret:
                log.cl_error("failed to send ISO files from local host [%s] "
                             "to host [%s]",
                             local_host.sh_hostname, hostname)
                return -1

        if uninstall_package_names is not None:
            ret = self._cih_uninstall_packages(log, uninstall_package_names)
            if ret:
                log.cl_error("failed to uninstall packages on host [%s]",
                             hostname)
                return -1

        if install_iso_package_patterns is not None:
            ret = self._cih_install_package_patterns(log, install_iso_package_patterns)
            if ret:
                log.cl_error("failed to install RPMs on host [%s]",
                             hostname)
                return -1

        # If local file, backup the send files in case overwritten by RPMs/pip
        if self.cih_is_local:
            for fpath in fpaths_need_backup:
                self._cih_backup_file(log, fpath)

        if self.cih_china_mirror:
            ret = china_mirrors_configure(log, host, workspace)
            if ret:
                log.cl_error("failed to configure local mirrors on host [%s]",
                             host.sh_hostname)
                return -1

        if len(package_names) > 0:
            ret = self.cih_packages_install(log, package_names)
            if ret:
                log.cl_error("failed to install packages [%s] on host [%s]",
                             utils.list2string(package_names), hostname)
                return -1

        if len(pip_names) > 0:
            ret = host.sh_install_pip3_packages(log, pip_names)
            if ret:
                log.cl_error("failed to install missing pip packages %s "
                             "on host [%s]",
                             pip_names, host.sh_hostname)
                return -1

        if coral_reinstall:
            ret = self._cih_services_pre_preserve(log)
            if ret:
                log.cl_error("failed to prepare for preserving Coral "
                             "services on host [%s]",
                             hostname)
                return -1

            ret = self.cih_coral_packages_reinstall(log)
            if ret:
                log.cl_error("failed to install Coral packages on host [%s]",
                             hostname)
                return -1

            ret = self._cih_services_preserve(log)
            if ret:
                log.cl_error("failed to preserve Coral services on host [%s]",
                             hostname)
                return -1

        # If local file, restore the send files in case overwritten by RPMs/pip
        if self.cih_is_local:
            for fpath in fpaths_need_backup:
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
                log.cl_error("failed to send file [%s] on local host [%s] to "
                             "dir [%s] on host [%s]",
                             local_fpath, socket.gethostname(), parent,
                             hostname)
                return -1
        return 0


class CoralInstallHostRPM(CoralInstallHost):
    """
    Each host that needs installation has an object of this type.
    """
    def cih_coral_packages_reinstall(self, log):
        """
        Reinstall Coral packages.
        """
        cluster = self.cih_cluster
        iso_dir = cluster.cic_iso_dir
        host = self.cih_host
        ret = coral_rpm_reinstall(log, host, iso_dir)
        if ret:
            log.cl_error("failed to install Coral RPMs on host [%s]",
                         host.sh_hostname)
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

    def cih_packages_install(self, log, package_names):
        """
        Install packages on the host.
        """
        cluster = self.cih_cluster
        workspace = cluster.cic_workspace
        repo_config_fpath = cluster.cicr_repo_config_fpath
        host = self.cih_host
        hostname = host.sh_hostname
        if not self.cih_is_local:
            command = ("mkdir -p %s" % (workspace))
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

            ret = host.sh_send_file(log, repo_config_fpath, workspace)
            if ret:
                log.cl_error("failed to send file [%s] on local host [%s] to "
                             "directory [%s] on host [%s]",
                             repo_config_fpath, socket.gethostname(), workspace,
                             hostname)
                return -1

        ret = _yum_repo_install(log, host, repo_config_fpath,
                                package_names)
        return ret

    def _cih_uninstall_packages(self, log, package_names):
        """
        Uninstall RPMs from the host.
        """
        host = self.cih_host
        uninstall_rpms = []
        for rpm_name in package_names:
            if not host.sh_has_rpm(log, rpm_name):
                continue
            uninstall_rpms.append(rpm_name)
        if len(uninstall_rpms) == 0:
            return 0

        log.cl_info("uninstalling RPMs on host [%s]", host.sh_hostname)
        command = "rpm -e"
        for rpm_name in uninstall_rpms:
            if not host.sh_has_rpm(log, rpm_name):
                continue
            command += " " + rpm_name

        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _cih_install_package_patterns(self, log, package_patterns):
        """
        Install package patterns from the ISO.
        """
        host = self.cih_host
        if len(package_patterns) == 0:
            return 0

        log.cl_info("installing RPMs on host [%s]", host.sh_hostname)
        command = "rpm -ivh --nodeps"
        for rpm_pattern in package_patterns:
            fpath_pattern = self.cih_packages_dir + "/" + rpm_pattern
            command += " " + fpath_pattern

        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0



class CoralInstallHostDeb(CoralInstallHost):
    """
    Each host that needs installation has an object of this type.
    """
    def _cihd_add_apt_source(self, log):
        """
        Add the apt source of ISO.
        """
        host = self.cih_host
        coral_line = "deb file:///var/log/coral/iso/Packages/    /"
        command = "grep coral /etc/apt/sources.list"
        retval = host.sh_run(log, command)
        if retval.cr_exit_status == 0:
            lines = retval.cr_stdout.strip().splitlines()
            if len(lines) != 1:
                log.cl_error("unexpected line number of command [%s] on host [%s], "
                             "stdout = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_stdout)
                return -1
            line = lines[0]
            if line != coral_line:
                log.cl_error("unexpected line [%s] of command [%s] on host [%s]",
                             line,
                             command,
                             host.sh_hostname)
                return -1
            return 0

        if retval.cr_exit_status != 1:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        command = "echo \"%s\" >> /etc/apt/sources.list" % coral_line
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

    def _cihd_add_apt_key(self, log):
        """
        Add the apt key of ISO to trusted.gpg.d
        """
        host = self.cih_host
        coral_gpg_fname = "coral.gpg"
        trusted_fpath = "/etc/apt/trusted.gpg.d/" + coral_gpg_fname
        iso_gpg_fpath = self.cih_packages_dir + "/" + constant.CORAL_PUBLIC_KEY_FNAME
        exists = host.sh_path_exists(log, trusted_fpath)
        if exists < 0:
            log.cl_error("failed to check whether file [%s] exists on host [%s]",
                         trusted_fpath, host.sh_hostname)
            return -1
        if exists != 0:
            command = "diff %s %s" % (iso_gpg_fpath, trusted_fpath)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status == 0:
                return 0
            if retval.cr_exit_status != 1:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
            log.cl_error("file [%s] exists but different with [%s] on host [%s]",
                         trusted_fpath, iso_gpg_fpath, host.sh_hostname)
            return -1

        command = ("cp %s %s" % (iso_gpg_fpath, trusted_fpath))
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

    def cih_packages_install(self, log, package_names):
        """
        Install packages on the host.
        """
        # pylint: disable=unused-argument,no-self-use
        host = self.cih_host
        ret = self._cihd_add_apt_source(log)
        if ret:
            log.cl_error("failed to add apt source of Coral")
            return -1

        ret = self._cihd_add_apt_key(log)
        if ret:
            log.cl_error("failed to add apt key of Coral")
            return -1

        command = "apt update"
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

        command = "apt install -y"
        for package_name in package_names:
            command += " " + package_name
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

    def cih_coral_packages_reinstall(self, log):
        """
        Reinstall Coral packages.
        """
        host = self.cih_host
        command = "dpkg -i --force-depends %s/coral-*.deb" % self.cih_packages_dir
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


def cluster_host_install(log, workspace, install_host):
    """
    Install on a host of CoralInstallCluster
    """
    # pylint: disable=unused-argument
    ret = install_host.cih_install(log)
    if ret:
        log.cl_error("failed to install on host [%s]",
                     install_host.cih_host.sh_hostname)
        return -1
    return 0


class CoralInstallCluster():
    # pylint: disable=too-few-public-methods
    """
    Install cluster config.
    :param workspace: The workspace on local and remote host
    :param hosts: Hosts to install
    :param iso_path: ISO file path on localhost
    """
    def __init__(self, host_type, workspace, local_host, iso_dir, distro):
        # The distro of the cluster. The distro of all hosts in the cluster
        # need to be the same with the local host.
        self.cic_distro = distro
        # Dict of CoralInstallHost. Key is hostname.
        self.cic_host_dict = {}
        # A list of CoralInstallHost
        self.cic_install_hosts = []
        # The workspace on local and remote host
        self.cic_workspace = workspace
        # The ISO dir. This dir will be sent to remote host to the same path.
        self.cic_iso_dir = iso_dir
        # Local host to run command
        self.cic_local_host = local_host
        # The type of CoralInstallHost
        self.cic_host_type = host_type

    def cic_hosts_add(self, log, hosts,
                      pip_names=None,
                      package_names=None,
                      send_fpath_dict=None,
                      fpaths_need_backup=None,
                      coral_reinstall=True, china_mirror=False,
                      disable_selinux=True, disable_firewalld=True,
                      change_sshd_max_startups=True,
                      config_rsyslog=True, send_iso_dir=True,
                      send_relative_path_patterns=None,
                      uninstall_package_names=None,
                      install_iso_package_patterns=None):
        """
        Add hosts.
        """
        # pylint: disable=too-many-locals
        for host in hosts:
            install_host = self.cic_host_type(self, host,
                                              pip_names=pip_names,
                                              package_names=package_names,
                                              send_fpath_dict=send_fpath_dict,
                                              fpaths_need_backup=fpaths_need_backup,
                                              coral_reinstall=coral_reinstall,
                                              china_mirror=china_mirror,
                                              disable_selinux=disable_selinux,
                                              disable_firewalld=disable_firewalld,
                                              change_sshd_max_startups=change_sshd_max_startups,
                                              config_rsyslog=config_rsyslog,
                                              send_iso_dir=send_iso_dir,
                                              send_relative_path_patterns=send_relative_path_patterns,
                                              uninstall_package_names=uninstall_package_names,
                                              install_iso_package_patterns=install_iso_package_patterns)
            if host.sh_hostname in self.cic_host_dict:
                log.cl_erro("host [%s] already exists in cluster",
                            host.sh_hostname)
                return -1
            self.cic_install_hosts.append(install_host)
        return 0

    def _cic_check_clocks(self, log):
        """
        Check the clocks in the cluster.
        """
        hosts = []
        for install_host in self.cic_install_hosts:
            hosts.append(install_host.cih_host)
        hosts.append(self.cic_local_host)
        ret = ssh_host.check_clocks_diff(log, hosts, max_diff=60)
        if ret:
            log.cl_error("failed to check clock difference between hosts")
            return -1
        return 0

    def cic_install(self, log, parallelism=10):
        """
        Install packages, PIP libs and send files
        """
        # pylint: disable=too-many-locals
        # Prepare the local repo config file
        ret = self._cic_check_clocks(log)
        if ret:
            log.cl_error("failed to check locks in the installation cluster")
            return -1

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

        args_array = []
        thread_ids = []
        for install_host in self.cic_install_hosts:
            args = (install_host,)
            args_array.append(args)
            thread_id = "host_install_%s" % install_host.cih_host.sh_hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(self.cic_workspace,
                                                    "install",
                                                    cluster_host_install,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=parallelism)
        if ret:
            log.cl_error("failed to install hosts in parallel")
            return -1
        return 0


class CoralInstallClusterRPM(CoralInstallCluster):
    # pylint: disable=too-few-public-methods
    """
    The installation cluster that uses RPM as package mangement.
    """
    def __init__(self, workspace, local_host, iso_dir, distro):
        super().__init__(CoralInstallHostRPM, workspace, local_host, iso_dir,
                         distro)

        # The generated repo config file under workspace on local host
        self.cicr_repo_config_fpath = workspace + "/coral.repo"

    def cic_install(self, log, parallelism=10):
        """
        Install packages, PIP libs and send files
        """
        # pylint: disable=too-many-locals
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
        _generate_repo_file(self.cicr_repo_config_fpath, packages_dir,
                            "Coral")

        ret = super().cic_install(log, parallelism=parallelism)
        return ret


class CoralInstallClusterDeb(CoralInstallCluster):
    """
    The installation cluster that uses RPM as package mangement.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, workspace, local_host, iso_dir, distro):
        # pylint: disable=useless-super-delegation
        super().__init__(CoralInstallHostDeb, workspace, local_host, iso_dir,
                         distro)


def get_cluster(log, workspace, local_host, iso_dir):
    """
    Return CoralInstallCluster
    """
    distro = local_host.sh_distro(log)
    if distro is None:
        log.cl_error("failed to get distro of host [%s]",
                     local_host.sh_hostname)
        return None
    distro_type = os_distro.distro2type(log, distro)
    if distro_type is None:
        log.cl_error("unsupported distro [%s] of host [%s]",
                     distro, local_host.sh_hostname)
        return None

    if distro_type == os_distro.DISTRO_TYPE_RHEL:
        return CoralInstallClusterRPM(workspace, local_host, iso_dir, distro)
    if distro_type == os_distro.DISTRO_TYPE_UBUNTU:
        return CoralInstallClusterDeb(workspace, local_host, iso_dir, distro)
    log.cl_error("unsupported distro [%s] of host [%s]",
                 distro, local_host.sh_hostname)
    return None



def install_package_and_pip(log, host, packages, pip_packages):
    """
    Install the dependent RPMs and pip packages
    """
    pip_package = "python3-pip"
    if len(pip_packages) != 0 and pip_package not in packages:
        packages.append(pip_package)

    ret = host.sh_install_packages(log, packages)
    if ret:
        log.cl_error("failed to install packages [%s]",
                     utils.list2string(packages))
        return -1

    if len(pip_packages) == 0:
        return 0

    ret = _install_pip3_packages(log, host, pip_packages)
    if ret:
        log.cl_error("failed to install pip packages [%s]",
                     utils.list2string(pip_packages))
        return -1
    return 0


def china_mirrors_configure(log, host, workspace):
    """
    Configure yum/apt/pip mirrors.
    """
    distro = host.sh_distro(log)
    if distro is None:
        log.cl_error("failed to get OS distro on host [%s]",
                     host.sh_hostname)
        return -1

    if distro not in (os_distro.DISTRO_RHEL7,
                      os_distro.DISTRO_RHEL8,
                      os_distro.DISTRO_UBUNTU2004,
                      os_distro.DISTRO_SHORT_UBUNTU2204):
        log.cl_error("unsupported OS distro [%s] on host [%s]",
                     distro, host.sh_hostname)
        return -1

    os_distributor = host.sh_os_distributor(log)
    if os_distributor is None:
        log.cl_error("failed to get OS distributor")
        return -1

    if distro in (os_distro.DISTRO_RHEL7,
                  os_distro.DISTRO_RHEL8):
        ret = yum_mirror.yum_mirror_configure(log, host)
        if ret:
            log.cl_error("failed to configure local mirror of yum on "
                         "host [%s]",
                         host.sh_hostname)
            return -1

    if distro in (os_distro.DISTRO_UBUNTU2004,
                  os_distro.DISTRO_SHORT_UBUNTU2204):
        ret = apt_mirror.apt_mirror_configure(log, host)
        if ret:
            log.cl_error("failed to configure local mirror of apt on host [%s]",
                         host.sh_hostname)
            return -1

    if distro == os_distro.DISTRO_RHEL8:
        ret = enable_yum_repo_powertools(log, host)
        if ret:
            log.cl_error("failed to enable yum repo of powertools on host [%s]",
                         host.sh_hostname)
            return -1

    ret = pip_mirror.pip_mirror_configure(log, host, workspace)
    if ret:
        log.cl_error("failed to configure local mirror of pip on host [%s]",
                     host.sh_hostname)
        return -1
    return 0


def check_yum_repo_powertools(log, host, distro):
    """
    Check whether the host has powertools repo.
    Only OS distro rhel8 needs this.
    """
    if distro != os_distro.DISTRO_RHEL8:
        return 0
    repo_ids = host.sh_yum_repo_ids(log)
    if repo_ids is None:
        log.cl_error("failed to get the yum repo IDs on host [%s]",
                     host.sh_hostname)
        return -1
    if "powertools" not in repo_ids:
        log.cl_error("yum repo [PowerTool] is not enabled on host [%s]",
                     host.sh_hostname)
        # you might want to have line [enabled=1] in file
        # [/etc/yum.repos.d/*-PowerTools.repo]
        return -1

    return 0

def enable_yum_repo_powertools(log, host):
    """
    Enable the powertools repo.
    """
    distro = host.sh_distro(log)
    if distro is None:
        log.cl_error("failed to detect OS distro on host [%s]",
                     host.sh_hostname)
        return -1

    if distro != os_distro.DISTRO_RHEL8:
        log.cl_error("repo powertools is not supported on distro [%s]")
        return -1

    repo_pattern = "/etc/yum.repos.d/*-PowerTools.repo"
    fpaths = host.sh_resolve_path(log, repo_pattern)
    if fpaths is None:
        log.cl_error("failed to find repo [%s] on host [%s]",
                     repo_pattern, host.sh_hostname)
        return -1
    if len(fpaths) == 0:
        log.cl_error("no file found with pattern [%s] on host [%s]",
                     repo_pattern, host.sh_hostname)
        return -1

    if len(fpaths) > 1:
        log.cl_error("multiple files found with pattern [%s] on host [%s]",
                     repo_pattern, host.sh_hostname)
        return -1

    repo_fpath = fpaths[0]

    command = ("sed -i 's|^enabled=0|enabled=1|' %s" % repo_fpath)
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

    return check_yum_repo_powertools(log, host, distro)


def check_yum_repo_base(log, host, distro):
    """
    Check whether the host has base repo.
    Only OS distro rhel8 needs this.
    """
    if distro not in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8):
        return 0
    if distro == os_distro.DISTRO_RHEL7:
        repo_name = "baseos"
    else:
        repo_name = "base"
    repo_ids = host.sh_yum_repo_ids(log)
    if repo_ids is None:
        log.cl_error("failed to get the yum repo IDs on host [%s]",
                     host.sh_hostname)
        return -1
    if repo_name not in repo_ids:
        log.cl_error("yum repo [%s] is not enabled on host [%s]",
                     repo_name, host.sh_hostname)
        return -1

    return 0


def check_yum_repo_epel(log, host, distro):
    """
    Check whether the host has epel repo.
    Only OS distro rhel8 needs this.
    """
    if distro not in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8):
        return 0
    repo_ids = host.sh_yum_repo_ids(log)
    if repo_ids is None:
        log.cl_error("failed to get the yum repo IDs on host [%s]",
                     host.sh_hostname)
        return -1
    if "epel" not in repo_ids:
        log.cl_error("yum repo [epel] is not enabled on host [%s]",
                     host.sh_hostname)
        log.cl_error("you might want to run command [dnf install -y epel-release]")
        return -1

    return 0
