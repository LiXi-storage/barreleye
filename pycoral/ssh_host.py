# pylint: disable=too-many-lines
"""
The host that localhost could use SSH to run command

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""

import time
import os
import glob
import shutil
import re
import stat
import socket
import traceback
import getpass
import datetime

# local libs
from pycoral import utils
from pycoral import clog
from pycoral import watched_io


# OS distribution RHEL6/CentOS6
DISTRO_RHEL6 = "rhel6"
# OS distribution RHEL7/CentOS7
DISTRO_RHEL7 = "rhel7"
# OS distribution RHEL8/CentOS8
DISTRO_RHEL8 = "rhel8"
# The shortest time that a reboot could finish. It is used to check whether
# a host has actually rebooted or not.
SHORTEST_TIME_REBOOT = 10
# The logest time that a reboot wil takes
LONGEST_TIME_REBOOT = 240
# The longest time that a simple command should finish
LONGEST_SIMPLE_COMMAND_TIME = 600
# Yum install is slow, so use a larger timeout value
LONGEST_TIME_YUM_INSTALL = LONGEST_SIMPLE_COMMAND_TIME * 3
# RPM install is slow, so use a larger timeout value
LONGEST_TIME_RPM_INSTALL = LONGEST_SIMPLE_COMMAND_TIME * 2
# The longest time that a issue reboot would stop the SSH server
LONGEST_TIME_ISSUE_REBOOT = 10


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


def sh_escape(command):
    """
    Escape special characters from a command so that it can be passed
    as a double quoted (" ") string in a (ba)sh command.

    Args:
            command: the command string to escape.

    Returns:
            The escaped command string. The required englobing double
            quotes are NOT added and so should be added at some point by
            the caller.

    See also: http://www.tldp.org/LDP/abs/html/escapingsection.html
    """
    command = command.replace("\\", "\\\\")
    command = command.replace("$", r'\$')
    command = command.replace('"', r'\"')
    command = command.replace('`', r'\`')
    return command


def scp_remote_escape(filename):
    """
    Escape special characters from a filename so that it can be passed
    to scp (within double quotes) as a remote file.

    Bis-quoting has to be used with scp for remote files, "bis-quoting"
    as in quoting x 2
    scp does not support a newline in the filename

    Args:
            filename: the filename string to escape.

    Returns:
            The escaped filename string. The required englobing double
            quotes are NOT added and so should be added at some point by
            the caller.
    """
    escape_chars = r' !"$&' + "'" + r'()*,:;<=>?[\]^`{|}'

    new_name = []
    for char in filename:
        if char in escape_chars:
            new_name.append("\\%s" % (char,))
        else:
            new_name.append(char)

    return sh_escape("".join(new_name))


def make_ssh_command(login_name="root", identity_file=None):
    """
    Return the ssh cmd string
    """
    extra_option = ""
    if identity_file is not None:
        extra_option = ("-i %s" % identity_file)
    full_command = ("ssh -a -x -l %s -o StrictHostKeyChecking=no "
                    "-o BatchMode=yes %s" %
                    (login_name, extra_option))
    return full_command


def ssh_command(hostname, command, login_name="root", identity_file=None):
    """
    Return the ssh command on a remote host
    """
    ssh_string = make_ssh_command(login_name=login_name,
                                  identity_file=identity_file)
    full_command = ("%s %s \"LANG=en_US %s\"" %
                    (ssh_string, hostname, sh_escape(command)))
    return full_command


def ssh_run(hostname, command, login_name="root", timeout=None,
            stdout_tee=None, stderr_tee=None, stdin=None,
            return_stdout=True, return_stderr=True,
            quit_func=None, identity_file=None, flush_tee=False):
    """
    Use ssh to run command on a remote host
    """
    if not isinstance(command, str):
        stderr = "type of command argument is not a basestring"
        return utils.CommandResult(stderr=stderr, exit_status=-1)

    full_command = ssh_command(hostname, command, login_name, identity_file)
    return utils.run(full_command, timeout=timeout, stdout_tee=stdout_tee,
                     stderr_tee=stderr_tee, stdin=stdin,
                     return_stdout=return_stdout, return_stderr=return_stderr,
                     quit_func=quit_func, flush_tee=flush_tee)


class SSHHost():
    """
    Each SSH host has an object of SSHHost
    """
    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    def __init__(self, hostname, identity_file=None, local=False,
                 ssh_for_local=True, login_name="root"):
        # The host has been checked to be local or not
        self.sh_cached_is_local = None
        if local:
            self.sh_host_desc = hostname + "(local)"
        else:
            self.sh_host_desc = hostname
        self.sh_hostname = hostname
        self.sh_identity_file = identity_file
        # If the host is inited as local, then it is local for sure
        self.sh_inited_as_local = local
        # The cached distro of the host
        self.sh_cached_distro = None
        # Key: command name, Value: True/False
        self.sh_cached_has_commands = {}
        self.sh_latest_uptime = 0
        # Dir name of crash dump
        self.sh_kdump_subdirs = []
        # The dir that saves crash dump of kdump
        self.sh_kdump_path = "/var/crash"
        # The cached lscpu diction
        self.sh_cached_lscpu_dict = None
        # Use ssh to run command even the host is local
        self.sh_ssh_for_local = ssh_for_local
        # The login name of user for ssh
        self.sh_login_name = login_name
        # The hostname got from "hostname" commnad
        self.sh_real_hostname = None

    def sh_is_up(self, log, timeout=60):
        """
        Whether this host is up now
        """
        ret = self.sh_run(log, "true", timeout=timeout)
        if ret.cr_exit_status != 0:
            return False
        return True

    def sh_expect_retval(self, retval, args):
        """
        Return 0 if got expected retval
        """
        # pylint: disable=no-self-use
        expect_exit_status = args[0]
        expect_stdout = args[1]
        expect_stderr = args[2]
        diff_exit_status = args[3]
        diff_stdout = args[4]
        diff_stderr = args[5]
        if (expect_exit_status is not None and
                expect_exit_status != retval.cr_exit_status):
            return -1

        if (diff_exit_status is not None and
                diff_exit_status == retval.cr_exit_status):
            return -1

        if (expect_stdout is not None and
                expect_stdout != retval.cr_stdout):
            return -1

        if (diff_stdout is not None and
                diff_stdout == retval.cr_stdout):
            return -1

        if (expect_stderr is not None and
                expect_stderr != retval.cr_stderr):
            return -1

        if (diff_stderr is not None and
                diff_stderr == retval.cr_stderr):
            return -1
        return 0

    def sh_wait_condition(self, log, command, condition_func, args, timeout=90,
                          sleep_interval=1, quiet=False):
        """
        Wait until the condition_func returns 0
        """
        time_start = time.time()
        while True:
            retval = self.sh_run(log, command)
            ret = condition_func(retval, args)
            if ret == 0:
                return 0

            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed < timeout:
                time.sleep(sleep_interval)
                continue
            if not quiet:
                log.cl_error("timeout of command [%s] on host [%s] after [%s] "
                             "seconds, ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname, timeout,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return -1

    def sh_wait_update(self, log, command, expect_exit_status=None,
                       expect_stdout=None, expect_stderr=None,
                       diff_exit_status=None, diff_stdout=None,
                       diff_stderr=None, timeout=90, sleep_interval=1,
                       quiet=False):
        """
        Wait until the command result on a host changed to expected values
        """
        args = [expect_exit_status, expect_stdout, expect_stderr,
                diff_exit_status, diff_stdout, diff_stderr]
        return self.sh_wait_condition(log, command, self.sh_expect_retval,
                                      args, timeout=timeout,
                                      sleep_interval=sleep_interval,
                                      quiet=quiet)

    def sh_wait_up(self, log, timeout=LONGEST_TIME_REBOOT):
        """
        Wait until the host is up
        """
        return self.sh_wait_update(log, "true", expect_exit_status=0,
                                   timeout=timeout)

    def sh_distro(self, log):
        """
        Return the distro of this host
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        if self.sh_cached_distro is not None:
            return self.sh_cached_distro

        no_lsb = False
        retval = self.sh_run(log, "which lsb_release")
        if retval.cr_exit_status != 0:
            log.cl_debug("lsb_release is needed on host [%s] for accurate "
                         "distro identification", self.sh_hostname)
            no_lsb = True

        if no_lsb:
            command = "cat /etc/redhat-release"
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
            if (retval.cr_stdout.startswith("CentOS Linux release 7.") or
                    retval.cr_stdout.startswith("Red Hat Enterprise Linux Server release 7.")):
                self.sh_cached_distro = DISTRO_RHEL7
                return DISTRO_RHEL7
            if (retval.cr_stdout.startswith("CentOS Linux release 8.") or
                    retval.cr_stdout.startswith("Red Hat Enterprise Linux Server release 8.")):
                self.sh_cached_distro = DISTRO_RHEL8
                return DISTRO_RHEL8
            if (retval.cr_stdout.startswith("CentOS Linux release 6.") or
                    retval.cr_stdout.startswith("Red Hat Enterprise Linux Server release 6.")):
                self.sh_cached_distro = DISTRO_RHEL6
                return DISTRO_RHEL6
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        command = "lsb_release -s -i"
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
        name = retval.cr_stdout.strip('\n')

        command = "lsb_release -s -r"
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
        version = retval.cr_stdout.strip('\n')

        if (name in ("RedHatEnterpriseServer", "ScientificSL", "CentOS")):
            if version.startswith("7"):
                self.sh_cached_distro = DISTRO_RHEL7
                return DISTRO_RHEL7
            if version.startswith("6"):
                self.sh_cached_distro = DISTRO_RHEL6
                return DISTRO_RHEL6
            if version.startswith("8"):
                self.sh_cached_distro = DISTRO_RHEL8
                return DISTRO_RHEL8
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "rhel", self.sh_hostname)
            return None
        if name == "EnterpriseEnterpriseServer":
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "oel", self.sh_hostname)
            return None
        if name == "SUSE LINUX":
            # PATCHLEVEL=$(sed -n -e 's/^PATCHLEVEL = //p' /etc/SuSE-release)
            # version="${version}.$PATCHLEVEL"
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "sles", self.sh_hostname)
            return None
        if name == "Fedora":
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "fc", self.sh_hostname)
            return None
        log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                     version, name, self.sh_hostname)
        return None

    def sh_file_line_number(self, log, fpath):
        """
        Return the line number of a file
        """
        command = "wc -l %s" % (fpath)
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
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected number of stdout lines for command [%s] on "
                         "host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        line = lines[0]
        fields = line.split()
        if len(fields) != 2:
            log.cl_error("unexpected number of stdout fields for command [%s] on "
                         "host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        line_number_str = fields[0]
        try:
            line_number = int(line_number_str)
        except:
            log.cl_error("invalid number of stdout for command [%s] on "
                         "host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return line_number

    def sh_prepare_user(self, log, name, uid, gid):
        """
        Add an user if it doesn't exist
        """
        # pylint: disable=too-many-return-statements
        ret = self.sh_run(log, "grep '%s:%s' /etc/passwd | wc -l" % (uid, gid))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to check uid [%s] gid [%s] on host "
                           "[%s], ret = [%d], stdout = [%s], stderr = [%s]",
                           uid, gid, self.sh_hostname, ret.cr_exit_status,
                           ret.cr_stdout, ret.cr_stderr)
            return -1

        if ret.cr_stdout.strip() != "0":
            log.cl_debug("user [%s] with uid [%s] gid [%s] already exists "
                         "on host [%s], will not create it",
                         name, uid, gid, self.sh_hostname)
            return 0

        ret = self.sh_run(log, "getent group %s" % (gid))
        if ret.cr_exit_status != 0 and len(ret.cr_stdout.strip()) != 0:
            log.cl_warning("failed to check gid [%s] on host "
                           "[%s], ret = [%d], stdout = [%s], stderr = [%s]",
                           gid, self.sh_hostname, ret.cr_exit_status,
                           ret.cr_stdout, ret.cr_stderr)
            return -1

        if ret.cr_exit_status == 0 and len(ret.cr_stdout.strip()) != 0:
            log.cl_debug("group [%s] with gid [%s] already exists on "
                         "host [%s], will not create it",
                         name, gid, self.sh_hostname)
            return 0

        ret = self.sh_run(log, "groupadd -g %s %s" % (gid, name))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to add group [%s] with gid [%s] on "
                           "host [%s], ret = [%d], stdout = [%s], "
                           "stderr = [%s]",
                           name, gid, self.sh_hostname,
                           ret.cr_exit_status,
                           ret.cr_stdout,
                           ret.cr_stderr)
            return -1

        ret = self.sh_run(log, "useradd -u %s -g %s %s" % (uid, gid, name))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to add user [%s] with uid [%s] gid [%s] "
                           "on host [%s], ret = [%d], stdout = [%s], "
                           "stderr = [%s]",
                           name, uid, gid, self.sh_hostname,
                           ret.cr_exit_status, ret.cr_stdout, ret.cr_stderr)
            return -1
        return 0

    def sh_umount(self, log, device):
        """
        Umount the file system of a device
        """
        command = "umount %s" % device
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_warning("failed to run command [%s] on host [%s], "
                           "ret = [%d], stdout = [%s], stderr = [%s]",
                           command, self.sh_hostname,
                           retval.cr_exit_status,
                           retval.cr_stdout, retval.cr_stderr)

            command = "umount -f %s" % device
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
                return -1
        return 0

    def sh_get_uptime(self, log, quiet=False):
        """
        Get the uptime of the host
        """
        command = ("expr $(date +%s) - $(cat /proc/uptime | "
                   "awk -F . '{print $1}')")
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            if not quiet:
                log.cl_error("can't get uptime on host [%s], command = [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             self.sh_hostname, command,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return int(retval.cr_stdout)

    def sh_nfs_exports(self, log):
        """
        Return a list of exported directory on this host.
        """
        command = "exportfs"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return None
        lines = retval.cr_stdout.splitlines()
        line_number = len(lines)
        if line_number % 2 == 1:
            log.cl_error("unexpected line number of output for command "
                         "[%s] on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return None
        exports = []
        for export_index in range(line_number // 2):
            line_index = export_index * 2
            line = lines[line_index]
            exports.append(line)
        return exports

    def sh_export_nfs(self, log, nfs_path):
        """
        Export NFS server on the host
        """
        log.cl_info("exporting nfs directory [%s] on host [%s]",
                    nfs_path, self.sh_hostname)
        nfs_commands = ["service nfs start", "mount | grep nfsd",
                        ("exportfs -o rw,no_root_squash *:%s" % nfs_path),
                        ("exportfs | grep %s" % nfs_path)]
        for command in nfs_commands:
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s]",
                             command, self.sh_hostname)
                return -1
        log.cl_info("exported nfs directory [%s] on host [%s]",
                    nfs_path, self.sh_hostname)
        return 0

    def sh_make_rsync_compatible_globs(self, log, path, is_local):
        """
        Given an rsync-style path, returns a list of globbed paths
        that will hopefully provide equivalent behaviour for scp. Does not
        support the full range of rsync pattern matching behaviour, only that
        exposed in the get/send_file interface (trailing slashes).

        The is_local param is flag indicating if the paths should be
        interpreted as local or remote paths.
        """

        # non-trailing slash paths should just work
        if len(path) == 0 or path[-1] != "/":
            return [path]

        # make a function to test if a pattern matches any files
        if is_local:
            def glob_matches_files(log, path, pattern):
                """
                Match the files on local host
                """
                # pylint: disable=unused-argument
                return len(glob.glob(path + pattern)) > 0
        else:
            def glob_matches_files(log, path, pattern):
                """
                Match the files on remote host
                """
                result = self.sh_run(log, "ls \"%s\"%s" %
                                     (sh_escape(path), pattern))
                return result.cr_exit_status == 0

        # take a set of globs that cover all files, and see which are needed
        patterns = ["*", ".[!.]*"]
        patterns = [p for p in patterns if glob_matches_files(log, path, p)]

        # convert them into a set of paths suitable for the commandline
        if is_local:
            return ["\"%s\"%s" % (sh_escape(path), pattern)
                    for pattern in patterns]
        return [scp_remote_escape(path) + pattern
                for pattern in patterns]

    def sh_make_scp_cmd(self, sources, dest):
        """
        Given a list of source paths and a destination path, produces the
        appropriate scp command for encoding it. Remote paths must be
        pre-encoded.
        """
        # pylint: disable=no-self-use
        extra_option = ""
        if self.sh_identity_file is not None:
            extra_option = ("-i %s" % self.sh_identity_file)
        command = ("scp -rqp -o StrictHostKeyChecking=no %s "
                   "%s '%s'")
        return command % (extra_option, " ".join(sources), dest)

    def _sh_make_rsync_compatible_source(self, log, source, is_local):
        """
        Applies the same logic as sh_make_rsync_compatible_globs, but
        applies it to an entire list of sources, producing a new list of
        sources, properly quoted.
        """
        return sum((self.sh_make_rsync_compatible_globs(log, path, is_local)
                    for path in source), [])

    def sh_encode_remote_paths(self, paths, escape=True):
        """
        Given a list of file paths, encodes it as a single remote path, in
        the style used by rsync and scp.
        """
        if escape:
            paths = [scp_remote_escape(path) for path in paths]
        if self.sh_inited_as_local and not self.sh_ssh_for_local:
            return '"%s"' % (" ".join(paths))
        return 'root@%s:"%s"' % (self.sh_hostname, " ".join(paths))

    def sh_set_umask_perms(self, dest):
        """
        Given a destination file/dir (recursively) set the permissions on
        all the files and directories to the max allowed by running umask.

        now this looks strange but I haven't found a way in Python to _just_
        get the umask, apparently the only option is to try to set it
        """
        # pylint: disable=no-self-use
        umask = os.umask(0)
        os.umask(umask)

        max_privs = 0o777 & ~umask

        def set_file_privs(filename):
            """
            Set the privileges of a file
            """
            file_stat = os.stat(filename)

            file_privs = max_privs
            # if the original file permissions do not have at least one
            # executable bit then do not set it anywhere
            if not file_stat.st_mode & 0o111:
                file_privs &= ~0o111

            os.chmod(filename, file_privs)

        # try a bottom-up walk so changes on directory permissions won't cut
        # our access to the files/directories inside it
        for root, dirs, files in os.walk(dest, topdown=False):
            # when setting the privileges we emulate the chmod "X" behaviour
            # that sets to execute only if it is a directory or any of the
            # owner/group/other already has execute right
            for dirname in dirs:
                os.chmod(os.path.join(root, dirname), max_privs)

            for filename in files:
                set_file_privs(os.path.join(root, filename))

        # now set privs for the dest itself
        if os.path.isdir(dest):
            os.chmod(dest, max_privs)
        else:
            set_file_privs(dest)

    def sh_get_file(self, log, source, dest, delete_dest=False,
                    preserve_perm=True):
        """
        copy the file/dir from the host to local host

        Currently, get file is based on scp. For bettern scalability
        we should improve to rsync.
        scp has no equivalent to --delete, just drop the entire dest dir
        """
        if self.sh_is_localhost():
            # This will prevent problems like copy to the same file
            # Removing the original file and etc.
            log.cl_error("returning failure to avoid getting file from the "
                         "local host")
            return -1

        dest = os.path.abspath(dest)
        if delete_dest and os.path.isdir(dest):
            shutil.rmtree(dest)
            ret = utils.mkdir(dest)
            if ret:
                log.cl_error("failed to mkdir [%s] on localhost",
                             dest)
                return -1

        if isinstance(source, str):
            source = [source]

        remote_source = self._sh_make_rsync_compatible_source(log, source,
                                                              False)
        if remote_source:
            # _sh_make_rsync_compatible_source() already did the escaping
            remote_source = self.sh_encode_remote_paths(remote_source,
                                                        escape=False)
            local_dest = sh_escape(dest)
            scp = self.sh_make_scp_cmd([remote_source], local_dest)
            retval = utils.run(scp)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to get file [%s] on host [%s] to "
                             "directory [%s] on local host [%s], "
                             "command = [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             source, self.sh_hostname, dest,
                             socket.gethostname(), scp,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        if not preserve_perm:
            # we have no way to tell scp to not try to preserve the
            # permissions so set them after copy instead.
            # for rsync we could use "--no-p --chmod=ugo=rwX" but those
            # options are only in very recent rsync versions
            self.sh_set_umask_perms(dest)
        return 0

    def sh_make_rsync_cmd(self, sources, dest, delete_dest, preserve_symlinks):
        """
        Given a list of source paths and a destination path, produces the
        appropriate rsync command for copying them. Remote paths must be
        pre-encoded.
        """
        # pylint: disable=no-self-use
        if self.sh_inited_as_local and not self.sh_ssh_for_local:
            ssh_option = ""
        else:
            ssh_cmd = make_ssh_command(identity_file=self.sh_identity_file)
            ssh_option = " --rsh='%s'" % ssh_cmd
        if delete_dest:
            delete_flag = " --delete"
        else:
            delete_flag = ""
        if preserve_symlinks:
            symlink_flag = ""
        else:
            symlink_flag = " -L"
        command = "rsync%s%s --timeout=1800%s -az %s %s"
        return command % (symlink_flag, delete_flag, ssh_option,
                          " ".join(sources), dest)

    def sh_has_command(self, log, command):
        """
        Check whether host has a command
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        if command in self.sh_cached_has_commands:
            return self.sh_cached_has_commands[command]

        ret = self.sh_run(log, "which %s" % command)
        if ret.cr_exit_status != 0:
            result = False
        else:
            result = True
        self.sh_cached_has_commands[command] = result
        return result

    def sh_has_zpool(self, log):
        """
        Check whether host has zpool command
        """
        return self.sh_has_command(log, "zpool")

    def sh_zfspool_list(self, log):
        """
        Return the ZFS pools
        """
        command = "lsmod | grep ^zfs"
        retval = self.sh_run(log, command)
        if (retval.cr_exit_status == 1 and retval.cr_stdout == ""):
            return []

        ret = self.sh_has_zpool(log)
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

    def sh_has_rsync(self, log):
        """
        Check whether host has rsync
        """
        return self.sh_has_command(log, "rsync")

    def sh_send_file(self, log, source, dest, delete_dest=False,
                     preserve_symlinks=False,
                     from_local=True,
                     remote_host=None,
                     timeout=None):
        """
        Send file/dir from a host to another host
        If from_local is True, the file will be sent from local host;
        Otherwise, it will be sent from this host (self).
        If remot_host is not none, the file will be sent to that host;
        Otherwise, it will be sent to this host (self).
        """
        # pylint: disable=too-many-locals
        if not self.sh_has_rsync(log):
            log.cl_debug("host [%s] doesnot have rsync, trying to install",
                         self.sh_hostname)
            command = "yum install rsync -y"
            retval = self.sh_run(log, "yum install rsync -y")
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1
            self.sh_cached_has_commands["rsync"] = True

        if isinstance(source, str):
            source = [source]
        if remote_host is None:
            remote_host = self
        remote_dest = remote_host.sh_encode_remote_paths([dest], False)

        local_sources = [sh_escape(path) for path in source]
        rsync = remote_host.sh_make_rsync_cmd(local_sources, remote_dest,
                                              delete_dest, preserve_symlinks)
        if from_local:
            ret = utils.run(rsync, timeout=timeout)
            from_host = socket.gethostname()
        else:
            from_host = self.sh_hostname
            ret = self.sh_run(log, rsync, timeout=timeout)
        if ret.cr_exit_status:
            log.cl_error("failed to send file [%s] on host [%s] to dest [%s] "
                         "on host [%s], command = [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         source, from_host, dest, remote_host.sh_hostname,
                         rsync, ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_run(self, log, command, silent=False,
               timeout=LONGEST_SIMPLE_COMMAND_TIME, stdout_tee=None,
               stderr_tee=None, stdin=None, return_stdout=True,
               return_stderr=True, quit_func=None, flush_tee=False,
               checking_hostname=False):
        """
        Run a command on the host
        """
        # pylint: disable=too-many-locals
        login_name = self.sh_login_name
        if not silent:
            log.cl_debug("starting command [%s] on host [%s]", command,
                         self.sh_hostname)
        if self.sh_inited_as_local and not self.sh_ssh_for_local:
            ret = utils.run(command, timeout=timeout, stdout_tee=stdout_tee,
                            stderr_tee=stderr_tee, stdin=stdin,
                            return_stdout=return_stdout,
                            return_stderr=return_stderr,
                            quit_func=quit_func, flush_tee=flush_tee)
        else:
            ret = ssh_run(self.sh_hostname, command, login_name=login_name,
                          timeout=timeout,
                          stdout_tee=stdout_tee, stderr_tee=stderr_tee,
                          stdin=stdin, return_stdout=return_stdout,
                          return_stderr=return_stderr, quit_func=quit_func,
                          identity_file=self.sh_identity_file,
                          flush_tee=flush_tee)
        if not silent:
            log.cl_debug("ran [%s] on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command, self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout,
                         ret.cr_stderr)
        if not checking_hostname and self.sh_real_hostname is None:
            retval = self.sh_run(log, "hostname", checking_hostname=True)
            if retval.cr_exit_status == 0:
                self.sh_real_hostname = retval.cr_stdout.strip()
                if self.sh_real_hostname != self.sh_hostname:
                    log.cl_warning("the real hostname of host [%s] is [%s], "
                                   "please correct the config to avoid "
                                   "unexpected error",
                                   self.sh_hostname, self.sh_real_hostname)
        return ret

    def sh_run_with_logs(self, log, command, stdout_fpath, stderr_fpath,
                         silent=False, timeout=LONGEST_SIMPLE_COMMAND_TIME,
                         stdin=None,
                         quit_func=None):
        """
        Run a command on the host and saving the stdout/stderr to log files
        """
        with open(stdout_fpath, "wb") as stdout_fd:
            with open(stderr_fpath, "wb") as stderr_fd:
                message = ("Running command [%s] on host [%s]\n" %
                           (command, self.sh_hostname))
                stderr_fd.write(message.encode())
                # Test could be a little bit time consuming.
                retval = self.sh_run(log, command,
                                     stdout_tee=stdout_fd,
                                     stderr_tee=stderr_fd,
                                     silent=silent,
                                     timeout=timeout,
                                     stdin=stdin,
                                     quit_func=quit_func,
                                     return_stdout=False,
                                     return_stderr=False)
                end = ("Returning value [%s] from command [%s] on host [%s]\n" %
                       (retval.cr_exit_status, command, self.sh_hostname))
                stderr_fd.write(end.encode())
        return retval

    def sh_get_kernel_ver(self, log):
        """
        Get the kernel version of the remote machine
        """
        ret = self.sh_run(log, "/bin/uname -r")
        if ret.cr_exit_status != 0:
            return None
        return ret.cr_stdout.rstrip()

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

    def sh_check_rpm_file_integrity(self, log, fpath):
        """
        Check the integrity of a RPM file
        """
        command = "rpm -K --nosignature %s" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_remove_dir(self, log, directory):
        """
        Remove directory recursively
        """
        dangerous_dirs = ["/"]
        for dangerous_dir in dangerous_dirs:
            if dangerous_dir == directory:
                log.cl_error("removing directory [%s] is dangerous",
                             directory)
                return -1

        ret = self.sh_run(log, "rm -fr %s" % (directory))
        if ret.cr_exit_status != 0:
            log.cl_error("failed to remove directory [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         directory, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_remove_file(self, log, fpath):
        """
        Remove file
        """
        ret = self.sh_run(log, "rm -f %s" % (fpath))
        if ret.cr_exit_status != 0:
            log.cl_error("failed to remove file [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         fpath, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_command_job(self, command, timeout=None, stdout_tee=None,
                       stderr_tee=None, stdin=None):
        """
        Return the command job on a host
        """
        full_command = ssh_command(self.sh_hostname, command)
        job = utils.CommandJob(full_command, timeout, stdout_tee, stderr_tee,
                               stdin)
        return job

    def sh_detect_device_fstype(self, log, device):
        """
        Return the command job on a host
        """
        command = ("blkid -o value -s TYPE %s" % device)
        ret = self.sh_run(log, command)
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return None
        if ret.cr_stdout == "":
            return None
        lines = ret.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("command [%s] on host [%s] has unexpected output "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return None

        return lines[0]

    def sh_mkfs(self, log, device, fstype):
        """
        Format the device to a given fstype
        """
        command = ("mkfs.%s %s" % (fstype, device))
        ret = self.sh_run(log, command)
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_rmdir_if_exist(self, log, directory):
        """
        Remote an empty directory if it exists
        """
        command = ("test -e %s" % (directory))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            command = ("rmdir %s" % (directory))
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
        if retval.cr_exit_status == 1:
            return 0
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_device_umount_all(self, log, device):
        """
        Umount all mounts of a device
        """
        command = ("cat /proc/mounts | grep \"%s \"" % device)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status not in [0, 1]:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            fields = line.split()
            assert fields[0] == device
            tmp_mount_point = fields[1]
            ret = self.sh_filesystem_umount(log, tmp_mount_point)
            if ret:
                log.cl_error("failed to umount [%s]", tmp_mount_point)
                return -1
        return 0

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

    def sh_device_mounted(self, log, device):
        """
        Whether device is mounted
        """
        return self.sh_filesystem_mounted(log, device)

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

    def sh_btrfs_df(self, log, mount_point):
        """
        report Btrfs file system disk space usage
        only return used bytes, because total bytes is not "accurate" since
        it will grow when keep on using
        """
        used = 0
        command = ("btrfs file df -b %s" % (mount_point))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, used
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 6:
            log.cl_error("command [%s] has unexpected output, "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, used

        df_pattern = (r"^.+: total=(?P<total>\d+), used=(?P<used>\d+)$")
        df_regular = re.compile(df_pattern)
        # ignore GlobalReserve line
        for line in lines[:5]:
            log.cl_debug("parsing line [%s]", line)
            match = df_regular.match(line)
            if not match:
                log.cl_error("command [%s] has unexpected output, "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1, used
            used += int(match.group("used"))

        return 0, used

    def sh_dumpe2fs(self, log, device):
        """
        Dump ext4 super information
        Return a direction
        """
        info_dict = {}
        command = ("dumpe2fs -h %s" % (device))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        lines = retval.cr_stdout.splitlines()
        for line in lines:
            if line == "":
                continue
            name = ""
            pointer = 0
            for character in line:
                if character != ":":
                    name += character
                    pointer += 1
                else:
                    pointer += 1
                    break

            for character in line[pointer:]:
                if character != " ":
                    break
                pointer += 1

            value = line[pointer:]
            info_dict[name] = value
            log.cl_debug("dumpe2fs name: [%s], value: [%s]", name, value)
        return info_dict

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

    def sh_pkill(self, log, process_cmd, special_signal=None):
        """
        Kill the all processes that are running command
        """
        signal_string = ""
        if special_signal is not None:
            signal_string = " --signal " + special_signal

        command = ("pkill%s -f -x -c '%s'" % (signal_string, process_cmd))
        log.cl_debug("start to run command [%s] on host [%s]", command,
                     self.sh_hostname)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status not in [0, 1]:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        instance_count = retval.cr_stdout.strip()
        log.cl_debug("killed [%s] instance of command [%s]",
                     instance_count, process_cmd)
        return 0

    def sh_md5sum(self, log, fpath):
        """
        Calculate the md5sum of a file
        """
        command = "md5sum %s | awk '{print $1}'" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def sh_gunzip_md5sum(self, log, fpath):
        """
        Use gunzip to decompress the file and then calculate the md5sum
        """
        command = "cat %s | gunzip | md5sum | awk '{print $1}'" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def sh_unquip_md5sum(self, log, fpath):
        """
        Use quip to decompress the file and then calculate the md5sum
        """
        command = "cat %s | quip -d -o fastq -c | md5sum | awk '{print $1}'" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def sh_truncate(self, log, fpath, size):
        """
        Truncate the size of a file
        """
        command = "truncate -s %s %s" % (size, fpath)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return retval.cr_exit_status
        return 0

    def sh_fill_random_binary_file(self, log, fpath, size):
        """
        Generate random binary file with the given size
        """
        block_count = (size + 1048575) / 1048576
        if block_count == 1:
            command = "dd if=/dev/urandom of=%s bs=%s count=1" % (fpath, size)
            written = size
        else:
            command = ("dd if=/dev/urandom of=%s bs=1M count=%s" %
                       (fpath, block_count))
            written = 1048576 * block_count

        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return retval.cr_exit_status
        if written == size:
            return 0
        return self.sh_truncate(log, fpath, size)

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

    def sh_selinux_status(self, log):
        """
        Check the current status of SELinux
        """
        command = "getenforce"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout.strip()

    def sh_disable_selinux(self, log):
        """
        Disable SELinux permanently
        """
        command = "sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        status = self.sh_selinux_status(log)
        if status in ("Disabled", "Permissive"):
            log.cl_debug("SELinux is already [%s] on host [%s]",
                         status, self.sh_hostname)
            return 0

        command = "setenforce 0"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        status = self.sh_selinux_status(log)
        if status not in ("Disabled", "Permissive"):
            log.cl_error("SELinux still has [%s] status on host [%s] after "
                         "command running [%s]",
                         status, self.sh_hostname, command)
            return -1
        return 0

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

    def sh_ping(self, log, silent=False):
        """
        Check whether local host can ping this host
        """
        command = "ping -c 1 %s" % self.sh_hostname
        retval = utils.run(command, silent=silent)
        if retval.cr_exit_status:
            if not silent:
                log.cl_error("failed to run command [%s] on local host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             socket.gethostname(),
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return 0

    def sh_kernel_set_default(self, log, kernel):
        """
        Set the default boot kernel
        Example of kernel string:
        /boot/vmlinuz-2.6.32-573.22.1.el6_lustre.2.7.15.3.x86_64
        """
        if self.sh_distro(log) == DISTRO_RHEL7:
            # This is not necessary for normal cases, but just in case of
            # broken grubenv file caused by repair
            command = ("grub2-editenv create")
            ret = self.sh_run(log, command)
            if ret.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s]"
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             ret.cr_exit_status,
                             ret.cr_stdout,
                             ret.cr_stderr)
                return -1

        command = ("grubby --set-default=%s" % (kernel))
        ret = self.sh_run(log, command)
        if ret.cr_exit_status:
            log.cl_error("command [%s] failed on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         ret.cr_exit_status,
                         ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_reboot_issue(self, log, force=False):
        """
        Issuing the reboot command on host
        """
        log.cl_debug("issuing rebooting of host [%s]",
                     self.sh_hostname)

        # Check whether the host is up first, to avoid unncessary wait
        if not self.sh_is_up(log):
            log.cl_error("unable to issue reboot of host [%s] because it is "
                         "not up", self.sh_hostname)
            return -1

        if force:
            retval = self.sh_run(log, "echo b > /proc/sysrq-trigger &")
        else:
            retval = self.sh_run(log, "reboot &")

        # We can't really trust the return value or output, sometimes the
        # reboot is so quick that the pipe might be broken and print
        # "Write failed: Broken pipe", so do not check the return value or
        # outputs at all, just wait and check after running the command.
        log.cl_debug("issued reboot on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return 0

    def sh_uptime_update(self, log):
        """
        update sh_latest_uptime to detect reboot
        """
        uptime = self.sh_get_uptime(log)
        if uptime < 0:
            log.cl_error("failed to get uptime on host [%s]",
                         self.sh_hostname)
            return -1

        self.sh_latest_uptime = uptime
        return 0

    def sh_rebooted(self, log, uptime_before_reboot):
        """
        Check whether the host rebooted
        """
        if uptime_before_reboot == 0:
            log.cl_debug("host [%s] does not even start rebooting",
                         self.sh_hostname)
            return False

        if not self.sh_is_up(log):
            log.cl_debug("host [%s] is not up yet",
                         self.sh_hostname)
            return False

        uptime = self.sh_get_uptime(log, quiet=True)
        if uptime < 0:
            return False

        if (uptime_before_reboot + SHORTEST_TIME_REBOOT >
                uptime):
            log.cl_debug("the uptime of host [%s] doesn't look "
                         "like rebooted, uptime now: [%d], uptime "
                         "before reboot: [%d], keep on waiting",
                         self.sh_hostname, uptime,
                         uptime_before_reboot)
            return False
        return True

    def sh_wait_reboot(self, log, uptime_before_rebooted):
        """
        Wait until host rebooted
        """
        log.cl_info("waiting until host [%s] rebooted", self.sh_hostname)
        time_start = time.time()
        sleep_interval = 1
        while not self.sh_rebooted(log, uptime_before_rebooted):
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed < LONGEST_TIME_REBOOT:
                time.sleep(sleep_interval)
                continue
            log.cl_error("booting of host [%s] timeouts after [%f] seconds",
                         self.sh_hostname, elapsed)
            return -1
        return 0

    def sh_reboot(self, log):
        """
        Reboot the host
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        log.cl_info("rebooting host [%s]", self.sh_hostname)

        # Do not try to reboot myself!
        if self.sh_is_localhost():
            log.cl_error("will not reboot host [%s], because it is local host",
                         self.sh_hostname)
            return -1

        none_force_issued = False
        uptime = self.sh_get_uptime(log)
        if uptime < 0:
            log.cl_error("failed to get uptime on host [%s]",
                         self.sh_hostname)
            return -1

        force = False
        ret = self.sh_run(log, "sync", timeout=120)
        if ret.cr_exit_status:
            log.cl_error("failed to sync on host [%s], force reboot",
                         self.sh_hostname)
            force = True

        if not force:
            ret = self.sh_reboot_issue(log, force=False)
            if ret:
                log.cl_error("failed to issue none-force reboot on host [%s]",
                             self.sh_hostname)
                return -1

            none_force_issued = True
            if self.sh_wait_reboot(log, uptime):
                log.cl_info("none-force reboot of host [%s] failed, trying "
                            "force reboot", self.sh_hostname)
            else:
                ret = self.sh_uptime_update(log)
                if ret:
                    log.cl_error("failed to update uptime on host [%s]",
                                 self.sh_hostname)
                    return -1
                log.cl_info("none-force reboot of host [%s] finished",
                            self.sh_hostname)
                return 0

        ret = self.sh_reboot_issue(log, force=True)
        if ret:
            if not none_force_issued:
                log.cl_error("failed to issue force reboot on host [%s]",
                             self.sh_hostname)
                return -1
            log.cl_info("failed to issue force reboot on host [%s], but "
                        "none-force reboot was issued successfully, "
                        "waiting again in case reboot takes longer than "
                        "expected",
                        self.sh_hostname)

        if self.sh_wait_reboot(log, uptime):
            log.cl_error("reboot of host [%s] failed",
                         self.sh_hostname)
            return -1

        ret = self.sh_uptime_update(log)
        if ret:
            log.cl_error("can't update uptime on host [%s]",
                         self.sh_hostname)
            return -1
        log.cl_info("reboot of host [%s] finished",
                    self.sh_hostname)
        return 0

    def sh_poweroff_issue(self, log, force=False):
        """
        Issuing the poweroff command on host
        """
        log.cl_debug("issuing poweroff of host [%s]",
                     self.sh_hostname)

        # Check whether the host is up first, to avoid unncessary wait
        if not self.sh_is_up(log):
            log.cl_error("unable to issue poweroff of host [%s] because it is "
                         "not up", self.sh_hostname)
            return -1

        if force:
            retval = self.sh_run(log, "echo o > /proc/sysrq-trigger &")
        else:
            retval = self.sh_run(log, "poweroff &")

        # We can't really trust the return value or output, sometimes the
        # shutdown is so quick that the pipe might be broken and print
        # "Write failed: Broken pipe", so do not check the return value or
        # outputs at all, just wait and check after running the command.
        log.cl_debug("issued shutdown on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return 0

    def sh_lustre_umount(self, log):
        """
        Umount all file systems of Lustre
        """
        retval = self.sh_run(log, "mount | grep 'type lustre' | awk '{print $1}'")
        if retval.cr_exit_status != 0:
            log.cl_error("failed to get lustre mount points on host "
                         "[%s]",
                         self.sh_hostname)
            return -1

        ret = 0
        devices = retval.cr_stdout.splitlines()
        # Umount client first, so as to prevent dependency
        for device in devices[:]:
            if device.startswith("/dev"):
                continue
            ret = self.sh_umount(log, device)
            if ret:
                break
            devices.remove(device)
        if ret == 0:
            for device in devices:
                ret = self.sh_umount(log, device)
                if ret:
                    break
        return ret

    def sh_chattr_has_projid_support(self, log):
        """
        Check whether chattr command has projid support
        """
        command = "man chattr | grep project"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return False
        return True

    def sh_lsscsi(self, log):
        """
        Return the devices
        Format:
        [scsi_host:channel:target_number:LUN tuple] peripheral_type \
        vendor_name model_name revision_string primary_device_node_name
        """
        command = "lsscsi -s"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, []

        lines = retval.cr_stdout.splitlines()
        output_pattern = (r"^.+ +(?P<device>\S+) +(?P<size>\S+)$")
        output_regular = re.compile(output_pattern)
        devices = []
        for line in lines:
            match = output_regular.match(line)
            if match:
                log.cl_debug("matched pattern [%s] with line [%s]",
                             output_pattern, line)
                device = match.group("device")
                devices.append(device)
        return 0, devices

    def sh_device_serial(self, log, device):
        """
        Return the serial of a device
        """
        command = "udevadm info %s | grep ID_SERIAL= | awk -F = '{print $2}'" % device
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout.strip()

    def sh_yum_repo_ids(self, log):
        """
        Get the repo IDs of yum
        """
        command = "yum repolist -v | grep Repo-id | awk '{print $3}'"
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

        repo_ids = []
        lines = retval.cr_stdout.splitlines()
        for line in lines:
            fields = line.split()
            repo_field = fields[0]
            # Some repo ID looks like "base/7/x86_64"
            repo_fields = repo_field.split("/")
            repo_id = repo_fields[0]
            repo_ids.append(repo_id)
        return repo_ids

    def sh_uuid(self, log):
        """
        Get the uuid of the host
        """
        command = "dmidecode -s system-uuid"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout.strip()

    def sh_is_same_host(self, log, host):
        """
        Create a random file in /tmp/, and check that in
        the other host. If that file exists, that is local host.
        """
        fpath = "/tmp/" + utils.random_word(16)
        command = "touch %s" % fpath
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

        ret = host.sh_path_exists(log, fpath)

        # Cleanup gabage
        command = "rm -f %s" % fpath
        self.sh_run(log, command)

        return ret

    def sh_is_localhost(self):
        """
        Create a random file in /tmp/, and check that in
        local file. If that file exists, that is local host.
        """
        if self.sh_inited_as_local:
            return 1

        if self.sh_cached_is_local is not None:
            return self.sh_cached_is_local

        local_hostname = socket.gethostname()
        if local_hostname == self.sh_hostname:
            ret = 1
        else:
            ret = 0

        self.sh_cached_is_local = ret
        return ret

    def sh_file_executable(self, log, fpath):
        """
        Check the file is executable
        """
        command = ("test -f %s && test -x %s " % (fpath, fpath))
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

    def sh_service_is_active(self, log, service_name):
        """
        If is active, return 1. If inactive, return 0. Return -1 on error.
        """
        command = "systemctl is-active %s" % service_name
        retval = self.sh_run(log, command)
        # Unknown service means this service might have not been configured
        if retval.cr_stdout == "unknown\n":
            return 0

        if retval.cr_stdout == "inactive\n":
            return 0

        if retval.cr_stdout == "failed\n":
            return 0

        if retval.cr_stdout in ["active\n", "activating\n"]:
            return 1
        return -1

    def sh_service_stop(self, log, service_name):
        """
        Stop the service if it is running
        """
        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        # Unknown service means this service might have not been configured
        if retval.cr_stdout == "unknown\n":
            return 0

        if retval.cr_stdout == "inactive\n":
            return 0

        if retval.cr_stdout == "failed\n":
            return 0

        if retval.cr_stdout in ["active\n", "activating\n"]:
            command = ("systemctl stop %s" % (service_name))
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

            command = ("systemctl is-active %s" % (service_name))
            retval = self.sh_run(log, command)
            if retval.cr_stdout == "unknown\n":
                return 0

            if retval.cr_stdout == "inactive\n":
                return 0

            if retval.cr_stdout == "failed\n":
                return 0
            log.cl_error("failed to stop service [%s] on host [%s], "
                         "command = [%s], ret = [%d], stdout = [%s], stderr = [%s]",
                         service_name,
                         self.sh_hostname,
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_start(self, log, service_name):
        """
        Start the service if it is not running
        """
        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 0

        command = ("systemctl start %s" % (service_name))
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

        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 0

        log.cl_error("failed to start service [%s] on host [%s], "
                     "command = [%s], ret = [%d], stdout = [%s], "
                     "stderr = [%s]",
                     service_name,
                     self.sh_hostname,
                     command,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_restart(self, log, service_name):
        """
        Restart the service if it is not running
        """
        command = ("systemctl restart %s" % (service_name))
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

        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 0

        log.cl_error("failed to restart service [%s] after checking it "
                     "using command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     service_name,
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_is_enabled(self, log, service_name):
        """
        Check whether service is enabled.
        """
        command = "systemctl is-enabled %s" % service_name
        retval = self.sh_run(log, command)
        if retval.cr_stdout == "disabled\n":
            return 0

        if retval.cr_stdout == "enabled\n":
            return 1

        log.cl_error("unexpected command of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def sh_service_disable(self, log, service_name):
        """
        Disable the service from starting automatically
        """
        command = ("systemctl disable %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status and retval.cr_stderr != "":
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_check_service_exists(self, log, service_name):
        """
        Check whether service exist.
        """
        command = ("systemctl cat -- %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            return -1
        return 0

    def sh_service_enable(self, log, service_name):
        """
        Disable the service from starting automatically
        """
        command = ("systemctl enable %s" % (service_name))
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

    def sh_service_start_enable(self, log, service_name, restart=False):
        """
        Start and enable a service
        """
        skip_wait = False
        if restart:
            ret = self.sh_service_restart(log, service_name)
            command = "restart"
        else:
            command = ("systemctl is-active %s" % (service_name))
            retval = self.sh_run(log, command)
            if retval.cr_stdout == "active\n":
                skip_wait = True
                ret = 0
            else:
                ret = self.sh_service_start(log, service_name)
            command = "start"
        if ret:
            log.cl_error("failed to %s service [%s] on host [%s]",
                         command, service_name, self.sh_hostname)
            return -1

        ret = self.sh_service_enable(log, service_name)
        if ret:
            log.cl_error("failed to enable service [%s] on host [%s]",
                         service_name, self.sh_hostname)
            return -1

        if skip_wait:
            return 0

        command = ("systemctl status %s" % service_name)
        ret = self.sh_wait_update(log, command, diff_exit_status=0,
                                  timeout=5, quiet=True)
        if ret:
            # Still active after timeout
            return 0
        log.cl_error("service [%s] is not active on host [%s]",
                     service_name, self.sh_hostname)
        return -1

    def sh_create_inode(self, log, inode_path, inode_type=stat.S_IFREG,
                        major=None, minor=None, path=None):
        """
        Create inode with a specifi type
        """
        if inode_type == stat.S_IFREG:
            command = ("touch %s" % (inode_path))
        elif inode_type == stat.S_IFDIR:
            command = ("mkdir %s" % (inode_path))
        elif inode_type == stat.S_IFBLK:
            command = ("mknod %s b %s %s" % (inode_path, major, minor))
        elif inode_type == stat.S_IFCHR:
            command = ("mknod %s c %s %s" % (inode_path, major, minor))
        elif inode_type == stat.S_IFLNK:
            command = ("ln -s %s %s" % (path, inode_path))
        elif inode_type == stat.S_IFSOCK:
            command = ("python -c \"import socket;"
                       "sock = socket.socket(socket.AF_UNIX);"
                       "sock.bind('%s')\"" % (inode_path))
        elif inode_type == stat.S_IFIFO:
            command = ("mknod %s p" % (inode_path))
        else:
            log.cl_error("unknown type [%s]", inode_type)
            return -1

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

    def sh_remove_inode(self, log, inode_path, inode_type=stat.S_IFREG):
        """
        Create inode with a specifi type
        """
        if inode_type == stat.S_IFDIR:
            command = ("rmdir %s" % (inode_path))
        else:
            command = ("rm -f %s" % (inode_path))

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

    def sh_watched_run(self, log, command, stdout_file, stderr_file,
                       silent=False, timeout=None, stdin=None,
                       return_stdout=True, return_stderr=True, quit_func=None,
                       flush_tee=False,
                       stdout_watch_func=watched_io.log_watcher_stdout_simplified,
                       stderr_watch_func=watched_io.log_watcher_stderr_simplified,
                       console_format=clog.FMT_QUIET):
        """
        Run the command and watching the output
        """
        # pylint: disable=too-many-locals
        if not silent:
            log.cl_debug("start to run command [%s] on host [%s]", command,
                         self.sh_hostname)

        origin_console_format = log.cl_console_format
        if origin_console_format != console_format:
            log.cl_change_config(console_format=console_format,
                                 resultsdir=log.cl_resultsdir)
        args = {}
        args[watched_io.WATCHEDIO_LOG] = log
        args[watched_io.WATCHEDIO_HOSTNAME] = self.sh_hostname
        stdout_fd = watched_io.watched_io_open(stdout_file,
                                               stdout_watch_func,
                                               args)
        stderr_fd = watched_io.watched_io_open(stderr_file,
                                               stderr_watch_func,
                                               args)
        retval = self.sh_run(log, command, stdout_tee=stdout_fd,
                             stderr_tee=stderr_fd, silent=silent,
                             timeout=timeout, stdin=stdin,
                             return_stdout=return_stdout,
                             return_stderr=return_stderr, quit_func=quit_func,
                             flush_tee=flush_tee)
        if origin_console_format != console_format:
            log.cl_change_config(console_format=origin_console_format,
                                 resultsdir=log.cl_resultsdir)
        stdout_fd.close()
        stderr_fd.close()
        if not silent:
            log.cl_debug("finished command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
        return retval

    def sh_mkdir(self, log, path):
        """
        Create directory if necessary, return -1 on error
        """
        retval = self.sh_run(log, "test -e %s" % path)
        if retval.cr_exit_status != 0:
            retval = self.sh_run(log, "mkdir -p %s" % path)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to create directory [%s]", path)
                return -1

        retval = self.sh_run(log, "test -d %s" % path)
        if retval.cr_exit_status != 0:
            log.cl_error("[%s] is not directory", path)
            return -1

        return 0

    def sh_path_exists(self, log, path):
        """
        Path exists or not
        Return 1 if exist, 0 if not, negative if error
        """
        command = "stat %s" % path
        retval = self.sh_run(log, command)
        output = retval.cr_stderr.strip()
        if retval.cr_exit_status == 0:
            return 1
        if output.endswith("No such file or directory"):
            return 0
        log.cl_error("failed to run [%s] on host [%s], ret = [%d], "
                     "stdout = [%s], stderr = [%s]", command,
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return -1

    def sh_path_isreg(self, log, path):
        """
        Path is regular file or not
        Return 1 if exists and at the same time is a regular file, 0 if not,
        netagive on error.
        """
        stat_result = self.sh_stat(log, path)
        if stat_result is None:
            return -1
        return stat.S_ISREG(stat_result.st_mode)

    def sh_path_isdir(self, log, path):
        """
        Path is directory or not
        Return 1 if exists and at the same time is a directory, 0 if not,
        netagive on error.
        """
        stat_result = self.sh_stat(log, path)
        if stat_result is None:
            return -1
        return stat.S_ISDIR(stat_result.st_mode)

    def sh_stat(self, log, path, quiet=False):
        """
        Stat file, return os.stat_result. Return None on error
        """
        # stat_result accepts tuple of at least 10 integers with order:
        # st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size,
        # st_atime, st_mtime, st_ctime.
        #
        # 3.3: Added st_atime_ns, st_mtime_ns, st_ctime_ns
        # 3.5: Added st_file_attributes
        # 3.7: Added st_fstype
        # 3.8: Added st_reparse_tag
        command = "stat -c '%f %i %D %h %u %g %s %X %Y %Z' " + path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            if not quiet:
                log.cl_error("failed to run [%s] on host [%s], ret = [%d], "
                             "stdout = [%s], stderr = [%s]", command,
                             self.sh_hostname, retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            if not quiet:
                log.cl_error("unexpected stat result of path [%s] on host [%s], "
                             "stdout = [%s]",
                             path, self.sh_hostname, retval.cr_stdout)
            return None

        fields = lines[0].split()
        if len(fields) != 10:
            if not quiet:
                log.cl_error("unexpected field number of stat result for "
                             "path [%s] on host [%s], stdout = [%s]",
                             path, self.sh_hostname, retval.cr_stdout)
            return None

        args = []
        bases = [16, 10, 16, 10, 10, 10, 10, 10, 10, 10]
        field_number = 0
        for field in fields:
            base = bases[field_number]
            try:
                args.append(int(field, base))
            except ValueError:
                if not quiet:
                    log.cl_error("unexpected field [%d] with value [%s] for path "
                                 "[%s] on host [%s], stdout = [%s]",
                                 field_number, field, path, self.sh_hostname,
                                 retval.cr_stdout)
                return None
            field_number += 1
        return os.stat_result(tuple(args))

    def sh_get_fstype(self, log, path):
        """
        Return fstype of a path.
        """
        command = "stat -f -c '%T' " + path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run [%s] on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]", command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected line number in stdout of command [%s] "
                         "on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        fields = lines[0].split()
        if len(fields) != 1:
            log.cl_error("unexpected field number in stdout of command [%s] "
                         "on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        return fields[0]

    def sh_get_file_space_usage(self, log, path):
        """
        Return the allocated space of a file in byte, return -1 on error
        """
        command = "stat -c '%b %B' " + path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run [%s] on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]", command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected line number in stdout of command [%s] "
                         "on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        fields = lines[0].split()
        if len(fields) != 2:
            log.cl_error("unexpected field number in stdout of command [%s] "
                         "on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        blocks = 1
        for field in fields:
            try:
                field_number = int(field, 10)
            except ValueError:
                log.cl_error("invalid field [%s] in stdout of command [%s] "
                             "on host [%s], ret = [%d], "
                             "stdout = [%s], stderr = [%s]",
                             field, command,
                             self.sh_hostname, retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
                return -1
            blocks *= field_number

        return blocks

    def sh_get_file_size(self, log, path, size=True, quiet=False):
        """
        Return the file size or blocks in byte, return -1 on error
        """
        if size:
            stat_result = self.sh_stat(log, path, quiet=quiet)
            if stat_result is None:
                if not quiet:
                    log.cl_error("failed to stat file [%s] on host [%s]",
                                 path, self.sh_hostname)
                return -1

            return stat_result.st_size
        return self.sh_get_file_space_usage(log, path)

    def sh_pcs_resources(self, log):
        """
        Return a list of resources
        """
        command = "pcs resource show"
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
        if len(lines) == 1 and lines[0] == "NO resources configured":
            return []

        resources = []
        output_pattern = r"^\s+(?P<name>\S+)\s+(?P<path>\S+)\s+(?P<status>\S+).+$"
        output_regular = re.compile(output_pattern)
        for line in lines:
            match = output_regular.match(line)
            if not match:
                log.cl_error("command [%s] has unexpected output on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return None

            name = match.group("name")
            resources.append(name)

        return resources

    def sh_pcs_resources_clear(self, log):
        """
        Clear all PCS resources
        """
        resources = self.sh_pcs_resources(log)
        if resources is None:
            log.cl_error("failed to get PCS resources on host [%s]",
                         self.sh_hostname)
            return -1
        for name in resources:
            command = "pcs resource delete " + name
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
        resources = self.sh_pcs_resources(log)
        if resources is None:
            log.cl_error("failed to get PCS resources on host [%s]",
                         self.sh_hostname)
            return -1
        if len(resources) != 0:
            log.cl_error("PCS resources %s still exist on host [%s]",
                         resources, self.sh_hostname)
            return -1
        return 0

    def sh_crm_resouce_order(self, log, order_id, first, then):
        """
        Set the resource order
        """
        command = ("crm configure order %s %s %s" %
                   (order_id, first, then))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_get_checksum(self, log, path, checksum_command="sha256sum"):
        """
        Get a file's checksum.
        """
        command = "%s %s" % (checksum_command, path)
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
        if len(lines) != 1:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command, self.sh_hostname, retval.cr_stdout)
            return None

        fields = lines[0].split()
        if len(fields) != 2:
            log.cl_error("unexpected field number of output of command "
                         "[%s] on host [%s], stdout = [%s]",
                         command, self.sh_hostname, retval.cr_stdout)
            return None

        calculated_checksum = fields[0]
        return calculated_checksum

    def sh_check_checksum(self, log, path, expected_checksum,
                          checksum_command="sha256sum"):
        """
        Check whether the file's checksum is expected
        """
        exists = self.sh_path_exists(log, path)
        if exists < 0:
            log.cl_error("failed to check whether file [%s] exists on host [%s]",
                         path, self.sh_hostname)
            return -1
        if exists == 0:
            return -1

        calculated_checksum = self.sh_get_checksum(log, path,
                                                   checksum_command=checksum_command)
        if calculated_checksum is None:
            log.cl_error("failed to calculate the checksum of file [%s] on "
                         "host [%s]",
                         path, self.sh_hostname)
            return -1

        if expected_checksum != calculated_checksum:
            log.cl_error("unexpected [%s] checksum [%s] of file [%s] on "
                         "host [%s], expected [%s]",
                         checksum_command, calculated_checksum, path,
                         self.sh_hostname, expected_checksum)
            return -1
        return 0

    def sh_resolve_path(self, log, path_pattern, quiet=False):
        """
        Resolve a path pattern to a path
        Wildcard can be used in the pattern.
        """
        # Need -d otherwise directory will list its content
        command = ("ls -d %s" % (path_pattern))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 2:
            return []
        if retval.cr_exit_status < 0:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None

        fnames = retval.cr_stdout.split()
        return fnames

    def sh_real_path(self, log, path, quiet=False):
        """
        Return the real path from a path.
        """
        command = ("realpath %s" % (path))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status < 0:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None

        fpaths = retval.cr_stdout.splitlines()
        if len(fpaths) != 1:
            if not quiet:
                log.cl_error("unexpected stdout line number of command [%s] "
                             "on host [%s], ret = [%d], stdout = [%s], "
                             "stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None
        return fpaths[0]

    def sh_install_pip3_packages(self, log, pip_packages, pip_dir,
                                 quiet=False, tsinghua_mirror=False):
        """
        Install pip3 packages on a host
        """
        if len(pip_packages) == 0:
            return 0
        command = "pip3 install"
        if pip_dir is not None:
            command += " --no-index --find-links %s" % pip_dir
        elif tsinghua_mirror:
            command += " -i https://pypi.tuna.tsinghua.edu.cn/simple"
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

    def sh_show_pip3_packages(self, log, pip_package):
        """
        Get information of pip3 package on a host.
        Return a dict
        """
        command = ("pip3 show %s" % pip_package)
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

        info_dict = {}
        lines = retval.cr_stdout.splitlines()
        seperator = ": "
        for line in lines:
            seperator_index = line.find(seperator)
            if seperator_index <= 0:
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
        package_info_dict = self.sh_show_pip3_packages(log, pip_package)
        if package_info_dict is None:
            log.cl_error("failed to get info about pip3 packages [%s] on "
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

    def sh_stop_sshd(self, log):
        """
        Stop the sshd service on host
        """
        log.cl_debug("stopping sshd of host [%s]",
                     self.sh_hostname)

        command = "systemctl stop sshd &"
        retval = self.sh_run(log, command)

        # We can't really trust the return value or output, because the pipe
        # might be broken and print "Write failed: Broken pipe", so do not
        # check the return value or outputs at all, just wait and check after
        # running the command.
        log.cl_debug("issued command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)

        # Quick test that we succed
        if self.sh_is_up(log, timeout=5):
            log.cl_debug("host is still up after running command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1
        return 0

    def sh_target_cpu(self, log):
        """
        Return the target CPU, e.g. x86_64 or aarch64
        """
        command = "uname -i"
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
        return retval.cr_stdout.strip()

    def sh_lsmod(self, log):
        """
        Returm the inserted module names
        """
        command = "lsmod"
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
        if len(lines) <= 1:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_stdout)
            return None
        lines = lines[1:]
        modules = []
        for line in lines:
            fields = line.split()
            if len(fields) == 0:
                log.cl_error("unexpected line [%s] of command [%s] on host "
                             "[%s]",
                             line,
                             command,
                             self.sh_hostname)
                return None
            modules.append(fields[0])
        return modules

    def sh_git_fetch_from_url(self, log, git_dir, git_url, commit=None,
                              ssh_identity_file=None):
        """
        Fetch the soure codes from Git server. Assuming the dir
        has been inited by git.
        If commit is None, fetch without checking out.
        """
        command = ("cd %s && git config remote.origin.url %s && "
                   "git fetch --tags %s +refs/heads/*:refs/remotes/origin/*" %
                   (git_dir, git_url, git_url))
        if commit is not None:
            command += " && git checkout %s -f" % commit
        if ssh_identity_file is not None:
            # Git 2.3.0+ has GIT_SSH_COMMAND, but earlier versions do not.
            command = ("ssh-agent sh -c 'ssh-add " + ssh_identity_file +
                       " && " + command + "'")

        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1
        return 0

    def sh_git_rmdir_and_fetch_from_url(self, log, git_dir, git_url, commit=None,
                                        ssh_identity_file=None):
        """
        Get the soure codes from Git server.
        """
        command = ("rm -fr %s && mkdir -p %s && git init %s" %
                   (git_dir, git_dir, git_dir))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        ret = self.sh_git_fetch_from_url(log, git_dir, git_url, commit=commit,
                                         ssh_identity_file=ssh_identity_file)
        return ret

    def sh_get_dir_fnames(self, log, directory, all_fnames=False):
        """
        Return fnames under a dir
        """
        command = "ls %s" % (directory)
        if all_fnames:
            command += " --all"
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
        fnames = retval.cr_stdout.splitlines()
        return fnames

    def sh_check_dir_content(self, log, directory, contents,
                             ignoral_extra_contents=False, cleanup=False):
        """
        Check that the directory has expected content
        """
        existing_fnames = self.sh_get_dir_fnames(log, directory)
        if existing_fnames is None:
            log.cl_error("failed to get the file names under [%s] of "
                         "host [%s]",
                         directory, self.sh_hostname)
            return -1

        for fname in contents:
            if fname not in existing_fnames:
                log.cl_error("can not find necessary content [%s] under "
                             "directory [%s] of host [%s]", fname, directory,
                             self.sh_hostname)
                return -1
            existing_fnames.remove(fname)

        if ignoral_extra_contents:
            return 0

        for fname in existing_fnames:
            fpath = directory + "/" + fname
            if cleanup:
                log.cl_debug("found unnecessary content [%s] under directory "
                             "[%s] of host [%s], removing it", fname, directory,
                             self.sh_hostname)
                ret = self.sh_remove_dir(log, fpath)
                if ret:
                    return -1
            else:
                log.cl_error("found unnecessary content [%s] under directory "
                             "[%s] of host [%s]", fname, directory,
                             self.sh_hostname)
                return -1
        return 0

    def sh_get_device_size(self, log, device):
        """
        Get the device size (KB)
        """
        command = ("lsblk -b -d %s --output SIZE | "
                   "sed -n '2,1p' | awk '{print $1/1024}'" %
                   device)
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

        try:
            device_size = int(retval.cr_stdout.strip())
        except:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return device_size

    def sh_fuser_kill(self, log, fpath):
        """
        Run "fuser -km" to a fpath
        """
        command = ("fuser -km %s" % (fpath))
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

    def sh_kdump_init(self, log):
        """
        Init kdump in case of kernel crash
        """
        command = "grep ^path /etc/kdump.conf | awk '{print $2}'"
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

        if retval.cr_stdout != "":
            self.sh_kdump_path = retval.cr_stdout.strip()
            log.cl_debug("kdump path on host [%s] is configured as [%s]",
                         self.sh_hostname, self.sh_kdump_path)
        else:
            log.cl_info("kdump path on host [%s] is not configured, using "
                        "default value [%s]",
                        self.sh_hostname, self.sh_kdump_path)

        command = "ls %s" % self.sh_kdump_path
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
        self.sh_kdump_subdirs = retval.cr_stdout.strip().split("\n")
        return 0

    def sh_kdump_get(self, log, local_kdump_path,
                     check_timeout=LONGEST_SIMPLE_COMMAND_TIME):
        """
        Return the new kdump number since the last copy
        If local_kdump_path is None, do not copy.
        """
        local_hostname = socket.gethostname()
        command = "ls %s" % self.sh_kdump_path
        retval = self.sh_run(log, command, timeout=check_timeout)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        kdump_subdirs = retval.cr_stdout.strip().split("\n")

        ret = 0
        for subdir in kdump_subdirs:
            if subdir in self.sh_kdump_subdirs:
                continue
            kdump_dir = ("%s/%s" % (self.sh_kdump_path, subdir))
            if local_kdump_path is None:
                log.cl_info("found a new crash dump [%s] on host [%s]",
                            kdump_dir, self.sh_hostname)
            else:
                log.cl_info("found a new crash dump [%s] on host [%s], "
                            "copying to [%s] on local host [%s]",
                            kdump_dir, self.sh_hostname,
                            local_kdump_path, local_hostname)

                if not os.path.exists(local_kdump_path):
                    os.makedirs(local_kdump_path)

                rc = self.sh_get_file(log, kdump_dir, local_kdump_path)
                if rc:
                    log.cl_error("failed to get file [%s] from host [%s] to "
                                 "path [%s] on local host [%s]",
                                 kdump_dir, self.sh_hostname,
                                 local_kdump_path, local_hostname)
                    return -1
                self.sh_kdump_subdirs.append(subdir)
            ret += 1
        return ret

    def sh_git_short_sha(self, log, path):
        """
        Return the short SHA of the HEAD of a git directory
        """
        command = "cd %s && git rev-parse --short HEAD" % path
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
        return retval.cr_stdout.strip()

    def sh_git_subject(self, log, path):
        """
        Return subject of the last commit.
        """
        command = ("cd " + path +
                   ' && git log --pretty=format:%s -1')
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
        if len(lines) != 1:
            log.cl_error("unexpected output lines of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        git_subject = lines[0]
        return git_subject

    def sh_epoch_seconds(self, log):
        """
        Return the seconds since 1970-01-01 00:00:00 UTC
        """
        command = "date +%s"
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
        second_string = retval.cr_stdout.strip()
        try:
            seconds = int(second_string)
        except:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "stdout = [%s]", command, self.sh_hostname,
                         retval.cr_stdout)
            return -1
        return seconds

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

    def sh_files_equal(self, log, fpath0, fpath1):
        """
        Compare two files. If equal, return 1. If not equal, return 0.
        Return -1 on error.
        """
        command = "diff %s %s -1" % (fpath0, fpath1)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            return 1
        if retval.cr_exit_status == 1 and retval.cr_stdout != "":
            return 0

        log.cl_error("failed to run command [%s] on host [%s], ret = [%d], "
                     "stdout = [%s], stderr = [%s]", command,
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return -1

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

    def sh_sync_two_dirs(self, log, src, dest_parent):
        """
        Sync a dir to another dir. dest_parent is the target parent dir. And
        the dir with the same name under that parent will be removed.
        """
        # Stripe / otherwise it will has different meaning
        dest_parent = dest_parent.rstrip("/")
        command = ("rsync --delete --sparse -azv %s %s" %
                   (src, dest_parent))
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

    def sh_download_file(self, log, url, fpath, expected_checksum,
                         output_fname=None, checksum_command="sha1sum",
                         use_curl=False, no_check_certificate=False):
        """
        Download file and check checksum after downloading.
        Use existing file if checksum is expected.
        If output_fname exists, need to add -O to wget because sometimes the URL
        does not save the download file with expected fname.
        """
        # pylint: disable=too-many-branches,too-many-locals
        target_dir = os.path.dirname(fpath)
        fname = os.path.basename(fpath)
        url_fname = os.path.basename(url)
        if output_fname is None:
            if fname != url_fname:
                log.cl_error("url [%s] and fpath [%s] are not consistent",
                             url, fpath)
                return -1
        elif fname != output_fname:
            log.cl_error("url [%s] and output fname [%s] are not consistent",
                         url, output_fname)
            return -1

        ret = 0
        if expected_checksum is not None:
            rc = self.sh_check_checksum(log, fpath, expected_checksum,
                                        checksum_command=checksum_command)
            if rc:
                ret = rc
        if fpath.endswith(".rpm"):
            rc = self.sh_check_rpm_file_integrity(log, fpath)
            if rc:
                ret = rc

        if ret == 0:
            return 0

        log.cl_info("downloading file [%s] from [%s]", fpath, url)
        # Cleanup the file
        command = "rm -f %s" % fpath
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

        if output_fname is None:
            if use_curl:
                download_command = "curl -O %s" % url
            else:
                download_command = "wget %s" % url
        else:
            if use_curl:
                download_command = ("curl %s -o %s" %
                                    (url, output_fname))
            else:
                download_command = ("wget %s -O %s" %
                                    (url, output_fname))

        # Curl does not care about certificate
        if not use_curl and no_check_certificate:
            download_command += " --no-check-certificate"

        command = ("mkdir -p %s && cd %s && %s" %
                   (target_dir, target_dir, download_command))
        log.cl_info("running command [%s] on host [%s]",
                    command, self.sh_hostname)
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

        if fpath.endswith(".rpm"):
            ret = self.sh_check_rpm_file_integrity(log, fpath)
            if ret:
                log.cl_error("newly downloaded RPM [%s] is broken",
                             fpath)
                return -1

        if expected_checksum is None:
            return 0

        ret = self.sh_check_checksum(log, fpath, expected_checksum,
                                     checksum_command=checksum_command)
        if ret:
            log.cl_error("newly downloaded file [%s] does not have expected "
                         "sha1sum [%s]",
                         fpath, expected_checksum)
            return -1
        return 0

    def sh_lvm_volumes(self, log):
        """
        Return volume names with the format of vg_name/lv_name
        """
        command = "lvs"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, self.sh_hostname)
            return None
        lines = retval.cr_stdout.splitlines()
        if len(lines) == 0:
            return []
        lines = lines[1:]
        volume_names = []
        for line in lines:
            fields = line.split()
            if len(fields) < 2:
                log.cl_error("unexpected stdout [%s] of command [%s] on "
                             "host [%s]",
                             command, self.sh_hostname)
                return None
            lv_name = fields[0]
            vg_name = fields[1]
            volume_names.append(vg_name + "/" + lv_name)
        return volume_names

    def sh_lvm_volume_groups(self, log):
        """
        Return volume groups
        """
        command = "vgs"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, self.sh_hostname)
            return None
        stdout = retval.cr_stdout
        lines = stdout.splitlines()
        if len(lines) == 0:
            return []
        lines = lines[1:]
        vg_names = []
        for line in lines:
            fields = line.split()
            if len(fields) < 1:
                log.cl_error("unexpected stdout [%s] of command [%s] on "
                             "host [%s]",
                             stdout, command, self.sh_hostname)
                return None
            vg_name = fields[0]
            vg_names.append(vg_name)
        return vg_names

    def sh_lvm_vg_or_lv_dict(self, log, vg_or_lv_name, vg=True):
        """
        Return a dict of VG or LV from command "vgdisplay" or "lvdisplay"
        """
        # pylint: disable=too-many-locals,too-many-branches
        if vg:
            command = "vgdisplay %s" % vg_or_lv_name
        else:
            command = "lvdisplay %s" % vg_or_lv_name

        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, self.sh_hostname)
            return None

        stdout = retval.cr_stdout
        lines = stdout.splitlines()
        if len(lines) < 2:
            log.cl_error("unexpected line number of stdout [%s] from "
                         "command [%s] on host [%s]",
                         stdout, command, self.sh_hostname)
            return None
        striped_lines = []
        for line in lines:
            line = line.lstrip()
            if len(line) == 0:
                continue
            striped_lines.append(line)
        if vg:
            leading = "--- Volume group ---"
        else:
            leading = "--- Logical volume ---"
        if striped_lines[0] != leading:
            log.cl_error("unexpected first line of stdout [%s] from "
                         "command [%s] on host [%s]",
                         stdout, command, self.sh_hostname)
            return None

        lv_or_vg_dict = {}
        lines = striped_lines[1:]
        for line in lines:
            # It is assume that key and value are seperated with two or
            # more spaces.
            #
            # But "LV Creation host, time " does not keep this assumption.
            lv_special = "LV Creation host, time"
            if not vg and line.startswith(lv_special + " "):
                key = lv_special
                value = line[len(lv_special) + 1:]
            else:
                seperate_index = line.find("  ")
                if seperate_index < 0:
                    log.cl_error("unexpected missing seperator of line [%s] in "
                                 "stdout [%s] from command [%s] on host [%s]",
                                 line, stdout, command, self.sh_hostname)
                    return None
                key = line[:seperate_index]
                value = line[seperate_index + 2:].lstrip()
            if key in lv_or_vg_dict:
                log.cl_error("unexpected multiple values for key [%s] in stdout "
                             "[%s] from command [%s] on host [%s]",
                             key, stdout, command, self.sh_hostname)
                return None
            lv_or_vg_dict[key] = value

        return lv_or_vg_dict

    def sh_lvm_vg_dict(self, log, vg_name):
        """
        Return a dict of VG from command "vgdisplay"
        """
        return self.sh_lvm_vg_or_lv_dict(log, vg_name, vg=True)

    def sh_lvm_vg_uuid(self, log, vg_name):
        """
        Return the UUID of VG from command "vgdisplay"
        """
        vg_dict = self.sh_lvm_vg_dict(log, vg_name)
        if vg_dict is None:
            return None
        if "VG UUID" not in vg_dict:
            log.cl_error("no field UUID in vgdisplay command")
            return None
        vg_uuid = vg_dict["VG UUID"]
        return vg_uuid

    def sh_lvm_lv_dict(self, log, lv_name):
        """
        Return a dict of VG from command "lvdisplay"
        """
        return self.sh_lvm_vg_or_lv_dict(log, lv_name, vg=False)

    def sh_lvm_lv_uuid(self, log, lv_name):
        """
        Return the UUID of lV from command "lvdisplay"
        """
        lv_dict = self.sh_lvm_lv_dict(log, lv_name)
        if lv_dict is None:
            return None
        if "LV UUID" not in lv_dict:
            log.cl_error("no field UUID in lvdisplay command")
            return None
        lv_uuid = lv_dict["LV UUID"]
        return lv_uuid

    def sh_blockdev_size(self, log, dev):
        """
        Return the size of block device in bytes
        """
        command = "blockdev --getsize64 %s" % dev
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
        size_str = retval.cr_stdout.strip()
        try:
            size = int(size_str)
        except ValueError:
            log.cl_error("invalid size printed by command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return size

    def sh_tree_used_bytes(self, log, path):
        """
        Return the used bytes of a file/dir tree in bytes
        """
        command = "du --summarize --block-size=1 %s" % path
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
        fields = retval.cr_stdout.split()
        if len(fields) != 2:
            log.cl_error("invalid stdout of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        bytes_str = fields[0]
        try:
            used_bytes = int(bytes_str)
        except ValueError:
            log.cl_error("invalid size printed by command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return used_bytes

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
            log.cl_error("failed to get IP addresses of NFS server [%s]",
                         self.sh_hostname)
            return -1
        if ip_address not in ip_addresses:
            log.cl_error("NFS server [%s] still does not have IP [%s] after "
                         "adding it",
                         self.sh_hostname, ip_address)
            return -1
        return 0

    def sh_shuffle_file(self, log, input_fpath, output_fpath):
        """
        Shuffle the file and generate shuffled file.
        """
        command = ("shuf %s -o %s" %
                   (input_fpath, output_fpath))
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


def get_local_host(ssh=True, host_type=SSHHost):
    """
    Return local host
    """
    local_hostname = socket.gethostname()
    login_name = getpass.getuser()
    local_host = host_type(local_hostname, local=True, ssh_for_local=ssh,
                           login_name=login_name)
    return local_host


def read_file(log, local_host, fpath, max_size=None):
    """
    Return the file content of file
    Return None if failure.
    """
    stat_result = local_host.sh_stat(log, fpath)
    if stat_result is None:
        log.cl_error("failed to get stat of path [%s] on "
                     "local host [%s]", fpath,
                     local_host.sh_hostname)
        return None

    if not stat.S_ISREG(stat_result.st_mode):
        log.cl_error("path [%s] on local host [%s] is not a regular "
                     "file", fpath,
                     local_host.sh_hostname)
        return None

    size = stat_result.st_size
    if max_size is not None and size > max_size:
        log.cl_error("size [%s] of file [%s] on local host [%s] "
                     "is too big", size, fpath,
                     local_host.sh_hostname)
        return None

    try:
        with open(fpath, "r", encoding='utf-8') as new_file:
            content = new_file.read()
    except:
        log.cl_error("failed to read from file [%s] from host [%s]: %s",
                     fpath, local_host.sh_hostname,
                     traceback.format_exc())
        return None
    return content

SSH_KEY_DIR = "ssh_keys"
SSH_KEY_SUFFIX = ".ssh_key"


def ssh_key_fpath(workspace, hostname):
    """
    Return the file path of SSH key
    """
    key_dir = workspace + "/" + SSH_KEY_DIR
    fpath = key_dir + "/" + hostname + SSH_KEY_SUFFIX
    return fpath


def send_ssh_keys(log, host, workspace):
    """
    Send the ssh key dir from local host to remote host
    """
    key_dir = workspace + "/" + SSH_KEY_DIR
    ret = host.sh_send_file(log, key_dir, workspace)
    if ret:
        log.cl_error("failed to send dir [%s] on local host [%s] to "
                     "dir [%s] on host [%s]",
                     key_dir, socket.gethostname(),
                     workspace, host.sh_hostname)
        return -1
    return 0


def write_ssh_key(log, workspace, local_host, hostname, content):
    """
    Write ssh key
    """
    fpath = ssh_key_fpath(workspace, hostname)
    key_dir = os.path.dirname(fpath)

    command = "mkdir -p %s" % key_dir
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, local_host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None

    try:
        with open(fpath, "w", encoding='utf-8') as fd:
            fd.write(content)
    except:
        log.cl_error("failed to write file [%s] on host [%s]: %s",
                     fpath, local_host.sh_hostname,
                     traceback.format_exc())
        return None

    command = "chmod 600 %s" % fpath
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, local_host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None
    return fpath


def get_or_add_host_to_dict(log, host_dict, hostname, ssh_key,
                            host_type=SSHHost):
    """
    If the hostname exists in the dict, check that the key is the same.
    If the hostname does not exist in the dict, create and add the host to
    the dict.
    """
    if hostname not in host_dict:
        host = host_type(hostname, identity_file=ssh_key)
        host_dict[hostname] = host
    else:
        host = host_dict[hostname]
        if host.sh_identity_file != ssh_key:
            log.cl_error("host [%s] was configured to use ssh key [%s], now [%s]",
                         hostname, host.sh_identity_file, ssh_key)
            return None
    return host


def check_clock_diff(log, host0, host1, max_diff=60):
    """
    Return -1 if the clock diff is too large.
    """
    seconds0 = host0.sh_epoch_seconds(log)
    if seconds0 < 0:
        log.cl_error("failed to get epoch seconds of host [%s]",
                     host0.sh_hostname)
        return -1

    seconds1 = host1.sh_epoch_seconds(log)
    if seconds1 < 0:
        log.cl_error("failed to get epoch seconds of host [%s]",
                     host1.sh_hostname)
        return -1

    if seconds0 + max_diff < seconds1 or seconds1 + max_diff < seconds0:
        log.cl_error("diff of clocks of host [%s] and [%s] is larger "
                     "than [%s] seconds",
                     host0.sh_hostname, host1.sh_hostname, max_diff)
        return -1
    return 0


def check_clocks_diff(log, hosts, max_diff=60):
    """
    Return -1 if the clocks of the hosts differ a lot.
    """
    if len(hosts) < 2:
        return 0
    host = hosts[0]
    for compare_host in hosts[1:]:
        ret = check_clock_diff(log, compare_host, host, max_diff=max_diff)
        if ret:
            log.cl_error("failed to check clock diff of hosts [%s] and [%s]",
                         host.sh_hostname,
                         compare_host.sh_hostname)
            return -1
    return 0
