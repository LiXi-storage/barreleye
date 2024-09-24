# pylint: disable=too-many-lines
"""
Use SSH to run commands on a remote host.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import time
import socket
from pycoral import clog
from pycoral import watched_io
from pycoral import utils
from pycoral import constant

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


def retval_check_expected(retval, args):
    """
    Return 0 if got expected retval
    """
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

    if (expect_stdout is not None and expect_stdout != retval.cr_stdout):
        return -1

    if (diff_stdout is not None and diff_stdout == retval.cr_stdout):
        return -1

    if (expect_stderr is not None and expect_stderr != retval.cr_stderr):
        return -1

    if (diff_stderr is not None and diff_stderr == retval.cr_stderr):
        return -1
    return 0


class SSHBasicHost():
    """
    Baisc SSH host.

    This class only implement basic functionals:
    1) Running a command.
    2) Waiting for expect result of command.
    """
    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    def __init__(self, hostname, identity_file=None, ssh_port=22,
                 local=False, ssh_for_local=True, login_name="root"):
        # The host has been checked to be local or not
        self._sbh_cached_is_local = None
        # The hostname
        self.sh_hostname = hostname
        # The port for SSH connection.
        self.sh_ssh_port = ssh_port
        self.sh_identity_file = identity_file
        # If the host is inited as local, then it is local for sure
        self._sbh_inited_as_local = local
        # Key: command name, Value: True/False
        self._sbh_cached_has_commands = {}
        # The cached lscpu diction
        self.sh_cached_lscpu_dict = None
        # Use ssh to run command even the host is local
        self._sbh_ssh_for_local = ssh_for_local
        # The login name of user for ssh
        self.sh_login_name = login_name
        # The hostname got from "hostname" commnad
        self._sh_real_hostname = None

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
        if self._sbh_inited_as_local:
            return 1

        if self._sbh_cached_is_local is not None:
            return self._sbh_cached_is_local

        local_hostname = socket.gethostname()
        if local_hostname == self.sh_hostname:
            ret = 1
        else:
            ret = 0

        self._sbh_cached_is_local = ret
        return ret

    def _get_ssh_command(self):
        """
        Return the ssh cmd string
        """
        extra_option = ""
        if self.sh_identity_file is not None:
            extra_option += (" -i %s" % self.sh_identity_file)
        if self.sh_ssh_port != 22:
            extra_option += (" -p %s" % self.sh_ssh_port)
        return ("ssh -a -x -l %s -o StrictHostKeyChecking=no "
                "-o BatchMode=yes%s" %
                (self.sh_login_name, extra_option))

    def _sh_run(self, log, command, silent=False, timeout=None,
                stdout_tee=None, stderr_tee=None, stdin=None,
                return_stdout=True, return_stderr=True, quit_func=None,
                flush_tee=False):
        """
        Use ssh to run command on the host
        """
        # pylint: disable=too-many-locals,unused-argument
        if not isinstance(command, str):
            stderr = "type of command argument is not a basestring"
            return utils.CommandResult(stderr=stderr, exit_status=-1)

        ssh_string = self._get_ssh_command()
        full_command = ("%s %s \"LANG=en_US %s\"" %
                        (ssh_string, self.sh_hostname, sh_escape(command)))

        return utils.run(full_command, timeout=timeout, stdout_tee=stdout_tee,
                         stderr_tee=stderr_tee, stdin=stdin,
                         return_stdout=return_stdout, return_stderr=return_stderr,
                         quit_func=quit_func, flush_tee=flush_tee)

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
            break
        if not quiet:
            log.cl_error("timeout of command [%s] on host [%s] after [%s] "
                         "seconds, ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, timeout,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
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
        return self.sh_wait_condition(log, command, retval_check_expected,
                                      args, timeout=timeout,
                                      sleep_interval=sleep_interval,
                                      quiet=quiet)

    def sh_run(self, log, command, silent=False,
               timeout=constant.LONGEST_SIMPLE_COMMAND_TIME, stdout_tee=None,
               stderr_tee=None, stdin=None, return_stdout=True,
               return_stderr=True, quit_func=None, flush_tee=False):
        """
        Run a command on the host
        """
        # pylint: disable=too-many-locals
        if not silent:
            log.cl_debug("starting command [%s] on host [%s]", command,
                         self.sh_hostname)
        if self._sbh_inited_as_local and not self._sbh_ssh_for_local:
            ret = utils.run(command, timeout=timeout, stdout_tee=stdout_tee,
                            stderr_tee=stderr_tee, stdin=stdin,
                            return_stdout=return_stdout,
                            return_stderr=return_stderr,
                            quit_func=quit_func, flush_tee=flush_tee)
        else:
            ret = self._sh_run(log, command,
                               silent=silent,
                               timeout=timeout,
                               stdout_tee=stdout_tee, stderr_tee=stderr_tee,
                               stdin=stdin, return_stdout=return_stdout,
                               return_stderr=return_stderr, quit_func=quit_func,
                               flush_tee=flush_tee)
        if not silent:
            log.cl_debug("ran [%s] on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command, self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout,
                         ret.cr_stderr)
        return ret

    def sh_run_with_logs(self, log, command, stdout_fpath, stderr_fpath,
                         silent=False, timeout=constant.LONGEST_SIMPLE_COMMAND_TIME,
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

    def sh_has_command(self, log, command):
        """
        Check whether host has a command
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        if command in self._sbh_cached_has_commands:
            return self._sbh_cached_has_commands[command]

        ret = self.sh_run(log, "which %s" % command)
        if ret.cr_exit_status != 0:
            result = False
        else:
            result = True
        self._sbh_cached_has_commands[command] = result
        return result

    def sh_real_hostname(self, log):
        """
        Return the hostname from running "hostname" command.
        """
        # pylint: disable=access-member-before-definition,attribute-defined-outside-init
        if self._sh_real_hostname is not None:
            return self._sh_real_hostname
        retval = self.sh_run(log, "hostname")
        if retval.cr_exit_status == 0:
            self._sh_real_hostname = retval.cr_stdout.strip()
            return self._sh_real_hostname
        return None
