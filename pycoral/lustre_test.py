"""
Lustre test library
"""
import time

# Local libs
from pycoral import utils
from pycoral import watched_io

MULTIOP = "/usr/lib64/lustre/tests/multiop"
PAUSING = "PAUSING\n"


def check_file_executable(log, host, fpath):
    """
    Check the file is executable
    """
    command = ("test -f %s && test -x %s " % (fpath, fpath))
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


class Multiop():
    """
    multiop process on a host
    """
    def __init__(self, host, fpath, args, stdout_file, stderr_file):
        # pylint: disable=too-many-arguments
        self.mop_host = host
        self.mop_command = MULTIOP + " " + fpath + " " + args
        self.mop_stdout_file = stdout_file
        self.mop_stderr_file = stderr_file
        self.mop_retval = None
        self.mop_stdout = ""
        self.mop_exited = False

    def mop_wait_pausing(self, log):
        """
        Wait until the multiop is pausing
        """
        return self._mop_wait_output(log, PAUSING)

    def _mop_wait_output(self, log, expected, timeout=60, sleep_interval=1):
        """
        Wait until the output is expected
        """
        waited = 0
        while True:
            if self.mop_stdout == expected:
                log.cl_debug("got expected output [%s]", expected)
                return 0

            if waited < timeout:
                waited += sleep_interval
                time.sleep(sleep_interval)
                continue
            log.cl_error("timeout when waiting output, expected [%s], "
                         "got [%s]", expected, self.mop_stdout)
            return -1
        return -1

    def mop_watcher_stdout(self, args, new_log):
        """
        log watcher of stdout
        """
        # pylint: disable=unused-argument
        log = args["log"]
        if len(new_log) == 0:
            return
        self.mop_stdout += new_log
        log.cl_debug("stdout of multiop [%s]: [%s]", self.mop_command,
                     new_log)

    def mop_watcher_stderr(self, args, new_log):
        """
        log wather of stderr
        """
        log = args["log"]
        # pylint: disable=unused-argument
        if len(new_log) == 0:
            return
        log.cl_debug("stderr of multiop [%s]: [%s]", self.mop_command,
                     new_log)

    def _mop_thread_main(self, log):
        """
        Thread of running multiop
        """
        host = self.mop_host
        args = {}
        args["log"] = log
        stdout_fd = watched_io.watched_io_open(self.mop_stdout_file,
                                               self.mop_watcher_stdout, args)
        stderr_fd = watched_io.watched_io_open(self.mop_stderr_file,
                                               self.mop_watcher_stderr, args)
        log.cl_debug("start to run command [%s] on host [%s]",
                     self.mop_command, host.sh_hostname)
        retval = host.sh_run(log, self.mop_command, stdout_tee=stdout_fd,
                             stderr_tee=stderr_fd, return_stdout=False,
                             return_stderr=False, timeout=None, flush_tee=True)
        stdout_fd.close()
        stderr_fd.close()

        log.cl_debug("thread of multiop [%s] is exiting",
                     self.mop_command)
        self.mop_retval = retval
        self.mop_exited = True

    def mop_start(self, log):
        """
        Start the process of multiop
        """
        utils.thread_start(self._mop_thread_main, (log))

    def mop_pkill(self, log):
        """
        Kill the process of running multiop
        """
        return self.mop_host.sh_pkill(log, self.mop_command)

    def mop_signal(self, log):
        """
        Send USR1 singal to the process
        """
        return self.mop_host.sh_pkill(log, self.mop_command,
                                      special_signal="USR1")

    def mop_wait_exit(self, log, timeout=60, sleep_interval=1, quiet=False):
        """
        Wait until the process exits
        """
        waited = 0
        while True:
            if self.mop_exited:
                log.cl_debug("multiop thread exited")
                return 0

            if waited < timeout:
                waited += sleep_interval
                time.sleep(sleep_interval)
                continue
            if not quiet:
                log.cl_error("timeout when waiting the multiop thread to exit")
            return -1
        return -1
