"""
Misc utility library

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""


import os
import errno
import time
import signal
import subprocess
import io
import select
import logging
import logging.handlers
import threading
import random
import string
import stat
import socket


def read_one_line(filename):
    """
    Open file and read one line
    """
    with open(filename, 'r', encoding='utf-8') as fd:
        line = fd.readline().rstrip('\n')
    return line


def pid_is_alive(pid):
    """
    True if process pid exists and is not yet stuck in Zombie state.
    Zombies are impossible to move between cgroups, etc.
    pid can be integer, or text of integer.
    """
    path = '/proc/%s/stat' % pid

    try:
        proc_stat = read_one_line(path)
    except IOError:
        if not os.path.exists(path):
            # file went away
            return False
        raise

    return proc_stat.split()[2] != 'Z'


def signal_pid(pid, sig):
    """
    Sends a signal to a process id. Returns True if the process terminated
    successfully, False otherwise.
    """
    # pylint: disable=unused-variable
    try:
        os.kill(pid, sig)
    except OSError:
        # The process may have died before we could kill it.
        pass

    for i in range(5):
        if not pid_is_alive(pid):
            return True
        time.sleep(1)

    # The process is still alive
    return False


def nuke_subprocess(subproc):
    """
    Kill the subprocess
    """
    # check if the subprocess is still alive, first
    if subproc.poll() is not None:
        return subproc.poll()

    # the process has not terminated within timeout,
    # kill it via an escalating series of signals.
    signal_queue = [signal.SIGTERM, signal.SIGKILL]
    for sig in signal_queue:
        signal_pid(subproc.pid, sig)
        if subproc.poll() is not None:
            return subproc.poll()
    return 0


class CommandResult():
    """
    All command will return a command result of this class
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, stdout="", stderr="",
                 exit_status=None, duration=0):
        self.cr_exit_status = exit_status
        self.cr_stdout = stdout
        self.cr_stderr = stderr
        self.cr_duration = duration
        # Whether timeout happens
        self.cr_timeout = False

    def cr_clear(self):
        """
        Clear the result
        """
        self.cr_stdout = ""
        self.cr_stderr = ""
        self.cr_duration = 0
        self.cr_exit_status = None


class CommandJob():
    """
    Each running of a command has an object of this class
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, command, timeout=None, stdout_tee=None,
                 stderr_tee=None, stdin=None, return_stdout=True,
                 return_stderr=True, quit_func=None,
                 flush_tee=False, silent=False):
        self.cj_command = command
        self.cj_result = CommandResult()
        self.cj_timeout = timeout
        self.cj_stdout_tee = stdout_tee
        self.cj_stderr_tee = stderr_tee
        self.cj_quit_func = quit_func
        self.cj_silent = silent
        # allow for easy stdin input by string, we'll let subprocess create
        # a pipe for stdin input and we'll write to it in the wait loop
        if isinstance(stdin, str):
            self.cj_string_stdin = stdin
            self.cj_stdin = subprocess.PIPE
        else:
            self.cj_string_stdin = None
            self.cj_stdin = None
        if return_stdout:
            self.cj_stdout_file = io.BytesIO()
        if return_stderr:
            self.cj_stderr_file = io.BytesIO()
        self.cj_started = False
        self.cj_killed = False
        self.cj_start_time = None
        self.cj_stop_time = None
        self.cj_max_stop_time = None
        self.cj_subprocess = None
        self.cj_return_stdout = return_stdout
        self.cj_return_stderr = return_stderr
        self.cj_flush_tee = flush_tee

    def cj_run_start(self):
        """
        Start to run the command
        """
        # RHEL8 build would fail without bad-option-value since it does not
        # understand consider-using-with.
        # pylint: disable=bad-option-value,consider-using-with
        if self.cj_started:
            return -1

        self.cj_started = True
        self.cj_start_time = time.time()
        if self.cj_timeout:
            self.cj_max_stop_time = self.cj_timeout + self.cj_start_time
        shell = '/bin/bash'
        self.cj_subprocess = subprocess.Popen("LANG=en_US.UTF-8 " + self.cj_command,
                                              stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE,
                                              shell=True,
                                              executable=shell,
                                              stdin=self.cj_stdin)
        return 0

    def cj_run_stop(self):
        """
        Stop the job even when it is running
        """
        self.cj_kill()
        self.cj_post_exit()
        return self.cj_result

    def cj_post_exit(self):
        """
        After exit, process the outputs and calculate the duration
        """
        self.cj_process_output(is_stdout=True, final_read=True)
        self.cj_process_output(is_stdout=False, final_read=True)
        if self.cj_stdout_tee:
            self.cj_stdout_tee.flush()
        if self.cj_stderr_tee:
            self.cj_stderr_tee.flush()
        self.cj_subprocess.stdout.close()
        self.cj_subprocess.stderr.close()
        self.cj_stop_time = time.time()
        if self.cj_return_stdout:
            self.cj_result.cr_stdout = self.cj_stdout_file.getvalue().decode()
        if self.cj_return_stderr:
            self.cj_result.cr_stderr = self.cj_stderr_file.getvalue().decode()
        self.cj_result.cr_duration = self.cj_stop_time - self.cj_start_time
        self.cj_result.cr_timeout = self.cj_killed
        if not self.cj_silent:
            logging.debug("command [%s] finished, "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          self.cj_command,
                          self.cj_result.cr_exit_status,
                          self.cj_result.cr_stdout,
                          self.cj_result.cr_stderr)

    def cj_run(self):
        """
        Run the command, wait until it exits and return the results
        """
        # Do not allow run for more than twice currently
        if self.cj_started:
            return self.cj_result

        ret = self.cj_run_start()
        if ret:
            self.cj_result.cr_exit_status = ret
            logging.debug("command [%s] failed to start, "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          self.cj_command,
                          self.cj_result.cr_exit_status,
                          self.cj_result.cr_stdout,
                          self.cj_result.cr_stderr)

        self.cj_wait_for_command()
        self.cj_post_exit()
        return self.cj_result

    def cj_process_output(self, is_stdout=True, final_read=False):
        """
        Process the stdout or stderr
        """
        buf = None
        if is_stdout:
            pipe = self.cj_subprocess.stdout
            if self.cj_return_stdout:
                buf = self.cj_stdout_file
            tee = self.cj_stdout_tee
        else:
            pipe = self.cj_subprocess.stderr
            if self.cj_return_stderr:
                buf = self.cj_stderr_file
            tee = self.cj_stderr_tee

        if final_read:
            # read in all the data we can from pipe and then stop
            data = bytes()
            while select.select([pipe], [], [], 0)[0]:
                tmp_data = os.read(pipe.fileno(), 1024)
                if len(tmp_data) == 0:
                    break
                data += tmp_data
        else:
            # perform a single read
            data = os.read(pipe.fileno(), 1024)
        if buf is not None:
            buf.write(data)
        if tee:
            tee.write(data)

    def cj_kill(self):
        """
        Kill the job
        """
        nuke_subprocess(self.cj_subprocess)
        self.cj_result.cr_exit_status = self.cj_subprocess.poll()
        self.cj_killed = True

    def cj_wait_for_command(self):
        """
        Wait until the command exits
        """
        # pylint: disable=too-many-branches
        read_list = []
        write_list = []
        reverse_dict = {}

        read_list.append(self.cj_subprocess.stdout)
        read_list.append(self.cj_subprocess.stderr)
        reverse_dict[self.cj_subprocess.stdout] = True
        reverse_dict[self.cj_subprocess.stderr] = False

        if self.cj_string_stdin is not None:
            write_list.append(self.cj_subprocess.stdin)

        if self.cj_timeout:
            time_left = self.cj_max_stop_time - time.time()
        else:
            time_left = None  # so that select never times out

        while not self.cj_timeout or time_left > 0:
            # select will return when we may write to stdin or when there is
            # stdout/stderr output we can read (including when it is
            # EOF, that is the process has terminated).
            # To check for processes which terminate without producing any
            # output, a 1 second timeout is used in select.
            read_ready, write_ready, _ = select.select(read_list, write_list,
                                                       [], 1)

            # os.read() has to be used instead of
            # subproc.stdout.read() which will otherwise block
            for file_obj in read_ready:
                is_stdout = reverse_dict[file_obj]
                self.cj_process_output(is_stdout)
                if self.cj_flush_tee:
                    if is_stdout:
                        self.cj_stdout_tee.flush()
                    else:
                        self.cj_stderr_tee.flush()

            for file_obj in write_ready:
                # we can write PIPE_BUF bytes without blocking
                # POSIX requires PIPE_BUF is >= 512
                file_obj.write(self.cj_string_stdin[:512])
                self.cj_string_stdin = self.cj_string_stdin[512:]
                # no more input data, close stdin, remove it from the select
                # set
                if not self.cj_string_stdin:
                    file_obj.close()
                    write_list.remove(file_obj)

            self.cj_result.cr_exit_status = self.cj_subprocess.poll()
            if self.cj_result.cr_exit_status is not None:
                return

            if self.cj_timeout:
                time_left = self.cj_max_stop_time - time.time()

            if self.cj_quit_func is not None and self.cj_quit_func():
                break

        # Kill process if timeout
        self.cj_kill()
        return


def run(command, timeout=None, stdout_tee=None, stderr_tee=None, stdin=None,
        return_stdout=True, return_stderr=True, quit_func=None,
        flush_tee=False, silent=False):
    """
    Run a command
    """
    if not isinstance(command, str):
        stderr = "type of command argument is not a basestring"
        return CommandResult(stderr=stderr, exit_status=-1)

    job = CommandJob(command, timeout=timeout, stdout_tee=stdout_tee,
                     stderr_tee=stderr_tee, stdin=stdin,
                     return_stdout=return_stdout, return_stderr=return_stderr,
                     quit_func=quit_func, flush_tee=flush_tee, silent=silent)
    return job.cj_run()


def thread_start(target, args):
    """
    Wrap the target function and start a thread to run it
    """
    run_thread = threading.Thread(target=target,
                                  args=args)
    run_thread.setDaemon(True)
    run_thread.start()
    return run_thread


def random_word(length):
    """
    Return random lowercase word with given length
    """
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))


def is_exe(fpath):
    """
    Whether the fpath is an executable file
    """
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)


def which(program):
    """
    Return the full path of (shell) commands.
    """
    # pylint: disable=unused-variable
    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


def mkdir(dirname):
    """
    Create directory ignoring the existing error
    """
    try:
        os.mkdir(dirname)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            return exc.errno
    return 0

# If the condition_func returns WAIT_CONDITION_QUIT, quit the check
WAIT_CONDITION_QUIT = 8192


def wait_condition(log, condition_func, args, timeout=90, sleep_interval=1):
    """
    Wait until the condition_func returns 0

    The first argument of condition_func should be log
    The other arguments of funct should be args
    The return value should be an integer or a tuple starts with an integer
    If the integer returned by condition_func is zero, quit waiting
    """
    time_start = time.time()
    while True:
        retval = condition_func(log, *args)
        if isinstance(retval, int):
            ret = retval
        else:
            ret = retval[0]

        if ret == 0:
            break

        time_now = time.time()
        elapsed = time_now - time_start

        if elapsed < timeout:
            if sleep_interval > 0:
                time.sleep(sleep_interval)
            continue
        log.cl_error("waiting times out after [%d] seconds", elapsed)
        break
    return retval


def config_value(config, key):
    """
    Return value of a key in config
    """
    if key not in config:
        return None
    return config[key]


def file_type2string(inode_type):
    """
    Return the string of file type
    """
    if inode_type == stat.S_IFDIR:
        return "directory"
    if inode_type == stat.S_IFCHR:
        return "character_device"
    if inode_type == stat.S_IFBLK:
        return "block_device"
    if inode_type == stat.S_IFREG:
        return "regular_file"
    if inode_type == stat.S_IFIFO:
        return "fifo"
    if inode_type == stat.S_IFLNK:
        return "symbolic_link"
    if inode_type == stat.S_IFSOCK:
        return "socket"
    return None


class LimitResource():
    """
    A resource shared by multiple threads
    """
    def __init__(self, number, name):
        self.lr_number = number
        self.lr_used = 0
        self.lr_condition = threading.Condition()
        self.lr_name = name

    def lr_acquire(self, number, info_string):
        """
        Acquire resource
        """
        self.lr_condition.acquire()
        waited = False
        while self.lr_number < number:
            logging.info("[%s] not enough resource of [%s], waiting",
                         info_string, self.lr_name)
            waited = True
            self.lr_condition.wait()
        if waited:
            logging.info("[%s] got enough resource of [%s]",
                         info_string, self.lr_name)
        self.lr_number -= number
        self.lr_condition.release()
        logging.debug("[%s] got [%d] resource of [%s]",
                      info_string, number, self.lr_name)

    def lr_release(self, number, info_string):
        """
        Release resource
        """
        self.lr_condition.acquire()
        self.lr_number += number
        self.lr_condition.notifyAll()
        self.lr_condition.release()
        logging.debug("[%s] released [%d] resource of [%s]",
                      info_string, number, self.lr_name)


def is_valid_ipv4_address(address):
    """
    Check whether the address is valid ipv4 IP
    """
    try:
        socket.inet_pton(socket.AF_INET, address)
    except AttributeError:  # no inet_pton here, sorry
        try:
            socket.inet_aton(address)
        except socket.error:
            return False
        return address.count('.') == 3
    except socket.error:  # not a valid address
        return False

    return True


def merge_files(file_list, output_file):
    """
    Merge files in file_list into output_file
    """
    with open(output_file, 'wb') as output_fd:
        for item in file_list:
            # skip invalid file
            if not os.path.isfile(item):
                continue

            with open(item, 'rb') as sub_fd:
                while True:
                    buff = sub_fd.read(8 * 1024 * 1024)
                    if not buff:
                        break
                    output_fd.write(buff)


def random_kvm_mac():
    """
    Return a mac string for qemu/kvm
    """
    # 52:54:00 is the OUI of qemu/kvm
    mac = [0x52, 0x54, 0x00, random.randint(0x00, 0x7f),
           random.randint(0x00, 0xff), random.randint(0x00, 0xff)]
    return ':'.join(map(lambda x: "%02x" % x, mac))
