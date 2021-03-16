"""
HSM library
"""

import time

# Local libs
from pycoral import utils
from pycoral import watched_io
from pycoral import daemon
from pycoral import time_util


REMOVER_INTERVAL = 60
COMMAND_HSM_REMOVER = "hsm_remover"
COMMAND_LSHMTOOL_POSIX = "lhsmtool_posix"


class HSMCopytool():
    """
    Each SSH host has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    # pylint: disable=too-many-arguments
    def __init__(self, copytool_id, host, archive_id, hsm_root,
                 lustre_mount_point, parent_directory, compress=False):
        self.hc_copytool_id = copytool_id
        self.hc_host = host
        self.hc_archive_id = archive_id
        self.hc_hsm_root = hsm_root
        self.hc_lustre_mount_point = lustre_mount_point
        self.hc_thread = None
        self.hc_status = 0
        self.hc_workspace = parent_directory + "/" + copytool_id
        self.hc_stdout_file = (self.hc_workspace + "/" +
                               "copytool_command_watching.stdout")
        self.hc_stderr_file = (self.hc_workspace + "/" +
                               "copytool_command_watching.stderr")
        self.hc_compress = compress
        compress_string = ""
        if compress:
            compress_string = " --compress"
        # Should not introduced any extra space, otherwise pkill will not kill
        # any process
        self.hc_command = ("%s --no_shadow%s --hsm-root %s "
                           "--archive=%s %s" %
                           (COMMAND_LSHMTOOL_POSIX, compress_string,
                            self.hc_hsm_root, self.hc_archive_id,
                            self.hc_lustre_mount_point))

    def hc_killall(self, log):
        """
        Kill the process of running copytool
        """
        host = self.hc_host
        command = ("pkill -f -x -c '%s'" % (self.hc_command))
        log.cl_debug("start to run command [%s] on host [%s]", command,
                     host.sh_hostname)
        retval = host.sh_run(log, log, command)
        if (retval.cr_stderr != "" or
                (retval.cr_exit_status != 0 and retval.cr_exit_status != 1)):
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        instance_count = retval.cr_stdout.strip()
        log.cl_debug("killed [%s] instance of HSM copytool [%s]",
                     instance_count, self.hc_copytool_id)
        return 0

    def hc_thread_main(self, parent_log):
        """
        Thread of running copytool
        """
        # pylint: disable=too-many-locals,too-many-statements
        # pylint: disable=too-many-return-statements,too-many-arguments
        log = parent_log.cl_get_child("copytool", resultsdir=self.hc_workspace)

        host = self.hc_host
        args = {}
        args[watched_io.WATCHEDIO_LOG] = host.sh_hostname
        args[watched_io.WATCHEDIO_HOSTNAME] = log
        stdout_fd = watched_io.watched_io_open(self.hc_stdout_file,
                                               watched_io.log_watcher_debug, args)
        stderr_fd = watched_io.watched_io_open(self.hc_stderr_file,
                                               watched_io.log_watcher_error, args)
        log.cl_debug("start to run command [%s] on host [%s]",
                     self.hc_command, host.sh_hostname)
        retval = host.sh_run(log, self.hc_command, stdout_tee=stdout_fd,
                             stderr_tee=stderr_fd, return_stdout=False,
                             return_stderr=False, timeout=None, flush_tee=True)
        stdout_fd.close()
        stderr_fd.close()
        if daemon.SHUTTING_DOWN:
            log.cl_debug("finished running command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         self.hc_command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
        else:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         self.hc_command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            self.hc_status = -1

        log.cl_debug("thread of copytool [%s] is exiting",
                     self.hc_copytool_id)

    def hc_thread_start(self, log):
        """
        Start the thread
        """
        if utils.mkdir(self.hc_workspace):
            log.cl_error("failed to create directory [%s] on local host, "
                         "exiting the thread",
                         self.hc_workspace)
            self.hc_status = -1
            return
        self.hc_thread = utils.thread_start(self.hc_thread_main, (log, ))

    def hc_thread_join(self):
        """
        Join the thread
        """
        if self.hc_thread is not None:
            self.hc_thread.join()


class HSMRemover():
    """
    Each SSH host has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    # pylint: disable=too-many-arguments
    # mdt_uuid: MDT0000
    def __init__(self, log, remover_id, host, fsname, mdti, hsm_root,
                 parent_directory):
        self.hr_remover_id = remover_id
        self.hr_host = host
        self.hr_mdti = mdti
        self.hr_fsname = fsname
        self.hr_thread = None
        self.hr_changelog_user = mdti.mdti_changelog_register(log)
        if self.hr_changelog_user is None:
            reason = "failed to register changelog user"
            raise Exception(reason)
        self.hr_hsm_root = hsm_root
        self.hr_workspace = parent_directory + "/" + remover_id
        self.hr_command = ("%s --hsm_root %s --mdt=%s-%s "
                           "--changelog_user %s" %
                           (COMMAND_HSM_REMOVER, hsm_root,
                            fsname, mdti.lsi_service.ls_index_string,
                            self.hr_changelog_user))

    def hr_run(self, log):
        """
        Thread of running remover
        """
        # pylint: disable=too-many-locals,too-many-statements
        # pylint: disable=too-many-return-statements,too-many-arguments
        identity = time_util.local_strftime(time_util.utcnow(), "%Y-%m-%d-%H_%M_%S")
        workspace = self.hr_workspace + "/" + identity
        ret = utils.mkdir(workspace)
        if ret:
            log.cl_error("failed to create directory [%s] on local host",
                         workspace)
            return -1
        stdout_file = workspace + "/" + "remover_watching.stdout"
        stderr_file = workspace + "/" + "remover_watching.stderr"

        host = self.hr_host
        args = {}
        args["hostname"] = host.sh_hostname
        args["log"] = log
        stdout_fd = watched_io.watched_io_open(stdout_file,
                                               watched_io.log_watcher_debug, args)
        stderr_fd = watched_io.watched_io_open(stderr_file,
                                               watched_io.log_watcher_error, args)
        log.cl_debug("start to run command [%s] on host [%s]",
                     self.hr_command, host.sh_hostname)
        retval = host.sh_run(log, self.hr_command, stdout_tee=stdout_fd,
                             stderr_tee=stderr_fd, return_stdout=False,
                             return_stderr=False, timeout=None)
        stdout_fd.close()
        stderr_fd.close()
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         self.hr_command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
        else:
            log.cl_debug("finished running command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         self.hr_command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
        return retval.cr_exit_status

    def hr_thread_main(self, parent_log):
        """
        Thread of reapting running remover
        """
        log = parent_log.cl_get_child("hsm_remover",
                                      resultsdir=self.hr_workspace)
        while not daemon.SHUTTING_DOWN:
            ret = self.hr_run(log)
            if ret:
                log.cl_error("failed to run remover")
            time.sleep(REMOVER_INTERVAL)

    def hr_thread_start(self, parent_log):
        """
        Start the thread
        """
        ret = utils.mkdir(self.hr_workspace)
        if ret:
            return ret
        self.hr_thread = utils.thread_start(self.hr_thread_main, (parent_log, ))
        return 0

    def hr_killall(self, log):
        """
        Kill the process of running remover
        """
        host = self.hr_host
        command = ("pkill -f -x -c '%s'" % (self.hr_command))
        log.cl_debug("start to run command [%s] on host [%s]", command,
                     host.sh_hostname)
        retval = host.sh_run(log, command)
        if (retval.cr_stderr != "" or
                (retval.cr_exit_status != 0 and
                 retval.cr_exit_status != 1)):
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        instance_count = retval.cr_stdout.strip()
        log.cl_debug("killed [%s] instance of HSM remover [%s]",
                     instance_count, self.hr_remover_id)
        return 0

    def hr_fini(self, log):
        """
        Cleanup resource allocated when initing
        """
        ret = self.hr_mdti.mdti_changelog_deregister(log, self.hr_changelog_user)
        return ret

    def hr_thread_join(self):
        """
        Join the thread
        """
        if self.hr_thread is not None:
            self.hr_thread.join()


def check_hsm_remover_storage(log, removers):
    """
    Check the HSM storage status of removers
    """
    random_fpaths = []
    for remover_id, remover in removers.items():
        random_fpath = remover.hr_hsm_root + "/" + utils.random_word(8)
        command = "touch %s" % random_fpath
        retval = remover.hr_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on remover host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         remover_id,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        random_fpaths.append(random_fpath)

    for random_fpath in random_fpaths:
        for remover_id, remover in removers.items():
            command = "ls %s" % random_fpath
            retval = remover.hr_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on remover host "
                             "[%s], ret = [%d], "
                             "stdout = [%s], stderr = [%s]",
                             command,
                             remover_id,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        remover = list(removers.values())[0]
        command = "rm -f %s" % random_fpath
        retval = remover.hr_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on remover host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         remover_id,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
    return 0


def check_hsm_copytool_storage(log, copytools):
    """
    Check the HSM storage status of copytools
    """
    random_fpaths = []
    for copytool_id, copytool in copytools.items():
        random_fpath = copytool.hc_hsm_root + "/" + utils.random_word(8)
        command = "touch %s" % random_fpath
        retval = copytool.hc_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on copytool host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         copytool_id,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        random_fpaths.append(random_fpath)

    for random_fpath in random_fpaths:
        for copytool_id, copytool in copytools.items():
            command = "ls %s" % random_fpath
            retval = copytool.hc_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on copytool "
                             "host [%s], ret = [%d], "
                             "stdout = [%s], stderr = [%s]",
                             command,
                             copytool_id,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        copytool = list(copytools.values())[0]
        command = "rm -f %s" % random_fpath
        retval = copytool.hc_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on copytool host "
                         "[%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         copytool_id,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
    return 0
