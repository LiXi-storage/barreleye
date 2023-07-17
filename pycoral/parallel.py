"""
Library to execute a function in multiple threads

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import traceback
import time
from pycoral import utils


class ParallelThread():
    """
    There are multiple threads for each ParallelExecute
    Each thread has this object
    """
    # pylint: disable=too-many-instance-attributes
    STATUS_NOT_STARTED = "not_started"
    STATUS_RUNNING = "running"
    STATUS_ABORTING = "aborting"
    STATUS_STOPPED = "stopped"

    def __init__(self, parallel_execute, thread_index, funct, args,
                 thread_id=None):
        # ParallelExecute this thread belongs to
        self.pt_parallel_execute = parallel_execute
        # Index of this thread in ParallelExecute
        self.pt_index = thread_index
        # Thread that runs the function
        self.pt_thread = None
        # Function to run
        # The first argument of funct should be log
        # The second argumnt of funct should be workspace
        # The other arguments of funct should be args
        # The return value should be an integer
        self.pt_funct = funct
        # The arguments of the function
        self.pt_args = args
        if thread_id is None:
            self.pt_thread_id = "thread_%d" % thread_index
        else:
            self.pt_thread_id = thread_id
        if parallel_execute.pe_workspace is None:
            self.pt_workspace = None
        else:
            self.pt_workspace = (parallel_execute.pe_workspace +
                                 "/" + self.pt_thread_id)
        self.pt_log = None
        self.pt_status = ParallelThread.STATUS_NOT_STARTED

    def pt_main(self):
        """
        Main thread
        """
        def target_wrap(log, workspace, *args, **kwargs):
            """
            Wrap the target function
            """
            # pylint: disable=bare-except
            ret = None
            try:
                ret = self.pt_funct(log, workspace, *args, **kwargs)
            except:
                log.cl_error("exception when running thread: [%s]",
                             traceback.format_exc())
                ret = -1
            return ret

        log = self.pt_log
        ret = target_wrap(log, self.pt_workspace, *self.pt_args)
        log.cl_debug("thread [%s] returned [%s]", self.pt_thread_id, ret)
        log.cl_result.cr_exit_status = ret

    def pt_thread_start(self, parent_log):
        """
        Start the thread
        """
        if parent_log.cl_resultsdir is None:
            resultsdir = None
        else:
            ret = utils.mkdir(self.pt_workspace)
            if ret:
                parent_log.cl_error("failed to create directory [%s], errno=[%s]",
                                    self.pt_workspace, ret)
                return -1
            resultsdir = self.pt_workspace

        # The log for this thread
        log = parent_log.cl_get_child(self.pt_thread_id, resultsdir=resultsdir)
        self.pt_log = log
        log.cl_result.cr_clear()
        self.pt_status = ParallelThread.STATUS_RUNNING
        self.pt_thread = utils.thread_start(self.pt_main, ())
        return 0

    def pt_thread_abort(self):
        """
        Abort the thread
        This is async abort, depends on the implementation of the function
        """
        self.pt_status = ParallelThread.STATUS_ABORTING

    def pt_thread_join(self):
        """
        Join the thread
        """
        if self.pt_thread is not None:
            self.pt_thread.join()
            self.pt_status = ParallelThread.STATUS_STOPPED

    def pt_fini(self):
        """
        Cleanup the thread
        """
        self.pt_log.cl_fini()
        self.pt_log = None


class ParallelExecute():
    """
    Each execute instance has an object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, workspace, name, main, args_array,
                 thread_ids=None):
        # Workspace to save logs of threads
        self.pe_workspace = workspace
        # Name of this parallel execute
        self.pe_name = name
        # List of all ParallelThread
        self.pe_threads = []
        thread_index = 0
        if thread_ids is not None:
            if len(thread_ids) != len(args_array):
                reason = ("different array length for thread IDs [%d] and "
                          "arguments [%d]" %
                          (len(thread_ids), len(args_array)))
                raise Exception(reason)
        for args in args_array:
            thread_id = thread_ids[thread_index]
            parallel_thread = ParallelThread(self, thread_index, main, args,
                                             thread_id=thread_id)
            self.pe_threads.append(parallel_thread)
            thread_index += 1

    def pe_start_threads(self, log):
        """
        Start to run the threads. pe_join_threads() needs to be run latter
        if this function returns 0.
        """
        ret = 0
        for parallel_thread in self.pe_threads:
            log.cl_debug("starting thread [%s] of [%s]",
                         parallel_thread.pt_thread_id, self.pe_name)
            rc = parallel_thread.pt_thread_start(log)
            if rc:
                log.cl_error("failed to start thread [%s] of [%s]",
                             parallel_thread.pt_thread_id,
                             self.pe_name)
                ret = -1
                break

        if ret:
            for parallel_thread in self.pe_threads:
                if parallel_thread.pt_status != ParallelThread.STATUS_RUNNING:
                    continue
                parallel_thread.pt_thread_abort()
            self.pe_join_threads(log)
        return ret

    def pe_join_threads(self, log):
        """
        Wait until the thread exits.
        """
        for parallel_thread in self.pe_threads:
            parallel_thread.pt_thread_join()

        ret = 0
        for parallel_thread in self.pe_threads:
            # Some thread might never starts, so pt_log might be None
            if parallel_thread.pt_log is not None:
                rc = parallel_thread.pt_log.cl_result.cr_exit_status
                if rc:
                    log.cl_error("failed to run thread [%s] of [%s]",
                                 parallel_thread.pt_thread_id, self.pe_name)
                    ret = -1
                parallel_thread.pt_fini()

        return ret

    def pe_run(self, log, quiet=True, sleep_interval=1, timeout=None,
               quit_on_error=True, parallelism=-1):
        """
        Start to run the threads
        If timeout is None, threads will be aborted after the timeout
        """
        # pylint: disable=too-many-branches,too-many-locals,too-many-statements
        time_start = time.time()
        ret = 0
        not_started_threads = list(self.pe_threads)
        running_threads = []
        finished_threads = []
        while True:
            # Start threads
            while (len(not_started_threads) > 0 and
                   (parallelism == -1 or
                    parallelism > len(running_threads))):
                parallel_thread = not_started_threads[0]
                log.cl_debug("starting thread [%s] of [%s]",
                             parallel_thread.pt_thread_id, self.pe_name)
                rc = parallel_thread.pt_thread_start(log)
                if rc:
                    log.cl_error("failed to start thread [%s] of [%s]",
                                 parallel_thread.pt_thread_id, self.pe_name)
                    ret = -1
                    if quit_on_error:
                        break
                running_threads.append(parallel_thread)
                not_started_threads.remove(parallel_thread)

            if ret and quit_on_error:
                break

            for parallel_thread in running_threads[:]:
                if not parallel_thread.pt_thread.is_alive():
                    running_threads.remove(parallel_thread)
                    finished_threads.append(parallel_thread)
                    log.cl_debug("thread [%s] of [%s] finished",
                                 parallel_thread.pt_thread_id, self.pe_name)

            if len(running_threads) == 0 and len(not_started_threads) == 0:
                log.cl_debug("all threads of [%s] finished",
                             self.pe_name)
                break

            time_now = time.time()
            elapsed = time_now - time_start
            if timeout is not None and elapsed > timeout:
                ret = -1
                log.cl_error("parallel execute [%s] timeout after [%d] "
                             "seconds, aborting", self.pe_name, elapsed)
                break
            time.sleep(sleep_interval)

        for parallel_thread in running_threads:
            log.cl_debug("aborting thread [%s] of [%s] finished",
                         parallel_thread.pt_thread_id, self.pe_name)
            parallel_thread.pt_thread_abort()

        for parallel_thread in running_threads:
            log.cl_debbug("joining thread [%s] of [%s] finished",
                          parallel_thread.pt_thread_id, self.pe_name)
            parallel_thread.pt_thread_join()

        for parallel_thread in self.pe_threads:
            # Some thread might never starts, so pt_log might be None
            if parallel_thread.pt_log is not None:
                rc = parallel_thread.pt_log.cl_result.cr_exit_status
                if rc:
                    if not quiet:
                        log.cl_error("failed to run thread [%s] of [%s]",
                                     parallel_thread.pt_thread_id, self.pe_name)
                    ret = -1
                parallel_thread.pt_fini()

        time_now = time.time()
        elapsed = time_now - time_start
        log.cl_debug("parallel execute [%s] finished after [%d] seconds "
                     "with retval [%d]",
                     self.pe_name, elapsed, ret)
        return ret
