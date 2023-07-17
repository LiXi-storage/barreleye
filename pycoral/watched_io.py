"""
Wathed IO is an file IO which will call callbacks when reading/writing from/to
the file

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""

import io
import os
import logging
import traceback


WATCHEDIO_LOG = "log"
WATCHEDIO_HOSTNAME = "hostname"


def watched_io_open(fname, func, args):
    """Open watched IO file.
    Codes copied from io.py
    """
    if fname is not None:
        if not isinstance(fname, (str)):
            raise TypeError("invalid file: %s" % fname)

    if fname is not None:
        mode = "w"
        raw = io.FileIO(fname, mode)
        buffering = io.DEFAULT_BUFFER_SIZE
        try:
            blksize = os.fstat(raw.fileno()).st_blksize
        except (os.error, AttributeError):
            pass
        else:
            if blksize > 1:
                buffering = blksize
        buffer_writer = io.BufferedWriter(raw, buffering)
    else:
        buffer_writer = None
    text = WatchedIO(buffer_writer, func, args)
    return text


class WatchedIO(io.TextIOWrapper):
    """
    WatchedIO object
    The func will be called when writting to the file
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, buffered_io, func, args):
        self.wi_has_buffered_io = bool(buffered_io is not None)
        if self.wi_has_buffered_io:
            super().__init__(buffered_io)
        self.wi_func = func
        self.wi_args = args

    def write(self, data):
        """
        Need unicode() otherwise will hit problem:
        TypeError: can't write str to text stream
        And also, even the encoding should be utf-8
        there will be some error, so need to ignore it.
        """
        # pylint: disable=bare-except
        if (not self.wi_has_buffered_io) and self.wi_func is None:
            return
        data = str(data, encoding='utf-8', errors='ignore')
        if self.wi_has_buffered_io:
            try:
                super().write(data)
            except:
                logging.error("failed to write data: %s",
                              traceback.format_exc())
        if self.wi_func is not None:
            self.wi_func(self.wi_args, data)

    def close(self):
        """
        Close
        """
        if self.wi_has_buffered_io:
            super().close()

    def flush(self):
        """
        Flush
        """
        if self.wi_has_buffered_io:
            super().flush()


def log_watcher_debug(args, new_log):
    """
    Watch log dump to clog.cl_debug
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    log.cl_debug("log from host [%s]: [%s]",
                 args[WATCHEDIO_HOSTNAME], new_log)


def log_watcher_info(args, new_log):
    """
    Watch log dump to clog.cl_info
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    log.cl_info("log from host [%s]: [%s]",
                args[WATCHEDIO_HOSTNAME], new_log)


def log_watcher_error(args, new_log):
    """
    Watch log dump to clog.cl_error
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    log.cl_error("log from host [%s]: [%s]",
                 args[WATCHEDIO_HOSTNAME], new_log)


def log_watcher_stdout_simplified(args, new_log):
    """
    Watch log dump to clog.cl_stdout
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    if new_log.endswith("\n"):
        new_log = new_log[:-1]
    log.cl_stdout(new_log)


def log_watcher_stderr_simplified(args, new_log):
    """
    Watch log dump to clog.cl_error
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    if new_log.endswith("\n"):
        new_log = new_log[:-1]
    log.cl_error(new_log)
