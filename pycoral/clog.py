"""
Logging library
Rerfer to /usr/lib64/python2.7/logging for more information

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import logging
import os
import threading
import inspect
import sys
import traceback
import re

from pycoral import constant
from pycoral import utils

LOG_DEBUG_FNAME = "debug.log"
LOG_INFO_FNAME = "info.log"
LOG_WARNING_FNAME = "warning.log"
LOG_ERROR_FNAME = "error.log"

#
# _CLOG_SRC_FILE is used when walking the stack to check when we've got the
# first caller stack frame.
#
if __file__[-4:].lower() in ['.pyc', '.pyo']:
    _CLOG_SRC_FILE = __file__[:-4] + '.py'
else:
    _CLOG_SRC_FILE = __file__
_CLOG_SRC_FILE = os.path.normcase(_CLOG_SRC_FILE)


# pylint: disable=pointless-statement
try:
    str
    _UNICODE = True
except NameError:
    _UNICODE = False

# This should be different with logging.ERROR etc.
LEVEL_STDOUT = 1048576


def find_caller(src_file):
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.
    """
    frame = inspect.currentframe()
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    if frame is not None:
        frame = frame.f_back
    ret = "(unknown file)", 0, "(unknown function)"
    while hasattr(frame, "f_code"):
        code_object = frame.f_code
        filename = os.path.normcase(code_object.co_filename)
        # src_file is always absolute path, but the filename might not be
        # absolute path
        if filename.startswith("/"):
            if filename == src_file:
                frame = frame.f_back
                continue
        else:
            if src_file.endswith(filename):
                frame = frame.f_back
                continue
        ret = (code_object.co_filename, frame.f_lineno, code_object.co_name)
        break
    return ret


class CoralLogs():
    """
    Global log object to track what logs have been allocated
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        self.cls_condition = threading.Condition()
        self.cls_logs = {}
        self.cls_root_log = None

    def cls_log_add_or_get(self, log):
        """
        Add a new log
        """
        name = log.cl_name
        old_log = None
        self.cls_condition.acquire()
        if name is None:
            if self.cls_root_log is not None:
                old_log = self.cls_root_log
                self.cls_condition.release()
                return old_log
            self.cls_root_log = log
        else:
            if name in self.cls_logs:
                old_log = self.cls_logs[name]
                self.cls_condition.release()
                return old_log
            self.cls_logs[name] = log
        self.cls_condition.release()
        return log

    def cls_log_fini(self, log):
        """
        Cleanup a log
        """
        name = log.cl_name
        self.cls_condition.acquire()
        if self.cls_root_log is log:
            self.cls_root_log = None
        elif name in self.cls_logs:
            del self.cls_logs[name]
        else:
            log.cl_warning("log [%s] doesn't exist when cleaning up, ignoring",
                           name)
        self.cls_condition.release()


GLOBAL_LOGS = CoralLogs()


def get_message(msg, args):
    """
    Return the message.

    Please check LogRecord.getMessage of logging for more info
    """
    try:
        msg = str(msg)
        if args:
            msg = msg % args
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        sys.stderr.write("log error: %s" % traceback.format_exc())
        sys.stderr.write('Message: %r\n'
                         'Arguments: %s\n' % (msg, args))
        msg = "== LOG ERROR ==\n"
    return msg


STDOUT_KEY = "@stdout@:"
CORAL_SKIP_CONSOLE = "skip_console"


def need_print_record_to_console(record):
    """
    Do not print to console since it should already been printed.
    """
    if CORAL_SKIP_CONSOLE in record.__dict__:
        skip_console = record.__dict__[CORAL_SKIP_CONSOLE]
        if skip_console:
            return False
    return True


# Sequences need to get colored ouput
COLOR_SEQ_BLACK = '\033[0;30m'
COLOR_SEQ_RED = '\033[0;31m'
COLOR_SEQ_GREEN = '\033[0;32m'
COLOR_SEQ_BROWN = '\033[0;33m'
COLOR_SEQ_BLUE = '\033[0;34m'
COLOR_SEQ_PURPLE = '\033[0;35m'
COLOR_SEQ_CYAN = '\033[0;36m'
COLOR_SEQ_GREY = '\033[0;37m'
COLOR_SEQ_DARK_GREY = '\033[1;30m'
COLOR_SEQ_LIGHT_RED = '\033[1;31m'
COLOR_SEQ_LIGHT_GREEN = '\033[1;32m'
COLOR_SEQ_YELLOW = '\033[1;33m'
COLOR_SEQ_LIGHT_BLUE = '\033[1;34m'
COLOR_SEQ_LIGHT_PURPLE = '\033[1;35m'
COLOR_SEQ_LIGHT_CYAN = '\033[1;36m'
COLOR_SEQ_WHITE = '\033[1;37m'
COLOR_SEQ_RESET = "\033[0m"

# The exported colors, do not use sequences out of this list
COLOR_WARNING = "WARNING"
COLOR_INFO = "INFO"
COLOR_DEBUG = "DEBUG"
COLOR_CRITICAL = "CRITICAL"
COLOR_ERROR = "ERROR"
COLOR_RED = "RED"
COLOR_GREEN = "GREEN"
COLOR_YELLOW = "YELLOW"
COLOR_LIGHT_BLUE = "LIGHT_BLUE"
COLOR_TABLE_FIELDNAME = "TABLE_FIELDNAME"

COLORS = {
    COLOR_WARNING: COLOR_SEQ_YELLOW,
    COLOR_INFO: COLOR_SEQ_LIGHT_BLUE,
    COLOR_DEBUG: COLOR_SEQ_GREY,
    COLOR_CRITICAL: COLOR_SEQ_RED,
    COLOR_ERROR: COLOR_SEQ_RED,
    COLOR_LIGHT_BLUE: COLOR_SEQ_LIGHT_BLUE,
    COLOR_RED: COLOR_SEQ_RED,
    COLOR_GREEN: COLOR_SEQ_GREEN,
    COLOR_YELLOW: COLOR_SEQ_YELLOW,
    COLOR_TABLE_FIELDNAME: COLOR_SEQ_CYAN,
}

COLOR_SEQS = [COLOR_SEQ_RESET]
for _color_seq in COLORS.values():
    if _color_seq not in COLOR_SEQS:
        COLOR_SEQS.append(_color_seq)


def colorful_message(color, message):
    """
    Return colorful message
    """
    if color not in COLORS:
        return str(message)
    return COLORS[color] + str(message) + COLOR_SEQ_RESET


def colorless_message(message):
    """
    Remove ANSI color/style sequences from a string. The set of all
    possibly ANSI sequences is large, so does not try to strip every
    possible one. But does strip some outliers seen not just in text
    generated by this module, but by other ANSI colorizers in the wild.
    Those include `\x1b[K` (aka EL or erase to end of line) and `\x1b[m`
    a terse version of the more common `\x1b[0m`.
    """
    return re.sub('\x1b\\[(K|.*?m)', '', message)


class ColoredFormatter(logging.Formatter):
    """
    Colorful log formatter
    """
    def __init__(self, fmt=None, datefmt=None):
        logging.Formatter.__init__(self, fmt=fmt, datefmt=datefmt)

    def format(self, record):
        levelname = record.levelname
        if levelname in COLORS:
            levelname_color = colorful_message(levelname, levelname)
            record.levelname = levelname_color
        text = logging.Formatter.format(self, record)
        record.levelname = levelname
        return text


FMT_NORMAL = "%(levelname)s: %(message)s"
FMT_QUIET = "%(message)s"
FMT_FULL = ("[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)s] "
            "%(message)s")
FMT_TIME = "[%(asctime)s] [%(levelname)s] %(message)s"
DATE_FMT = "%Y/%m/%d-%H:%M:%S"


class CoralLogRecord():
    """
    A record save in CoralLog
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, level, message, is_stdout=False,
                 is_raw_stderr=False):
        # logging.INFO and etc.
        self.clr_level = level
        # String of message
        self.clr_message = message
        # Whether print the stdout rather than stderr. The level will
        # be ignore if the record is for stdout.
        self.clr_is_stdout = is_stdout
        # Do not add format prefix.
        self.clr_is_raw_stderr = is_raw_stderr

    def clr_log(self, log):
        """
        Print myself to the log
        """
        if self.clr_is_stdout:
            log.cl_stdout(self.clr_message)
        elif self.clr_is_raw_stderr:
            log.cl_raw_stderr(self.clr_message)
        else:
            log.cl_log(self.clr_level, self.clr_message)


def rpc_dict2log_record(log, rpc_dict):
    """
    Transfer a dict of XMLRPC to CoralLogRecord
    """
    fields = ["clr_level", "clr_message", "clr_is_stdout"]
    for field in fields:
        if field not in rpc_dict:
            log.cl_error("no field [%s] in XML RPC dict: %s",
                         field, rpc_dict)
            return None
    record = CoralLogRecord(rpc_dict["clr_level"],
                            rpc_dict["clr_message"],
                            rpc_dict["clr_is_stdout"])
    return record


class CoralLog():
    """
    Log the ouput of a command
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, name=None, resultsdir=None, console_format=FMT_FULL,
                 condition=None, stdout_color=None, stderr_color=None,
                 remember_records=False, stdout=None, stderr=None):
        self.cl_name = name
        self.cl_result = utils.CommandResult()
        self.cl_resultsdir = resultsdir
        self.cl_console_format = console_format
        self.cl_logger = None
        if condition is not None:
            self.cl_condition = condition
        else:
            self.cl_condition = threading.Condition()
        self.cl_debug_handler = None
        self.cl_info_handler = None
        self.cl_warning_handler = None
        self.cl_error_handler = None
        self.cl_console_handler = None
        # Whether print colorful message to stdout
        self.cl_stdout_color = stdout_color
        # Whether print colorful message to stderr
        self.cl_stderr_color = stderr_color
        # Condition to protect cl_records
        self.cl_condition = threading.Condition()
        # List of CoralLogRecord
        if remember_records:
            self.cl_records = []
        else:
            self.cl_records = None
        if stdout is None:
            self.cl_sys_stdout = sys.stdout
        else:
            self.cl_sys_stdout = stdout
        if stderr is None:
            self.cl_sys_stderr = sys.stderr
        else:
            self.cl_sys_stderr = stderr

    def cl_set_propaget(self):
        """
        Whether log events to this logger to higher level loggers
        """
        self.cl_logger.propagate = True

    def cl_clear_propaget(self):
        """
        Whether log events to this logger to higher level loggers
        """
        self.cl_logger.propagate = False

    def cl_get_child(self, name, resultsdir=None, console_format=FMT_FULL,
                     overwrite=False, condition=None):
        """
        Get a child log
        If overwrite, the existing log will be overwritten
        """
        if self.cl_name is not None:
            name = self.cl_name + "." + name
        return get_log(name, resultsdir=resultsdir,
                       console_format=console_format,
                       overwrite=overwrite,
                       condition=condition)

    def cl_config(self, console_level=logging.INFO):
        """
        Config the log
        """
        # pylint: disable=too-many-locals,too-many-statements
        # pylint: disable=too-many-branches,bad-option-value,redefined-variable-type
        resultsdir = self.cl_resultsdir
        name = self.cl_name
        console_format = self.cl_console_format
        default_formatter = logging.Formatter(FMT_FULL, DATE_FMT)

        if self.cl_stderr_color is None:
            self.cl_stderr_color = self.cl_sys_stderr.isatty()
        colorful = self.cl_stderr_color
        if self.cl_stdout_color is None:
            self.cl_stdout_color = self.cl_sys_stdout.isatty()
        if console_format == FMT_QUIET:
            console_formatter = logging.Formatter(FMT_QUIET, DATE_FMT)
        elif console_format == FMT_FULL:
            if colorful:
                console_formatter = ColoredFormatter(console_format, DATE_FMT)
            else:
                console_formatter = logging.Formatter(console_format, DATE_FMT)
        else:
            if colorful:
                console_formatter = ColoredFormatter(console_format, DATE_FMT)
            else:
                console_formatter = logging.Formatter(console_format, DATE_FMT)
        if resultsdir is not None:
            fpath = resultsdir + "/" + LOG_DEBUG_FNAME
            debug_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                 maxBytes=10485760,
                                                                 backupCount=10)
            debug_handler.setLevel(logging.DEBUG)
            debug_handler.setFormatter(default_formatter)

            fpath = resultsdir + "/" + LOG_INFO_FNAME
            info_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                maxBytes=10485760,
                                                                backupCount=10)
            info_handler.setLevel(logging.INFO)
            info_handler.setFormatter(default_formatter)

            fpath = resultsdir + "/" + LOG_WARNING_FNAME
            warning_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                   maxBytes=10485760,
                                                                   backupCount=10)
            warning_handler.setLevel(logging.WARNING)
            warning_handler.setFormatter(default_formatter)

            fpath = resultsdir + "/" + LOG_ERROR_FNAME
            error_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                 maxBytes=10485760,
                                                                 backupCount=10)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(default_formatter)

        if name is None:
            logger = logging.getLogger()
        else:
            logger = logging.getLogger(name=name)

        logger.handlers = []
        logger.setLevel(logging.DEBUG)

        if name is None:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(console_level)
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            self.cl_console_handler = console_handler
            console_filter = logging.Filter()
            console_filter.filter = need_print_record_to_console
            self.cl_console_handler.addFilter(console_filter)

        if resultsdir is not None:
            logger.addHandler(debug_handler)
            logger.addHandler(info_handler)
            logger.addHandler(warning_handler)
            logger.addHandler(error_handler)
            self.cl_debug_handler = debug_handler
            self.cl_info_handler = info_handler
            self.cl_warning_handler = warning_handler
            self.cl_error_handler = error_handler
        self.cl_logger = logger

    def cl_change_config(self, console_format=FMT_FULL, resultsdir=None):
        """
        Change the config of the log
        """
        self.cl_resultsdir = resultsdir
        self.cl_console_format = console_format
        self.cl_config()

    def cl_emit(self, name, level, filename, lineno, func, message,
                skip_console=False):
        """
        Emit a log
        """
        record_args = None
        exc_info = None
        extra = None
        if skip_console:
            extra = {CORAL_SKIP_CONSOLE: True}
        record = self.cl_logger.makeRecord(name, level, filename, lineno,
                                           message, record_args, exc_info,
                                           func, extra)
        self.cl_logger.handle(record)

    def _cl_append_record(self, level, message, is_stdout=False,
                          is_raw_stderr=False):
        """
        Save the record
        """
        if self.cl_records is None:
            return
        record = CoralLogRecord(level, message, is_stdout=is_stdout,
                                is_raw_stderr=is_raw_stderr)
        self.cl_condition.acquire()
        self.cl_records.append(record)
        self.cl_condition.release()

    def _cl_log_raw(self, level, message, skip_console=False):
        """
        Emit the log.
        """
        try:
            filename, lineno, func = find_caller(_CLOG_SRC_FILE)
        except ValueError:
            filename, lineno, func = "(unknown file)", 0, "(unknown function)"

        name = self.cl_name
        self.cl_emit(name, level, filename, lineno, func, message,
                     skip_console=skip_console)

    def cl_log(self, level, msg, *args):
        """
        Emit the log and hande the records
        """
        message = get_message(msg, args)
        self._cl_append_record(level, message)
        self._cl_log_raw(level, message)

    def cl_debug(self, msg, *args):
        """
        Print the log to debug log, but not stdout/stderr
        """
        self.cl_log(logging.DEBUG, msg, *args)

    def cl_info(self, msg, *args):
        """
        Print the log to info log, but not stdout/stderr
        """
        self.cl_log(logging.INFO, msg, *args)

    def cl_warning(self, msg, *args):
        """
        Print the log to warning log, but not stdout/stderr
        """
        self.cl_log(logging.WARNING, msg, *args)

    def cl_error(self, msg, *args):
        """
        Print the log to error log, but not stdout/stderr
        """
        self.cl_log(logging.ERROR, msg, *args)

    def cl_fini(self):
        """
        Cleanup this log
        """
        return fini_log(self)

    def cl_stdout(self, msg, *args):
        """
        Print the message to stdout and at the same time log to info log.
        The info log might be saved to a file, and we use this to save
        the stdout to info log file.
        """
        message = get_message(msg, args)
        colorless = colorless_message(message)
        self._cl_append_record(0, message, is_stdout=True)
        self._cl_log_raw(logging.INFO, STDOUT_KEY + colorless,
                         skip_console=True)
        if self.cl_stdout_color:
            self.cl_sys_stdout.write(message + "\n")
        else:
            self.cl_sys_stdout.write(colorless + "\n")

    def cl_raw_stderr(self, msg, *args):
        """
        Print the message to stderr and at the same time log to error log.
        cl_error(), cl_warning() or cl_info() will add format prefix. By
        using this function, we can print the raw string.
        """
        message = get_message(msg, args)
        colorless = colorless_message(message)
        self._cl_append_record(0, message, is_raw_stderr=True)
        self._cl_log_raw(logging.ERROR, colorless, skip_console=True)
        if self.cl_stderr_color:
            self.cl_sys_stderr.write(message + "\n")
        else:
            self.cl_sys_stderr.write(colorless + "\n")


def get_log(name=None, resultsdir=None, console_format=FMT_FULL,
            overwrite=False, condition=None, console_level=logging.INFO,
            stdout_color=None, stderr_color=None, remember_records=False):
    """
    Get the log.
    If overwrite, the existing log will be overwritten.
    """
    log = CoralLog(name=name, resultsdir=resultsdir,
                   console_format=console_format,
                   condition=condition,
                   stdout_color=stdout_color,
                   stderr_color=stderr_color,
                   remember_records=remember_records)
    old_log = GLOBAL_LOGS.cls_log_add_or_get(log)
    if old_log is log:
        # Newly added, config it
        old_log.cl_config(console_level=console_level)
    else:
        if not overwrite:
            reason = ("log with name [%s] already exists" % name)
            raise Exception(reason)
        # If the config is not the same, config it
        if (old_log.cl_resultsdir != resultsdir or
                old_log.cl_console_format != console_format):
            old_log.cl_change_config(console_format=console_format,
                                     resultsdir=resultsdir)
    return old_log


def fini_log(log):
    """
    Cleanup the log so the name can be re-used again
    """
    return GLOBAL_LOGS.cls_log_fini(log)

ERROR_MSG = colorful_message(COLOR_RED, constant.CMD_MSG_ERROR)
