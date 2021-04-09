"""
Library for defining a command written in python
"""
import sys
import logging
import traceback
import os
import time
import socket

import prettytable
import toml
import yaml
from pycoral import clog
from pycoral import time_util
from pycoral import utils


def get_identity():
    """
    Return a unique identity based on time and random words
    """
    return time_util.local_strftime(time_util.utcnow(),
                                    "%Y-%m-%d-%H_%M_%S-") + utils.random_word(8)


def load_config(log, config_fpath):
    """
    Load a config file with YAML/TOML format
    """
    hostname = socket.gethostname()
    if not os.path.exists(config_fpath):
        log.cl_error("file [%s] does not exist on host [%s]",
                     config_fpath, hostname)
        return None

    if not os.path.isfile(config_fpath):
        log.cl_error("path [%s] is not a regular file on host [%s]",
                     config_fpath, hostname)
        return None

    try:
        config = toml.load(config_fpath)
    except:
        error = traceback.format_exc()
        try:
            config_fd = open(config_fpath)
            config = yaml.load(config_fd)
            config_fd.close()
        except:
            log.cl_error("failed to load file [%s] using TOML format: %s",
                         config_fpath, error)
            log.cl_error("failed to load file [%s] using YAML format: %s",
                         config_fpath, traceback.format_exc())
            config = None
    return config


def check_iso_fpath(iso):
    """
    Check the iso fpath is valid
    """
    if (not isinstance(iso, bool)) and isinstance(iso, int):
        iso = str(iso)
    if not isinstance(iso, str):
        print("ERROR: invalid iso path [%s], should be a string" % iso,
              file=sys.stderr)
        sys.exit(1)
    elif len(iso) == 0:
        print("ERROR: empty iso path", file=sys.stderr)
        sys.exit(1)


def init_env_noconfig(logdir, log_to_file, logdir_is_default,
                      prefix="", identity=None, force_stdout_color=False,
                      force_stderr_color=False):
    """
    Init log and workspace for commands that needs it
    """
    # pylint: disable=too-many-branches
    if identity is None:
        identity = prefix + get_identity()
    else:
        if (not isinstance(identity, bool)) and isinstance(identity, int):
            identity = str(identity)
        elif not isinstance(identity, str):
            print("ERROR: invalid identity [%s]" %
                  (identity), file=sys.stderr)
            sys.exit(1)

    if (not isinstance(logdir, bool)) and isinstance(logdir, int):
        logdir = str(logdir)
    if not isinstance(logdir, str):
        print("ERROR: invalid log dir [%s], should be a string" %
              (logdir), file=sys.stderr)
        sys.exit(1)
    elif len(logdir) == 0:
        print("ERROR: empty log dir", file=sys.stderr)
        sys.exit(1)

    if logdir_is_default:
        workspace = logdir + "/" + identity
    else:
        workspace = logdir

    if not isinstance(log_to_file, bool):
        print("ERROR: invalid debug option [%s], should be a bool type" %
              (log_to_file), file=sys.stderr)
        sys.exit(1)

    if log_to_file:
        command = "mkdir -p %s" % workspace
        retval = utils.run(command)
        if retval.cr_exit_status != 0:
            print("failed to run command [%s], "
                  "ret = [%d], stdout = [%s], stderr = [%s]" %
                  (command,
                   retval.cr_exit_status,
                   retval.cr_stdout,
                   retval.cr_stderr), file=sys.stderr)
            sys.exit(1)

        resultsdir = workspace
        print("INFO: log saved to dir [%s]" % resultsdir, file=sys.stderr)
    else:
        resultsdir = None

    if force_stdout_color:
        stdout_color = True
    else:
        stdout_color = None

    if force_stderr_color:
        stderr_color = True
    else:
        stderr_color = None

    log = clog.get_log(resultsdir=resultsdir, overwrite=True,
                       console_level=logging.INFO,
                       console_format=clog.FMT_NORMAL,
                       stdout_color=stdout_color,
                       stderr_color=stderr_color)

    return log, workspace


def init_env(config_fpath, logdir, log_to_file, logdir_is_default,
             prefix="", identity=None):
    """
    Init log, workspace and config for commands that needs it
    """
    log, workspace = init_env_noconfig(logdir, log_to_file, logdir_is_default,
                                       prefix=prefix, identity=identity)
    if (not isinstance(config_fpath, bool)) and isinstance(config_fpath, int):
        config_fpath = str(config_fpath)
    if not isinstance(config_fpath, str):
        log.cl_error("invalid config path [%s], should be a string" %
                     (config_fpath))
        sys.exit(1)
    elif len(config_fpath) == 0:
        log.cl_error("empty config path")
        sys.exit(1)

    config = load_config(log, config_fpath)
    if config is None:
        sys.exit(1)

    return log, workspace, config


def parse_list_substring(log, list_string):
    """
    Return a list of names
    Examples:
    # Invalid: empty string
    # Invalid: host[10, no closing bracket
    # Invalid: host10], illegal closing bracket
    # Invalid: host[], empty range
    # Invalid: host]1-3[, disordered brackets
    # Invalid: host[A], not a number
    # Invalid: host[10-], nothing after minus
    # Invalid: host[10-A], not a number after minus
    # Invalid: host[0x02], not a number (hexadecimal is not supported)
    # Invalid: host[1-3][1-2], multiple bracket pairs
    # Invalid: host[1-2-3], not a number after minus
    # Invalid: host[0-010], disordered name pattern
    # Invalid: host[10-0], disordered range
    # Invalid: host[-2], nothing before minus
    # Valid: host100
    # Valid: host[12], one host of host12
    # Valid: [10], one host of 10
    # Valid: host[100-100], one host of host100
    # Valid: host[0-100], 101 hosts from host0 to host100
    # Valid: host[001-010], 10 hosts from host001 to host010
    # Valid: [0-10]a, 11 hosts from 0a to 10a
    # Valid: host[0-10]a, 11 hosts from host0a to host10a
    """
    # pylint: disable=too-many-locals,too-many-branches
    # pylint: disable=too-many-statements
    if len(list_string) == 0:
        log.cl_error("invalid [%s]: empty string",
                     list_string)
        return None
    start = list_string.find("[")
    end = list_string.find("]")
    if start == -1 and end == -1:
        return [list_string]
    if start != -1 and end == -1:
        log.cl_error("invalid [%s]: no closing bracket",
                     list_string)
        return None
    if start == -1 and end != -1:
        log.cl_error("invalid [%s]: illegal closing bracket",
                     list_string)
        return None
    if start >= end:
        log.cl_error("invalid [%s]: disordered brackets",
                     list_string)
        return None

    if start + 1 == end:
        log.cl_error("invalid [%s]: empty range",
                     list_string)
        return None

    if start == 0:
        prefix = ""
    else:
        prefix = list_string[:start]

    if len(list_string) == end + 1:
        suffix = ""
    else:
        suffix = list_string[end + 1:]

    suffix_start = suffix.find("[")
    suffix_end = suffix.find("]")
    if suffix_start != -1 or suffix_end != -1:
        log.cl_error("invalid [%s]: multiple bracket pairs",
                     list_string)
        return None

    range_string = list_string[start + 1: end]
    minus = range_string.find("-")
    if minus == -1:
        try:
            number = int(range_string, 10)
        except:
            log.cl_error("invalid [%s]: not a number",
                         list_string)
            return None
        return [prefix + range_string + suffix]

    if minus == 0:
        log.cl_error("invalid [%s]: nothing before minus",
                     list_string)
        return None

    if minus == len(range_string) - 1:
        log.cl_error("invalid [%s]: nothing after minus",
                     list_string)
        return None

    range_start = range_string[:minus]
    range_end = range_string[minus + 1:]
    try:
        start_number = int(range_start, 10)
    except:
        log.cl_error("invalid [%s]: not a number before minus",
                     list_string)
        return None

    try:
        end_number = int(range_end, 10)
    except:
        log.cl_error("invalid [%s]: not a number after minus",
                     list_string)
        return None

    if start_number > end_number:
        log.cl_error("invalid [%s]: disordered range",
                     list_string)
        return None

    if ((range_start[0] == "0" and len(range_start) != 1) or
            (range_end[0] == "0" and len(range_end) != 1)):
        # Need to add 0s in the hostnames
        if len(range_start) != len(range_end):
            log.cl_error("invalid [%s]: disordered name pattern",
                         list_string)
            return None
        width = len(range_start)
    else:
        width = None

    names = []
    number = start_number
    while True:
        if number > end_number:
            break
        index_string = str(number)
        if width is not None:
            index_string = index_string.rjust(width, "0")
        name = prefix + index_string + suffix
        names.append(name)
        number += 1
    return names


def parse_list_string(log, list_string):
    """
    Return a list of names.
    The list string contains substring seperated by comma.
    Each substring need to be able parsed by parse_list_substring().
    """
    substrings = list_string.split(",")
    names = []
    for substring in substrings:
        sub_names = parse_list_substring(log, substring)
        if sub_names is None:
            log.cl_error("invalid list string [%s]",
                         list_string)
            return None

        names += sub_names
    return names

# Initial status. Next char should be key string or spaces. If next char is
# a letter, digit or underline, goto PTP_KEY.
PTP_INIT = "init"
# At least on char already got for key. If next char is space, goto PTP_INIT.
# If next char is =, goto PTP_VALUE. If next char is a letter, digit or
# underline, remain PTP_KEY.
PTP_KEY = "key"
# "=" has been got after PTP_KEY. If next char is space (without leading "\"),
# goto PTP_INIT.
PTP_VALUE = "value"


def add_parameter_pair(param_dict, key, value):
    """
    Add a pair of key/value
    """
    if key not in param_dict:
        param_dict[key] = value
        return 0
    existing_value = param_dict[key]
    if existing_value != value:
        return -1
    return 0


def parse_parameter(log, param):
    """
    A single line of key/value pairs or flags without leading "Test-Parameters:".
    Return a dict. Key is the key, value is the value.
    Please check doc/Test-Parameters.txt for more info about the format.
    Return None on error.
    """
    # pylint: disable=too-many-branches,too-many-statements
    if not isinstance(param, str):
        log.cl_error("invalid [%s]: not string",
                     param)
        return None

    # Key/value pair
    param_dict = {}
    key = ""
    value = ""
    status = PTP_INIT
    escape = False
    for char in param:
        if status == PTP_INIT:
            if char == " ":
                continue
            if char.isalnum() or char == "_":
                status = PTP_KEY
                key += char
            else:
                log.cl_error("invalid [%s]: key starts with illegal characters",
                             param)
                return None
        elif status == PTP_KEY:
            if char == " ":
                ret = add_parameter_pair(param_dict, key, True)
                if ret:
                    log.cl_error("invalid [%s]: ambiguous values",
                                 param)
                    return None
                key = ""
            elif char.isalnum() or char == "_":
                key += char
            elif char == "=":
                if len(key) == 0:
                    log.cl_error("invalid [%s]: empty key",
                                 param)
                    return None
                # The value starts
                status = PTP_VALUE
            else:
                log.cl_error("invalid [%s]: key contains with illegal characters",
                             param)
                return None
        else:
            assert status == PTP_VALUE
            if char == "\\":
                if escape:
                    value += "\\"
                escape = True
                # The escape will be handled together with next char.
                continue
            if char == " ":
                if escape:
                    value += " "
                    escape = False
                else:
                    # The key/value pair finishes
                    ret = add_parameter_pair(param_dict, key, value)
                    if ret:
                        log.cl_error("invalid [%s]: ambiguous values",
                                     param)
                        return None
                    status = PTP_INIT
                    value = ""
                    key = ""
                continue

            if char == "\n":
                if escape:
                    escape = False
                    continue
                log.cl_error("invalid [%s]: multiple lines",
                             param)
                return None

            if escape:
                value += "\\"
                escape = False
            value += char

    if status == PTP_KEY:
        ret = add_parameter_pair(param_dict, key, True)
        if ret:
            log.cl_error("invalid [%s]: ambiguous values",
                         param)
            return None
    elif status == PTP_VALUE:
        ret = add_parameter_pair(param_dict, key, value)
        if ret:
            log.cl_error("invalid [%s]: ambiguous values",
                         param)
            return None

    return param_dict


def parameter_dict_merge(log, param_dicts):
    """
    Merge the param dicts into a single one.
    If fail, return None
    """
    merged_dict = {}
    for param_dict in param_dicts:
        for key, value in param_dict.items():
            if key not in merged_dict:
                merged_dict[key] = value
                continue
            if merged_dict[key] != value:
                log.cl_error("ambiguous values for key [%s] in parameters",
                             key)
                return None
    return merged_dict


def table_add_field_names(table, field_names):
    """
    Add field names of a table
    """
    colorful_field_names = []
    for field_name in field_names:
        colorful_field_names.append(clog.colorful_message(clog.COLOR_TABLE_FIELDNAME,
                                                          field_name))
    table.field_names = colorful_field_names


def table_set_sortby(table, field_name):
    """
    Set the field to sortby
    """
    table.sortby = clog.colorful_message(clog.COLOR_TABLE_FIELDNAME, field_name)


def print_field(log, field_name, value):
    """
    print one field
    """
    field = clog.colorful_message(clog.COLOR_TABLE_FIELDNAME, "%s: " % field_name)
    log.cl_stdout("%s%s", field, value)


def cmd_exit(log, exist_status):
    """
    Print message and exit
    """
    if exist_status:
        log.cl_debug("command failed with status %s", exist_status)
    else:
        log.cl_debug("command succeeded")
    sys.exit(exist_status)


TEST_SKIPPED = 1


def run_test(log, workspace, only_test_names, first_test_names,
             local_host, reverse_order, start, stop, test_functs, args):
    """
    Run test.
    If only is specified together with start/stop, start/stop option will
    be ignored.
    If only is specified together with reverse_order, reverse_order option
    will be ignored.
    Only/first tests can repeat tests, e.g. first=testA,testA would repeat
    testA for twice.
    :param test_functs: a list of function that has the argument types of: test_funct(log, test_workspace, *args)
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-arguments
    # pylint: disable=too-many-statements,global-statement
    test_dict = {}
    for test_funct in test_functs:
        test_dict[test_funct.__name__] = test_funct

    if len(test_functs) == 0:
        log.cl_error("no test to run")
        return -1

    if test_functs[0].__name__ != "basic":
        log.cl_error("the first test is not [basic]")
        return -1
    basic_test = test_functs[0]

    # Reverse order won't change the order of first tests
    selected_tests = []
    if first_test_names is not None:
        for test_name in first_test_names:
            if test_name not in test_dict:
                log.cl_error("first test [%s] does not exist", test_name)
                return -1
            test_funct = test_dict[test_name]
            selected_tests.append(test_funct)

    if only_test_names is not None:
        for test_name in only_test_names:
            if test_name not in test_dict:
                log.cl_error("only test [%s] does not exist", test_name)
                return -1
            test_funct = test_dict[test_name]
            selected_tests.append(test_funct)
    else:
        start_index = 0
        if start is not None:
            if start not in test_dict:
                log.cl_error("start test [%s] does not exist", start)
                return -1
            for test_funct in test_functs:
                if test_funct.__name__ == start:
                    break
                start_index += 1
            if start_index == len(test_functs):
                log.cl_error("failed to find the index of start test [%s]",
                             start)
                return -1

        stop_index = len(test_functs) - 1
        if stop is not None:
            if stop not in test_dict:
                log.cl_error("stop test [%s] does not exist", stop)
                return -1
            stop_index = 0
            for test_funct in test_functs:
                if test_funct.__name__ == stop:
                    break
                stop_index += 1

            if stop_index == len(test_functs):
                log.cl_error("failed to find the index of start test [%s]",
                             stop)
                return -1

        if stop_index < start_index:
            log.cl_error("start test [%s] is behind stop test [%s]",
                         start, stop)
            return -1

        test_index = 0
        for test_funct in test_functs:
            if test_index > stop_index:
                break
            if test_index >= start_index:
                selected_tests.append(test_funct)
            test_index += 1

        if len(selected_tests) == 0:
            pass
        elif selected_tests[0].__name__ != "basic":
            if reverse_order:
                selected_tests.reverse()
            selected_tests.insert(0, basic_test)
        elif reverse_order:
            other_tests = selected_tests[1:]
            other_tests.reverse()
            selected_tests = [basic_test] + other_tests

    if len(selected_tests) > 0 and selected_tests[0].__name__ != "basic":
        selected_tests.insert(0, basic_test)

    table = prettytable.PrettyTable()
    table.field_names = ["Test name", "Result", "Duration"]

    exit_status = 0
    for test_func in selected_tests:
        test_name = test_func.__name__
        if exit_status:
            table.add_row([test_name, "Not Started", "0 seconds"])
            continue

        test_workspace = (workspace + "/" +
                          time_util.local_strftime(time_util.utcnow(),
                                                   "%Y-%m-%d-%H_%M_%S") +
                          "-" + test_name + "-" + utils.random_word(8))
        command = "mkdir -p %s" % test_workspace
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            table.add_row([test_name, "Not Started", "0 seconds"])
            exit_status = -1
            continue

        log.cl_info("starting test [%s]", test_name)
        start_time = time.time()
        ret = test_func(log, test_workspace, *args)
        duration_time = time.time() - start_time
        if ret < 0:
            log.cl_error("test [%s] failed, duration %f seconds", test_name,
                         duration_time)
            table.add_row([test_name, "Failed", "%f seconds" % duration_time])
            exit_status = -1
            continue
        if ret == TEST_SKIPPED:
            log.cl_warning("test [%s] skipped, duration %f seconds", test_name,
                           duration_time)
            table.add_row([test_name, "Skipped", "%f seconds" % duration_time])
        else:
            log.cl_info("test [%s] passed, duration %f seconds", test_name,
                        duration_time)
            table.add_row([test_name, "Passed", "%f seconds" % duration_time])

    for test_funct in test_functs:
        if test_funct not in selected_tests:
            test_name = test_funct.__name__
            table.add_row([test_name, "Excluded", "0"])

    log.cl_stdout(table)
    return exit_status


def get_table_field(log, host, field_number, command):
    """
    Return a dict for a given field of a table.
    Key is service/host/lustrefs/... name, should be the first column
    Value is the field value.
    """
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = %d, stdout = [%s], stderr = [%s]",
                     command, host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None

    lines = retval.cr_stdout.splitlines()
    if len(lines) < 1:
        log.cl_error("no output [%s] of command [%s] on host [%s]",
                     retval.cr_stdout, command, host.sh_hostname)
        return None

    lines = lines[1:]
    field_dict = {}
    for line in lines:
        fields = line.split()
        if len(fields) < field_number + 1:
            log.cl_error("no field with index [%d] in output [%s] of "
                         "command [%s] on host [%s]",
                         field_number, retval.cr_stdout, command,
                         host.sh_hostname)
            return None
        name = fields[0]
        field_dict[name] = fields[field_number]
    return field_dict


def get_status_dict(log, host, command, ignore_exit_status=True):
    """
    Return status dict of client/service/host
    """
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        # Some commands still print fields when return failure
        if ignore_exit_status:
            log.cl_debug("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
        else:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None

    lines = retval.cr_stdout.splitlines()
    status_dict = {}
    for line in lines:
        split_index = line.find(": ")
        if split_index < 0:
            log.cl_error("can not find [: ] in output line [%s] of "
                         "command [%s]", line, command)
            return None
        if split_index == 0:
            log.cl_error("no key before [: ] in output line [%s] of "
                         "command [%s]", line, command)
            return None
        if split_index + 2 >= len(line):
            log.cl_error("no value after [: ] in output line [%s] of "
                         "command [%s]", line, command)
            return None
        key = line[0:split_index]
        value = line[split_index + 2:]
        if key in status_dict:
            log.cl_error("multiple values for key [%s] of command [%s]",
                         key, command)
            return None
        status_dict[key] = value
    return status_dict


def parse_field_string(log, field_string, quick_fields, table_fields,
                       all_fields, print_table=False, print_status=False):
    """
    Return field names.
    """
    # pylint: disable=too-many-branches,too-many-arguments
    if field_string is None:
        if not print_table:
            field_names = all_fields
        elif print_status:
            field_names = table_fields
        else:
            field_names = quick_fields
    elif isinstance(field_string, tuple):
        field_names = list(field_string)
        for field_name in field_names:
            if field_name not in all_fields:
                log.cl_error("unknown field [%s]", field_name)
                return None
        if field_names[0] != quick_fields[0]:
            field_names.insert(0, quick_fields[0])
        if len(field_names) != len(set(field_names)):
            log.cl_error("duplicated fields in %s",
                         field_names)
            return None
    elif isinstance(field_string, str):
        field_names = field_string.split(",")
        for field_name in field_names:
            if field_name not in all_fields:
                log.cl_error("unknown field [%s]", field_name)
                return None
        if field_names[0] != quick_fields[0]:
            field_names.insert(0, quick_fields[0])
        if len(field_names) != len(set(field_names)):
            log.cl_error("duplicated fields in %s",
                         field_names)
            return None
    else:
        log.cl_error("invalid field string type [%s]",
                     type(field_string).__name__)
        return None
    return field_names


def print_all_fields(log, all_fields):
    """
    Print all field names
    """
    output = ""
    for field_name in all_fields:
        if output == "":
            output += field_name
        else:
            output += "," + field_name
    log.cl_stdout(output)


def print_list(log, item_list, quick_fields, slow_fields, none_table_fields,
               field_result_funct, print_status=False, print_table=True,
               field_string=None, sortby=None):
    """
    Print table of virtual machines
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    # pylint: disable=too-many-statements,too-many-arguments
    if len(quick_fields) == 0:
        log.cl_error("empty table")
        return -1

    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if not print_table and len(item_list) > 1:
        log.cl_error("failed to print non-table output with multiple items")
        return -1

    if isinstance(field_string, bool):
        print_all_fields(log, all_fields)
        return 0

    field_names = parse_field_string(log, field_string, quick_fields,
                                     table_fields, all_fields,
                                     print_status=print_status,
                                     print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s]",
                     field_string)
        return -1

    if print_table:
        table = prettytable.PrettyTable()
        table.set_style(prettytable.PLAIN_COLUMNS)
        table_add_field_names(table, field_names)

    rc = 0
    for item in item_list:
        results = []
        for field_name in field_names:
            ret, result = field_result_funct(log, item, field_name)

            if print_table:
                results.append(result)
            else:
                print_field(log, field_name, result)
            if ret:
                rc = ret

        if print_table:
            table.add_row(results)
            continue

    if print_table:
        table.align = "l"
        if sortby is None:
            table_set_sortby(table, field_names[0])
        else:
            table_set_sortby(table, sortby)
        log.cl_stdout(table)
    return rc


def check_argument_type(log, name, value, expected_type):
    """
    Check the argument is expected. If not, exit.
    """
    if not isinstance(value, expected_type):
        log.cl_error("unexpected type [%s] of value [%s] for argument "
                     "[--%s], expected [%s]",
                     type(value).__name__, value, name,
                     expected_type.__name__)
        cmd_exit(log, -1)


def check_argument_bool(log, name, value):
    """
    Check the argument is bool. If not, exit.
    """
    check_argument_type(log, name, value, bool)


def check_argument_int(log, name, value):
    """
    Check the argument is int. If not, exit.
    """
    check_argument_type(log, name, value, int)


def check_argument_str(log, name, value):
    """
    Check the argument is str. If not, exit.
    """
    check_argument_types(log, name, value, allow_none=False,
                         allow_tuple=False, allow_str=True,
                         allow_int=True, allow_bool=False)
    if isinstance(value, int):
        value = str(value)
    return value


def check_argument_list_str(log, name, value):
    """
    Check the argument is a list of str. If not, exit.
    """
    check_argument_types(log, name, value, allow_none=False,
                         allow_tuple=True, allow_str=True,
                         allow_int=True, allow_bool=False)
    if isinstance(value, int):
        value = str(value)
    elif isinstance(value, tuple):
        ret_value = ""
        for item in value:
            if ret_value == "":
                ret_value = item
            else:
                ret_value += "," + item
        value = ret_value
    return value


def check_argument_types(log, name, value, allow_none=False,
                         allow_tuple=False, allow_str=False,
                         allow_int=False, allow_bool=False):
    """
    Check the argument is str. If not, exit.
    """
    # pylint: disable=too-many-arguments
    if allow_bool and isinstance(value, bool):
        return

    if (not allow_bool) and isinstance(value, bool):
        log.cl_error("unexpected type [%s] of value [%s] for argument "
                     "[--%s]",
                     type(value).__name__, value, name)
        cmd_exit(log, -1)

    if allow_none and value is None:
        return

    if allow_int and isinstance(value, int):
        return

    if allow_tuple and isinstance(value, tuple):
        return

    if allow_str and isinstance(value, str):
        return

    log.cl_error("unexpected type [%s] of value [%s] for argument "
                 "[--%s]",
                 type(value).__name__, value, name)
    cmd_exit(log, -1)


def get_command_name(command):
    """
    Only alpha/number/-/_/+ will be kept. Spaces will be replaced to ".".
    Others will be replaced to "+".
    """
    name = ""
    for char in command:
        if char.isalnum() or char == "-" or char == "_":
            name += char
        elif char == " ":
            name += "."
        else:
            name += "+"
    return name
