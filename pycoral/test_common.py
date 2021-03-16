"""
Library for running tests
"""
import os
import socket
from pycoral import install_common
from pycoral import constant


def coral_install_rpm(log, workspace, host, iso_path):
    """
    Install Coral RPMs from ISO
    """
    mnt_path = workspace + "/mnt"
    command = ("mkdir -p %s && mount -o loop %s %s" %
               (mnt_path, iso_path, mnt_path))
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

    ret = install_common.coral_rpm_install(log, host, mnt_path)
    if ret:
        log.cl_error("failed to install Coral RPMs")

    command = ("umount %s" % (mnt_path))
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
    return ret


def coral_reinstall(log, workspace, test_host, local_iso_path, config_fpaths,
                    config_dir=constant.ETC_CORAL_DIR):
    """
    Reinstall Coral RPMs from ISO
    """
    # pylint: disable=too-many-arguments,too-many-branches
    local_hostname = socket.gethostname()
    command = "mkdir -p %s" % workspace
    retval = test_host.sh_run(log, command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = %d, stdout = [%s], stderr = [%s]",
                     command, test_host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1, None

    # Send the ISO file to the test host
    ret = test_host.sh_is_localhost(log)
    if ret < 0:
        log.cl_error("failed to check whether host [%s] is local host",
                     test_host.sh_hostname)
        return -1, None
    if local_iso_path is None:
        iso_path = None
    elif ret != 0:
        iso_path = local_iso_path
    else:
        log.cl_info("sending file [%s] on local host [%s] to "
                    "dir [%s] on host [%s]",
                    local_iso_path, local_hostname,
                    workspace, test_host.sh_hostname)
        basename = os.path.basename(local_iso_path)
        ret = test_host.sh_send_file(log, local_iso_path, workspace)
        if ret:
            log.cl_error("failed to send file [%s] on local host [%s] to "
                         "dir [%s] on host [%s]",
                         local_iso_path, local_hostname, workspace,
                         test_host.sh_hostname)
            return -1, None

        iso_path = workspace + "/" + basename

    if iso_path is not None:
        ret = test_host.sh_rpm_find_and_uninstall(log, "grep coral")
        if ret:
            log.cl_error("failed to uninstall Coral rpms on host [%s]",
                         test_host.sh_hostname)
            return -1, None

        ret = coral_install_rpm(log, workspace, test_host, iso_path)
        if ret:
            log.cl_error("failed to install Coral RPMs from ISO")
            return -1, None

    # Send the config file to the correct place on install server
    ret = test_host.sh_send_file(log, config_fpaths, config_dir)
    if ret:
        log.cl_error("failed to send file %s on local host [%s] to "
                     "[%s] on host [%s]",
                     local_hostname, config_fpaths,
                     config_dir,
                     test_host.sh_hostname)
        return -1, None
    return 0, iso_path


def coral_install_command(log, workspace, test_host, local_iso_path,
                          config_fpaths, install_command,
                          config_dir=constant.ETC_CORAL_DIR):
    """
    Run the install command from ISO
    """
    # pylint: disable=too-many-arguments,too-many-branches
    ret, iso_path = coral_reinstall(log, workspace, test_host, local_iso_path,
                                    config_fpaths, config_dir=config_dir)
    if ret:
        log.cl_error("failed to install RPMs and config from ISO")
        return -1

    command = install_command
    if iso_path is not None:
        command += " --iso " + iso_path

    log.cl_info("running command [%s] on host [%s]",
                command, test_host.sh_hostname)
    retval = test_host.sh_run(log, command, timeout=None)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = %d, stdout = [%s], stderr = [%s]",
                     command, test_host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return 0


def parse_testlist_substring(log, list_string):
    """
    Return a list of names
    Examples:
    # Invalid: test*, no closing multiplication
    # Invalid: *, no test name before multiplication
    # Invalid: *10, no test name before multiplication
    # Invalid: test**10, not a number after multiplication
    # Invalid: test*A, not a number after multiplication
    # Invalid: empty string
    """
    # pylint: disable=unused-variable
    if len(list_string) == 0:
        return None
    multiply = list_string.find("*")
    if multiply == -1:
        return [list_string]
    if multiply == 0:
        log.cl_error("invalid [%s]: no test name before multiplication",
                     list_string)
        return None
    testname = list_string[:multiply]
    if multiply + 1 >= len(list_string):
        log.cl_error("invalid [%s]: no closing multiplication",
                     list_string)
        return None

    repeat_string = list_string[multiply + 1:]
    try:
        repeat_number = int(repeat_string, 10)
    except:
        log.cl_error("invalid [%s]: not a number after multiplication",
                     list_string)
        return None

    if repeat_number < 0:
        log.cl_error("invalid [%s]: negative number",
                     list_string)
        return None

    if repeat_number == 0:
        log.cl_error("invalid [%s]: zero number",
                     list_string)
        return None

    testnames = []
    for repeat in range(repeat_number):
        testnames.append(testname)
    return testnames


def parse_testlist_string(log, list_string):
    """
    Return a list of tests.
    The list string contains substring seperated by comma.
    Each substring need to be able parsed by parse_testlist_substring().
    """
    substrings = list_string.split(",")
    names = []
    for substring in substrings:
        sub_names = parse_testlist_substring(log, substring)
        if sub_names is None:
            log.cl_error("testlist string [%s] is invalid",
                         list_string)
            return None

        names += sub_names
    return names


def parse_testlist_argument(log, argument):
    """
    Return field names.
    """
    # pylint: disable=too-many-branches,too-many-arguments
    if argument is None:
        return 0, None
    if isinstance(argument, tuple):
        substrings = list(argument)
        test_names = []
        for substring in substrings:
            new_names = parse_testlist_substring(log, substring)
            if new_names is None:
                log.cl_error("failed to parse test list [%s]", substring)
                return -1, None
            test_names += new_names
        return 0, test_names
    if isinstance(argument, str):
        if argument == "":
            return 0, []
        test_names = []
        substrings = argument.split(",")
        for substring in substrings:
            new_names = parse_testlist_substring(log, substring)
            if new_names is None:
                log.cl_error("failed to parse test list [%s]", substring)
                return -1, None
            test_names += new_names
        return 0, test_names
    log.cl_error("invalid test list type [%s]",
                 type(argument).__name__)
    return -1, None
