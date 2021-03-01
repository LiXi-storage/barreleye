"""
Test Library for Barreleye
"""
# pylint: disable=too-many-lines
from pycoral import cmd_general
from pycoral import test_common

ISO_PATH = None
BARRELEYE_TESTS = []


def basic(log, workspace, cinstance, install_server):
    """
    Install Barreleye in the cluster.
    """
    # pylint: disable=too-many-statements,too-many-branches
    log.cl_info("installing Barreleye on installation host [%s]",
                install_server.sh_hostname)

    ret = test_common.coral_install_command(log, workspace, install_server,
                                            ISO_PATH,
                                            [cinstance.bei_config_fpath],
                                            "barrele cluster install")
    if ret:
        log.cl_error("failed to install Barreleye on host [%s]",
                     install_server.sh_hostname)
        return -1

    return 0


BARRELEYE_TESTS.append(basic)


def barreleye_test(log, workspace, binstance, install_server,
                   only_tests, first_tests, iso_path,
                   reverse_order, start, stop):
    """
    Run test
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-arguments
    # pylint: disable=too-many-statements,global-statement
    global ISO_PATH
    ISO_PATH = iso_path

    ret = cmd_general.run_test(log, workspace, only_tests, first_tests,
                               binstance.bei_local_host, reverse_order,
                               start, stop, BARRELEYE_TESTS,
                               (binstance, install_server))
    if ret:
        log.cl_error("failed to run Barreleye tests")
    return ret
