"""
Test Library for Barreleye

Each test case can assume following things (except basic test) when starting,
and it should make sure the correctness of these assumptions before finishing:

1) Barreleye and its dependency is properly installed.

2) Barreleye RPM is installed already on the installation server. Test basic
can assume this too.

"""
# pylint: disable=too-many-lines
from pycoral import cmd_general
from pycoral import test_common
from pybarreleye import barrele_constant


CORAL_BARRELE_RPM_PREFIX = "coral-barreleye-"

ISO_PATH = None
BARRELEYE_TESTS = []


def basic(log, workspace, binstance, install_server):
    """
    Install Barreleye in the cluster.
    """
    log.cl_info("installing Barreleye on installation host [%s]",
                install_server.sh_hostname)

    ret = test_common.coral_install_command(log, workspace, install_server,
                                            ISO_PATH,
                                            [binstance.bei_config_fpath],
                                            "barrele cluster install")
    if ret:
        log.cl_error("failed to install Barreleye on host [%s]",
                     install_server.sh_hostname)
        return -1

    return 0


BARRELEYE_TESTS.append(basic)


def version_test(log, workspace, binstance, install_server):
    """
    Check that "barrele version" prints the same version with the RPM name
    """
    # pylint: disable=unused-argument
    command = "rpm -qa | grep %s" % CORAL_BARRELE_RPM_PREFIX
    retval = install_server.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = %d, stdout = [%s], stderr = [%s]",
                     command, install_server.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    lines = retval.cr_stdout.splitlines()
    if len(lines) != 1:
        log.cl_error("unexpected output [%s] of command [%s] on host [%s]",
                     retval.cr_stdout, command, install_server.sh_hostname)
        return -1

    rpm_name = lines[0]
    if not rpm_name.startswith(CORAL_BARRELE_RPM_PREFIX):
        log.cl_error("unexpected RPM name [%s] of coral-barreleye on "
                     "host [%s], should starts with [%s]",
                     rpm_name, install_server.sh_hostname,
                     CORAL_BARRELE_RPM_PREFIX)
        return -1

    rpm_version_str = rpm_name[len(CORAL_BARRELE_RPM_PREFIX):]
    split_index = rpm_version_str.find("-")
    if split_index < 0:
        log.cl_error("unexpected version string [%s] of coral-barreleye on "
                     "host [%s], should have [-]",
                     rpm_version_str, install_server.sh_hostname)
        return -1

    rpm_version_str = rpm_version_str[:split_index]

    command = "barrele version"
    retval = install_server.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = %d, stdout = [%s], stderr = [%s]",
                     command, install_server.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    lines = retval.cr_stdout.splitlines()
    if len(lines) != 1:
        log.cl_error("unexpected output [%s] of command [%s] on host [%s]",
                     retval.cr_stdout, command, install_server.sh_hostname)
        return -1
    version_str = lines[0]
    if rpm_version_str != version_str:
        log.cl_error("RPM version [%s] is inconsistent with version [%s] "
                     "printed by [barrele version] on host [%s]",
                     rpm_version_str, version_str, install_server.sh_hostname)
        return -1
    return 0


BARRELEYE_TESTS.append(version_test)


def get_agents_field(log, host, field_number):
    """
    Return a dict for a given field of "barrele agent ls --status".
    Key is hostname.
    Value is the field value.
    """
    command = "barrele agent ls --status"
    return cmd_general.get_table_field(log, host, field_number, command)


# Hostname of the agent
AGENT_HOST = 0
# Whether the host is up
AGENT_HOST_UP = 1
# Whether Collectd is running
AGENT_COLLECTD = 2


def get_agent_up_dict(log, host):
    """
    Return a dict for up status of "barrele agent ls --status".
    Key is hostname.
    Value is the field value.
    """
    return get_agents_field(log, host,
                            AGENT_HOST_UP)


def get_agent_collectd_dict(log, host):
    """
    Return a dict for Collectd status of "barrele agent ls --status".
    Key is hostname.
    Value is the field value.
    """
    return get_agents_field(log, host,
                            AGENT_COLLECTD)


def agent_collectd_status_check(log, binstance, install_server,
                                inactive_hostnames):
    """
    Check the Collectd status is expected
    """
    agent_collectd_dict = get_agent_collectd_dict(log, install_server)
    if agent_collectd_dict is None:
        log.cl_error("failed to get the Collectd dict of agents")
        return -1

    for agent in binstance.bei_agent_dict.values():
        hostname = agent.bea_host.sh_hostname
        if hostname not in agent_collectd_dict:
            log.cl_error("failed to find host [%s] in the up dict",
                         hostname)
            return -1
        if hostname in inactive_hostnames:
            expect_status = barrele_constant.BARRELE_AGENT_INACTIVE
        else:
            expect_status = barrele_constant.BARRELE_AGENT_ACTIVE

        if agent_collectd_dict[hostname] != expect_status:
            log.cl_error("unexpected status [%s] of host [%s], expected [%s]",
                         agent_collectd_dict[hostname], hostname, expect_status)
            return -1

    return 0


def get_agent_status_dict(log, host, hostname):
    """
    Return status dict of an agent host
    """
    return cmd_general.get_status_dict(log, host,
                                       "barrele agent status %s" % hostname)


def get_agent_up_status(log, host, hostname):
    """
    Return the up status of an agent host
    """
    status_dict = get_agent_status_dict(log, host, hostname)
    if status_dict is None:
        log.cl_error("failed to get status of agent host [%s]",
                     hostname)
        return None

    if barrele_constant.BARRELE_FIELD_UP not in status_dict:
        log.cl_error("no [Up] in [barrele agent status %s] output "
                     "on host [%s]", hostname, host.sh_hostname)
        return None

    return status_dict[barrele_constant.BARRELE_FIELD_UP]


def get_agent_collectd_status(log, host, hostname):
    """
    Return the collectd status of an agent host
    """
    status_dict = get_agent_status_dict(log, host, hostname)
    if status_dict is None:
        log.cl_error("failed to get status of agent host [%s]",
                     hostname)
        return None

    if barrele_constant.BARRELE_FIELD_COLLECTD not in status_dict:
        log.cl_error("no [%s] in [barrele agent status %s] output "
                     "on host [%s]", barrele_constant.BARRELE_FIELD_COLLECTD,
                     hostname, host.sh_hostname)
        return None

    return status_dict[barrele_constant.BARRELE_FIELD_COLLECTD]


def check_agent_status(log, host, hostname,
                       up=barrele_constant.BARRELE_AGENT_UP,
                       collectd=barrele_constant.BARRELE_AGENT_ACTIVE):
    """
    Check the status of an agent
    """
    status_dict = get_agent_status_dict(log, host, hostname)
    if status_dict is None:
        log.cl_error("failed to get status of agent host [%s]",
                     hostname)
        return -1

    if barrele_constant.BARRELE_FIELD_UP not in status_dict:
        log.cl_error("no [%s] in [barrele agent status %s] output "
                     "on host [%s]", barrele_constant.BARRELE_FIELD_UP,
                     hostname, host.sh_hostname)
        return -1

    if status_dict[barrele_constant.BARRELE_FIELD_UP] != up:
        log.cl_error("unexpected value [%s] of [%s] in "
                     "[barrele agent status %s] output on host [%s], "
                     "expected [%s]",
                     status_dict[barrele_constant.BARRELE_FIELD_UP],
                     barrele_constant.BARRELE_FIELD_UP, hostname,
                     host.sh_hostname, up)
        return -1

    if barrele_constant.BARRELE_FIELD_COLLECTD not in status_dict:
        log.cl_error("no [%s] in [barrele agent status %s] output "
                     "on host [%s]", barrele_constant.BARRELE_FIELD_COLLECTD,
                     hostname, host.sh_hostname)
        return -1

    if status_dict[barrele_constant.BARRELE_FIELD_COLLECTD] != collectd:
        log.cl_error("unexpected value [%s] of [%s] in "
                     "[barrele agent status %s] output on host [%s], "
                     "expected [%s]",
                     status_dict[barrele_constant.BARRELE_FIELD_COLLECTD],
                     barrele_constant.BARRELE_FIELD_COLLECTD, hostname,
                     host.sh_hostname, collectd)
        return -1
    return 0


def agent_status_test(log, workspace, binstance, install_server):
    """
    Check "barrele agent ls --status", "barrele agent status" and
    "barrele agent start|stop"
    """
    # pylint: disable=unused-argument,too-many-branches
    agent_up_dict = get_agent_up_dict(log, install_server)
    if agent_up_dict is None:
        log.cl_error("failed to get the up dict of agents")
        return -1

    for agent in binstance.bei_agent_dict.values():
        hostname = agent.bea_host.sh_hostname
        if hostname not in agent_up_dict:
            log.cl_error("failed to find host [%s] in the up dict",
                         hostname)
            return -1
        if agent_up_dict[hostname] != barrele_constant.BARRELE_AGENT_UP:
            log.cl_error("unexpected status [%s] of host [%s], expected [up]",
                         agent_up_dict[hostname], hostname)
            return -1

    inactive_hostnames = []
    ret = agent_collectd_status_check(log, binstance, install_server,
                                      inactive_hostnames)
    if ret:
        log.cl_error("failed to check the status of Collectd")
        return -1

    for agent in binstance.bei_agent_dict.values():
        hostname = agent.bea_host.sh_hostname
        ret = check_agent_status(log, install_server,
                                 hostname,
                                 up=barrele_constant.BARRELE_AGENT_UP,
                                 collectd=barrele_constant.BARRELE_AGENT_ACTIVE)
        if ret:
            log.cl_error("failed to check the status of agent [%s]",
                         hostname)
            return -1

        command = "barrele agent stop %s" % hostname
        retval = install_server.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, install_server.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        inactive_hostnames = [hostname]
        ret = agent_collectd_status_check(log, binstance, install_server,
                                          inactive_hostnames)
        if ret:
            log.cl_error("failed to check the status of Collectd")
            return -1

        ret = check_agent_status(log, install_server,
                                 hostname,
                                 up=barrele_constant.BARRELE_AGENT_UP,
                                 collectd=barrele_constant.BARRELE_AGENT_INACTIVE)
        if ret:
            log.cl_error("failed to check the status of agent [%s]",
                         hostname)
            return -1

        command = "barrele agent start %s" % hostname
        retval = install_server.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, install_server.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        inactive_hostnames = []
        ret = agent_collectd_status_check(log, binstance, install_server,
                                          inactive_hostnames)
        if ret:
            log.cl_error("failed to check the status of Collectd")
            return -1

        ret = check_agent_status(log, install_server,
                                 hostname,
                                 up=barrele_constant.BARRELE_AGENT_UP,
                                 collectd=barrele_constant.BARRELE_AGENT_ACTIVE)
        if ret:
            log.cl_error("failed to check the status of agent [%s]",
                         hostname)
            return -1

    return 0


BARRELEYE_TESTS.append(agent_status_test)


def get_server_status_dict(log, host):
    """
    Return status dict of Barreleye server
    """
    return cmd_general.get_status_dict(log, host,
                                       "barrele server status")


def check_server_status(log, host, hostname,
                        up=barrele_constant.BARRELE_AGENT_UP,
                        influxdb=barrele_constant.BARRELE_AGENT_ACTIVE,
                        grafana=barrele_constant.BARRELE_AGENT_RUNNING):
    """
    Check the status of a server
    """
    # pylint: disable=too-many-function-args
    status_dict = get_server_status_dict(log, host)
    if status_dict is None:
        log.cl_error("failed to get status of Barreleye server")
        return -1

    if barrele_constant.BARRELE_FIELD_HOST not in status_dict:
        log.cl_error("no [%s] in [barrele server status] output "
                     "on host [%s]", barrele_constant.BARRELE_FIELD_HOST,
                     host.sh_hostname)
        return -1

    if status_dict[barrele_constant.BARRELE_FIELD_HOST] != hostname:
        log.cl_error("unexpected value [%s] of [%s] in "
                     "[barrele agent status] output on host [%s], "
                     "expected [%s]",
                     status_dict[barrele_constant.BARRELE_FIELD_HOST],
                     barrele_constant.BARRELE_FIELD_HOST,
                     host.sh_hostname, hostname)
        return -1

    if barrele_constant.BARRELE_FIELD_UP not in status_dict:
        log.cl_error("no [%s] in [barrele server status] output "
                     "on host [%s]", barrele_constant.BARRELE_FIELD_UP,
                     host.sh_hostname)
        return -1

    if status_dict[barrele_constant.BARRELE_FIELD_UP] != up:
        log.cl_error("unexpected value [%s] of [%s] in "
                     "[barrele agent status] output on host [%s], "
                     "expected [%s]",
                     status_dict[barrele_constant.BARRELE_FIELD_UP],
                     barrele_constant.BARRELE_FIELD_UP,
                     host.sh_hostname, up)
        return -1

    if barrele_constant.BARRELE_FIELD_INFLUXDB not in status_dict:
        log.cl_error("no [%s] in [barrele agent status] output "
                     "on host [%s]", barrele_constant.BARRELE_FIELD_INFLUXDB,
                     host.sh_hostname)
        return -1

    if status_dict[barrele_constant.BARRELE_FIELD_INFLUXDB] != influxdb:
        log.cl_error("unexpected value [%s] of [%s] in "
                     "[barrele agent status] output on host [%s], "
                     "expected [%s]",
                     status_dict[barrele_constant.BARRELE_FIELD_INFLUXDB],
                     barrele_constant.BARRELE_FIELD_INFLUXDB,
                     host.sh_hostname, influxdb)
        return -1

    if barrele_constant.BARRELE_FIELD_GRAFANA not in status_dict:
        log.cl_error("no [%s] in [barrele agent status] output "
                     "on host [%s]", barrele_constant.BARRELE_FIELD_GRAFANA,
                     host.sh_hostname)
        return -1

    if status_dict[barrele_constant.BARRELE_FIELD_GRAFANA] != grafana:
        log.cl_error("unexpected value [%s] of [%s] in "
                     "[barrele agent status] output on host [%s], "
                     "expected [%s]",
                     status_dict[barrele_constant.BARRELE_FIELD_GRAFANA],
                     barrele_constant.BARRELE_FIELD_GRAFANA,
                     host.sh_hostname, grafana)
        return -1
    return 0


def server_status_test(log, workspace, binstance, install_server):
    """
    Check "barrele server status"
    """
    # pylint: disable=unused-argument,too-many-branches
    hostname = binstance.bei_barreleye_server.bes_server_host.sh_hostname
    ret = check_server_status(log, install_server, hostname,
                              up=barrele_constant.BARRELE_AGENT_UP,
                              influxdb=barrele_constant.BARRELE_AGENT_ACTIVE,
                              grafana=barrele_constant.BARRELE_AGENT_RUNNING)
    if ret:
        log.cl_error("failed to check the status of Barreleye server")
        return -1
    return 0


BARRELEYE_TESTS.append(server_status_test)


def server_host_test(log, workspace, binstance, install_server):
    """
    Check "barrele server host"
    """
    # pylint: disable=unused-argument,too-many-branches
    command = "barrele server host"
    retval = install_server.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = %d, stdout = [%s], stderr = [%s]",
                     command, install_server.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    hostname = retval.cr_stdout.strip()
    expected_hostname = binstance.bei_barreleye_server.bes_server_host.sh_hostname
    if hostname != expected_hostname:
        log.cl_error("wrong hostname [%s] printed by command [%s] on host [%s], "
                     "expected [%s]", hostname, command,
                     install_server.sh_hostname,
                     expected_hostname)
        return -1
    return 0


BARRELEYE_TESTS.append(server_host_test)


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
