"""
Barreleye is a performance monitoring system for Lustre
"""
import inspect
from pycoral import ssh_host
from pycoral import cmd_general
from pycoral import test_common
from pycoral import version
from pybarreleye import barrele_test
from pybarreleye import barrele_instance
from pybarreleye import barrele_constant
import fire


def init_env(config_fpath, logdir, log_to_file):
    """
    Init log and instance for commands that needs it
    """
    log_dir_is_default = (logdir == barrele_constant.BARRELE_LOG_DIR)
    log, workspace, barrele_config = cmd_general.init_env(config_fpath,
                                                          logdir,
                                                          log_to_file,
                                                          log_dir_is_default)
    barreleye_instance = barrele_instance.barrele_init_instance(log, workspace,
                                                                barrele_config,
                                                                config_fpath,
                                                                log_to_file,
                                                                log_dir_is_default)
    if barreleye_instance is None:
        log.cl_error("failed to init Barreleye instance")
        cmd_general.cmd_exit(log, 1)
    return log, barreleye_instance


class BarreleSelfTestCommand(object):
    """
    Commands to test the functionality of Barreleye itself.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._bstc_config_fpath = config
        self._bstc_logdir = logdir
        self._bstc_log_to_file = log_to_file
        self._bstc_iso = iso

    def run(self, host=None, ssh_identity_file=None, only=None,
            first=None, reverse_order=False, start=None, stop=None):
        """
        Run tests of Barreleye.
        If only is specified together with start/stop, start/stop option will
        be ignored.
        :param host: Host to trigger the test, default: localhost.
        :param ssh_identity_file: The path to the private key to use
        to SSH into the host.
        :param only: Test names to run, seperated by comma.
        :param first: Test names to run first, seperated by comma.
        :param reverse_order: run the test cases in reverse order (the basic
            test case will still be run first). The order in the first test
            cases will not be reversed. The order in the only test cases will
            not be reversed either.
        :param start: The test name to start at (include).
            Default: the first test.
         :param stop: The test name to stop at (include).
             Default: the last test.
        """
        # pylint: disable=too-many-arguments,too-many-branches
        log, barreleye_instance = init_env(self._bstc_config_fpath,
                                           self._bstc_logdir,
                                           self._bstc_log_to_file)
        cmd_general.check_argument_types(log, "only", only,
                                         allow_none=True,
                                         allow_tuple=True, allow_str=True,
                                         allow_bool=False)
        cmd_general.check_argument_types(log, "first", first,
                                         allow_none=True,
                                         allow_tuple=True, allow_str=True,
                                         allow_bool=False)
        cmd_general.check_argument_bool(log, "reverse_order", reverse_order)
        if ssh_identity_file is not None:
            cmd_general.check_argument_str(log, "ssh_identity_file",
                                           ssh_identity_file)
        if host is None:
            install_server = ssh_host.get_local_host()
        else:
            cmd_general.check_argument_str(log, "host", host)
            install_server = ssh_host.SSHHost(host,
                                              identity_file=ssh_identity_file)

        ret, only_test_names = test_common.parse_testlist_argument(log, only)
        if ret:
            log.cl_error("failed to parse arugment [%s] of --only", only)
            cmd_general.cmd_exit(log, -1)

        ret, first_test_names = test_common.parse_testlist_argument(log, first)
        if ret:
            log.cl_error("failed to parse arugment [%s] of --first", first)
            cmd_general.cmd_exit(log, -1)

        rc = barrele_test.barreleye_test(log, self._bstc_logdir,
                                         barreleye_instance,
                                         install_server, only_test_names,
                                         first_test_names, self._bstc_iso,
                                         reverse_order, start, stop)
        cmd_general.cmd_exit(log, rc)

    def ls(self, full=False):
        """
        List test names of Barreleye.
        :param full: Print introductions of the tests.
        """
        log, _ = init_env(self._bstc_config_fpath, self._bstc_logdir,
                          self._bstc_log_to_file)
        cmd_general.check_argument_bool(log, "full", full)
        for test_func in barrele_test.BARRELEYE_TESTS:
            log.cl_stdout("%s", test_func.__name__)
            if full:
                docstring = inspect.getdoc(test_func)
                lines = docstring.splitlines()
                for line in lines:
                    log.cl_stdout("    %s", line)
        cmd_general.cmd_exit(log, 0)


class BarreleClusterCommand(object):
    """
    Commands to manage the whole Barreleye cluster.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._bcc_config_fpath = config
        self._bcc_logdir = logdir
        self._bcc_log_to_file = log_to_file
        self._bcc_iso = iso

    def install(self, erase_influxdb=False, drop_database=False):
        """
        Install the Barreleye packages in the whole cluster.

        All Barreleye RPMs, configuration files will be cleaned up and
        re-configured.
        :param erase_influxdb: Whether to erase all data and metadata of
        the existing Influxdb.
        :param drop_database: Whether to drop the old Influxdb data. This
        will remove all existing data in "barreleye_database" in Influxdb,
        but will not touch other databases if any.
        """
        log, barreleye_instance = init_env(self._bcc_config_fpath,
                                           self._bcc_logdir,
                                           self._bcc_log_to_file)
        cmd_general.check_argument_bool(log, "erase_influxdb", erase_influxdb)
        cmd_general.check_argument_bool(log, "drop_database", drop_database)
        rc = barreleye_instance.bei_cluster_install(log, iso=self._bcc_iso,
                                                    erase_influxdb=erase_influxdb,
                                                    drop_database=drop_database)
        cmd_general.cmd_exit(log, rc)


def barrele_version(barrele_command):
    """
    Print Barreleye version on local host.
    """
    # pylint: disable=unused-argument,
    log, _ = init_env(barrele_command.bc_config_fpath,
                      barrele_command.bc_logdir,
                      barrele_command.bc_log_to_file)
    log.cl_stdout(version.CORAL_VERSION)
    cmd_general.cmd_exit(log, 0)


class BarreleCommand(object):
    """
    The command line utility for Barreleye, a performance monitoring system
    for Lustre file systems.
    :param config: Config file of Barreleye, default: /etc/coral/barreleye.conf
    :param log: Log directory, default: /var/log/coral/barrele/${TIMESTAMP}.
    :param debug: Whether to dump debug logs into files, default: False.
    :param iso: The ISO tarball to use for installation, default: None.
    """
    # pylint: disable=too-few-public-methods
    cluster = BarreleClusterCommand()
    self_test = BarreleSelfTestCommand()
    version = barrele_version

    def __init__(self, config=barrele_constant.BARRELE_CONFIG,
                 log=barrele_constant.BARRELE_LOG_DIR,
                 debug=False,
                 iso=None):
        # pylint: disable=protected-access,unused-argument
        self.bc_config_fpath = config
        self.bc_logdir = log
        self.bc_log_to_file = debug
        self.bc_iso = iso
        if iso is not None:
            cmd_general.check_iso_fpath(iso)
        self.cluster._init(config, log, debug, iso)
        self.self_test._init(config, log, debug, iso)


def main():
    """
    Main routine of Barreleye commands
    """
    fire.Fire(BarreleCommand)
