"""
Barreleye is a performance monitoring system for Lustre
"""
from fire import Fire
from pycoral import parallel
from pycoral import cmd_general
from pycoral import version
from pycoral import clog
from pycoral import constant
from pycoral import lustre_version
from pycoral import ssh_host
from pybarrele import barrele_instance
from pybarrele import barrele_constant
from pybarrele import barrele_collectd


def init_env(config_fpath, logdir, log_to_file, iso):
    """
    Init log and instance for commands that needs it
    """
    log_dir_is_default = (logdir == barrele_constant.BARRELE_LOG_DIR)
    log, workspace, barrele_config = cmd_general.init_env(config_fpath,
                                                          logdir,
                                                          log_to_file,
                                                          log_dir_is_default)
    local_host = ssh_host.get_local_host(ssh=False)
    if iso is not None:
        iso = cmd_general.check_argument_fpath(log, local_host, iso)
    barreleye_instance = barrele_instance.barrele_init_instance(log, workspace,
                                                                barrele_config,
                                                                config_fpath,
                                                                log_to_file,
                                                                log_dir_is_default,
                                                                iso)
    if barreleye_instance is None:
        log.cl_error("failed to init Barreleye instance")
        cmd_general.cmd_exit(log, 1)
    return log, barreleye_instance


class BarreleClusterCommand():
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
                                           self._bcc_log_to_file,
                                           self._bcc_iso)
        cmd_general.check_argument_bool(log, "erase_influxdb", erase_influxdb)
        cmd_general.check_argument_bool(log, "drop_database", drop_database)
        rc = barreleye_instance.bei_cluster_install(log,
                                                    erase_influxdb=erase_influxdb,
                                                    drop_database=drop_database)
        cmd_general.cmd_exit(log, rc)


def barrele_version(barrele_command):
    """
    Print Barreleye version on local host and exit.
    """
    # pylint: disable=unused-argument,protected-access
    logdir = barrele_command._bec_logdir
    log_to_file = barrele_command._bec_log_to_file
    simple = True
    if simple:
        logdir_is_default = (logdir == barrele_constant.BARRELE_LOG_DIR)
        log, _ = cmd_general.init_env_noconfig(logdir, log_to_file,
                                               logdir_is_default)
        cmd_general.print_field(log, constant.TITLE_CURRENT_RELEASE,
                                version.CORAL_VERSION)
        cmd_general.cmd_exit(log, 0)


def lustre_version_field(log, lversion, field_name):
    """
    Return (0, result) for a field of LustreVersion
    """
    # pylint: disable=unused-argument
    ret = 0
    if field_name == barrele_constant.BARRELE_FIELD_LUSTRE_VERSION:
        result = lversion.lv_name
    else:
        log.cl_error("unknown field [%s] of Lustre version", field_name)
        result = clog.ERROR_MSG
        ret = -1

    return ret, result


def print_lustre_versions(log, lustre_versions, status=False,
                          print_table=True, field_string=None):
    """
    Print table of BarreleAgent.
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if not print_table and len(lustre_versions) > 1:
        log.cl_error("failed to print non-table output with multiple "
                     "Lustre versions")
        return -1

    quick_fields = [barrele_constant.BARRELE_FIELD_LUSTRE_VERSION]
    slow_fields = []
    none_table_fields = []
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for Lustre version",
                     field_string)
        return -1

    rc = cmd_general.print_list(log, lustre_versions, quick_fields,
                                slow_fields, none_table_fields,
                                lustre_version_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    return rc


def barrele_lustre_versions(barrele_command):
    """
    Print Lustres versions supported by Barreleye.

    The Lustre version name can be used in "lustre_fallback_version" of
    /etc/coral/barreleye.conf.
    """
    # pylint: disable=unused-argument,protected-access
    logdir = barrele_command._bec_logdir
    log_to_file = barrele_command._bec_log_to_file
    logdir_is_default = (logdir == barrele_constant.BARRELE_LOG_DIR)
    log, _ = cmd_general.init_env_noconfig(logdir, log_to_file,
                                           logdir_is_default)
    lustre_versions = []
    for lversion in lustre_version.LUSTRE_VERSION_DICT.values():
        fname = barrele_collectd.lustre_version_xml_fname(log, lversion, quiet=True)
        if fname is None:
            continue
        lustre_versions.append(lversion)
    ret = print_lustre_versions(log, lustre_versions)
    cmd_general.cmd_exit(log, ret)


class BarreleAgentStatusCache():
    """
    This object saves temporary status of a Barreleye agent.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, barreleye_instance, agent):
        # Instance
        self.basc_barreleye_instance = barreleye_instance
        # Any failure when getting this cached status
        self.basc_failed = False
        # BarreleAgent
        self.basc_agent = agent
        # Whether host is up
        self._basc_is_up = None
        # Whether the host is running Collectd. Negative on error.
        self.basc_running_collectd = None
        # The Collectd version. clog.ERROR_MSG on error.
        self.basc_collectd_version = None

    def basc_is_up(self, log):
        """
        Return whether the host is up
        """
        if self._basc_is_up is None:
            self._basc_is_up = self.basc_agent.bea_host.sh_is_up(log)
            if not self._basc_is_up:
                self.basc_failed = True
        return self._basc_is_up

    def basc_init_collectd_up(self, log):
        """
        Init the status of whether collectd is up
        """
        if not self.basc_is_up(log):
            self.basc_running_collectd = -1
            return
        self.basc_running_collectd = \
            self.basc_agent.bea_collectd_running(log)
        if self.basc_running_collectd < 0:
            self.basc_failed = True

    def basc_init_collectd_version(self, log):
        """
        Init the status of collectd version
        """
        if not self.basc_is_up(log):
            self.basc_collectd_version = clog.ERROR_MSG
            return
        collectd_version = self.basc_agent.bea_collectd_version(log)
        if collectd_version is None:
            self.basc_collectd_version = clog.ERROR_MSG
            self.basc_failed = True
        else:
            self.basc_collectd_version = collectd_version

    def basc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=no-self-use
        for field in field_names:
            if field == barrele_constant.BARRELE_FIELD_HOST:
                continue
            if field == barrele_constant.BARRELE_FIELD_UP:
                self.basc_is_up(log)
            elif field == barrele_constant.BARRELE_FIELD_COLLECTD:
                self.basc_init_collectd_up(log)
            elif field == barrele_constant.BARRELE_FIELD_COLLECTD_VERSION:
                self.basc_init_collectd_version(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def basc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches
        agent = self.basc_agent
        hostname = agent.bea_host.sh_hostname
        ret = 0
        if field_name == barrele_constant.BARRELE_FIELD_HOST:
            result = hostname
        elif field_name == barrele_constant.BARRELE_FIELD_UP:
            if self._basc_is_up is None:
                log.cl_error("up status of host [%s] is not inited",
                             hostname)
                ret = -1
                result = clog.ERROR_MSG
            elif self._basc_is_up:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               barrele_constant.BARRELE_AGENT_UP)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               barrele_constant.BARRELE_AGENT_DOWN)
        elif field_name == barrele_constant.BARRELE_FIELD_COLLECTD:
            if self.basc_running_collectd is None:
                log.cl_error("Collectd status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.basc_running_collectd < 0:
                result = clog.ERROR_MSG
            elif self.basc_running_collectd:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               barrele_constant.BARRELE_AGENT_ACTIVE)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               barrele_constant.BARRELE_AGENT_INACTIVE)
        elif field_name == barrele_constant.BARRELE_FIELD_COLLECTD_VERSION:
            if self.basc_collectd_version is None:
                log.cl_error("Collectd version of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            else:
                result = self.basc_collectd_version
        else:
            log.cl_error("unknown field [%s] of agent", field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.basc_failed:
            ret = -1
        return ret, result

    def basc_can_skip_init_fields(self, field_names):
        """
        Whether basc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(barrele_constant.BARRELE_FIELD_HOST)
        if len(fields) == 0:
            return True
        return False


def agent_status_init(log, workspace, agent_status, field_names):
    """
    Init status of a agent
    """
    # pylint: disable=unused-argument
    return agent_status.basc_init_fields(log, field_names)


def agent_status_field(log, agent_status, field_name):
    """
    Return (0, result) for a field of BarreleAgentStatusCache
    """
    # pylint: disable=unused-argument
    return agent_status.basc_field_result(log, field_name)


def print_agents(log, barreleye_instance, agents, status=False,
                 print_table=True, field_string=None):
    """
    Print table of BarreleAgent.
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if not print_table and len(agents) > 1:
        log.cl_error("failed to print non-table output with multiple "
                     "agents")
        return -1

    quick_fields = [barrele_constant.BARRELE_FIELD_HOST]
    slow_fields = [barrele_constant.BARRELE_FIELD_UP,
                   barrele_constant.BARRELE_FIELD_COLLECTD,
                   barrele_constant.BARRELE_FIELD_COLLECTD_VERSION]
    none_table_fields = []
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for agent",
                     field_string)
        return -1

    agent_status_list = []
    for agent in agents:
        agent_status = BarreleAgentStatusCache(barreleye_instance,
                                               agent)
        agent_status_list.append(agent_status)

    if (len(agent_status_list) > 0 and
            not agent_status_list[0].basc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for agent_status in agent_status_list:
            args = (agent_status, field_names)
            args_array.append(args)
            agent = agent_status.basc_agent
            hostname = agent.bea_host.sh_hostname
            thread_id = "agent_status_%s" % hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(barreleye_instance.bei_workspace,
                                                    "agent_status",
                                                    agent_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for agents",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, agent_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                agent_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    return rc


class BarreleServerStatusCache():
    """
    This object saves temporary status of a Barreleye server.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, barreleye_instance, server):
        # Instance
        self.bssc_barreleye_instance = barreleye_instance
        # Any failure when getting this cached status
        self.bssc_failed = False
        # BarreleServer
        self.bssc_server = server
        # Whether host is up
        self._bssc_is_up = None
        # Whether the host is running Influxdb. Negative on error.
        self.bssc_running_influxdb = None
        # Whether the host is running Grafana. Negative on error.
        self.bssc_running_grafana = None
        # The version of Grafana
        self.bssc_grafana_version = None
        # The version of Influxdb
        self.bssc_influxdb_version = None

    def bssc_is_up(self, log):
        """
        Return whether the host is up
        """
        if self._bssc_is_up is None:
            self._bssc_is_up = self.bssc_server.bes_server_host.sh_is_up(log)
            if not self._bssc_is_up:
                self.bssc_failed = True
        return self._bssc_is_up

    def bssc_init_grafana_up(self, log):
        """
        Init the status of whether Grafana is up
        """
        if not self.bssc_is_up(log):
            self.bssc_running_grafana = -1
            return
        self.bssc_running_grafana = \
            self.bssc_server.bes_grafana_running(log)
        if self.bssc_running_grafana < 0:
            self.bssc_failed = True

    def bssc_init_influxdb_up(self, log):
        """
        Init the status of whether Influxdb is up
        """
        if not self.bssc_is_up(log):
            self.bssc_running_influxdb = -1
            return
        self.bssc_running_influxdb = \
            self.bssc_server.bes_influxdb_running(log)
        if self.bssc_running_influxdb < 0:
            self.bssc_failed = True

    def bssc_init_influxdb_version(self, log):
        """
        Init the status of influxdb version
        """
        if not self.bssc_is_up(log):
            self.bssc_influxdb_version = clog.ERROR_MSG
            return
        influxdb_version = self.bssc_server.bes_influxdb_version(log)
        if influxdb_version is None:
            self.bssc_influxdb_version = clog.ERROR_MSG
            self.bssc_failed = True
        else:
            self.bssc_influxdb_version = influxdb_version

    def bssc_init_grafana_version(self, log):
        """
        Init the status of grafana version
        """
        if not self.bssc_is_up(log):
            self.bssc_grafana_version = clog.ERROR_MSG
            return
        grafana_version = self.bssc_server.bes_grafana_version(log)
        if grafana_version is None:
            self.bssc_grafana_version = clog.ERROR_MSG
            self.bssc_failed = True
        else:
            self.bssc_grafana_version = grafana_version

    def bssc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=no-self-use
        for field in field_names:
            if field == barrele_constant.BARRELE_FIELD_HOST:
                continue
            if field == barrele_constant.BARRELE_FIELD_UP:
                self.bssc_is_up(log)
            elif field == barrele_constant.BARRELE_FIELD_GRAFANA:
                self.bssc_init_grafana_up(log)
            elif field == barrele_constant.BARRELE_FIELD_INFLUXDB:
                self.bssc_init_influxdb_up(log)
            elif field == barrele_constant.BARRELE_FIELD_GRAFANA_VERSION:
                self.bssc_init_grafana_version(log)
            elif field == barrele_constant.BARRELE_FIELD_INFLUXDB_VERSION:
                self.bssc_init_influxdb_version(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def bssc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        server = self.bssc_server
        hostname = server.bes_server_host.sh_hostname
        ret = 0
        if field_name == barrele_constant.BARRELE_FIELD_HOST:
            result = hostname
        elif field_name == barrele_constant.BARRELE_FIELD_UP:
            if self._bssc_is_up is None:
                log.cl_error("up status of host [%s] is not inited",
                             hostname)
                ret = -1
                result = clog.ERROR_MSG
            elif self._bssc_is_up:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               barrele_constant.BARRELE_AGENT_UP)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               barrele_constant.BARRELE_AGENT_DOWN)
        elif field_name == barrele_constant.BARRELE_FIELD_GRAFANA:
            if self.bssc_running_grafana is None:
                log.cl_error("Grafana status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.bssc_running_grafana < 0:
                result = clog.ERROR_MSG
            elif self.bssc_running_grafana:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               barrele_constant.BARRELE_AGENT_RUNNING)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               barrele_constant.BARRELE_AGENT_STOPPED)
        elif field_name == barrele_constant.BARRELE_FIELD_INFLUXDB:
            if self.bssc_running_influxdb is None:
                log.cl_error("Influxdb status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.bssc_running_influxdb < 0:
                result = clog.ERROR_MSG
            elif self.bssc_running_influxdb:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               barrele_constant.BARRELE_AGENT_ACTIVE)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               barrele_constant.BARRELE_AGENT_INACTIVE)

        elif field_name == barrele_constant.BARRELE_FIELD_INFLUXDB_VERSION:
            if self.bssc_influxdb_version is None:
                log.cl_error("Influxdb version of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            else:
                result = self.bssc_influxdb_version
        elif field_name == barrele_constant.BARRELE_FIELD_GRAFANA_VERSION:
            if self.bssc_grafana_version is None:
                log.cl_error("Grafana version of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            else:
                result = self.bssc_grafana_version
        else:
            log.cl_error("unknown field [%s] of server", field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.bssc_failed:
            ret = -1
        return ret, result

    def bssc_can_skip_init_fields(self, field_names):
        """
        Whether basc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(barrele_constant.BARRELE_FIELD_HOST)
        if len(fields) == 0:
            return True
        return False


def server_status_init(log, workspace, server_status, field_names):
    """
    Init status of a server
    """
    # pylint: disable=unused-argument
    return server_status.bssc_init_fields(log, field_names)


def server_status_field(log, server_status, field_name):
    """
    Return (0, result) for a field of BarreleServerStatusCache
    """
    # pylint: disable=unused-argument
    return server_status.bssc_field_result(log, field_name)


def print_servers(log, barreleye_instance, servers, status=False,
                  print_table=True, field_string=None):
    """
    Print table of BarreleServer.
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if not print_table and len(servers) > 1:
        log.cl_error("failed to print non-table output with multiple "
                     "servers")
        return -1

    quick_fields = [barrele_constant.BARRELE_FIELD_HOST]
    slow_fields = [barrele_constant.BARRELE_FIELD_UP,
                   barrele_constant.BARRELE_FIELD_INFLUXDB,
                   barrele_constant.BARRELE_FIELD_GRAFANA]
    none_table_fields = [barrele_constant.BARRELE_FIELD_INFLUXDB_VERSION,
                         barrele_constant.BARRELE_FIELD_GRAFANA_VERSION]
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for server",
                     field_string)
        return -1

    server_status_list = []
    for server in servers:
        server_status = BarreleServerStatusCache(barreleye_instance,
                                                 server)
        server_status_list.append(server_status)

    if (len(server_status_list) > 0 and
            not server_status_list[0].bssc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for server_status in server_status_list:
            args = (server_status, field_names)
            args_array.append(args)
            server = server_status.bssc_server
            hostname = server.bes_server_host.sh_hostname
            thread_id = "server_status_%s" % hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(barreleye_instance.bei_workspace,
                                                    "server_status",
                                                    server_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for servers",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, server_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                server_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    return rc


class BarreleServerCommand():
    """
    Commands to manage a Barreleye server.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._bsc_config_fpath = config
        self._bsc_logdir = logdir
        self._bsc_log_to_file = log_to_file
        self._bsc_iso = iso

    def status(self):
        """
        Print the status of the Barreleye server.
        """
        log, barreleye_instance = init_env(self._bsc_config_fpath,
                                           self._bsc_logdir,
                                           self._bsc_log_to_file,
                                           self._bsc_iso)
        server = barreleye_instance.bei_barreleye_server
        servers = [server]
        ret = print_servers(log, barreleye_instance, servers, status=True,
                            print_table=False)
        cmd_general.cmd_exit(log, ret)

    def host(self):
        """
        Print the hostname of the Barreleye server.
        """
        log, barreleye_instance = init_env(self._bsc_config_fpath,
                                           self._bsc_logdir,
                                           self._bsc_log_to_file,
                                           self._bsc_iso)
        server = barreleye_instance.bei_barreleye_server
        log.cl_stdout(server.bes_server_host.sh_hostname)
        cmd_general.cmd_exit(log, 0)


class BarreleAgentCommand():
    """
    Commands to manage a Barreleye agent.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._bac_config_fpath = config
        self._bac_logdir = logdir
        self._bac_log_to_file = log_to_file
        self._bac_iso = iso

    def ls(self, status=False):
        """
        List all Barreleye agents.
        :param status: print the status of agents, default: False.
        """
        log, barreleye_instance = init_env(self._bac_config_fpath,
                                           self._bac_logdir,
                                           self._bac_log_to_file,
                                           self._bac_iso)
        cmd_general.check_argument_bool(log, "status", status)
        agents = list(barreleye_instance.bei_agent_dict.values())
        ret = print_agents(log, barreleye_instance, agents, status=status)
        cmd_general.cmd_exit(log, ret)

    def status(self, host):
        """
        Print the status of a agent host.
        :param host: the name of the agent host.
        """
        log, barreleye_instance = init_env(self._bac_config_fpath,
                                           self._bac_logdir,
                                           self._bac_log_to_file,
                                           self._bac_iso)
        host = cmd_general.check_argument_str(log, "host", host)
        if host not in barreleye_instance.bei_agent_dict:
            log.cl_error("host [%s] is not configured as Barreleye agent",
                         host)
            cmd_general.cmd_exit(log, -1)
        agent = barreleye_instance.bei_agent_dict[host]
        agents = [agent]
        ret = print_agents(log, barreleye_instance, agents, status=True,
                           print_table=False)
        cmd_general.cmd_exit(log, ret)

    def start(self, host):
        """
        Start Collectd service on the agent host.
        :param host: the name of the agent host. Could be a list.
        """
        log, barreleye_instance = init_env(self._bac_config_fpath,
                                           self._bac_logdir,
                                           self._bac_log_to_file,
                                           self._bac_iso)
        host = cmd_general.check_argument_str(log, "host", host)

        hostnames = cmd_general.parse_list_string(log, host)
        if hostnames is None:
            log.cl_error("host list [%s] is invalid",
                         host)
            cmd_general.cmd_exit(log, -1)
        ret = barreleye_instance.bei_start_agents(log, hostnames)
        cmd_general.cmd_exit(log, ret)

    def stop(self, host):
        """
        Stop Collectd service on the agent host.
        :param host: the name of the agent host. Could be a list.
        """
        log, barreleye_instance = init_env(self._bac_config_fpath,
                                           self._bac_logdir,
                                           self._bac_log_to_file,
                                           self._bac_iso)
        host = cmd_general.check_argument_str(log, "host", host)

        hostnames = cmd_general.parse_list_string(log, host)
        if hostnames is None:
            log.cl_error("host list [%s] is invalid",
                         host)
            cmd_general.cmd_exit(log, -1)
        ret = barreleye_instance.bei_stop_agents(log, hostnames)
        cmd_general.cmd_exit(log, ret)


class BarreleCommand():
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
    version = barrele_version
    agent = BarreleAgentCommand()
    server = BarreleServerCommand()
    lustre_versions = barrele_lustre_versions

    def __init__(self, config=barrele_constant.BARRELE_CONFIG,
                 log=barrele_constant.BARRELE_LOG_DIR,
                 debug=False,
                 iso=None):
        # pylint: disable=protected-access,unused-argument
        self._bec_config_fpath = config
        self._bec_logdir = log
        self._bec_log_to_file = debug
        self._bec_iso = iso
        self.cluster._init(config, log, debug, iso)
        self.agent._init(config, log, debug, iso)
        self.server._init(config, log, debug, iso)


def main():
    """
    Main routine of Barreleye commands
    """
    Fire(BarreleCommand)
