"""
Library for Barreleye.
Barreleye is a performance monitoring system for Lustre.
"""
from pycoral import utils
from pycoral import lustre_version
from pycoral import constant
from pycoral import install_common
from pycoral import ssh_host
from pycoral import cmd_general
from pybarrele import barrele_constant
from pybarrele import barrele_collectd
from pybarrele import barrele_server
from pybarrele import barrele_agent

# Default collect interval in seconds
BARRELE_COLLECT_INTERVAL = 60
# Default continuous query periods
BARRELE_CONTINUOUS_QUERY_PERIODS = 4
# The Lustre version to use, if the Lustre RPMs installed on the agent(s)
# is not with the supported version.
BARRELE_LUSTRE_FALLBACK_VERSION = lustre_version.LUSTRE_VERSION_NAME_2_12
# Default dir of Barreleye data
BARRELE_DATA_DIR = "/var/log/coral/barreleye_data"


class BarreleInstance():
    """
    This instance saves the global Barreleye information.
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, workspace, config, config_fpath, log_to_file,
                 logdir_is_default, iso_fpath, local_host, collect_interval,
                 continuous_query_periods, jobstat_pattern, lustre_fallback_version,
                 enable_lustre_exp_mdt, enable_lustre_exp_ost, host_dict,
                 agent_dict, barreleye_server):
        # pylint: disable=too-many-locals
        # Log to file for debugging
        self.bei_log_to_file = log_to_file
        # Whether the workspace is generated under default path
        self.bei_logdir_is_default = logdir_is_default
        # Config content
        self.bei_config = config
        # The config fpath that generates this intance
        self.bei_config_fpath = config_fpath
        # Workspace to save log
        self.bei_workspace = workspace
        # Collect interval of data points in seconds
        self.bei_collect_interval = collect_interval
        # Continuous query periods of Influxdb
        self.bei_continuous_query_periods = continuous_query_periods
        # The jobstat pattern configured in Lustre
        self.bei_jobstat_pattern = jobstat_pattern
        # The Lustre version to use, if the Lustre RPMs installed
        # is not with the supported version.
        self.bei_lustre_fallback_version = lustre_fallback_version
        # Whether Barreleye agents collect exp_md_stats_* metrics from Lustre
        # MDT.
        self.bei_enable_lustre_exp_mdt = enable_lustre_exp_mdt
        # Whether Barreleye agents collect exp_ost_stats_* metrics from Lustre
        # OST.
        self.bei_enable_lustre_exp_ost = enable_lustre_exp_ost
        # Diction of host. Key is hostname, value is SSHHost
        self.bei_host_dict = host_dict
        # Diction of agents. Key is hostname, value is BarreleAgent
        self.bei_agent_dict = agent_dict
        # Local host to run commands
        self.bei_local_host = local_host
        # The dir of the ISO
        self.bei_iso_dir = constant.CORAL_ISO_DIR
        # The server of barreleye
        self.bei_barreleye_server = barreleye_server
        # ISO file path
        self.bei_iso_fpath = iso_fpath

    def _bei_get_collectd_package_type_dict(self, log):
        """
        Return a dict. Key is the RPM/deb type, value is the file name.
        """
        packages_dir = self.bei_iso_dir + "/" + constant.BUILD_PACKAGES
        return barrele_collectd.get_collectd_package_type_dict(log,
                                                               self.bei_local_host,
                                                               packages_dir)

    def _bei_cluster_install_rpms(self, log):
        """
        Install RPMs on the cluster
        """

        rpm_type_dict = self._bei_get_collectd_package_type_dict(log)
        if rpm_type_dict is None:
            log.cl_error("failed to get Collectd RPM types")
            return -1

        for rpm_type in (barrele_collectd.LIBCOLLECTDCLIENT_TYPE_NAME,
                         barrele_collectd.COLLECTD_TYPE_NAME):
            if rpm_type not in rpm_type_dict:
                log.cl_error("failed to find Collectd RPM [%s]",
                             rpm_type)
                return -1

        for agent in self.bei_agent_dict.values():
            ret = agent.bea_generate_configs(log, self)
            if ret:
                log.cl_error("failed to generate Barreleye agent configs on host [%s]",
                             agent.bea_host.sh_hostname)
                return -1

        install_cluster = \
            install_common.CoralInstallationCluster(self.bei_workspace,
                                                    self.bei_local_host,
                                                    self.bei_iso_dir)

        need_backup_fpaths = []
        send_fpath_dict = {}
        send_fpath_dict[self.bei_config_fpath] = self.bei_config_fpath
        agent_rpms = constant.CORAL_DEPENDENT_RPMS[:]
        agent_rpms += barrele_constant.BARRELE_AGENT_DEPENDENT_RPMS
        agent_on_server = None
        for agent in self.bei_agent_dict.values():
            if agent.bea_host.sh_hostname == self.bei_barreleye_server.bes_server_host.sh_hostname:
                if agent_on_server is not None:
                    log.cl_error("multiple agents for Barreleye server [%s]",
                                 self.bei_barreleye_server.bes_server_host.sh_hostname)
                    return -1
                agent_on_server = agent
                continue
            rpms = agent_rpms + agent.bea_needed_collectd_rpm_types
            install_cluster.cic_add_hosts([agent.bea_host],
                                          [],
                                          rpms,
                                          send_fpath_dict,
                                          need_backup_fpaths,
                                          coral_reinstall=True)

        server_rpms = constant.CORAL_DEPENDENT_RPMS[:]
        server_rpms += barrele_constant.BARRELE_SERVER_DEPENDENT_RPMS
        if agent_on_server is not None:
            server_rpms += barrele_constant.BARRELE_AGENT_DEPENDENT_RPMS
            server_rpms += agent_on_server.bea_needed_collectd_rpm_types
        install_cluster.cic_add_hosts([self.bei_barreleye_server.bes_server_host],
                                      [],
                                      server_rpms,
                                      send_fpath_dict,
                                      need_backup_fpaths,
                                      coral_reinstall=True)
        ret = install_cluster.cic_install(log)
        if ret:
            log.cl_error("failed to install dependent RPMs on all hosts of "
                         "the cluster")
            return -1
        return 0

    def bei_cluster_install(self, log, erase_influxdb=False,
                            drop_database=False):
        """
        Install Barrele on all host (could include localhost).
        """
        # Gives a little bit time for canceling the command
        iso = self.bei_iso_fpath
        if erase_influxdb:
            log.cl_warning("data and metadata of Influxdb on host [%s] "
                           "will be all erased",
                           self.bei_barreleye_server.bes_server_host.sh_hostname)
        if drop_database:
            log.cl_warning("database [%s] of Influxdb on host [%s] will be "
                           "dropped",
                           barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME,
                           self.bei_barreleye_server.bes_server_host.sh_hostname)
        if iso is not None:
            ret = install_common.sync_iso_dir(log, self.bei_workspace,
                                              self.bei_local_host, iso,
                                              self.bei_iso_dir)
            if ret:
                log.cl_error("failed to sync ISO files from [%s] to dir [%s] "
                             "on local host [%s]",
                             iso, self.bei_iso_dir,
                             self.bei_local_host.sh_hostname)
                return -1

        ret = self._bei_cluster_install_rpms(log)
        if ret:
            log.cl_error("failed to install RPMs in the cluster")
            return -1

        server = self.bei_barreleye_server
        ret = server.bes_server_reinstall(log, self,
                                          erase_influxdb=erase_influxdb,
                                          drop_database=drop_database)
        if ret:
            log.cl_error("failed to reinstall Barreleye server")
            return -1

        for agent in self.bei_agent_dict.values():
            ret = agent.bea_config_agent(log, self)
            if ret:
                log.cl_error("failed to configure Barreleye agent")
                return -1

        log.cl_info("URL of the dashboards is [%s]",
                    server.bes_grafana_url())
        log.cl_info("please login by [%s:%s] for viewing",
                    server.bes_grafana_viewer_login,
                    server.bes_grafana_viewer_password)
        log.cl_info("please login by [%s:%s] for administrating",
                    server.bes_grafana_admin_login,
                    server.bes_grafana_admin_password)
        return 0

    def bei_stop_agents(self, log, hostnames):
        """
        Stop agents
        """
        for hostname in hostnames:
            if hostname not in self.bei_agent_dict:
                log.cl_error("host [%s] is not configured as Barreleye agent",
                             hostname)
                return -1
            agent = self.bei_agent_dict[hostname]
            ret = agent.bea_collectd_stop(log)
            if ret:
                log.cl_error("failed to stop Barreleye agent on host [%s]",
                             hostname)
                return -1
        return 0

    def bei_start_agents(self, log, hostnames):
        """
        Start agents
        """
        for hostname in hostnames:
            if hostname not in self.bei_agent_dict:
                log.cl_error("host [%s] is not configured as Barreleye agent",
                             hostname)
                return -1
            agent = self.bei_agent_dict[hostname]
            ret = agent.bea_collectd_start(log)
            if ret:
                log.cl_error("failed to start Barreleye agent on host [%s]",
                             hostname)
                return -1
        return 0

    def bei_install_agent_locally(self, log):
        """
        Install Barreleye agent on local host.
        """
        distro = self.bei_local_host.sh_distro(log)
        if distro in (ssh_host.DISTRO_RHEL7, ssh_host.DISTRO_RHEL8):
            log.cl_error("please install agent on host [%s] by using [barrele cluster install] "
                         "command",
                         self.bei_local_host.sh_hostname)
            return -1
        if distro not in (ssh_host.DISTRO_UBUNTU2204):
            log.cl_error("distro [%s] of host [%s] is not supported",
                         distro, self.bei_local_host.sh_hostname)
            return -1

        iso = self.bei_iso_fpath
        if iso is not None:
            ret = install_common.sync_iso_dir(log, self.bei_workspace,
                                              self.bei_local_host, iso,
                                              self.bei_iso_dir)
            if ret:
                log.cl_error("failed to sync ISO files from [%s] to dir [%s] "
                             "on local host [%s]",
                             iso, self.bei_iso_dir,
                             self.bei_local_host.sh_hostname)
                return -1

        # Install Barreleye deb file.
        package_dir = self.bei_iso_dir + "/" + constant.BUILD_PACKAGES
        command = ("dpkg -i %s/coral-*.deb" % (package_dir))
        retval = self.bei_local_host.sh_run(log, command, timeout=None)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.bei_local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        package_type_dict = self._bei_get_collectd_package_type_dict(log)
        if package_type_dict is None:
            log.cl_error("failed to get Collectd package types")
            return -1

        for deb_type in ("collectd-core", "collectd-utils",
                         "collectd", "libcollectdclient1"):
            if deb_type not in package_type_dict:
                log.cl_error("failed to find Collectd deb [%s]",
                             deb_type)
                return -1

        packages_dir = self.bei_iso_dir + "/" + constant.BUILD_PACKAGES
        ret = barrele_collectd.collectd_debs_install(log, self.bei_local_host,
                                                     packages_dir)
        if ret:
            log.cl_error("failed to install Collectd debs on local host [%s]",
                         self.bei_local_host.sh_hostname)
            return -1

        agent = barrele_agent.BarreleAgent(self.bei_local_host,
                                           self.bei_barreleye_server,
                                           enable_disk=False,
                                           enable_lustre_oss=False,
                                           enable_lustre_mds=False,
                                           enable_lustre_client=True,
                                           enable_infiniband=False)
        ret = agent.bea_generate_configs(log, self)
        if ret:
            log.cl_error("failed to generate configs for Barreleye agent [%s]",
                         agent.bea_host.sh_hostname)
            return -1

        ret = agent.bea_config_agent(log, self)
        if ret:
            log.cl_error("failed to configure Barreleye agent [%s]",
                         agent.bea_host.sh_hostname)
            return -1

        log.cl_info("Barreleye agent has been installed successfully on host [%s]",
                    agent.bea_host.sh_hostname)
        return 0


def parse_server_config(log, config, config_fpath, host_dict):
    """
    Parse server config.
    """
    server_config = utils.config_value(config, barrele_constant.BRL_SERVER)
    if server_config is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     barrele_constant.BRL_SERVER, config_fpath)
        return None

    hostname = utils.config_value(server_config,
                                  barrele_constant.BRL_HOSTNAME)
    if hostname is None:
        log.cl_error("can NOT find [%s] in the config of server, "
                     "please correct file [%s]",
                     barrele_constant.BRL_HOSTNAME, config_fpath)
        return None

    data_path = utils.config_value(server_config,
                                   barrele_constant.BRL_DATA_PATH)
    if data_path is None:
        log.cl_debug("no [%s] configured, using default value [%s]",
                     barrele_constant.BRL_DATA_PATH, BARRELE_DATA_DIR)
        data_path = BARRELE_DATA_DIR

    ssh_identity_file = utils.config_value(server_config,
                                           barrele_constant.BRL_SSH_IDENTITY_FILE)

    host = ssh_host.get_or_add_host_to_dict(log, host_dict, hostname,
                                            ssh_identity_file)
    if host is None:
        return None
    return barrele_server.BarreleServer(host, data_path)


def barrele_init_instance(log, workspace, config, config_fpath, log_to_file,
                          logdir_is_default, iso_fpath):
    """
    Parse the config and init the instance
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    collect_interval = utils.config_value(config,
                                          barrele_constant.BRL_COLLECT_INTERVAL)
    if collect_interval is None:
        log.cl_debug("no [%s] is configured in the config file [%s], "
                     "using default value [%s]",
                     barrele_constant.BRL_COLLECT_INTERVAL,
                     config_fpath, BARRELE_COLLECT_INTERVAL)
        collect_interval = BARRELE_COLLECT_INTERVAL

    continuous_query_periods = utils.config_value(config,
                                                  barrele_constant.BRL_CONTINUOUS_QUERY_PERIODS)
    if continuous_query_periods is None:
        log.cl_debug("no [%s] is configured in the config file [%s], "
                     "using default value [%s]",
                     barrele_constant.BRL_CONTINUOUS_QUERY_PERIODS,
                     config_fpath, BARRELE_CONTINUOUS_QUERY_PERIODS)
        continuous_query_periods = BARRELE_CONTINUOUS_QUERY_PERIODS

    jobstat_pattern = utils.config_value(config, barrele_constant.BRL_JOBSTAT_PATTERN)
    if jobstat_pattern is None:
        log.cl_debug("no [%s] is configured in the config file [%s], "
                     "using default value [%s]",
                     barrele_constant.BRL_JOBSTAT_PATTERN,
                     config_fpath,
                     barrele_constant.BARRELE_JOBSTAT_PATTERN_UNKNOWN)
        jobstat_pattern = barrele_constant.BARRELE_JOBSTAT_PATTERN_UNKNOWN
    if jobstat_pattern not in barrele_constant.BARRELE_JOBSTAT_PATTERNS:
        log.cl_error("unsupported jobstat_pattern [%s], supported: %s",
                     jobstat_pattern, barrele_constant.BARRELE_JOBSTAT_PATTERNS)
        return None

    lustre_fallback_version_name = \
        utils.config_value(config,
                           barrele_constant.BRL_LUSTRE_FALLBACK_VERSION)
    if lustre_fallback_version_name is None:
        log.cl_debug("no [%s] is configured in the config file [%s], "
                     "using default value [%s]",
                     barrele_constant.BRL_LUSTRE_FALLBACK_VERSION,
                     config_fpath, BARRELE_LUSTRE_FALLBACK_VERSION)
        lustre_fallback_version_name = BARRELE_LUSTRE_FALLBACK_VERSION

    if lustre_fallback_version_name not in lustre_version.LUSTRE_VERSION_DICT:
        log.cl_error("unsupported Lustre version [%s] is configured in the "
                     "config file [%s]", lustre_fallback_version_name,
                     config_fpath)
        return None

    lustre_fallback_version = \
        lustre_version.LUSTRE_VERSION_DICT[lustre_fallback_version_name]

    enable_lustre_exp_mdt = utils.config_value(config,
                                               barrele_constant.BRL_ENABLE_LUSTRE_EXP_MDT)
    if enable_lustre_exp_mdt is None:
        log.cl_debug("no [%s] is configured in the config file [%s], "
                     "using default value [False]",
                     barrele_constant.BRL_ENABLE_LUSTRE_EXP_MDT,
                     config_fpath)
        enable_lustre_exp_mdt = False

    enable_lustre_exp_ost = utils.config_value(config,
                                               barrele_constant.BRL_ENABLE_LUSTRE_EXP_OST)
    if enable_lustre_exp_ost is None:
        log.cl_debug("no [%s] is configured in the config file [%s], "
                     "using default value [False]",
                     barrele_constant.BRL_ENABLE_LUSTRE_EXP_OST,
                     config_fpath)
        enable_lustre_exp_ost = False

    agent_configs = utils.config_value(config, barrele_constant.BRL_AGENTS)
    if agent_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     barrele_constant.BRL_AGENTS, config_fpath)
        return None

    host_dict = {}
    barreleye_server = parse_server_config(log, config, config_fpath,
                                           host_dict)
    if barreleye_server is None:
        log.cl_error("failed to parse server config")
        return None

    agent_dict = {}
    for agent_config in agent_configs:
        hostname_config = utils.config_value(agent_config,
                                             barrele_constant.BRL_HOSTNAME)
        if hostname_config is None:
            log.cl_error("can NOT find [%s] in the config of SSH host "
                         "[%s], please correct file [%s]",
                         barrele_constant.BRL_HOSTNAME, hostname_config,
                         config_fpath)
            return None

        hostnames = cmd_general.parse_list_string(log, hostname_config)
        if hostnames is None:
            log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                         hostname_config, barrele_constant.BRL_HOSTNAME,
                         config_fpath)
            return None

        ssh_identity_file = utils.config_value(agent_config,
                                               barrele_constant.BRL_SSH_IDENTITY_FILE)

        enable_disk = utils.config_value(agent_config,
                                         barrele_constant.BRL_ENABLE_DISK)
        if enable_disk is None:
            log.cl_debug("no [%s] is configured in the config file [%s], "
                         "using default value [False]",
                         barrele_constant.BRL_ENABLE_DISK,
                         config_fpath)
            enable_disk = False

        enable_infiniband = utils.config_value(agent_config,
                                               barrele_constant.BRL_ENABLE_INFINIBAND)
        if enable_infiniband is None:
            log.cl_debug("no [%s] is configured in the config file [%s], "
                         "using default value [False]",
                         barrele_constant.BRL_ENABLE_INFINIBAND,
                         config_fpath)
            enable_infiniband = False

        enable_lustre_client = utils.config_value(agent_config,
                                                  barrele_constant.BRL_ENABLE_LUSTRE_CLIENT)
        if enable_lustre_client is None:
            log.cl_debug("no [%s] is configured in the config file [%s], "
                         "using default value [False]",
                         barrele_constant.BRL_ENABLE_LUSTRE_CLIENT,
                         config_fpath)
            enable_lustre_client = False

        enable_lustre_mds = utils.config_value(agent_config,
                                               barrele_constant.BRL_ENABLE_LUSTRE_MDS)
        if enable_lustre_mds is None:
            log.cl_debug("no [%s] is configured in the config file [%s], "
                         "using default value [True]",
                         barrele_constant.BRL_ENABLE_LUSTRE_MDS,
                         config_fpath)
            enable_lustre_mds = True

        enable_lustre_oss = utils.config_value(agent_config,
                                               barrele_constant.BRL_ENABLE_LUSTRE_OSS)
        if enable_lustre_oss is None:
            log.cl_debug("no [%s] is configured in the config file [%s], "
                         "using default value [True]",
                         barrele_constant.BRL_ENABLE_LUSTRE_OSS,
                         config_fpath)
            enable_lustre_oss = True

        for hostname in hostnames:
            if hostname in agent_dict:
                log.cl_error("agent of host [%s] is configured for multiple times",
                             hostname)
                return None
            host = ssh_host.get_or_add_host_to_dict(log, host_dict,
                                                    hostname,
                                                    ssh_identity_file)
            if host is None:
                return None

            agent = barrele_agent.BarreleAgent(host, barreleye_server,
                                               enable_disk=enable_disk,
                                               enable_lustre_oss=enable_lustre_oss,
                                               enable_lustre_mds=enable_lustre_mds,
                                               enable_lustre_client=enable_lustre_client,
                                               enable_infiniband=enable_infiniband)
            agent_dict[hostname] = agent

    local_host = ssh_host.get_local_host()
    instance = BarreleInstance(workspace, config, config_fpath, log_to_file,
                               logdir_is_default, iso_fpath, local_host, collect_interval,
                               continuous_query_periods, jobstat_pattern,
                               lustre_fallback_version, enable_lustre_exp_mdt,
                               enable_lustre_exp_ost, host_dict,
                               agent_dict, barreleye_server)
    return instance
