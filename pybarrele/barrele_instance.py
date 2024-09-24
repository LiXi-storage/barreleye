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
from pycoral import os_distro
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
BARRELE_LUSTRE_FALLBACK_VERSION = "2.15"
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
                 agent_dict, barreleye_server,
                 lustre_version_db):
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
        # The server of barreleye, BarreleServer.
        self.bei_barreleye_server = barreleye_server
        # ISO file path
        self.bei_iso_fpath = iso_fpath
        # LustreVersionDatabase
        self.bei_lustre_version_db = lustre_version_db


    def _bei_get_collectd_package_type_dict(self, log):
        """
        Return a dict. Key is the RPM/deb type, value is the file name.
        """
        packages_dir = self.bei_iso_dir + "/" + constant.BUILD_PACKAGES
        return barrele_collectd.get_collectd_package_type_dict(log,
                                                               self.bei_local_host,
                                                               packages_dir)

    def _bei_cluster_install_package(self, log, agents, only_agent=False,
                                     disable_selinux=True,
                                     disable_firewalld=True,
                                     change_sshd_max_startups=True,
                                     config_rsyslog=True):
        """
        Install the packages in the cluster and send the configs.
        """
        # pylint: disable=too-many-branches,too-many-locals
        distro_type = self._bei_sync_iso_and_check_packages(log)
        if distro_type is None:
            log.cl_info("found invalid packages in ISO")
            return -1

        for agent in agents:
            ret = agent.bea_generate_configs(log)
            if ret:
                log.cl_error("failed to generate Barreleye agent configs on host [%s]",
                             agent.bea_host.sh_hostname)
                return -1
        install_cluster = \
            install_common.get_cluster(log, self.bei_workspace,
                                       self.bei_local_host,
                                       self.bei_iso_dir)
        if install_cluster is None:
            log.cl_error("failed to init installation cluster")
            return -1
        send_fpath_dict = {}
        send_fpath_dict[self.bei_config_fpath] = self.bei_config_fpath
        if distro_type == os_distro.DISTRO_TYPE_RHEL:
            agent_packages = constant.CORAL_DEPENDENT_RPMS[:]
            agent_packages += barrele_constant.BARRELE_AGENT_DEPENDENT_RPMS
        elif distro_type == os_distro.DISTRO_TYPE_UBUNTU:
            agent_packages = ["collectd", "collectd-core", "collectd-utils",
                              "libcollectdclient1"]
        else:
            log.cl_error("unsupported distro type [%s]",
                         distro_type)
            return -1

        agent_on_server = None
        for agent in agents:
            if agent.bea_host.sh_hostname == self.bei_barreleye_server.bes_server_host.sh_hostname:
                if only_agent:
                    log.cl_error("not allowed to reinstall agent on Barreleye server [%s]",
                                 self.bei_barreleye_server.bes_server_host.sh_hostname)
                if agent_on_server is not None:
                    log.cl_error("multiple agents for Barreleye server [%s]",
                                 self.bei_barreleye_server.bes_server_host.sh_hostname)
                    return -1
                agent_on_server = agent
                continue
            package_names = agent_packages
            if distro_type == os_distro.DISTRO_TYPE_RHEL:
                package_names += agent.bea_needed_collectd_package_types
            ret = install_cluster.cic_hosts_add(log,
                                                [agent.bea_host],
                                                package_names=package_names,
                                                send_fpath_dict=send_fpath_dict,
                                                disable_selinux=disable_selinux,
                                                disable_firewalld=disable_firewalld,
                                                change_sshd_max_startups=change_sshd_max_startups,
                                                config_rsyslog=config_rsyslog)
            if ret:
                return -1

        if not only_agent:
            if distro_type == os_distro.DISTRO_TYPE_RHEL:
                server_packages = constant.CORAL_DEPENDENT_RPMS[:]
                server_packages += barrele_constant.BARRELE_SERVER_DEPENDENT_RPMS
                if agent_on_server is not None:
                    server_packages += barrele_constant.BARRELE_AGENT_DEPENDENT_RPMS
                    server_packages += agent_on_server.bea_needed_collectd_package_types
            else:
                log.cl_error("unsupported distro type [%s] for Barreleye server",
                             distro_type)
                return -1

            ret = install_cluster.cic_hosts_add(log,
                                                [self.bei_barreleye_server.bes_server_host],
                                                package_names=server_packages,
                                                send_fpath_dict=send_fpath_dict,
                                                disable_selinux=disable_selinux,
                                                disable_firewalld=disable_firewalld,
                                                change_sshd_max_startups=change_sshd_max_startups,
                                                config_rsyslog=config_rsyslog)
            if ret:
                return -1
        ret = install_cluster.cic_install(log)
        if ret:
            log.cl_error("failed to install cluster")
            return -1
        return 0

    def _bei_sync_iso_and_check_packages(self, log):
        """
        Sync the ISO to local host and check whether it include necessary packages
        """
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
                return None

        package_type_dict = self._bei_get_collectd_package_type_dict(log)
        if package_type_dict is None:
            log.cl_error("failed to get Collectd RPM types")
            return None

        local_host = self.bei_local_host
        local_distro = local_host.sh_distro(log)
        if local_distro is None:
            log.cl_error("failed to get distro of local host [%s]",
                         local_host.sh_hostname)
            return None

        distro_type = os_distro.distro2type(log, local_distro)
        if distro_type is None:
            log.cl_error("unsupported distro [%s] of host [%s]",
                         local_distro, local_host.sh_hostname)
            return None

        if distro_type == os_distro.DISTRO_TYPE_RHEL:
            package_types = (barrele_collectd.LIBCOLLECTDCLIENT_TYPE_NAME,
                             barrele_collectd.COLLECTD_TYPE_NAME)
        elif distro_type == os_distro.DISTRO_TYPE_UBUNTU:
            package_types = ("collectd-core", "collectd-utils",
                             "collectd", "libcollectdclient1")
        else:
            log.cl_error("unsupported distro [%s] of host [%s]",
                         local_distro, local_host.sh_hostname)
            return None

        for package_type in package_types:
            if package_type not in package_type_dict:
                log.cl_error("failed to find Collectd package [%s]",
                             package_type)
                return None
        return distro_type

    def bei_cluster_install(self, log, erase_influxdb=False,
                            drop_database=False):
        """
        Install Barrele on all host (could include localhost).
        """
        # Gives a little bit time for canceling the command
        if erase_influxdb:
            log.cl_warning("data and metadata of Influxdb on host [%s] "
                           "will be all erased",
                           self.bei_barreleye_server.bes_server_host.sh_hostname)
        if drop_database:
            log.cl_warning("database [%s] of Influxdb on host [%s] will be "
                           "dropped",
                           barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME,
                           self.bei_barreleye_server.bes_server_host.sh_hostname)


        agents = list(self.bei_agent_dict.values())
        ret = self._bei_cluster_install_package(log, agents)
        if ret:
            log.cl_error("failed to install packages in the cluster")
            return -1

        server = self.bei_barreleye_server
        ret = server.bes_server_reinstall(log, self,
                                          erase_influxdb=erase_influxdb,
                                          drop_database=drop_database)
        if ret:
            log.cl_error("failed to reinstall Barreleye server")
            return -1

        for agent in self.bei_agent_dict.values():
            ret = agent.bea_config_agent(log)
            if ret:
                log.cl_error("failed to configure Barreleye agent [%s]",
                             agent.bea_host.sh_hostname)
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

    def bei_get_agent(self, log, hostname, ssh_identity_file=None):
        """
        Return BarreleAgent.
        """
        # pylint: disable=unused-argument
        if hostname in self.bei_agent_dict:
            return self.bei_agent_dict[hostname]

        log.cl_warning("agent [%s] is standalone out of Barreleye.conf",
                       hostname)
        if hostname in self.bei_host_dict:
            host = self.bei_host_dict[hostname]
        else:
            host = ssh_host.SSHHost(hostname,
                                    identity_file=ssh_identity_file)

        agent = barrele_agent.BarreleAgent(host,
                                           self.bei_barreleye_server,
                                           enable_disk=False,
                                           enable_lustre_oss=False,
                                           enable_lustre_mds=False,
                                           enable_lustre_client=True,
                                           enable_infiniband=False)
        agent.bea_instance = self
        return agent

    def bei_get_agents(self, log, hostnames, ssh_identity_file=None):
        """
        Return a list of agents.
        """
        agents = []
        for hostname in hostnames:
            agent = self.bei_get_agent(log, hostname, ssh_identity_file=ssh_identity_file)
            if agent is None:
                return None
            agents.append(agent)
        return agents

    def bei_agents_start(self, log, hostnames, ssh_identity_file=None):
        """
        Start agents.
        """
        agents = self.bei_get_agents(log, hostnames, ssh_identity_file=ssh_identity_file)
        if agents is None:
            return -1

        for agent in agents:
            ret = agent.bea_collectd_start(log)
            if ret:
                log.cl_error("failed to start Barreleye agent on host [%s]",
                             agent.bea_host.sh_hostname)
                return -1
        return 0

    def bei_agents_stop(self, log, hostnames, ssh_identity_file=None):
        """
        Stop agents.
        """
        agents = self.bei_get_agents(log, hostnames, ssh_identity_file=ssh_identity_file)
        if agents is None:
            return -1

        for agent in agents:
            ret = agent.bea_collectd_stop(log)
            if ret:
                log.cl_error("failed to stop Barreleye agent on host [%s]",
                             agent.bea_host.sh_hostname)
                return -1
        return 0

    def bei_agents_install(self, log, hostnames, ssh_identity_file=None):
        """
        Install Barreleye agent on hosts.
        """
        local_host = self.bei_local_host
        local_distro = local_host.sh_distro(log)
        if local_distro is None:
            log.cl_error("failed to get distro of local host [%s]",
                         local_host.sh_hostname)
            return -1
        agents = self.bei_get_agents(log, hostnames, ssh_identity_file=ssh_identity_file)
        if agents is None:
            return -1
        for agent in agents:
            host = agent.bea_host
            hostname = host.sh_hostname
            if hostname == self.bei_barreleye_server.bes_server_host.sh_hostname:
                log.cl_error("not allowed to reinstall agent on Barreleye server [%s]",
                             hostname)
                return -1

            distro = host.sh_distro(log)
            if distro is None:
                log.cl_error("failed to get distro of host [%s]",
                             host.sh_hostname)
                return -1
            if distro != local_distro:
                log.cl_error("distro of host [%s] is different with local "
                             "host [%s], [%s] v.s. [%s]",
                             host.sh_hostname,
                             local_host.sh_hostname,
                             distro,
                             local_distro)
                return -1

        ret = self._bei_cluster_install_package(log, agents, only_agent=True,
                                                disable_selinux=False,
                                                disable_firewalld=False,
                                                change_sshd_max_startups=False,
                                                config_rsyslog=False)
        if ret:
            log.cl_error("failed to install packages on the agents")
            return -1

        for agent in agents:
            ret = agent.bea_config_agent(log)
            if ret:
                log.cl_error("failed to configure Barreleye agent [%s]",
                             agent.bea_host.sh_hostname)
                return -1
        log.cl_info("installed agents on [%s]", utils.list2string(hostnames))
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


def barrele_init_instance(log, local_host, workspace, config, config_fpath,
                          log_to_file, logdir_is_default, iso_fpath):
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

    definition_dir = constant.LUSTRE_VERSION_DEFINITION_DIR
    version_db = lustre_version.load_lustre_version_database(log, local_host,
                                                             definition_dir)
    if version_db is None:
        log.cl_error("failed to load version database [%s]",
                     definition_dir)
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

    if lustre_fallback_version_name not in version_db.lvd_version_dict:
        log.cl_error("Lustre version [%s] unsupported in [%s] is configured in the "
                     "config file [%s]",
                     lustre_fallback_version_name,
                     definition_dir,
                     config_fpath)
        return None

    lustre_fallback_version = \
        version_db.lvd_version_dict[lustre_fallback_version_name]

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

    if local_host.sh_hostname not in host_dict:
        host_dict[local_host.sh_hostname] = local_host

    instance = BarreleInstance(workspace, config, config_fpath, log_to_file,
                               logdir_is_default, iso_fpath, local_host, collect_interval,
                               continuous_query_periods, jobstat_pattern,
                               lustre_fallback_version, enable_lustre_exp_mdt,
                               enable_lustre_exp_ost, host_dict,
                               agent_dict, barreleye_server,
                               version_db)
    for agent in agent_dict.values():
        agent.bea_instance = instance
    return instance
