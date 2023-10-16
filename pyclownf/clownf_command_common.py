"""
Common libarary for Clownf commands
"""
# pylint: disable=too-many-lines
# Local libs
from pycoral import parallel
from pycoral import cmd_general
from pycoral import lustre
from pycoral import clog
from pycoral import constant
from pycoral import consul
from pycoral import watched_io
from pyclownf import clownf_constant
from pyclownf import clownf_instance
from pyclownf import clownf_consul


def init_env(config_fpath, logdir, log_to_file, iso
             ):
    """
    Init log and instance for commands that needs it
    """
    log_dir_is_default = (logdir == clownf_constant.CLF_LOG_DIR)
    log, workspace, clownfish_config = cmd_general.init_env(config_fpath,
                                                            logdir,
                                                            log_to_file,
                                                            log_dir_is_default)
    clownfish_instance = clownf_instance.clownf_init_instance(log, workspace,
                                                              clownfish_config,
                                                              config_fpath,
                                                              log_dir_is_default,
                                                              iso)
    if clownfish_instance is None:
        log.cl_error("failed to init Clownfish instance")
        cmd_general.cmd_exit(log, 1)
    clownfish_instance.ci_log_to_file = log_to_file

    return log, clownfish_instance


def exit_env(log, clownfish_instance, ret):
    """
    Cleanup and exit
    """
    clownfish_instance.ci_fini(log)
    cmd_general.cmd_exit(log, ret)


def host_watcher_init(log, workspace, clownfish_instance,
                      host_watcher_dict, consul_hostname,
                      watching_hostname):
    """
    Init the host watcher of dict
    """
    # pylint: disable=unused-argument
    ret, watcher = clownf_consul.host_get_watcher(log,
                                                  clownfish_instance.ci_consul_cluster,
                                                  watching_hostname,
                                                  consul_hostname=consul_hostname)
    if ret:
        log.cl_error("failed to get the watcher of host [%s]",
                     watching_hostname)
        return
    host_watcher_dict[watching_hostname] = watcher


def service_watcher_init(log, workspace, clownfish_instance,
                         service_watcher_dict, consul_hostname,
                         watching_service_name):
    """
    Init the service watcher of dict
    """
    # pylint: disable=unused-argument
    ret, watcher = clownf_consul.service_get_watcher(log,
                                                     clownfish_instance.ci_consul_cluster,
                                                     watching_service_name,
                                                     consul_hostname=consul_hostname)
    if ret:
        log.cl_error("failed to get the watcher of host [%s]",
                     watching_service_name)
        return
    service_watcher_dict[watching_service_name] = watcher


class HostStatusCache():
    """
    This object saves temporary status of a host.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status, host):
        # Instance
        self.hsc_clownfish_instance = clownfish_instance
        # Any failure when getting this cached status
        self.hsc_failed = False
        # LustreHost
        self.hsc_host = host
        # Whether the host is up
        self._hsc_is_up = None
        # Whether the host is running clownf_agent. Negative on error.
        self.hsc_running_agent = None
        # Whether the host is running operation command. Negative on error.
        self.hsc_operating = None
        # Mounted service number. 0 on error.
        self.hsc_mounted_service_number = None
        # Whether hsc_load_status is inited
        self.hsc_load_status_inited = False
        # The return value of lsh_get_load_status()
        self.hsc_load_status = None
        # Health status of Lustre host, lustre.LustreHost.LSH_*
        self.hsc_health_status = None
        # Consul role of the host. server, client or none.
        self.hsc_consul_role = None
        # Status of Consul, consul.CONSUL_STATUS_*
        self.hsc_consul_status = None
        # Whether autostart of the host is enabled. Negative on error.
        self.hsc_autostart_enabled = None
        # If no watcher, keep it as None.
        self.hsc_watcher = None
        # If inited successfully 0, negtavie on error.
        self.hsc_watcher_init_status = None
        # Whether candidates of the host. Negative on error.
        self.hsc_watcher_candidate_number = None
        # Cluster status, ClusterStatusCache
        self.hsc_cluster_status = cluster_status
        # Number of hosts watched by this host
        self.hsc_watching_host_number = None
        # Number of services watched by this host
        self.hsc_watching_service_number = None
        # Hostnames that could be watched by this host
        self.hsc_watching_candidate_hosts = None
        # Names of services that could be watched by this host
        self.hsc_watching_candidate_services = None

    def hsc_is_up(self, log):
        """
        Return whether the host is up
        """
        if self._hsc_is_up is None:
            self._hsc_is_up = self.hsc_host.sh_is_up(log)
            if not self.hsc_is_up:
                self.hsc_failed = True
        return self._hsc_is_up

    def hsc_init_running_agent(self, log):
        """
        Init hsc_running_agent.
        """
        clownfish_instance = self.hsc_clownfish_instance
        if not self.hsc_is_up(log):
            self.hsc_running_agent = -1
            return
        self.hsc_running_agent = \
            clownfish_instance.ci_host_check_running_agent(log,
                                                           self.hsc_host)
        if self.hsc_running_agent < 0:
            self.hsc_failed = True

    def hsc_init_operating(self, log):
        """
        Init hsc_operating.
        """
        clownfish_instance = self.hsc_clownfish_instance
        if not self.hsc_is_up(log):
            self.hsc_operating = -1
            return

        self.hsc_operating = \
            clownfish_instance.ci_host_check_operating(log,
                                                       self.hsc_host)
        if self.hsc_operating < 0:
            self.hsc_failed = True

    def hsc_init_mounted_service_number(self, log):
        """
        Init hsc_mounted_service_number.
        """
        if not self.hsc_is_up(log):
            self.hsc_mounted_service_number = 0
            return

        mounted_number = 0
        for instance in self.hsc_host.lsh_instance_dict.values():
            ret = instance.lsi_check_mounted(log)
            if ret < 0:
                self.hsc_failed = True
            elif ret:
                mounted_number += 1
        self.hsc_mounted_service_number = mounted_number

    def hsc_init_load_status(self, log):
        """
        Init hsc_load_status.
        """
        self.hsc_load_status_inited = True
        clownfish_instance = self.hsc_clownfish_instance
        if not self.hsc_is_up(log):
            self.hsc_load_status = None
            return

        if not clownfish_instance.ci_is_symmetric(log):
            self.hsc_load_status = None
            return

        self.hsc_load_status = self.hsc_host.lsh_get_load_status(log)
        if self.hsc_load_status is None:
            self.hsc_failed = True

    def hsc_init_health_status(self, log):
        """
        Init hsc_health_status
        """
        if not self.hsc_is_up(log):
            self.hsc_health_status = lustre.LustreHost.LSH_ERROR
            return

        self.hsc_health_status = self.hsc_host.lsh_healty_check(log)
        if self.hsc_health_status == lustre.LustreHost.LSH_ERROR:
            self.hsc_failed = True

    def hsc_init_consul_role(self, log):
        """
        Init hsc_consul_role
        """
        hostname = self.hsc_host.sh_hostname
        clownfish_instance = self.hsc_clownfish_instance
        self.hsc_consul_role = clownfish_instance.ci_host_consul_role(log,
                                                                      hostname)

    def hsc_init_consul_status(self, log):
        """
        Init hsc_consul_status
        """
        if not self.hsc_is_up(log):
            self.hsc_consul_status = consul.CONSUL_STATUS_HOST_DOWN
            return

        self.hsc_consul_status = consul.host_consul_status(log, self.hsc_host)
        if self.hsc_consul_status != consul.CONSUL_STATUS_ALIVE:
            self.hsc_failed = True

    def hsc_init_autostart_enabled(self, log):
        """
        Init hsc_autostart_enabled
        """
        hostname = self.hsc_host.sh_hostname
        clownfish_instance = self.hsc_clownfish_instance
        cluster_status = self.hsc_cluster_status
        consul_hostname = cluster_status.csc_consul_hostname(log)
        if consul_hostname is None:
            self.hsc_autostart_enabled = -1
            return
        rc = clownfish_instance.ci_host_autostart_check_enabled(log,
                                                                hostname,
                                                                consul_hostname=consul_hostname)
        if rc < 0:
            log.cl_error("failed to check whether autostart of host [%s] is enabled",
                         hostname)
            self.hsc_failed = True
        self.hsc_autostart_enabled = rc

    def hsc_init_watcher(self, log):
        """
        Init hsc_watcher
        """
        hostname = self.hsc_host.sh_hostname
        clownfish_instance = self.hsc_clownfish_instance
        cluster_status = self.hsc_cluster_status
        consul_hostname = cluster_status.csc_consul_hostname(log)
        if consul_hostname is None:
            self.hsc_watcher_init_status = -1
            self.hsc_autostart_enabled = -1
            return
        rc, self.hsc_watcher = clownfish_instance.ci_host_watcher(log, hostname,
                                                                  consul_hostname=consul_hostname)
        if rc:
            log.cl_error("failed to get the watcher of host [%s]", hostname)
            self.hsc_failed = True
            self.hsc_watcher_init_status = -1
            return
        self.hsc_watcher_init_status = 0

    def hsc_init_watcher_candidate_number(self, log):
        """
        Init hsc_watcher_candidate_number
        """
        hostname = self.hsc_host.sh_hostname
        clownfish_instance = self.hsc_clownfish_instance
        watcher_candidates = \
            clownfish_instance.ci_host_watcher_candidates(log, hostname)
        self.hsc_watcher_candidate_number = len(watcher_candidates)

    def hsc_can_skip_init_fields(self, field_names):
        """
        Whether hsc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_HOST)
        if len(fields) == 0:
            return True
        return False

    def hsc_init_watching_host_number(self, log):
        """
        Init hsc_watching_host_number
        """
        # pylint: disable=unused-argument
        hostname = self.hsc_host.sh_hostname
        host_watcher_dict = self.hsc_cluster_status.csc_host_watcher_dict
        self.hsc_watching_host_number = 0
        for watcher in host_watcher_dict.values():
            if hostname == watcher:
                self.hsc_watching_host_number += 1

    def hsc_init_watching_service_number(self, log):
        """
        Init hsc_watching_service_number
        """
        # pylint: disable=unused-argument
        hostname = self.hsc_host.sh_hostname
        host_watcher_dict = self.hsc_cluster_status.csc_service_watcher_dict
        self.hsc_watching_service_number = 0
        for watcher in host_watcher_dict.values():
            if hostname == watcher:
                self.hsc_watching_service_number += 1

    def hsc_init_candiate_watching_hosts(self, log):
        """
        Init hsc_watching_candidate_hosts
        """
        host = self.hsc_host
        clownfish_instance = self.hsc_clownfish_instance
        self.hsc_watching_candidate_hosts = \
            clownfish_instance.ci_host_watching_candidate_hosts(log, host)

    def hsc_init_candiate_watching_services(self, log):
        """
        Init hsc_watching_candidate_services
        """
        host = self.hsc_host
        clownfish_instance = self.hsc_clownfish_instance
        self.hsc_watching_candidate_services = \
            clownfish_instance.ci_host_watching_candidate_services(log, host)

    def hsc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        for field in field_names:
            if field == clownf_constant.CLOWNF_FIELD_HOST:
                continue
            if field == clownf_constant.CLOWNF_FIELD_UP:
                self.hsc_is_up(log)
            elif field == clownf_constant.CLOWNF_FIELD_AGENT:
                self.hsc_init_running_agent(log)
            elif field == clownf_constant.CLOWNF_FIELD_MOUNTED_SERVICES:
                self.hsc_init_mounted_service_number(log)
            elif field == clownf_constant.CLOWNF_FIELD_LOAD_BALANCED:
                self.hsc_init_load_status(log)
            elif field == clownf_constant.CLOWNF_FIELD_HEALTHY:
                self.hsc_init_health_status(log)
            elif field == clownf_constant.CLOWNF_FIELD_OPERATING:
                self.hsc_init_operating(log)
            elif field == clownf_constant.CLOWNF_FIELD_CONSUL_ROLE:
                self.hsc_init_consul_role(log)
            elif field == clownf_constant.CLOWNF_FIELD_CONSUL_STATUS:
                self.hsc_init_consul_status(log)
            elif field == clownf_constant.CLOWNF_FIELD_AUTOSTART:
                self.hsc_init_autostart_enabled(log)
            elif field == clownf_constant.CLOWNF_FIELD_WATCHER_HOST:
                self.hsc_init_watcher(log)
            elif field == clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES:
                self.hsc_init_watcher_candidate_number(log)
            elif field == clownf_constant.CLOWNF_FIELD_WATCHING_HOSTS:
                self.hsc_init_watching_host_number(log)
            elif field == clownf_constant.CLOWNF_FIELD_CWATCHING_HOSTS:
                self.hsc_init_candiate_watching_hosts(log)
            elif field == clownf_constant.CLOWNF_FIELD_WATCHING_SERVICES:
                self.hsc_init_watching_service_number(log)
            elif field == clownf_constant.CLOWNF_FIELD_CWATCHING_SERVICES:
                self.hsc_init_candiate_watching_services(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def hsc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        host = self.hsc_host
        hostname = host.sh_hostname
        clownfish_instance = self.hsc_clownfish_instance
        ret = 0
        if field_name == clownf_constant.CLOWNF_FIELD_HOST:
            result = hostname
        elif field_name == clownf_constant.CLOWNF_FIELD_UP:
            if self._hsc_is_up is None:
                log.cl_error("up status of host [%s] is not inited",
                             hostname)
                ret = -1
                result = clog.ERROR_MSG
            elif self._hsc_is_up:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_UP)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               clownf_constant.CLOWNF_VALUE_DOWN)
        elif field_name == clownf_constant.CLOWNF_FIELD_AGENT:
            if self.hsc_running_agent is None:
                log.cl_error("agent status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.hsc_running_agent < 0:
                result = clog.ERROR_MSG
            elif self.hsc_running_agent:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_ACTIVE)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               clownf_constant.CLOWNF_VALUE_INACTIVE)
        elif field_name == clownf_constant.CLOWNF_FIELD_OPERATING:
            if self.hsc_operating is None:
                log.cl_error("operating status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.hsc_operating < 0:
                result = clog.ERROR_MSG
            elif self.hsc_operating:
                result = clownf_constant.CLOWNF_VALUE_OPERATING
            else:
                result = clownf_constant.CLOWNF_VALUE_IDLE
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNTED_SERVICES:
            if self.hsc_mounted_service_number is None:
                log.cl_error("mounted service number of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            else:
                instance_number = len(host.lsh_instance_dict)
                result = "%d/%d" % (self.hsc_mounted_service_number, instance_number)
        elif field_name == clownf_constant.CLOWNF_FIELD_LOAD_BALANCED:
            if not self.hsc_load_status_inited:
                log.cl_error("load balance status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif not clownfish_instance.ci_is_symmetric(log):
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_UNKNOWN)
            elif self.hsc_load_status is None:
                result = clog.ERROR_MSG
            elif self.hsc_load_status > 0:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_OVERLOADED)
            elif self.hsc_load_status < 0:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_UNDERLOADED)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_BALANCED)
        elif field_name == clownf_constant.CLOWNF_FIELD_HEALTHY:
            if self.hsc_health_status is None:
                log.cl_error("health status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.hsc_health_status == lustre.LustreHost.LSH_ERROR:
                result = clog.ERROR_MSG
            elif self.hsc_health_status == lustre.LustreHost.LSH_HEALTHY:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               self.hsc_health_status)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               self.hsc_health_status)
        elif field_name == clownf_constant.CLOWNF_FIELD_CONSUL_ROLE:
            if self.hsc_consul_role is None:
                log.cl_error("Consul role of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            else:
                result = self.hsc_consul_role
        elif field_name == clownf_constant.CLOWNF_FIELD_CONSUL_STATUS:
            if self.hsc_consul_status is None:
                log.cl_error("Consul status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.hsc_consul_status == consul.CONSUL_STATUS_ALIVE:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               self.hsc_consul_status)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               self.hsc_consul_status)
        elif field_name == clownf_constant.CLOWNF_FIELD_AUTOSTART:
            if self.hsc_autostart_enabled is None:
                log.cl_error("autostart status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.hsc_autostart_enabled < 0:
                result = clog.ERROR_MSG
            elif self.hsc_autostart_enabled:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_AUTOSTART)
            else:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_NOT_AUTOSTART)
        elif field_name == clownf_constant.CLOWNF_FIELD_WATCHER_HOST:
            if self.hsc_watcher_init_status is None:
                log.cl_error("watcher status of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.hsc_watcher_init_status < 0:
                result = clog.ERROR_MSG
            elif self.hsc_watcher is None:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               constant.CMD_MSG_NONE)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               self.hsc_watcher)
        elif field_name == clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES:
            if self.hsc_watcher_candidate_number is None:
                log.cl_error("watcher candidate number of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            elif self.hsc_watcher_candidate_number < 0:
                result = clog.ERROR_MSG
            elif self.hsc_watcher_candidate_number == 0:
                result = clog.colorful_message(clog.COLOR_RED,
                                               self.hsc_watcher_candidate_number)
            elif self.hsc_watcher_candidate_number == 1:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               self.hsc_watcher_candidate_number)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               self.hsc_watcher_candidate_number)
        elif field_name == clownf_constant.CLOWNF_FIELD_WATCHING_HOSTS:
            if self.hsc_watching_host_number is None:
                log.cl_error("watching host number of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            else:
                result = str(self.hsc_watching_host_number)
        elif field_name == clownf_constant.CLOWNF_FIELD_CWATCHING_HOSTS:
            result = len(self.hsc_watching_candidate_hosts)
        elif field_name == clownf_constant.CLOWNF_FIELD_WATCHING_SERVICES:
            if self.hsc_watching_service_number is None:
                log.cl_error("watching service number of host [%s] is not inited",
                             hostname)
                result = clog.ERROR_MSG
                ret = -1
            else:
                result = str(self.hsc_watching_service_number)
        elif field_name == clownf_constant.CLOWNF_FIELD_CWATCHING_SERVICES:
            result = str(len(self.hsc_watching_candidate_services))
        else:
            log.cl_error("unknown field [%s] of host", field_name)
            result = clog.ERROR_MSG
            ret = -1
        if self.hsc_failed:
            ret = -1
        return ret, result


def host_status_init(log, workspace, host_status, field_names):
    """
    Init status of a host
    """
    # pylint: disable=unused-argument
    return host_status.hsc_init_fields(log, field_names)


def host_status_field(log, host_status, field_name):
    """
    Return (0, result) for a field of HostStatusCache
    """
    # pylint: disable=unused-argument,too-many-branches
    return host_status.hsc_field_result(log, field_name)


class ClusterStatusCache():
    """
    This instance saves cluster wide status.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, clownfish_instance):
        # Instance
        self.csc_clownfish_instance = clownfish_instance
        # Whether csc_consul_hostname is inited
        self.csc_consul_hostname_inited = False
        # The active Consul server hostname
        self._csc_consul_hostname = None
        # Any failure when getting this cached status
        self.csc_failed = False
        # Dict. Key is the hostname, value is the watcher hostname of the
        # host.
        self.csc_host_watcher_dict = None
        # Dict. Key is the service name, value is the watcher hostname of
        # the service.
        self.csc_service_watcher_dict = None

    def _csc_init_consul_hostname(self, log):
        """
        Init hostname of active Consul server.
        """
        clownfish_instance = self.csc_clownfish_instance
        consul_cluster = clownfish_instance.ci_consul_cluster
        agent = consul_cluster.cclr_alive_agent(log)
        if agent is None:
            log.cl_error("no Consul agent is up in the system")
            self.csc_failed = True
            consul_hostname = None
        else:
            consul_hostname = agent.cs_host.sh_hostname
        self.csc_consul_hostname_inited = True
        self._csc_consul_hostname = consul_hostname

    def _csc_init_host_watchers(self, log, only_hostnames=None):
        """
        Init csc_host_watcher_dict
        """
        clownfish_instance = self.csc_clownfish_instance
        self.csc_host_watcher_dict = {}
        consul_hostname = self.csc_consul_hostname(log)
        if consul_hostname is None:
            return

        if only_hostnames is None:
            only_hostnames = list(clownfish_instance.ci_host_dict.keys())

        thread_ids = []
        args_array = []
        for only_hostname in only_hostnames:
            args = (clownfish_instance, self.csc_host_watcher_dict,
                    consul_hostname, only_hostname)
            args_array.append(args)
            thread_id = "host_watcher_%s" % only_hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "host_watcher",
                                                    host_watcher_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to get the watcher of hosts")
            self.csc_failed = True
            return

    def _csc_init_service_watchers(self, log, only_service_names=None):
        """
        Init csc_service_watcher_dict
        """
        clownfish_instance = self.csc_clownfish_instance
        self.csc_service_watcher_dict = {}
        consul_hostname = self.csc_consul_hostname(log)
        if consul_hostname is None:
            return

        if only_service_names is None:
            only_service_names = list(clownfish_instance.ci_service_dict.keys())

        thread_ids = []
        args_array = []
        for only_service_name in only_service_names:
            args = (clownfish_instance, self.csc_service_watcher_dict,
                    consul_hostname, only_service_name)
            args_array.append(args)
            thread_id = "service_watcher_%s" % only_service_name
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "service_watcher",
                                                    service_watcher_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to get the watcher of services")
            self.csc_failed = True
            return

    def csc_consul_hostname(self, log):
        """
        Return hostname of active Consul agent. No active agent, return None.
        """
        if not self.csc_consul_hostname_inited:
            log.cl_error("active Consul hostname is not inited")
            return None
        return self._csc_consul_hostname

    def csc_init_fields(self, log, init_consul_hostname=True,
                        init_host_watchers=False, init_service_watchers=False,
                        only_hostnames=None, only_service_names=None):
        """
        Init the fields
        """
        if init_host_watchers or init_service_watchers:
            init_consul_hostname = True
        if init_consul_hostname:
            self._csc_init_consul_hostname(log)
        if init_host_watchers:
            self._csc_init_host_watchers(log, only_hostnames=only_hostnames)
        if init_service_watchers:
            self._csc_init_service_watchers(log,
                                            only_service_names=only_service_names)
        return 0


def get_possible_watched_hostnames(log, clownfish_instance, hosts):
    """
    Return the hostnames possibly watched by a list of hosts.
    """
    possible_watched_hostnames = []
    for host in hosts:
        candidate_hosts = \
            clownfish_instance.ci_host_watching_candidate_hosts(log, host)
        for possible_watched_hostname in candidate_hosts:
            if possible_watched_hostname not in possible_watched_hostnames:
                possible_watched_hostnames.append(possible_watched_hostname)
    return possible_watched_hostnames


def get_possible_watched_service_names(log, clownfish_instance, hosts):
    """
    Return the service names possibly watched by a list of hosts.
    """
    possible_watched_service_names = []
    for host in hosts:
        candidate_services = \
            clownfish_instance.ci_host_watching_candidate_services(log, host)
        for possible_watched_service_name in candidate_services:
            if possible_watched_service_name not in possible_watched_service_names:
                possible_watched_service_names.append(possible_watched_service_name)
    return possible_watched_service_names


def print_hosts(log, clownfish_instance, hosts, print_status=False,
                print_table=True, field_string=None):
    """
    Print table of hosts
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    # pylint: disable=too-many-nested-blocks
    quick_fields = [clownf_constant.CLOWNF_FIELD_HOST]
    slow_fields = [clownf_constant.CLOWNF_FIELD_UP,
                   clownf_constant.CLOWNF_FIELD_AGENT,
                   clownf_constant.CLOWNF_FIELD_MOUNTED_SERVICES,
                   clownf_constant.CLOWNF_FIELD_LOAD_BALANCED,
                   clownf_constant.CLOWNF_FIELD_HEALTHY]
    none_table_fields = [clownf_constant.CLOWNF_FIELD_OPERATING,
                         clownf_constant.CLOWNF_FIELD_CONSUL_ROLE,
                         clownf_constant.CLOWNF_FIELD_CONSUL_STATUS,
                         clownf_constant.CLOWNF_FIELD_AUTOSTART,
                         clownf_constant.CLOWNF_FIELD_WATCHER_HOST,
                         clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES,
                         clownf_constant.CLOWNF_FIELD_WATCHING_HOSTS,
                         clownf_constant.CLOWNF_FIELD_CWATCHING_HOSTS,
                         clownf_constant.CLOWNF_FIELD_WATCHING_SERVICES,
                         clownf_constant.CLOWNF_FIELD_CWATCHING_SERVICES]
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=print_status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for host",
                     field_string)
        return -1

    cluster_status = ClusterStatusCache(clownfish_instance)
    init_consul_hostname = False
    init_host_watchers = False
    init_service_watchers = False
    only_hostnames = None
    only_service_names = None
    if (clownf_constant.CLOWNF_FIELD_WATCHING_HOSTS in field_names or
            clownf_constant.CLOWNF_FIELD_WATCHING_SERVICES in field_names):
        init_consul_hostname = True
        init_host_watchers = True
        init_service_watchers = True
        only_hostnames = get_possible_watched_hostnames(log, clownfish_instance,
                                                        hosts)
        if only_hostnames is None:
            log.cl_error("failed to get possible watched hostnames")
            return -1
        only_service_names = get_possible_watched_service_names(log,
                                                                clownfish_instance,
                                                                hosts)
        if only_service_names is None:
            log.cl_error("failed to get possible watched service names")
            return -1
    elif (clownf_constant.CLOWNF_FIELD_AUTOSTART in field_names or
          clownf_constant.CLOWNF_FIELD_WATCHER_HOST in field_names):
        init_consul_hostname = True
    ret = cluster_status.csc_init_fields(log, init_consul_hostname=init_consul_hostname,
                                         init_host_watchers=init_host_watchers,
                                         init_service_watchers=init_service_watchers,
                                         only_hostnames=only_hostnames,
                                         only_service_names=only_service_names)
    if ret:
        log.cl_error("failed to init cluster status")
        return -1

    host_status_list = []
    for host in hosts:
        host_status = HostStatusCache(clownfish_instance, cluster_status,
                                      host)
        host_status_list.append(host_status)

    if (len(host_status_list) > 0 and
            not host_status_list[0].hsc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for host_status in host_status_list:
            args = (host_status, field_names)
            args_array.append(args)
            thread_id = "host_status_%s" % host_status.hsc_host.sh_hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "host_status",
                                                    host_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for hosts",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, host_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                host_status_field,
                                print_table=print_table,
                                print_status=print_status,
                                field_string=field_string)
    if cluster_status.csc_failed:
        rc = -1
    return rc


class ServiceStatusCache():
    """
    This object saves temporary status of a service.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status, service):
        # Cluster status, ClusterStatusCache
        self.ssc_cluster_status = cluster_status
        # Instance
        self.ssc_clownfish_instance = clownfish_instance
        # Any failure when getting this cached status
        self.ssc_failed = False
        # LustreService
        self.ssc_service = service
        # The mounted instance of the service. If not mounted, None.
        self.ssc_mounted_instance = None
        # Whether ssc_mounted_instance is mounted
        self.ssc_mounted_instance_inited = False
        # Enabled hostnames. If error, None.
        self.ssc_enabled_hostnames = None
        # Whether ssc_enabled_hostnames is inited
        self.ssc_enabled_hostnames_inited = False
        # Whether autostart of the service is enabled. Negative on error.
        self.ssc_autostart_enabled = None
        # The watcher hostname. If no watcher, None.
        self.ssc_watcher = None
        # The status of initing ssc_watcher
        self.ssc_watcher_init_status = None
        # Number of watcher candidates. Negative on error.
        self.ssc_watcher_candidate_number = None

    def ssc_init_mounted_instance(self, log):
        """
        Init ssc_mounted_instance.
        """
        if self.ssc_mounted_instance_inited:
            return
        service = self.ssc_service
        self.ssc_mounted_instance = service.ls_mounted_instance(log)
        self.ssc_mounted_instance_inited = True

    def ssc_init_enabled_hostnames(self, log):
        """
        Init ssc_enabled_hostnames
        """
        if self.ssc_enabled_hostnames_inited:
            return
        clownfish_instance = self.ssc_clownfish_instance
        service = self.ssc_service
        self.ssc_enabled_hostnames = \
            clownfish_instance.ci_service_enabled_hostnames(log, service)
        if self.ssc_enabled_hostnames is None:
            self.ssc_failed = True
        self.ssc_enabled_hostnames_inited = True

    def ssc_init_autostart_enabled(self, log):
        """
        Init ssc_autostart_enabled
        """
        clownfish_instance = self.ssc_clownfish_instance
        service_name = self.ssc_service.ls_service_name
        cluster_status = self.ssc_cluster_status
        consul_hostname = cluster_status.csc_consul_hostname(log)
        if consul_hostname is None:
            self.ssc_autostart_enabled = -1
            return
        self.ssc_autostart_enabled = \
            clownfish_instance.ci_service_autostart_check_enabled(log,
                                                                  service_name,
                                                                  consul_hostname=consul_hostname)
        if self.ssc_autostart_enabled < 0:
            log.cl_error("failed to get the autostart status of service [%s]",
                         service_name)
            self.ssc_failed = True

    def ssc_init_watcher_hostname(self, log):
        """
        Init ssc_watcher
        """
        clownfish_instance = self.ssc_clownfish_instance
        service_name = self.ssc_service.ls_service_name
        cluster_status = self.ssc_cluster_status
        consul_hostname = cluster_status.csc_consul_hostname(log)
        if consul_hostname is None:
            self.ssc_watcher_init_status = -1
            return
        rc, self.ssc_watcher = \
            clownfish_instance.ci_service_watcher(log,
                                                  service_name,
                                                  consul_hostname=consul_hostname)
        if rc:
            log.cl_error("failed to get the watcher of service [%s]",
                         service_name)
            self.ssc_failed = True
            self.ssc_watcher_init_status = -1
            return
        self.ssc_watcher_init_status = 0

    def ssc_init_watcher_candidate_number(self, log):
        """
        Init ssc_watcher_candidate_number
        """
        clownfish_instance = self.ssc_clownfish_instance
        service = self.ssc_service
        service_name = service.ls_service_name
        rc, watcher_candidates = \
            clownfish_instance.ci_service_watcher_candidates(log, service)
        if rc:
            log.cl_error("failed to get the watcher candidates of service [%s]",
                         service_name)
            self.ssc_failed = True
            self.ssc_watcher_candidate_number = -1
        else:
            self.ssc_watcher_candidate_number = len(watcher_candidates)

    def ssc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        for field in field_names:
            if field == clownf_constant.CLOWNF_FIELD_SERVICE:
                continue
            if field == clownf_constant.CLOWNF_FIELD_BACKFS_TYPE:
                continue
            if field == clownf_constant.CLOWNF_FIELD_MOUNTED:
                self.ssc_init_mounted_instance(log)
            elif field == clownf_constant.CLOWNF_FIELD_MOUNT_HOST:
                self.ssc_init_mounted_instance(log)
            elif field == clownf_constant.CLOWNF_FIELD_ENABLED_HOSTS:
                self.ssc_init_enabled_hostnames(log)
            elif field == clownf_constant.CLOWNF_FIELD_AUTOSTART:
                self.ssc_init_autostart_enabled(log)
            elif field == clownf_constant.CLOWNF_FIELD_WATCHER_HOST:
                self.ssc_init_watcher_hostname(log)
            elif field == clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES:
                self.ssc_init_watcher_candidate_number(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def ssc_can_skip_init_fields(self, field_names):
        """
        Whether ssc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_SERVICE)
        fields.remove(clownf_constant.CLOWNF_FIELD_BACKFS_TYPE)
        if len(fields) == 0:
            return True
        return False

    def ssc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        service = self.ssc_service
        service_name = service.ls_service_name
        ret = 0
        if field_name == clownf_constant.CLOWNF_FIELD_SERVICE:
            result = service_name
        elif field_name == clownf_constant.CLOWNF_FIELD_BACKFS_TYPE:
            result = service.ls_backfstype
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNTED:
            if not self.ssc_mounted_instance_inited:
                log.cl_error("mounted host of service [%s] is not inited",
                             service_name)
                ret = -1
                result = clog.ERROR_MSG
            elif self.ssc_mounted_instance is None:
                result = clog.colorful_message(clog.COLOR_RED,
                                               clownf_constant.CLOWNF_VALUE_UMOUNTED)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_MOUNTED)
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNT_HOST:
            if not self.ssc_mounted_instance_inited:
                log.cl_error("mounted host of service [%s] is not inited",
                             service_name)
                ret = -1
                result = clog.ERROR_MSG
            elif self.ssc_mounted_instance is None:
                result = clog.colorful_message(clog.COLOR_RED,
                                               constant.CMD_MSG_NONE)
            else:
                hostname = self.ssc_mounted_instance.lsi_host.sh_hostname
                result = clog.colorful_message(clog.COLOR_GREEN, hostname)
        elif field_name == clownf_constant.CLOWNF_FIELD_ENABLED_HOSTS:
            if not self.ssc_enabled_hostnames_inited:
                log.cl_error("enabled hosts of service [%s] is not inited",
                             service_name)
                ret = -1
                result = clog.ERROR_MSG
            else:
                host_number = len(service.ls_instance_dict)
                if host_number == 0:
                    number_string = clog.colorful_message(clog.COLOR_RED,
                                                          host_number)
                else:
                    number_string = clog.colorful_message(clog.COLOR_GREEN,
                                                          host_number)
                hostnames = self.ssc_enabled_hostnames
                if hostnames is None:
                    enabled_number = clog.ERROR_MSG
                else:
                    number = len(hostnames)
                    if number == 0:
                        enabled_number = clog.colorful_message(clog.COLOR_YELLOW,
                                                               number)
                    else:
                        enabled_number = clog.colorful_message(clog.COLOR_GREEN,
                                                               number)
                result = enabled_number + "/" + number_string
        elif field_name == clownf_constant.CLOWNF_FIELD_AUTOSTART:
            if self.ssc_autostart_enabled is None:
                log.cl_error("Autostart status of service [%s] is not inited",
                             service_name)
                result = clog.ERROR_MSG
                ret = -1
            elif self.ssc_autostart_enabled < 0:
                result = clog.ERROR_MSG
            elif self.ssc_autostart_enabled:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_AUTOSTART)
            else:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_NOT_AUTOSTART)
        elif field_name == clownf_constant.CLOWNF_FIELD_WATCHER_HOST:
            if self.ssc_watcher_init_status is None:
                log.cl_error("watcher status of service [%s] is not inited",
                             service_name)
                result = clog.ERROR_MSG
                ret = -1
            elif self.ssc_watcher_init_status < 0:
                result = clog.ERROR_MSG
            elif self.ssc_watcher is None:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               constant.CMD_MSG_NONE)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               self.ssc_watcher)
        elif field_name == clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES:
            if self.ssc_watcher_candidate_number is None:
                log.cl_error("watcher candidate number of service [%s] is not inited",
                             service_name)
                result = clog.ERROR_MSG
                ret = -1
            elif self.ssc_watcher_candidate_number < 0:
                result = clog.ERROR_MSG
            elif self.ssc_watcher_candidate_number == 0:
                result = clog.colorful_message(clog.COLOR_RED,
                                               self.ssc_watcher_candidate_number)
            elif self.ssc_watcher_candidate_number == 1:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               self.ssc_watcher_candidate_number)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               self.ssc_watcher_candidate_number)
        else:
            log.cl_error("unknown field [%s] of service")
            result = clog.ERROR_MSG
            ret = -1

        if self.ssc_failed:
            ret = -1
        return ret, result


def service_status_init(log, workspace, service_status, field_names):
    """
    Init status of a service
    """
    # pylint: disable=unused-argument
    return service_status.ssc_init_fields(log, field_names)


def service_status_field(log, service_status, field_name):
    """
    Return (0, result) for a field of ServiceStatusCache
    """
    # pylint: disable=unused-argument,too-many-branches
    return service_status.ssc_field_result(log, field_name)


def print_services(log, clownfish_instance, services, status=False,
                   print_table=True, field_string=None):
    """
    Print table of services
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if not print_table and len(services) > 1:
        log.cl_error("failed to print non-table output with multiple services")
        return -1

    quick_fields = [clownf_constant.CLOWNF_FIELD_SERVICE,
                    clownf_constant.CLOWNF_FIELD_BACKFS_TYPE]
    slow_fields = [clownf_constant.CLOWNF_FIELD_MOUNTED,
                   clownf_constant.CLOWNF_FIELD_MOUNT_HOST]
    none_table_fields = [clownf_constant.CLOWNF_FIELD_ENABLED_HOSTS,
                         clownf_constant.CLOWNF_FIELD_AUTOSTART,
                         clownf_constant.CLOWNF_FIELD_WATCHER_HOST,
                         clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES]
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
        log.cl_error("invalid field string [%s] for service",
                     field_string)
        return -1

    cluster_status = ClusterStatusCache(clownfish_instance)
    if ((clownf_constant.CLOWNF_FIELD_ENABLED_HOSTS in field_names) or
            (clownf_constant.CLOWNF_FIELD_AUTOSTART in field_names) or
            (clownf_constant.CLOWNF_FIELD_WATCHER_HOST in field_names)):
        ret = cluster_status.csc_init_fields(log)
        if ret:
            log.cl_error("failed to init cluster status")
            return -1

    service_status_list = []
    for service in services:
        service_status = ServiceStatusCache(clownfish_instance, cluster_status,
                                            service)
        service_status_list.append(service_status)

    if (len(service_status_list) > 0 and
            not service_status_list[0].ssc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for service_status in service_status_list:
            args = (service_status, field_names)
            args_array.append(args)
            thread_id = "service_status_%s" % service_status.ssc_service.ls_service_name
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "service_status",
                                                    service_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for services",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, service_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                service_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    if cluster_status.csc_failed:
        rc = -1
    return rc


class InstanceStatusCache():
    """
    This object saves temporary status of a service instance.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status, instance):
        # Cluster status, ClusterStatusCache
        self.isc_cluster_status = cluster_status
        # Instance
        self.isc_clownfish_instance = clownfish_instance
        # Any failure when getting this cached status
        self.isc_failed = False
        # LustreService
        self.isc_service = instance.lsi_service
        # LustreServiceInstance
        self.isc_instance = instance
        # The mounted instance of the service. If not mounted, None.
        self.isc_mounted_instance = None
        # Whether ssc_mounted_instance is inited
        self.isc_mounted_instance_inited = False
        #  Whether the instance is enabled. Negative if error.
        self.isc_enabled = None

    def isc_init_mounted_instance(self, log):
        """
        Init isc_mounted_instance.
        """
        if self.isc_mounted_instance_inited:
            return
        service = self.isc_service
        self.isc_mounted_instance = service.ls_mounted_instance(log)
        self.isc_mounted_instance_inited = True

    def isc_init_enabled(self, log):
        """
        Init isc_enabled.
        """
        service_name = self.isc_service.ls_service_name
        hostname = self.isc_instance.lsi_hostname
        cluster_status = self.isc_cluster_status
        consul_hostname = cluster_status.csc_consul_hostname(log)
        if consul_hostname is None:
            self.isc_enabled = -1
            return
        clownfish_instance = self.isc_clownfish_instance
        self.isc_enabled =\
            clownfish_instance.ci_service_host_check_enabled(log,
                                                             service_name,
                                                             hostname,
                                                             consul_hostname=consul_hostname)
        if self.isc_enabled < 0:
            self.isc_failed = True

    def isc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        for field in field_names:
            if field == clownf_constant.CLOWNF_FIELD_SERVICE:
                continue
            if field == clownf_constant.CLOWNF_FIELD_DEVICE:
                continue
            if field == clownf_constant.CLOWNF_FIELD_MOUNT_POINT:
                continue
            if field == clownf_constant.CLOWNF_FIELD_MOUNTED:
                self.isc_init_mounted_instance(log)
            elif field == clownf_constant.CLOWNF_FIELD_ENABLED:
                self.isc_init_enabled(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def isc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        instance = self.isc_instance
        service = self.isc_service
        service_name = service.ls_service_name
        device = instance.lsi_device
        mount_point = instance.lsi_mnt
        ret = 0
        if field_name == clownf_constant.CLOWNF_FIELD_SERVICE:
            result = service_name
        elif field_name == clownf_constant.CLOWNF_FIELD_DEVICE:
            result = device
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNT_POINT:
            result = mount_point
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNTED:
            if not self.isc_mounted_instance_inited:
                log.cl_error("mounted instance of service [%s] is not inited",
                             service_name)
                ret = -1
                result = clog.ERROR_MSG
            elif self.isc_mounted_instance is None:
                result = clog.colorful_message(clog.COLOR_RED,
                                               clownf_constant.CLOWNF_VALUE_UMOUNTED)
            elif self.isc_mounted_instance == instance:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_LOCAL_MOUNTED)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_REMOTE_MOUNTED)
        elif field_name == clownf_constant.CLOWNF_FIELD_ENABLED:
            if self.isc_enabled is None:
                log.cl_error("instance enabled status of service [%s] is "
                             "not inited",
                             service_name)
                ret = -1
                result = clog.ERROR_MSG
            elif self.isc_enabled < 0:
                result = clog.ERROR_MSG
            elif self.isc_enabled:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_ENABLED)
            else:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_DISABLED)
        else:
            log.cl_error("unknown field [%s] of service")
            result = clog.ERROR_MSG
            ret = -1

        if self.isc_failed:
            ret = -1
        return ret, result

    def isc_can_skip_init_fields(self, field_names):
        """
        Whether isc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]

        fields.remove(clownf_constant.CLOWNF_FIELD_SERVICE)
        fields.remove(clownf_constant.CLOWNF_FIELD_DEVICE)
        fields.remove(clownf_constant.CLOWNF_FIELD_MOUNT_POINT)
        if len(fields) == 0:
            return True
        return False


def instance_status_init(log, workspace, instance_status, field_names):
    """
    Init status of a instance
    """
    # pylint: disable=unused-argument
    return instance_status.isc_init_fields(log, field_names)


def instance_status_field(log, instance_status, field_name):
    """
    Return (0, result) for a field of ServiceStatusCache
    """
    # pylint: disable=unused-argument,too-many-branches
    return instance_status.isc_field_result(log, field_name)


def print_instances(log, clownfish_instance, instances, status=False,
                    print_table=True, field_string=None):
    """
    Print table of service instances
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if not print_table and len(instances) > 1:
        log.cl_error("failed to print non-table output with multiple instances")
        return -1

    quick_fields = [clownf_constant.CLOWNF_FIELD_SERVICE, clownf_constant.CLOWNF_FIELD_DEVICE, clownf_constant.CLOWNF_FIELD_MOUNT_POINT]
    slow_fields = [clownf_constant.CLOWNF_FIELD_MOUNTED, clownf_constant.CLOWNF_FIELD_ENABLED]
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
        log.cl_error("invalid field string [%s] for service instance",
                     field_string)
        return -1

    cluster_status = ClusterStatusCache(clownfish_instance)
    if (clownf_constant.CLOWNF_FIELD_ENABLED in field_names):
        ret = cluster_status.csc_init_fields(log)
        if ret:
            log.cl_error("failed to init cluster status")
            return -1

    instance_status_list = []
    for instance in instances:
        instance_status = InstanceStatusCache(clownfish_instance, cluster_status,
                                              instance)
        instance_status_list.append(instance_status)

    if (len(instance_status_list) > 0 and
            not instance_status_list[0].isc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for instance_status in instance_status_list:
            args = (instance_status, field_names)
            args_array.append(args)
            instance = instance_status.isc_instance
            service = instance.lsi_service
            service_name = service.ls_service_name
            thread_id = "instance_status_%s" % service_name
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "instance_status",
                                                    instance_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for instances",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, instance_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                instance_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    if cluster_status.csc_failed:
        rc = -1
    return rc


def instance_print(log, clownfish_instance, service_name, hostname):
    """
    Print the status of a service instance on a host
    """
    service = clownfish_instance.ci_lustre_name2service(log, service_name)
    if service is None:
        log.cl_error("service [%s] is not configured",
                     service_name)
        return -1
    if hostname not in service.ls_instance_dict:
        log.cl_error("service [%s] has no instance on host [%s]",
                     service_name, hostname)
        return -1
    instance = service.ls_instance_dict[hostname]
    instances = [instance]
    rc = print_instances(log, clownfish_instance, instances, status=True,
                         print_table=False)
    return rc


def _instance_enable_disable(log, clownfish_instance, hostname, service_name,
                             enable=True):
    """
    Enable or disable a service instance on a host
    """
    service = clownfish_instance.ci_lustre_name2service(log, service_name)
    if service is None:
        log.cl_error("service [%s] is not configured",
                     service_name)
        return -1
    if hostname not in service.ls_instance_dict:
        log.cl_error("service [%s] has no instance on host [%s]",
                     service_name, hostname)
        return -1
    ret = clownfish_instance.ci_host_enable_disable_service(log, hostname,
                                                            service_name,
                                                            enable=enable)
    if ret:
        return -1
    return 0


def instance_enable(log, clownfish_instance, hostname, service_name):
    """
    Enable a service instance on a host
    """
    return _instance_enable_disable(log, clownfish_instance, hostname,
                                    service_name, enable=True)


def instance_disable(log, clownfish_instance, hostname, service_name):
    """
    Disable a service instance on a host
    """
    return _instance_enable_disable(log, clownfish_instance, hostname,
                                    service_name, enable=False)


class LustreFilesystemStatusCache():
    """
    This object saves temporary status of a Lustre file system.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status, lustrefs):
        # Cluster status, ClusterStatusCache
        self.lfsc_cluster_status = cluster_status
        # Instance
        self.lfsc_clownfish_instance = clownfish_instance
        # Any failure when getting this cached status
        self.lfsc_failed = False
        # LustreFilesysem
        self.lfsc_lustrefs = lustrefs
        # Mounted instances of this file system. A list of
        # LustreServiceInstance.
        self.lfsc_mounted_instances = None
        # Mounted clients of this file system. A list of
        # LustreClient.
        self.lfsc_mounted_clients = None
        # True or False.
        self.lfsc_symmetric = None
        # If lfsc_symmetric is False, doesn't matter. Negative on error.
        self.lfsc_load_balanced = None
        # LustreHost.LSH_*
        self.lfsc_health_status = None

    def lfsc_init_mounted_instances(self, log):
        """
        Init lfsc_mounted_instances
        """
        self.lfsc_mounted_instances = []
        for service in self.lfsc_lustrefs.lf_service_dict.values():
            mounted_instance = service.ls_mounted_instance(log)
            if mounted_instance is not None:
                self.lfsc_mounted_instances.append(mounted_instance)

    def lfsc_init_mounted_clients(self, log):
        """
        Init lfsc_mounted_instances
        """
        self.lfsc_mounted_clients = []
        for client in self.lfsc_lustrefs.lf_client_dict.values():
            ret = client.lc_check_mounted(log)
            if ret < 0:
                log.cl_error("failed to check whether client [%s] is mounted",
                             client.lc_client_name)
                self.lfsc_failed = True
            elif ret:
                self.lfsc_mounted_clients.append(client)

    def lfsc_init_load_balanced(self, log):
        """
        Init lfsc_load_balanced and lfsc_symmetric
        """
        clownfish_instance = self.lfsc_clownfish_instance
        self.lfsc_symmetric = clownfish_instance.ci_is_symmetric(log)
        if not self.lfsc_symmetric:
            return

        host_dict = self.lfsc_lustrefs.lf_host_dict()
        self.lfsc_load_balanced = 1
        for host in host_dict.values():
            load_status = host.lsh_get_load_status(log)
            if load_status is None:
                log.cl_error("failed to check whether host [%s] is "
                             "load balanced",
                             host.sh_hostname)
                self.lfsc_failed = True
                self.lfsc_load_balanced = -1
            elif load_status != 0:
                self.lfsc_load_balanced = 0
                break

    def lfsc_init_health_status(self, log):
        """
        Init lfsc_health_status
        """
        host_dict = self.lfsc_lustrefs.lf_host_dict()
        self.lfsc_health_status = lustre.LustreHost.LSH_HEALTHY
        for host in host_dict.values():
            healty_result = host.lsh_healty_check(log)
            if healty_result == lustre.LustreHost.LSH_ERROR:
                log.cl_error("failed to check whether host [%s] is healthy",
                             host.sh_hostname)
                self.lfsc_health_status = lustre.LustreHost.LSH_ERROR
                self.lfsc_failed = True
                break
            if healty_result != lustre.LustreHost.LSH_HEALTHY:
                self.lfsc_health_status = healty_result
                break

    def lfsc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        for field in field_names:
            if field == clownf_constant.CLOWNF_FIELD_FSNAME:
                continue
            if field == clownf_constant.CLOWNF_FIELD_MOUNTED_SERVICES:
                self.lfsc_init_mounted_instances(log)
            elif field == clownf_constant.CLOWNF_FIELD_MOUNTED_CLIENTS:
                self.lfsc_init_mounted_clients(log)
            elif field == clownf_constant.CLOWNF_FIELD_LOAD_BALANCED:
                self.lfsc_init_load_balanced(log)
            elif field == clownf_constant.CLOWNF_FIELD_HEALTHY:
                self.lfsc_init_health_status(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def lfsc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        lustrefs = self.lfsc_lustrefs
        fsname = lustrefs.lf_fsname
        ret = 0
        if field_name == clownf_constant.CLOWNF_FIELD_FSNAME:
            result = fsname
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNTED_SERVICES:
            if self.lfsc_mounted_instances is None:
                log.cl_error("mounted instances of fs [%s] is not inited",
                             fsname)
                ret = -1
                result = clog.ERROR_MSG
            else:
                mounted_number = len(self.lfsc_mounted_instances)
                service_number = len(lustrefs.lf_service_dict)
                if service_number == mounted_number:
                    color = clog.COLOR_GREEN
                else:
                    color = clog.COLOR_RED
                result = clog.colorful_message(color,
                                               str(mounted_number))
                result += "/" + str(service_number)
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNTED_CLIENTS:
            if self.lfsc_mounted_clients is None:
                log.cl_error("mounted clients of fs [%s] is not inited",
                             fsname)
                ret = -1
                result = clog.ERROR_MSG
            else:
                mounted_number = len(self.lfsc_mounted_clients)
                service_number = len(lustrefs.lf_client_dict)
                if service_number == mounted_number:
                    color = clog.COLOR_GREEN
                else:
                    color = clog.COLOR_RED
                result = clog.colorful_message(color,
                                               str(mounted_number))
                result += "/" + str(service_number)
        elif field_name == clownf_constant.CLOWNF_FIELD_LOAD_BALANCED:
            if self.lfsc_symmetric is None:
                log.cl_error("load balanced status of fs [%s] is not inited",
                             fsname)
                ret = -1
                result = clog.ERROR_MSG
            elif not self.lfsc_symmetric:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_UNKNOWN)
            elif self.lfsc_load_balanced < 0:
                result = clog.ERROR_MSG
            elif self.lfsc_load_balanced:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_BALANCED)
            else:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               clownf_constant.CLOWNF_VALUE_IMBALANCED)
        elif field_name == clownf_constant.CLOWNF_FIELD_HEALTHY:
            health_status = self.lfsc_health_status
            if health_status is None:
                log.cl_error("health status of fs [%s] is not inited",
                             fsname)
                ret = -1
                result = clog.ERROR_MSG
            elif health_status == lustre.LustreHost.LSH_HEALTHY:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               health_status)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               health_status)
        else:
            log.cl_error("unknown field [%s] of Lustre file system",
                         field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.lfsc_failed:
            ret = -1
        return ret, result

    def lfsc_can_skip_init_fields(self, field_names):
        """
        Whether lfsc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_FSNAME)
        if len(fields) == 0:
            return True
        return False


def fs_status_init(log, workspace, fs_status, field_names):
    """
    Init status of a fs
    """
    # pylint: disable=unused-argument
    return fs_status.lfsc_init_fields(log, field_names)


def fs_status_field(log, fs_status, field_name):
    """
    Return (0, result) for a field of LustreFilesystemStatusCache
    """
    # pylint: disable=unused-argument
    return fs_status.lfsc_field_result(log, field_name)


def print_lustres(log, clownfish_instance, lustres, status=False,
                  print_table=True, field_string=None):
    """
    Print table of LustreFilesystem.
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if not print_table and len(lustres) > 1:
        log.cl_error("failed to print non-table output with multiple "
                     "file systems")
        return -1

    quick_fields = [clownf_constant.CLOWNF_FIELD_FSNAME]
    slow_fields = [clownf_constant.CLOWNF_FIELD_MOUNTED_SERVICES,
                   clownf_constant.CLOWNF_FIELD_MOUNTED_CLIENTS,
                   clownf_constant.CLOWNF_FIELD_LOAD_BALANCED,
                   clownf_constant.CLOWNF_FIELD_HEALTHY]
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
        log.cl_error("invalid field string [%s] for Lustre file system",
                     field_string)
        return -1

    cluster_status = ClusterStatusCache(clownfish_instance)

    fs_status_list = []
    for lustrefs in lustres:
        fs_status = LustreFilesystemStatusCache(clownfish_instance,
                                                cluster_status,
                                                lustrefs)
        fs_status_list.append(fs_status)

    if (len(fs_status_list) > 0 and
            not fs_status_list[0].lfsc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for fs_status in fs_status_list:
            args = (fs_status, field_names)
            args_array.append(args)
            lustrefs = fs_status.lfsc_lustrefs
            fsname = lustrefs.lf_fsname
            thread_id = "fs_status_%s" % fsname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "fs_status",
                                                    fs_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for file systems",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, fs_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                fs_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    if cluster_status.csc_failed:
        rc = -1
    return rc


class LustreClientStatusCache():
    """
    This object saves temporary status of a host.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status, lustre_client):
        # Instance
        self.lcsc_clownfish_instance = clownfish_instance
        # Any failure when getting this cached status
        self.lcsc_failed = False
        # LustreHost
        self.lcsc_host = lustre_client.lc_host
        # Status of the host
        self.lcsc_host_status = HostStatusCache(clownfish_instance,
                                                cluster_status,
                                                self.lcsc_host)
        # Status of the cluster
        self.lcsc_cluster_status = cluster_status
        # Whether any failure happened
        self.lcsc_failed = False
        # Lustre client
        self.lcsc_lustre_client = lustre_client
        # Whether the client is mounted
        self.lcsc_mounted = None

    def lcsc_init_mounted(self, log):
        """
        Init lcsc_mounted
        """
        client = self.lcsc_lustre_client
        self.lcsc_mounted = client.lc_check_mounted(log)
        if self.lcsc_mounted < 0:
            log.cl_error("failed to check whether Lustre client [%s] is mounted",
                         self.lcsc_lustre_client.lc_client_name)
            self.lcsc_failed = True

    def lcsc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        for field in field_names:
            if field == clownf_constant.CLOWNF_FIELD_CLIENT:
                continue
            if field == clownf_constant.CLOWNF_FIELD_FSNAME:
                continue
            if field == clownf_constant.CLOWNF_FIELD_MOUNTED:
                self.lcsc_init_mounted(log)
            elif field == clownf_constant.CLOWNF_FIELD_HEALTHY:
                self.lcsc_host_status.hsc_init_health_status(log)
                if self.lcsc_host_status.hsc_failed:
                    self.lcsc_failed = True
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def lcsc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        ret = 0
        client = self.lcsc_lustre_client
        client_name = client.lc_client_name
        if field_name == clownf_constant.CLOWNF_FIELD_CLIENT:
            result = client_name
        elif field_name == clownf_constant.CLOWNF_FIELD_FSNAME:
            result = client.lc_lustre_fs.lf_fsname
        elif field_name == clownf_constant.CLOWNF_FIELD_MOUNTED:
            if self.lcsc_mounted is None:
                log.cl_error("mounted status of Lustre client [%s] is not inited",
                             client_name)
                ret = -1
                result = clog.ERROR_MSG
            elif self.lcsc_mounted < 0:
                result = clog.ERROR_MSG
            elif self.lcsc_mounted:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_MOUNTED)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               clownf_constant.CLOWNF_VALUE_UMOUNTED)
        elif field_name == clownf_constant.CLOWNF_FIELD_HEALTHY:
            host_status = self.lcsc_host_status
            ret, result = host_status.hsc_field_result(log, field_name)
        else:
            log.cl_error("unknown field [%s] of Lustre client",
                         field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.lcsc_failed:
            ret = -1
        return ret, result

    def lcsc_can_skip_init_fields(self, field_names):
        """
        Whether lcsc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_CLIENT)
        fields.remove(clownf_constant.CLOWNF_FIELD_FSNAME)
        if len(fields) == 0:
            return True
        return False


def client_status_init(log, workspace, client_status, field_names):
    """
    Init status of a client
    """
    # pylint: disable=unused-argument
    return client_status.lcsc_init_fields(log, field_names)


def client_status_field(log, client_status, field_name):
    """
    Return (0, result) for a field of LustreClientStatusCache
    """
    # pylint: disable=unused-argument,too-many-branches
    return client_status.lcsc_field_result(log, field_name)


def print_clients(log, clownfish_instance, clients, print_status=False,
                  print_table=True, field_string=None):
    """
    Print table of clients
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    # pylint: disable=too-many-nested-blocks
    quick_fields = [clownf_constant.CLOWNF_FIELD_CLIENT,
                    clownf_constant.CLOWNF_FIELD_FSNAME]
    slow_fields = [clownf_constant.CLOWNF_FIELD_MOUNTED,
                   clownf_constant.CLOWNF_FIELD_HEALTHY]
    none_table_fields = []
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=print_status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for host",
                     field_string)
        return -1

    cluster_status = ClusterStatusCache(clownfish_instance)
    client_status_list = []
    for client in clients:
        client_status = LustreClientStatusCache(clownfish_instance,
                                                cluster_status,
                                                client)
        client_status_list.append(client_status)

    if (len(client_status_list) > 0 and
            not client_status_list[0].lcsc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for client_status in client_status_list:
            args = (client_status, field_names)
            args_array.append(args)
            thread_id = ("client_status_%s" %
                         client_status.lcsc_lustre_client.lc_client_name)
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "client_status",
                                                    client_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for Lustre clients",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, client_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                client_status_field,
                                print_table=print_table,
                                print_status=print_status,
                                field_string=field_string)
    if cluster_status.csc_failed:
        rc = -1
    return rc


class ServiceHostAutostartStatusCache():
    """
    The autostart status of a service/host.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status,
                 host_service, is_host):
        # Instance
        self.shasc_clownfish_instance = clownfish_instance
        # Service or host
        # Whether this name is host or service
        self.shasc_is_host = is_host
        # Cluster status, ClusterStatusCache
        self.shasc_cluster_status = cluster_status
        if is_host:
            self.shasc_host = host_service
            self.shasc_service = None
            self.shasc_service_status = None
            self.shasc_host_status = HostStatusCache(clownfish_instance,
                                                     cluster_status,
                                                     host_service)
            self.shasc_name = host_service.sh_hostname
        else:
            self.shasc_service_status = ServiceStatusCache(clownfish_instance,
                                                           cluster_status,
                                                           host_service)
            self.shasc_host_status = None
            self.shasc_host = None
            self.shasc_service = host_service
            self.shasc_name = host_service.ls_service_name

    def shasc_can_skip_init_fields(self, field_names):
        """
        Whether shasc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_NAME)
        fields.remove(clownf_constant.CLOWNF_FIELD_HOST_SERVICE)
        if len(fields) == 0:
            return True
        return False

    def shasc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_NAME)
        fields.remove(clownf_constant.CLOWNF_FIELD_HOST_SERVICE)
        if self.shasc_is_host:
            ret = self.shasc_host_status.hsc_init_fields(log, fields)
        else:
            ret = self.shasc_service_status.ssc_init_fields(log, fields)
        return ret

    def shasc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        ret = 0
        if field_name == clownf_constant.CLOWNF_FIELD_NAME:
            result = self.shasc_name
        elif field_name == clownf_constant.CLOWNF_FIELD_HOST_SERVICE:
            if self.shasc_is_host:
                result = clownf_constant.CLOWNF_VALUE_HOST
            else:
                result = clownf_constant.CLOWNF_VALUE_SERVICE
        else:
            if self.shasc_is_host:
                ret, result = \
                    self.shasc_host_status.hsc_field_result(log, field_name)
            else:
                ret, result = \
                    self.shasc_service_status.ssc_field_result(log, field_name)
        return ret, result


def autostart_status_init(log, workspace, autostart_status, field_names):
    """
    Init status of a ServiceHostAutostartStatusCache
    """
    # pylint: disable=unused-argument
    return autostart_status.shasc_init_fields(log, field_names)


def autostart_status_field(log, autostart_status, field_name):
    """
    Return (0, result) for a field of ServiceHostAutostartStatusCache
    """
    # pylint: disable=unused-argument,too-many-branches
    return autostart_status.shasc_field_result(log, field_name)


def print_autostart(log, clownfish_instance, hosts=None, services=None,
                    print_table=True, full=False, field_string=None):
    """
    Print table of autostart status.
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if hosts is None:
        if services is None:
            log.cl_error("hosts and services are both None")
            return -1
        number = len(services)
    else:
        number = len(hosts)
    if not print_table and number > 1:
        log.cl_error("failed to print non-table output with multiple "
                     "hosts/services")
        return -1

    quick_fields = [clownf_constant.CLOWNF_FIELD_NAME,
                    clownf_constant.CLOWNF_FIELD_HOST_SERVICE,
                    clownf_constant.CLOWNF_FIELD_AUTOSTART]
    slow_fields = [clownf_constant.CLOWNF_FIELD_WATCHER_HOST,
                   clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES]
    none_table_fields = []
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=full,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for autostart",
                     field_string)
        return -1

    cluster_status = ClusterStatusCache(clownfish_instance)
    if (clownf_constant.CLOWNF_FIELD_AUTOSTART in field_names or
            clownf_constant.CLOWNF_FIELD_WATCHER_HOST in field_names or
            clownf_constant.CLOWNF_FIELD_WATCHER_CANDIDATES in field_names):
        ret = cluster_status.csc_init_fields(log)
        if ret:
            log.cl_error("failed to init cluster status")
            return -1

    autostart_status_list = []
    if hosts is not None:
        for host in hosts:
            autostart_status = ServiceHostAutostartStatusCache(clownfish_instance,
                                                               cluster_status,
                                                               host, True)
            autostart_status_list.append(autostart_status)

    if services is not None:
        for service in services:
            autostart_status = ServiceHostAutostartStatusCache(clownfish_instance,
                                                               cluster_status,
                                                               service, False)
            autostart_status_list.append(autostart_status)

    if (len(autostart_status_list) > 0 and
            not autostart_status_list[0].shasc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for autostart_status in autostart_status_list:
            args = (autostart_status, field_names)
            args_array.append(args)
            thread_id = "autostart_status_%s" % autostart_status.shasc_name
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "autostart_status",
                                                    autostart_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for autotart status",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, autostart_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                autostart_status_field,
                                print_table=print_table,
                                print_status=full,
                                field_string=field_string)
    if cluster_status.csc_failed:
        rc = -1
    return rc


class WatcherStatusCache(HostStatusCache):
    """
    This object saves temporary status of a watcher host.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status, host,
                 watcher_hostname):
        super().__init__(clownfish_instance, cluster_status, host)
        self.wsc_is_watcher = bool(host.sh_hostname == watcher_hostname)

    def wsc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_WATCHER)
        return self.hsc_init_fields(log, fields)

    def wsc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        if field_name == clownf_constant.CLOWNF_FIELD_WATCHER:
            if self.wsc_is_watcher:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_WATCHER)
            else:
                result = clownf_constant.CLOWNF_VALUE_CANDIDATE
            ret = 0
        else:
            ret, result = self.hsc_field_result(log, field_name)
        return ret, result


def watcher_status_init(log, workspace, watcher_status, field_names):
    """
    Init status of a watcher host
    """
    # pylint: disable=unused-argument
    return watcher_status.wsc_init_fields(log, field_names)


def watcher_status_field(log, watcher_status, field_name):
    """
    Return (0, result) for a field of WatcherStatusCache
    """
    # pylint: disable=unused-argument,too-many-branches
    return watcher_status.wsc_field_result(log, field_name)


def print_watchers(log, clownfish_instance, watcher_hostname,
                   candidate_hosts, print_status=False, print_table=True,
                   field_string=None):
    """
    Print table of watcher hosts
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    # pylint: disable=too-many-nested-blocks
    quick_fields = [clownf_constant.CLOWNF_FIELD_HOST,
                    clownf_constant.CLOWNF_FIELD_WATCHER,
                    clownf_constant.CLOWNF_FIELD_UP,
                    clownf_constant.CLOWNF_FIELD_AGENT]
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
                                                 print_status=print_status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for host",
                     field_string)
        return -1

    # No field needs to check Consul
    cluster_status = ClusterStatusCache(clownfish_instance)
    host_status_list = []
    for host in candidate_hosts:
        watcher_status = WatcherStatusCache(clownfish_instance,
                                            cluster_status,
                                            host, watcher_hostname)
        host_status_list.append(watcher_status)

    if (len(host_status_list) > 0 and
            not host_status_list[0].hsc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for host_status in host_status_list:
            args = (host_status, field_names)
            args_array.append(args)
            thread_id = "host_status_%s" % host_status.hsc_host.sh_hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "host_status",
                                                    watcher_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for hosts",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, host_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                watcher_status_field,
                                print_table=print_table,
                                print_status=print_status,
                                field_string=field_string)
    if cluster_status.csc_failed:
        rc = -1
    return rc


def print_watching(log, clownfish_instance, hosts):
    """
    Print table about how many hosts/services each host is watching
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    field_string = (clownf_constant.CLOWNF_FIELD_HOST + "," +
                    clownf_constant.CLOWNF_FIELD_WATCHING_HOSTS + "," +
                    clownf_constant.CLOWNF_FIELD_CWATCHING_HOSTS + "," +
                    clownf_constant.CLOWNF_FIELD_WATCHING_SERVICES + "," +
                    clownf_constant.CLOWNF_FIELD_CWATCHING_SERVICES)
    rc = print_hosts(log, clownfish_instance, hosts,
                     field_string=field_string)
    return rc


def host_watcher_stdout(args, new_log, is_stdout=True):
    """
    Watch log dump to clog.cl_stdout or clog.cl_error
    """
    if len(new_log) == 0:
        return
    log = args[watched_io.WATCHEDIO_LOG]
    hostname = args[watched_io.WATCHEDIO_HOSTNAME]
    prefix = clog.colorful_message(clog.COLOR_LIGHT_BLUE,
                                   hostname + ": ")
    for line in new_log.splitlines():
        if is_stdout:
            log.cl_stdout(prefix + line)
        else:
            log.cl_error(prefix + line)


def host_watcher_stderr(args, new_log):
    """
    Watch log dump to clog.cl_error
    """
    return host_watcher_stdout(args, new_log, is_stdout=False)


def host_exec(log, workspace, host, command):
    """
    Run command on host
    """
    # pylint: disable=unused-argument
    retval = host.sh_watched_run(log, command, None, None,
                                 return_stdout=False, return_stderr=False,
                                 stdout_watch_func=host_watcher_stdout,
                                 stderr_watch_func=host_watcher_stderr)
    return retval.cr_exit_status


def hosts_exec(log, hosts, command):
    """
    Run command on all hosts
    """
    args_array = []
    thread_ids = []
    for host in hosts:
        args = (host, command)
        args_array.append(args)
        thread_id = "%s" % host.sh_hostname
        thread_ids.append(thread_id)

    parallel_execute = parallel.ParallelExecute(None,
                                                "exec",
                                                host_exec,
                                                args_array,
                                                thread_ids=thread_ids)
    ret = parallel_execute.pe_run(log, quit_on_error=False, parallelism=10)

    return ret
