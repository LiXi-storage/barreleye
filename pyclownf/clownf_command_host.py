"""
Host commands of Clownf
"""
# Local libs
from pycoral import clog
from pycoral import parallel
from pycoral import cmd_general
from pyclownf import clownf_command_common
from pyclownf import clownf_constant


class WatchingServiceHostStatusCache():
    """
    The status of a service/host that watched by a host.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, cluster_status, host,
                 host_service_name, is_host):
        # LustreHost that might be watching this host/service
        self.wshsc_host = host
        # Instance
        self.wshsc_clownfish_instance = clownfish_instance
        # Service name or host name that is being watched
        self.wshsc_host_service_name = host_service_name
        # Whether this name is host or service
        self.wshsc_is_host = is_host
        # Cluster status, ClusterStatusCache
        self.wshsc_cluster_status = cluster_status
        # Whether this service/host is being watching by the target host.
        # Negative on error.
        self.wshsc_watching = None
        # Whether any failure happened
        self.wshsc_failed = False
        # Whether the autostart of this service/host is enabled.
        # Negative on error.
        self.wshsc_autostart_status = None

    def wshsc_can_skip_init_fields(self, field_names):
        """
        Whether wshsc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(clownf_constant.CLOWNF_FIELD_NAME)
        fields.remove(clownf_constant.CLOWNF_FIELD_HOST_SERVICE)
        if len(fields) == 0:
            return True
        return False

    def wshsc_init_watching(self, log):
        """
        Init wshsc_watching
        """
        cluster_status = self.wshsc_cluster_status
        if self.wshsc_is_host:
            watcher_dict = cluster_status.csc_host_watcher_dict
        else:
            watcher_dict = cluster_status.csc_service_watcher_dict
        if watcher_dict is None:
            log.cl_error("the watcher status of cluster is not inited")
            self.wshsc_watching = -1
            self.wshsc_failed = True
            return
        if self.wshsc_host_service_name not in watcher_dict:
            self.wshsc_watching = 0
            return
        watcher = watcher_dict[self.wshsc_host_service_name]
        if watcher == self.wshsc_host.sh_hostname:
            self.wshsc_watching = 1
        else:
            self.wshsc_watching = 0

    def wshsc_init_autostart_status(self, log):
        """
        Init wshsc_autostart_status
        """
        clownfish_instance = self.wshsc_clownfish_instance
        cluster_status = self.wshsc_cluster_status
        consul_hostname = cluster_status.csc_consul_hostname(log)
        if consul_hostname is None:
            self.wshsc_autostart_status = -1
            return

        name = self.wshsc_host_service_name
        if self.wshsc_is_host:
            self.wshsc_autostart_status = \
                clownfish_instance.ci_host_autostart_check_enabled(log,
                                                                   name,
                                                                   consul_hostname=consul_hostname)
        else:
            self.wshsc_autostart_status = \
                clownfish_instance.ci_service_autostart_check_enabled(log,
                                                                      name,
                                                                      consul_hostname=consul_hostname)
        if self.wshsc_autostart_status < 0:
            log.cl_error("failed to get the autostart config of service/host [%s]",
                         name)
            self.wshsc_failed = True

    def wshsc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        for field in field_names:
            if field == clownf_constant.CLOWNF_FIELD_NAME:
                continue
            if field == clownf_constant.CLOWNF_FIELD_HOST_SERVICE:
                continue
            if field == clownf_constant.CLOWNF_FIELD_WATCHING:
                self.wshsc_init_watching(log)
            elif field == clownf_constant.CLOWNF_FIELD_AUTOSTART:
                self.wshsc_init_autostart_status(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def wshsc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        ret = 0
        if field_name == clownf_constant.CLOWNF_FIELD_NAME:
            result = self.wshsc_host_service_name
        elif field_name == clownf_constant.CLOWNF_FIELD_HOST_SERVICE:
            if self.wshsc_is_host:
                result = clownf_constant.CLOWNF_VALUE_HOST
            else:
                result = clownf_constant.CLOWNF_VALUE_SERVICE
        elif field_name == clownf_constant.CLOWNF_FIELD_WATCHING:
            if self.wshsc_watching is None:
                log.cl_error("watching status of host/service [%s] is not inited",
                             self.wshsc_host_service_name)
                result = clog.ERROR_MSG
                ret = -1
            elif self.wshsc_watching < 0:
                result = clog.ERROR_MSG
            elif self.wshsc_watching:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_WATCHING)
            else:
                result = clownf_constant.CLOWNF_VALUE_NOT_WATCHING
        elif field_name == clownf_constant.CLOWNF_FIELD_AUTOSTART:
            if self.wshsc_autostart_status is None:
                log.cl_error("autostart status of host/service [%s] is not inited",
                             self.wshsc_host_service_name)
                result = clog.ERROR_MSG
                ret = -1
            elif self.wshsc_autostart_status < 0:
                result = clog.ERROR_MSG
            elif self.wshsc_autostart_status:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               clownf_constant.CLOWNF_VALUE_AUTOSTART)
            else:
                result = clownf_constant.CLOWNF_VALUE_NOT_AUTOSTART
        else:
            log.cl_error("unknown field [%s]", field_name)
            result = clog.ERROR_MSG
            ret = -1
        if self.wshsc_failed:
            ret = -1
        return ret, result


def watching_status_init(log, workspace, watching_status, field_names):
    """
    Init status of a watching host/service
    """
    # pylint: disable=unused-argument
    return watching_status.wshsc_init_fields(log, field_names)


def watching_status_field(log, watching_status, field_name):
    """
    Return (0, result) for a field of WatchingServiceHostStatusCache
    """
    # pylint: disable=unused-argument,too-many-branches
    return watching_status.wshsc_field_result(log, field_name)


def print_host_watching(log, clownfish_instance, host, print_status=False,
                        print_table=True, field_string=None):
    """
    Print the host/service this host is watching
    """
    # pylint: disable=too-many-branches,too-many-statements,too-many-locals
    quick_fields = [clownf_constant.CLOWNF_FIELD_NAME,
                    clownf_constant.CLOWNF_FIELD_HOST_SERVICE,
                    clownf_constant.CLOWNF_FIELD_WATCHING,
                    clownf_constant.CLOWNF_FIELD_AUTOSTART]
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

    hostname = host.sh_hostname
    ret = 0
    candidate_hosts = \
        clownfish_instance.ci_host_watching_candidate_hosts(log, hostname)

    candidate_services = \
        clownfish_instance.ci_host_watching_candidate_services(log, host)

    cluster_status = clownf_command_common.ClusterStatusCache(clownfish_instance)
    watching_status_list = []
    for candidate_host in candidate_hosts:
        watching_status = WatchingServiceHostStatusCache(clownfish_instance,
                                                         cluster_status,
                                                         host,
                                                         candidate_host,
                                                         True)
        watching_status_list.append(watching_status)

    init_consul_hostname = False
    init_host_watchers = False
    init_service_watchers = False
    if clownf_constant.CLOWNF_FIELD_WATCHING in field_names:
        init_consul_hostname = True
        init_host_watchers = True
        init_service_watchers = True
    if clownf_constant.CLOWNF_FIELD_AUTOSTART in field_names:
        init_consul_hostname = True
    ret = cluster_status.csc_init_fields(log, init_consul_hostname=init_consul_hostname,
                                         init_host_watchers=init_host_watchers,
                                         init_service_watchers=init_service_watchers,
                                         only_hostnames=candidate_hosts,
                                         only_service_names=candidate_services)
    if ret:
        log.cl_error("failed to init cluster status")
        return -1

    for candidate_service in candidate_services:
        watching_status = WatchingServiceHostStatusCache(clownfish_instance,
                                                         cluster_status,
                                                         host,
                                                         candidate_service,
                                                         False)
        watching_status_list.append(watching_status)

    if (len(watching_status_list) > 0 and
            not watching_status_list[0].wshsc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for watching_status in watching_status_list:
            args = (watching_status, field_names)
            args_array.append(args)
            thread_id = ("watching_status_%s" %
                         watching_status.wshsc_host_service_name)
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "watching_status",
                                                    watching_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        rc = parallel_execute.pe_run(log, parallelism=10)
        if rc:
            log.cl_error("failed to init fields %s for hosts",
                         field_names)
            return -1

    ret = cmd_general.print_list(log, watching_status_list, quick_fields,
                                 slow_fields, none_table_fields,
                                 watching_status_field,
                                 print_table=print_table,
                                 print_status=print_status,
                                 field_string=field_string)
    if cluster_status.csc_failed:
        ret = -1
    return ret


class HostCommand():
    """
    Commands to manage hosts in Clownfish cluster.
    """
    # pylint: disable=too-many-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._hc_config_fpath = config
        self._hc_logdir = logdir
        self._hc_log_to_file = log_to_file
        self._hc_iso = iso

    def ls(self, status=False, fields=None):
        """
        List all configured hosts.
        :param status: print status of the hosts, default: False.
        :param fields: fields to print, seperated by comma.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        cmd_general.check_argument_bool(log, "status", status)
        cmd_general.check_argument_types(log, "fields", fields,
                                         allow_none=True,
                                         allow_tuple=True, allow_str=True,
                                         allow_bool=True)
        hosts = list(clownfish_instance.ci_host_dict.values())
        rc = clownf_command_common.print_hosts(log, clownfish_instance, hosts,
                                               print_status=status,
                                               field_string=fields)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def prepare(self, hostname, nolazy=False):
        """
        Prepare the host for providing Lustre service.

        All Lustre services on the host will be migrated to other hosts before
        preparation. All Kernel, Lustre, E2fsprogs and ZFS RPMs as well as
        their dependencies will be installed if they are not. Reboot of the
        host will be issued if need to run a different kernel.

        :param hostname: The name of the host to prepare.
        :param nolazy: Reinstall the RPMs even they are already installed, default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        cmd_general.check_argument_bool(log, "nolay", nolazy)
        rc = clownfish_instance.ci_host_prepare(log,
                                                clownfish_instance.ci_workspace,
                                                hostname,
                                                not nolazy)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def services(self, hostname, status=False):
        """
        List all services on a host
        :param status: print status of the services, default: False
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        cmd_general.check_argument_bool(log, "status", status)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        host = clownfish_instance.ci_host_dict[hostname]
        instances = list(host.lsh_instance_dict.values())
        rc = clownf_command_common.print_instances(log, clownfish_instance,
                                                   instances,
                                                   status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def services_migrate(self, hostname):
        """
        Migrate all Lustre services out of the host

        It is helpful to run this before rebooting the host gracefully.
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownfish_instance.ci_host_migrate_services(log, hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def services_enable(self, hostname):
        """
        Enable all Lustre services on the host

        Allow all Lustre services to mount on this host. After running this,
        autostart might start the Lustre services on this host (if auto-start
        is enabled).
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownfish_instance.ci_host_enable_services(log, hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def services_disable(self, hostname):
        """
        Disable all Lustre services on the host

        Do not allow any Lustre service to mount on this host. This will
        make sure that autostart won't start the Lustre services. Useful
        when want to migrate the services out of this host.
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownfish_instance.ci_host_disable_services(log, hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def service_status(self, hostname, service_name):
        """
        Print the status of a service instance on a host

        :param hostname: name of the host
        :param service_name: Lustre service name
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        rc = clownf_command_common.instance_print(log, clownfish_instance,
                                                  service_name, hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def service_disable(self, hostname, service_name):
        """
        Disable a service instance on a host

        :param hostname: name of the host
        :param service_name: Lustre service name
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        rc = clownf_command_common.instance_disable(log, clownfish_instance,
                                                    hostname, service_name)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def service_enable(self, hostname, service_name):
        """
        Enable a service instance on a host

        :param hostname: name of the host
        :param service_name: Lustre service name
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        rc = clownf_command_common.instance_enable(log, clownfish_instance,
                                                   hostname, service_name)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def operating(self, hostname):
        """
        Check whether any operation command is actively running

        If an operation command is running, print "Operating" and exit with
        status 0. Otherwise, print "Not operating" and exit with status 0.
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownfish_instance.ci_host_operating(log, hostname)
        if rc == 1:
            log.cl_stdout(clownf_constant.CLOWNF_STDOUT_OPERATING)
            rc = 0
        elif rc == 0:
            log.cl_stdout(clownf_constant.CLOWNF_STDOUT_NOT_OPERATING)
        else:
            log.cl_error("failed to check whether clownf command is running")
            rc = -1
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_enable(self, hostname):
        """
        Enable the autostart of the host
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownfish_instance.ci_host_autostart_enable(log,
                                                         hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_disable(self, hostname):
        """
        Disable the autostart of the host
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownfish_instance.ci_host_autostart_disable(log,
                                                          hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_status(self, hostname):
        """
        Print the autostart status of the host
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        host = clownfish_instance.ci_host_dict[hostname]
        hosts = [host]
        rc = clownf_command_common.print_autostart(log, clownfish_instance,
                                                   hosts=hosts,
                                                   print_table=False,
                                                   full=True)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def shutdown(self, hostname, yes=False, force=False, skip_migrate=False):
        """
        Shutdown the host.

        This command will migrate all the services on the host and then
        shutdown the host.
        :param hostname: Name of the host.
        :param yes: Do not ask for confirmation, just shutdown. Default: False
        :param force: Continue shutdown even hit failures like not able to
            migrate services. Default: False.
        :param skip_migrate: Shutdown the host without migrating services
            first. Default: False
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        cmd_general.check_argument_bool(log, "yes", yes)
        cmd_general.check_argument_bool(log, "force", force)
        cmd_general.check_argument_bool(log, "skip_migrate", skip_migrate)
        if not yes:
            migrate_string = ""
            if skip_migrate:
                migrate_string = " without migrating services"
            confirm = ("Are you sure to shutdown host \"%s\"%s? [y/N] " %
                       (hostname, migrate_string))
            input_result = input(confirm)
            if not input_result.startswith("y") and not input_result.startswith("Y"):
                log.cl_info("quiting without touching anything")
                clownf_command_common.exit_env(log, clownfish_instance, 0)
        rc = clownfish_instance.ci_host_shutdown(log, hostname, force=force,
                                                 skip_migrate=skip_migrate)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def start(self, hostname):
        """
        Start the host and wait util SSH can be connected.

        This command requires the libvirt/ipmi section configured for the host.
        :param hostname: The name of the host to start. A list with the
        format of "host[01-10],host100" can be specified.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        hostnames = cmd_general.parse_list_string(log, hostname)
        if hostnames is None:
            log.cl_error("hostname [%s] is an invalid list",
                         hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        rc = clownfish_instance.ci_hosts_start(log, hostnames)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def status(self, hostname):
        """
        Print the status of a host.

        :param hostname: Name of the host.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        host = clownfish_instance.ci_host_dict[hostname]
        hosts = [host]
        rc = clownf_command_common.print_hosts(log, clownfish_instance, hosts,
                                               print_status=True,
                                               print_table=False)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def watcher(self, hostname):
        """
        Print the watcher candidates of a host

        A watcher of a host would boot the host if the host is down for some
        reason (crash, poweroff etc.).
        :param hostname: name of the host that is being watched
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        rc, watcher = clownfish_instance.ci_host_watcher(log, hostname)
        if rc:
            log.cl_error("failed to get the watcher of host [%s]", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        watcher_candidates = \
            clownfish_instance.ci_host_watcher_candidates(log, hostname)

        candidate_hosts = []
        for watcher_candidate in watcher_candidates:
            if watcher_candidate not in clownfish_instance.ci_host_dict:
                log.cl_error("host [%s] is not configured but is a watcher "
                             "candidate", watcher_candidate)
                clownf_command_common.exit_env(log, clownfish_instance, -1)
            host = clownfish_instance.ci_host_dict[watcher_candidate]
            candidate_hosts.append(host)

        rc = clownf_command_common.print_watchers(log, clownfish_instance,
                                                  watcher, candidate_hosts)
        if rc:
            log.cl_error("failed to print watcher candidates of host [%s]",
                         hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        clownf_command_common.exit_env(log, clownfish_instance, 0)

    def watching(self, hostname):
        """
        Print the service/host names this host is watching.

        A host might watch multiple hosts and services.
        :param hostname: Name of the host.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        host = clownfish_instance.ci_host_dict[hostname]

        rc = print_host_watching(log, clownfish_instance, host)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def clients(self, hostname, status=False):
        """
        List all Lustre clients on the host.
        :param hostname: Name of the host.
        :param status: Print the status of clients, default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        cmd_general.check_argument_bool(log, "status", status)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        host = clownfish_instance.ci_host_dict[hostname]
        clients = list(host.lsh_clients.values())
        rc = clownf_command_common.print_clients(log, clownfish_instance,
                                                 clients,
                                                 print_status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)


    def run(self, hostname, command):
        """
        Run a command on a host
        :param hostname: name of the host
        :param command: command to run
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        command = cmd_general.check_argument_str(log, "command", command)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        host = clownfish_instance.ci_host_dict[hostname]
        rc = clownf_command_common.host_exec(log, clownfish_instance.ci_workspace,
                                             host, command)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def collect(self, hostname, path):
        """
        collect file/dir from a host
        :param hostname: name of the host
        :param path: the file/dir path to collect
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        path = cmd_general.check_argument_str(log, "path", path)
        if hostname not in clownfish_instance.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        host = clownfish_instance.ci_host_dict[hostname]
        logdir = clownfish_instance.ci_workspace + "/" + hostname
        rc = clownfish_instance.ci_host_collect(log, logdir, host, path)
        log.cl_info("log saved to [%s] on local host [%s]",
                    clownfish_instance.ci_workspace,
                    clownfish_instance.ci_local_host.sh_hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)


    def install(self, host):
        """
        Install the Clownfish packages on a host.

        All Clownfish RPMs, configuration files will cleaned up and
        re-configured on the host. Note that the Consul cluster will
        be cleaned up and re-configured in the whole cluster as a requirement
        of adding the host to the Consul cluster. Lustre RPMs not be
        installed. Lustre service will not be affected during this process.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._hc_config_fpath,
                                           self._hc_logdir,
                                           self._hc_log_to_file,
                                           self._hc_iso)
        host = cmd_general.check_argument_str(log, "host", host)
        hostnames = cmd_general.parse_list_string(log, host)
        hosts = []
        for hostname in hostnames:
            if hostname not in clownfish_instance.ci_host_dict:
                log.cl_error("host [%s] is not configured", hostname)
                clownf_command_common.exit_env(log, clownfish_instance, -1)
            ssh_host = clownfish_instance.ci_host_dict[hostname]
            if host not in hosts:
                hosts.append(ssh_host)
        iso = self._hc_iso
        if iso is not None:
            local_host = ssh_host.get_local_host(ssh=False)
            iso = cmd_general.check_argument_fpath(log, local_host, iso)
        rc = clownfish_instance.ci_cluster_install(log, iso=iso,
                                                   hosts=hosts)
        clownf_command_common.exit_env(log, clownfish_instance, rc)
