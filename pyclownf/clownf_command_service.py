"""
Service commands of Clownf
"""
from pycoral import cmd_general
from pyclownf import clownf_command_common


class LustreServiceCommand():
    """
    Commands to manage Lustre services.
    """
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._lsc_config_fpath = config
        self._lsc_logdir = logdir
        self._lsc_log_to_file = log_to_file
        self._lsc_iso = iso


    def mount(self, service_name):
        """
        Mount a Lustre service.
        :param service_name: Lustre service name.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        rc = clownfish_instance.ci_lustre_service_mount(log,
                                                        service_name)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def umount(self, service_name, force=False):
        """
        Umount a Lustre service
        :param service_name: Lustre service name.
        :param force: umount even autostart is enabled, default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        cmd_general.check_argument_bool(log, "force", force)
        rc = clownfish_instance.ci_lustre_service_umount(log,
                                                         service_name,
                                                         force=force)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def ls(self, status=False, fields=None):
        """
        List all Lustre services in the cluster.
        :param status: print the status of services, default: False.
        :param fields: fields to print, seperated by comma.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        cmd_general.check_argument_bool(log, "status", status)
        cmd_general.check_argument_types(log, "fields", fields,
                                         allow_none=True,
                                         allow_tuple=True, allow_str=True,
                                         allow_bool=True)
        services = list(clownfish_instance.ci_service_dict.values())
        rc = clownf_command_common.print_services(log, clownfish_instance,
                                                  services, status=status,
                                                  field_string=fields)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_enable(self, service_name):
        """
        Enable the autostart of the service
        :param service_name: Lustre service name.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        rc = clownfish_instance.ci_service_autostart_enable(log,
                                                            service_name)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_disable(self, service_name):
        """
        Disable the autostart of the service
        :param service_name: Lustre service name.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        rc = clownfish_instance.ci_service_autostart_disable(log,
                                                             service_name)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_status(self, service_name):
        """
        Show the autostart status of a service.
        :param service_name: Lustre service name
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        if service_name not in clownfish_instance.ci_service_dict:
            log.cl_error("service [%s] is not configured", service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        service = clownfish_instance.ci_service_dict[service_name]
        services = [service]
        rc = clownf_command_common.print_autostart(log, clownfish_instance,
                                                   services=services,
                                                   print_table=False,
                                                   full=True)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def status(self, service_name):
        """
        Print the status of a Lustre service.

        :param service_name: Lustre service name.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        service = clownfish_instance.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("Lustre service [%s] is not configured",
                         service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        services = [service]
        rc = clownf_command_common.print_services(log, clownfish_instance,
                                                  services, status=True,
                                                  print_table=False)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def format(self, service_name, yes=False):
        """
        Format a Lustre service.

        BE CAREFUL! This command will cleanup all data on this service,
        and might further cause data lost of this file system!

        This command is mostly useful for going on with the interrupted process
        of formatting a Lustre file system from the middle. If the purpose is
        to format a Lustre file system from scratch, please use
        "clownf cluster format" or "clownf fs format" command instead.

        The Lustre service should already be umounted before running this command.

        :param service_name: Lustre service name to format.
        :param yes: Do not ask for confirmation, just format. Default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        service = clownfish_instance.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("Lustre service [%s] is not configured",
                         service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        cmd_general.check_argument_bool(log, "yes", yes)
        if not yes:
            log.cl_info("Be careful! Data on the service [%s] will be "
                        "completely erased!", service_name)
            input_result = input("Are you sure to format Lustre service [%s]? [y/N] " % service_name)
            if ((not input_result.startswith("y")) and
                    (not input_result.startswith("Y"))):
                log.cl_info("quiting without touching anything")
                clownf_command_common.exit_env(log, clownfish_instance, 1)

        mounted_instance = service.ls_mounted_instance(log)
        if mounted_instance is not None:
            log.cl_error("service [%s] is mounted on host [%s], please umount first",
                         service.ls_service_name,
                         mounted_instance.lsi_host.sh_hostname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        rc = service.ls_format(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def hosts(self, service_name, status=False, disabled=False,
              enabled=False, fields=None):
        """
        List all hosts that could provide a Lustre service.
        :param service_name: Lustre service name.
        :param status: print status of the hosts, default: False.
        :param enabled: only print hosts that enable this service, default: False.
        :param disabled: only print hosts that disable this service, default: False.
        :param fields: fields to print, seperated by comma.
        """
        # pylint: disable=too-many-branches,too-many-locals
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        cmd_general.check_argument_bool(log, "status", status)
        cmd_general.check_argument_bool(log, "disabled", disabled)
        cmd_general.check_argument_bool(log, "enabled", enabled)
        cmd_general.check_argument_types(log, "fields", fields,
                                         allow_none=True,
                                         allow_tuple=True, allow_str=True,
                                         allow_bool=True)

        if disabled and enabled:
            log.cl_error("please do not specify [enabled] and [disabled] "
                         "options at the same time")
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        service = clownfish_instance.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("service [%s] is not configured",
                         service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        target_hostnames = None
        if disabled:
            target_hostnames = clownfish_instance.ci_service_disabled_hostnames(log,
                                                                                service)
            if target_hostnames is None:
                clownf_command_common.exit_env(log, clownfish_instance, -1)
        elif enabled:
            target_hostnames = clownfish_instance.ci_service_enabled_hostnames(log,
                                                                               service)
            if target_hostnames is None:
                clownf_command_common.exit_env(log, clownfish_instance, -1)

        host_dict = {}
        for service_instance in service.ls_instance_dict.values():
            host = service_instance.lsi_host
            hostname = host.sh_hostname
            if ((target_hostnames is not None) and
                    (hostname not in target_hostnames)):
                continue

            if hostname not in host_dict:
                host_dict[hostname] = host
        hosts = list(host_dict.values())
        rc = clownf_command_common.print_hosts(log, clownfish_instance, hosts,
                                               print_status=status,
                                               field_string=fields)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def host_status(self, service_name, hostname):
        """
        Print the status of a service instance on a host.

        :param service_name: Lustre service name
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownf_command_common.instance_print(log, clownfish_instance,
                                                  service_name, hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def host_disable(self, service_name, hostname):
        """
        Disable a service instance on a host

        :param service_name: Lustre service name
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownf_command_common.instance_disable(log, clownfish_instance,
                                                    hostname, service_name)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def host_enable(self, service_name, hostname):
        """
        Enable a service instance on a host

        :param service_name: Lustre service name
        :param hostname: name of the host
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        hostname = cmd_general.check_argument_str(log, "hostname", hostname)
        rc = clownf_command_common.instance_enable(log, clownfish_instance,
                                                   hostname, service_name)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def watcher(self, service_name):
        """
        Print the watcher candidates of a service

        A watcher of a service would mount the service if the service is
        umounted for reason (server reboot etc.).
        :param service_name: Lustre service name
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lsc_config_fpath,
                                           self._lsc_logdir,
                                           self._lsc_log_to_file,
                                           self._lsc_iso)
        service_name = cmd_general.check_argument_str(log, "service_name",
                                                      service_name)
        service = clownfish_instance.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("Lustre service [%s] does not exist",
                         service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        rc, watcher = clownfish_instance.ci_service_watcher(log, service_name)
        if rc:
            log.cl_error("failed to get the watcher of service [%s]",
                         service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

        rc, watcher_candidates = \
            clownfish_instance.ci_service_watcher_candidates(log, service)
        if rc:
            log.cl_error("failed to get the watcher candidates of service [%s]",
                         service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)

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
            log.cl_error("failed to print candidates of service [%s]",
                         service_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        clownf_command_common.exit_env(log, clownfish_instance, 0)
