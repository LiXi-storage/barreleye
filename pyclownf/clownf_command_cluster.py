"""
Cluster commands of Clownf
"""
from pycoral import ssh_host
from pycoral import clog
from pycoral import cmd_general
from pycoral import lustre as lustre_lib
from pyclownf import clownf_command_common
from pyclownf import clownf_constant


class ClusterCommand():
    """
    Commands to manage the whole Clownfish cluster.
    """
    # pylint: disable=too-many-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._cc_config_fpath = config
        self._cc_logdir = logdir
        self._cc_log_to_file = log_to_file
        self._cc_iso = iso


    def umount(self, force=False):
        """
        Umount all Lustre services and clients.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "force", force)
        rc = clownfish_instance.ci_cluster_umount(log, force=force)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def mount(self):
        """
        Mount all Lustre services and clients.

        All MGS will first been mounted. And then the other Lustre file system
        services.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        rc = clownfish_instance.ci_cluster_mount(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def prepare(self, nolazy=False, skip_umount=False):
        """
        Prepare all of the host in the cluster for providing Lustre service.

        All Lustre services will be umounted before preparation. All Kernel,
        Lustre, E2fsprogs and ZFS RPMs as well as their dependencies will be
        installed if they are not. Reboot of the host will be issued
        if need to run a different kernel.

        :param nolazy: Reinstall the RPMs even they are already installed, default: false.
        :param skip_umount: Skip the step of umounting services before preparing
        hosts, default: false. This is a debug option which is rarely used.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "nolazy", nolazy)
        cmd_general.check_argument_bool(log, "skip_umount", skip_umount)
        rc = clownfish_instance.ci_cluster_prepare(log,
                                                   clownfish_instance.ci_workspace,
                                                   not nolazy,
                                                   not skip_umount)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def format(self, yes=False):
        """
        format all Lustre devices in the cluster.

        BE CAREFUL that this command will cleanup all data on Lustre! All
        Lustre services will be umounted before formating.
        :param yes: Do not ask for confirmation, just format. Default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "yes", yes)
        if not yes:
            message = "Be careful! Data on the following services will be completely erased: "
            service_str = ""
            for service in clownfish_instance.ci_service_dict.values():
                if service_str != "":
                    service_str += ", "
                service_str += service.ls_service_name
            log.cl_info(message + service_str)
            input_result = input("Are you sure to format all devices in the cluster? [y/N] ")
            if not input_result.startswith("y") and not input_result.startswith("Y"):
                log.cl_info("quiting without touching anything")
                clownf_command_common.exit_env(log, clownfish_instance, 1)
        rc = clownfish_instance.ci_cluster_format(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def install(self):
        """
        Install the Clownfish packages in the whole cluster.

        All Clownfish RPMs, configuration files and Consul cluster will be
        cleaned up and re-configured. Lustre RPMs not be installed on the
        hosts. Lustre service will not be affected during this process.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        iso = self._cc_iso
        if iso is not None:
            local_host = ssh_host.get_local_host(ssh=False)
            iso = cmd_general.check_argument_fpath(log, local_host,
                                                   iso)
        rc = clownfish_instance.ci_cluster_install(log, iso)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def hosts(self, status=False, fields=None):
        """
        List all hosts in the cluster.
        :param status: print status of the hosts, default: False.
        :param fields: fields to print, seperated by comma.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
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

    def autostart_disable(self):
        """
        Disable autostart of all services and hosts
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        rc = clownfish_instance.ci_cluster_autostart_disable(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_enable(self):
        """
        Enable autostart of all services and hosts
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        rc = clownfish_instance.ci_cluster_autostart_enable(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_status(self, full=False):
        """
        Print the autostart status of all services and hosts
        :param full: print all status of autostart
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "full", full)
        hosts = list(clownfish_instance.ci_host_dict.values())
        services = list(clownfish_instance.ci_service_dict.values())
        rc = clownf_command_common.print_autostart(log, clownfish_instance,
                                                   hosts=hosts,
                                                   services=services,
                                                   full=full)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def watchers(self):
        """
        Print the watcher status of all services and hosts
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        hosts = list(clownfish_instance.ci_host_dict.values())
        services = list(clownfish_instance.ci_service_dict.values())
        rc = clownf_command_common.print_autostart(log, clownfish_instance,
                                                   hosts=hosts,
                                                   services=services,
                                                   full=True)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def watching(self):
        """
        Print how many hosts/services each host is watching
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        hosts = list(clownfish_instance.ci_host_dict.values())
        rc = clownf_command_common.print_watching(log, clownfish_instance,
                                                  hosts)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def services(self, status=False, fields=None):
        """
        List all Lustre services in the cluster
        :param status: print the status of services, default: False
        :param fields: fields to print, seperated by comma. Empty to print field names.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
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

    def fs(self, status=False):
        """
        List all file systems in the cluster
        :param status: print the status of file systems, default: False
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "status", status)
        lustres = list(clownfish_instance.ci_lustre_dict.values())
        rc = clownf_command_common.print_lustres(log, clownfish_instance,
                                                 lustres, status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def clients(self, status=False):
        """
        List all Lustre clients in the cluster
        :param status: print the status of clients, default: False
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "status", status)
        clients = list(clownfish_instance.ci_client_dict.values())
        rc = clownf_command_common.print_clients(log, clownfish_instance,
                                                 clients,
                                                 print_status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def status(self):
        """
        Print the status of the cluster
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        rc = 0
        if clownfish_instance.ci_is_symmetric(log):
            symmetric = clog.colorful_message(clog.COLOR_GREEN,
                                              clownf_constant.CLOWNF_VALUE_SYMMETRIC)
            if clownfish_instance.ci_load_balanced(log):
                balanced = clog.colorful_message(clog.COLOR_GREEN,
                                                 clownf_constant.CLOWNF_VALUE_BALANCED)
            else:
                balanced = clog.colorful_message(clog.COLOR_YELLOW,
                                                 clownf_constant.CLOWNF_VALUE_IMBALANCED)
        else:
            symmetric = clog.colorful_message(clog.COLOR_YELLOW,
                                              clownf_constant.CLOWNF_VALUE_ASYMMETRIC)
            balanced = clog.colorful_message(clog.COLOR_YELLOW,
                                             clownf_constant.CLOWNF_VALUE_UNKNOWN)
        cmd_general.print_field(log, clownf_constant.CLOWNF_FIELD_SYMMETRIC,
                                symmetric)
        cmd_general.print_field(log,
                                clownf_constant.CLOWNF_FIELD_LOAD_BALANCED,
                                balanced)

        healty = clog.colorful_message(clog.COLOR_GREEN,
                                       lustre_lib.LustreHost.LSH_HEALTHY)
        for host in clownfish_instance.ci_host_dict.values():
            healty_result = host.lsh_healty_check(log)
            if healty_result == lustre_lib.LustreHost.LSH_ERROR:
                healty = clog.ERROR_MSG
                rc = -1
                break
            if healty_result != lustre_lib.LustreHost.LSH_HEALTHY:
                healty = clog.colorful_message(clog.COLOR_RED,
                                               healty_result)
                break
        cmd_general.print_field(log, clownf_constant.CLOWNF_FIELD_HEALTHY,
                                healty)
        clownf_command_common.exit_env(log, clownfish_instance, rc)
