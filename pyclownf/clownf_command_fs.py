"""
fs commands of Clownf
"""
from pycoral import cmd_general
from pyclownf import clownf_command_common


class LustreFilesystemCommand():
    """
    Commands to manage the Lustre file systems.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._lfc_config_fpath = config
        self._lfc_logdir = logdir
        self._lfc_log_to_file = log_to_file
        self._lfc_iso = iso


    def ls(self, status=False):
        """
        List all Lustre file systems.
        :param status: print the status of file systems, default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        cmd_general.check_argument_bool(log, "status", status)
        lustres = list(clownfish_instance.ci_lustre_dict.values())
        rc = clownf_command_common.print_lustres(log, clownfish_instance,
                                                 lustres, status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def services(self, fsname, status=False, fields=None):
        """
        List all services in a Lustre file system.
        :param fsname: name of the Lustre file system
        :param status: print the status of services, default: False.
        :param fields: fields to print, seperated by comma. Empty to print
            field names.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        cmd_general.check_argument_bool(log, "status", status)
        cmd_general.check_argument_types(log, "fields", fields,
                                         allow_none=True,
                                         allow_tuple=True, allow_str=True,
                                         allow_bool=True)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        services = list(lustrefs.lf_service_dict.values())
        rc = clownf_command_common.print_services(log, clownfish_instance,
                                                  services, status=status,
                                                  field_string=fields)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def hosts(self, fsname, status=False, fields=None):
        """
        List all hosts in a Lustre file system.
        :param fsname: name of the Lustre file system.
        :param status: print status of the hosts, default: False.
        :param fields: fields to print, seperated by comma. Empty to print
            field names.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        cmd_general.check_argument_bool(log, "status", status)
        cmd_general.check_argument_types(log, "fields", fields,
                                         allow_none=True,
                                         allow_tuple=True, allow_str=True,
                                         allow_bool=True)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        host_dict = lustrefs.lf_host_dict()
        rc = clownf_command_common.print_hosts(log, clownfish_instance,
                                               list(host_dict.values()),
                                               print_status=status,
                                               field_string=fields)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def clients(self, fsname, status=False):
        """
        List all Lustre clients in a Lustre file system.
        :param fsname: name of the Lustre file system.
        :param status: print the status of clients, default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        cmd_general.check_argument_bool(log, "status", status)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        clients = list(lustrefs.lf_client_dict.values())
        rc = clownf_command_common.print_clients(log, clownfish_instance,
                                                 clients,
                                                 print_status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def status(self, fsname):
        """
        Print the status of a Lustre file system.
        :param fsname: name of the Lustre file system.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        lustres = [lustrefs]
        rc = clownf_command_common.print_lustres(log, clownfish_instance,
                                                 lustres, status=True,
                                                 print_table=False)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def mount(self, fsname):
        """
        Mount the services and clients of a Lustre file system.

        All MGS/MDTs/OSTs/Clients will be mounted.

        :param fsname: name of the Lustre file system.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        rc = clownfish_instance.ci_lustre_mount(log, lustrefs)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def umount(self, fsname, force=False):
        """
        Umount the services and clients of a Lustre file system.

        All MDTs/OSTs/Clients will be umounted. Standalone MGT that
        is not shared by any other file system will be umounted too.

        :param fsname: name of the Lustre file system
        :param force: umount even the autostart is enabled, default: False
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        rc = clownfish_instance.ci_lustre_umount(log, lustrefs, force=force)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def format(self, fsname, yes=False, force=False):
        """
        Format all Lustre devices in the Lustre file system.

        BE CAREFUL that this command will cleanup all data on this file
        system! All Lustre services of the file system will be umounted
        before formating.

        If the file system is sharing a MGS with another file system, this
        command will abort unless "--force" is specified.
        :param fsname: Name of the Lustre file system.
        :param yes: Do not ask for confirmation, just format. Default: False.
        :param force: Format the MGT even it is share by other file systems.
            Default: False
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        cmd_general.check_argument_bool(log, "yes", yes)
        cmd_general.check_argument_bool(log, "force", force)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]

        if not yes:
            message = "Be careful! Data on the following services will be completely erased: "
            service_str = ""
            for service in lustrefs.lf_service_dict.values():
                if service_str != "":
                    service_str += ", "
                service_str += service.ls_service_name
            log.cl_info(message + service_str)
            input_result = input("Are you sure to format all devices of Lustre [%s]? [y/N] " % fsname)
            if ((not input_result.startswith("y")) and
                    (not input_result.startswith("Y"))):
                log.cl_info("quiting without touching anything")
                clownf_command_common.exit_env(log, clownfish_instance, 1)

        rc = clownfish_instance.ci_lustre_format(log, lustrefs, force=force)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_enable(self, fsname):
        """
        Enable the autostart of all hosts/services in a Lustre file system
        :param fsname: name of the Lustre file system
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        rc = clownfish_instance.ci_lustre_autostart_enable(log, lustrefs)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_disable(self, fsname):
        """
        Disable the autostart of all hosts/services in a Lustre file system
        :param fsname: name of the Lustre file system
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        rc = clownfish_instance.ci_lustre_autostart_disable(log, lustrefs)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def autostart_status(self, fsname, full=False):
        """
        Print the autostart of all hosts/services in a Lustre file system
        :param fsname: name of the Lustre file system
        :param full: print all status of autostart
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        cmd_general.check_argument_bool(log, "full", full)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        hosts = list(lustrefs.lf_host_dict().values())
        services = list(lustrefs.lf_service_dict.values())
        rc = clownf_command_common.print_autostart(log, clownfish_instance,
                                                   hosts=hosts,
                                                   services=services,
                                                   full=full)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def watchers(self, fsname):
        """
        Print the watchers of all hosts/services in a Lustre file system
        :param fsname: name of the Lustre file system
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        hosts = list(lustrefs.lf_host_dict().values())
        services = list(lustrefs.lf_service_dict.values())
        rc = clownf_command_common.print_autostart(log, clownfish_instance,
                                                   hosts=hosts,
                                                   services=services, full=True)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def watching(self, fsname):
        """
        Print how many hosts/services each host in this Lustre system
        is watching
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lfc_config_fpath,
                                           self._lfc_logdir,
                                           self._lfc_log_to_file,
                                           self._lfc_iso)
        fsname = cmd_general.check_argument_str(log, "fsname", fsname)
        if fsname not in clownfish_instance.ci_lustre_dict:
            log.cl_error("Lustre file system [%s] is not configured",
                         fsname)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        lustrefs = clownfish_instance.ci_lustre_dict[fsname]
        hosts = list(lustrefs.lf_host_dict().values())
        rc = clownf_command_common.print_watching(log, clownfish_instance,
                                                  hosts)
        clownf_command_common.exit_env(log, clownfish_instance, rc)
