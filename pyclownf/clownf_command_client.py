"""
Client commands of Clownf
"""
from pycoral import cmd_general
from pyclownf import clownf_command_common


class LustreClientCommand():
    """
    Commands to manage Lustre clients.
    """
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._lcc_config_fpath = config
        self._lcc_logdir = logdir
        self._lcc_log_to_file = log_to_file
        self._lsc_iso = iso

    def ls(self, status=False):
        """
        List all Lustre clients in the cluster
        :param status: print the status of clients, default: False
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lcc_config_fpath,
                                           self._lcc_logdir,
                                           self._lcc_log_to_file,
                                           self._lsc_iso)
        cmd_general.check_argument_bool(log, "status", status)
        clients = list(clownfish_instance.ci_client_dict.values())
        rc = clownf_command_common.print_clients(log, clownfish_instance,
                                                 clients,
                                                 print_status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def mount(self, client_name):
        """
        Mount the Lustre client
        :param client_name: Lustre client name, has the format of HOST:MNT
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lcc_config_fpath,
                                           self._lcc_logdir,
                                           self._lcc_log_to_file,
                                           self._lsc_iso)
        client_name = cmd_general.check_argument_str(log, "client_name",
                                                     client_name)
        if client_name not in clownfish_instance.ci_client_dict:
            log.cl_error("Lustre client [%s] is not configured",
                         client_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        client = clownfish_instance.ci_client_dict[client_name]
        rc = client.lc_mount(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def umount(self, client_name):
        """
        Umount the Lustre client
        :param client_name: Lustre client name, has the format of HOST:MNT
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lcc_config_fpath,
                                           self._lcc_logdir,
                                           self._lcc_log_to_file,
                                           self._lsc_iso)
        client_name = cmd_general.check_argument_str(log, "client_name",
                                                     client_name)
        if client_name not in clownfish_instance.ci_client_dict:
            log.cl_error("Lustre client [%s] is not configured",
                         client_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        client = clownfish_instance.ci_client_dict[client_name]
        rc = client.lc_umount(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def status(self, client_name):
        """
        Print the status of a Lustre client
        :param client_name: Lustre client name, has the format of HOST:MNT
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._lcc_config_fpath,
                                           self._lcc_logdir,
                                           self._lcc_log_to_file,
                                           self._lsc_iso)
        client_name = cmd_general.check_argument_str(log, "client_name",
                                                     client_name)
        if client_name not in clownfish_instance.ci_client_dict:
            log.cl_error("Lustre client [%s] is not configured",
                         client_name)
            clownf_command_common.exit_env(log, clownfish_instance, -1)
        client = clownfish_instance.ci_client_dict[client_name]
        clients = [client]
        rc = clownf_command_common.print_clients(log, clownfish_instance,
                                                 clients,
                                                 print_status=True,
                                                 print_table=False)
        clownf_command_common.exit_env(log, clownfish_instance, rc)
