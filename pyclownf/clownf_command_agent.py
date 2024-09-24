"""
Agent commands of Clownf
"""
from pyclownf import clownf_command_common


class ClownfAgentCommand():
    """
    Commands to manage agents on hosts in Clownfish cluster.
    """
    # pylint: disable=too-many-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._cac_config_fpath = config
        self._cac_logdir = logdir
        self._cac_log_to_file = log_to_file
        self._cac_iso = iso

    def start(self):
        """
        Start the agents in the cluster.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cac_config_fpath,
                                           self._cac_logdir,
                                           self._cac_log_to_file,
                                           self._cac_iso)
        rc = clownfish_instance.ci_host_agents_start(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def stop(self):
        """
        Stop the agents in the cluster.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cac_config_fpath,
                                           self._cac_logdir,
                                           self._cac_log_to_file,
                                           self._cac_iso)
        rc = clownfish_instance.ci_host_agents_stop(log)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def status(self):
        """
        Show the status of the agents in the cluster.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cac_config_fpath,
                                           self._cac_logdir,
                                           self._cac_log_to_file,
                                           self._cac_iso)
        hosts = list(clownfish_instance.ci_host_dict.values())
        rc = clownf_command_common.print_agents(log, clownfish_instance,
                                                hosts,
                                                print_table=True)
        clownf_command_common.exit_env(log, clownfish_instance, rc)
