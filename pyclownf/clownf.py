"""
Clownfish command entrance
"""
from fire import Fire
from pyclownf import clownf_command_common
from pyclownf import clownf_command_host
from pyclownf import clownf_constant
from pyclownf import clownf_command_cluster
from pyclownf import clownf_command_fs
from pyclownf import clownf_command_consul
from pyclownf import clownf_command_client
from pyclownf import clownf_command_service


def clownf_simplify_config(ccommand):
    """
    Print simplified Clownfish config.

    The original Clownfish config file might not be trivial for a script
    to parse when it has name lists with the format of host[0-100]. Simplified
    config files, instead, do not contain any string of name list, yet are
    identical with the original config files.

    The printed config will be in Toml format.
    """
    # pylint: disable=protected-access
    config = ccommand._cfc_config_fpath
    logdir = ccommand._cfc_logdir
    log_to_file = ccommand._cfc_log_to_file
    log, clownfish_instance = \
        clownf_command_common.init_env(config,
                                       logdir,
                                       log_to_file,
                                       ccommand._cfc_iso
                                       )
    ret = clownfish_instance.ci_dump_simplified_config(log)
    clownf_command_common.exit_env(log, clownfish_instance, ret)


class ClownfCommand():
    """
    The command line utility for managing Lustre file systems.
    :param config: Config file of Clownfish. Default: /etc/coral/clownfish.conf.
    :param log: Log directory. Default: /var/log/coral/clownf/${TIMESTAMP}.
    :param debug: Whether to dump debug logs into files. Default: False.
    :param iso: The Coral ISO to use for installation. Default: None.
    """
    # pylint: disable=too-few-public-methods
    service = clownf_command_service.LustreServiceCommand()
    cluster = clownf_command_cluster.ClusterCommand()
    fs = clownf_command_fs.LustreFilesystemCommand()
    consul = clownf_command_consul.ConsulCommand()
    host = clownf_command_host.HostCommand()
    client = clownf_command_client.LustreClientCommand()
    simple_config = clownf_simplify_config

    def __init__(self, config=clownf_constant.CLOWNF_CONFIG,
                 log=clownf_constant.CLF_LOG_DIR, debug=False, iso=None):
        # pylint: disable=protected-access,unused-argument
        self._cfc_config_fpath = config
        self._cfc_logdir = log
        self._cfc_log_to_file = debug
        self._cfc_iso = iso
        self.service._init(config, log, debug, iso)
        self.cluster._init(config, log, debug, iso)
        self.fs._init(config, log, debug, iso)
        self.consul._init(config, log, debug, iso)
        self.host._init(config, log, debug, iso)
        self.client._init(config, log, debug, iso)


def main():
    """
    Main routine of Clownfish commands
    """
    Fire(ClownfCommand)
