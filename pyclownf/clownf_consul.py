"""
Consul library
"""
# pylint: disable=too-many-lines
import yaml
from pycoral import coral_yaml
from pycoral import utils
from pyclownf import clownf_constant


clownf_constant.CLOWNF_STR_AUTOSTART = "autostart"


class LustreServiceConfig():
    """
    The config of Lustre service on Consul.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, service_name, autostart=False, disabled_hostnames=None,
                 prefered_hostnames=None):
        # The name of the service
        self.lsc_service_name = service_name
        # Whether this service needs to be autostarted.
        self.lsc_autostart = autostart
        # The hostnames that do not allow to mount the service
        if disabled_hostnames is None:
            self.lsc_disabled_hostnames = []
        else:
            self.lsc_disabled_hostnames = disabled_hostnames
        # The hostnames that this service prefered to mount on
        if prefered_hostnames is None:
            self.lsc_prefered_hostnames = []
        else:
            self.lsc_prefered_hostnames = prefered_hostnames

    def lsc_set_config(self, log, workspace, local_host, consul_cluster):
        """
        Set the service config.
        """
        config_string = """#
# Run-time config of the Lustre service
#
# The config format is YAML.
#
# autostart: [true|false]
# Setting this to true will enable the autostart of the service.
# Whenever the service is not mounted because of server reboot, manual
# operations, or any other reasons, autostart mechanism will start the
# service automatically. The autostart mechanism will first try to start
# the service on a host with minimum load, and will try mounting on other
# hosts if the attempt of load balancing fails.
#
# disabled_hosts:
# Adding a hostname to this list will disable mounting the service on the
# host. Related commands (mount, move, migrate etc.) and autostart mechanism
# will all skip the host when trying to mount the service. Usually, this list
# should be kept empty, unless there are hosts that have some problems like
# degraded networks or disconnected storage.
# Example:
# disabled_hosts:
#   - host0
#   - host1
#
# prefered_hosts:
# Adding a hostname to this list will set a high priority on mounting the
# service on the host. Mount commands and autostart mechanism will first try
# to mount the service on the host, and then on other hosts. This is useful
# for load balance across servers.
# Example:
# prefered_hosts:
#   - host0
#   - host1
#
"""
        config = {}
        config[clownf_constant.CLOWNF_STR_AUTOSTART] = self.lsc_autostart
        config[clownf_constant.CLOWNF_STR_DISABLED_HOSTS] = self.lsc_disabled_hostnames
        config[clownf_constant.CLOWNF_STR_PREFERED_HOSTS] = self.lsc_prefered_hostnames

        config_string += yaml.dump(config, Dumper=coral_yaml.YamlDumper,
                                   default_flow_style=False)
        key = ("%s/%s/%s" %
               (clownf_constant.CLOWNF_CONSUL_SERVICE_PATH,
                self.lsc_service_name,
                clownf_constant.CLOWNF_CONSUL_CONFIG_KEY))
        return consul_cluster.cclr_set_config(log, workspace, local_host, key,
                                              config_string)


def service_get_config(log, consul_cluster, service_name, consul_hostname=None):
    """
    Get the service config.
    If not exist, return (0, None). Return (negative, None) on error.
    """
    key_path = ("%s/%s/%s" %
                (clownf_constant.CLOWNF_CONSUL_SERVICE_PATH,
                 service_name,
                 clownf_constant.CLOWNF_CONSUL_CONFIG_KEY))
    ret, config = consul_cluster.cclr_get_config(log, key_path,
                                                 consul_hostname=consul_hostname)
    if ret:
        return None

    if config is not None:
        autostart = utils.config_value(config, clownf_constant.CLOWNF_STR_AUTOSTART)
        autostart = bool(autostart)
        disabled_hostnames = utils.config_value(config,
                                                clownf_constant.CLOWNF_STR_DISABLED_HOSTS)
        prefered_hostnames = utils.config_value(config,
                                                clownf_constant.CLOWNF_STR_PREFERED_HOSTS)
    else:
        autostart = False
        disabled_hostnames = None
        prefered_hostnames = None
    return LustreServiceConfig(service_name, autostart=autostart,
                               disabled_hostnames=disabled_hostnames,
                               prefered_hostnames=prefered_hostnames)


def service_autostart_enable_disable(log, workspace, local_host, consul_cluster,
                                     service_name, enable=True):
    """
    Enable or disable the autotest of a service
    """
    config = service_get_config(log, consul_cluster, service_name)
    if config is None:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return -1

    config.lsc_autostart = enable
    ret = config.lsc_set_config(log, workspace, local_host, consul_cluster)
    if ret:
        log.cl_error("failed to set the config of service [%s]", service_name)
        return -1
    return 0


def get_watcher(log, consul_cluster, hostname=None, service_name=None,
                consul_hostname=None):
    """
    Get the watcher hostname of the host or service

    If not exist, return (0, None). Return (negative, None) on error.
    """
    if hostname is not None:
        path = clownf_constant.CLOWNF_CONSUL_HOST_PATH + "/" + hostname
    elif service_name is not None:
        path = clownf_constant.CLOWNF_CONSUL_SERVICE_PATH + "/" + service_name
    else:
        log.cl_error("either hostname or service name should be given")
        return -1, None

    lock_key_path = ("%s/%s" %
                     (path, clownf_constant.CLOWNF_CONSUL_LOCK_KEY))

    return consul_cluster.cclr_get_watcher(log, lock_key_path,
                                           consul_hostname=consul_hostname)


def host_get_watcher(log, consul_cluster, hostname,
                     consul_hostname=None):
    """
    Get the watcher hostname of the host

    If not exist, return (0, None). Return (negative, None) on error.
    """
    return get_watcher(log, consul_cluster, hostname=hostname,
                       consul_hostname=consul_hostname)


def service_get_watcher(log, consul_cluster, service_name,
                        consul_hostname=None):
    """
    Get the watcher hostname of a service

    If not exist, return (0, None). Return (negative, None) on error.
    """
    return get_watcher(log, consul_cluster, service_name=service_name,
                       consul_hostname=consul_hostname)


def service_host_check_enabled(log, consul_cluster, service_name, hostname,
                               consul_hostname=None):
    """
    Return 1 if the service can be mounted on host with hostname, 0 if not. Negative on error.
    """
    config = service_get_config(log, consul_cluster, service_name,
                                consul_hostname=consul_hostname)
    if config is None:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return -1

    if hostname in config.lsc_disabled_hostnames:
        return 0
    return 1


def service_disabled_hostnames(log, consul_cluster, service_name,
                               consul_hostname=None):
    """
    Return list of the disabled host names for a service
    """
    config = service_get_config(log, consul_cluster, service_name,
                                consul_hostname=consul_hostname)
    if config is None:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return None

    return config.lsc_disabled_hostnames


def service_host_enable_disable(log, workspace, local_host, consul_cluster,
                                service_name, hostname, enable=True):
    """
    Do not allow the service to run on host with hostname.
    If hostname is None and enable is False, meaning need to rewrite the
    config. That is useful for initing the config.
    """
    config = service_get_config(log, consul_cluster, service_name)
    if config is None:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return None

    if hostname is None:
        pass
    elif hostname in config.lsc_disabled_hostnames:
        if not enable:
            return 0
        config.lsc_disabled_hostnames.remove(hostname)
    else:
        if enable:
            return 0
        config.lsc_disabled_hostnames.append(hostname)

    ret = config.lsc_set_config(log, workspace, local_host, consul_cluster)
    if ret:
        log.cl_error("failed to set the service config")
        return -1
    return 0


def service_host_prefere_disprefer(log, workspace, local_host, consul_cluster,
                                   service_name, hostname, prefer=True):
    """
    Prefer or disprefer to mount a service on a host.
    If hostname is None and enable is False, meaning need to rewrite the
    config. That is useful for initing the config.
    """
    config = service_get_config(log, consul_cluster, service_name)
    if config is None:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return None

    if hostname is None:
        pass
    elif hostname in config.lsc_prefered_hostnames:
        if prefer:
            return 0
        config.lsc_prefered_hostnames.remove(hostname)
    else:
        if not prefer:
            return 0
        config.lsc_prefered_hostnames.append(hostname)

    ret = config.lsc_set_config(log, workspace, local_host, consul_cluster)
    if ret:
        log.cl_error("failed to set the service config")
        return -1
    return 0


def service_prefered_hostnames(log, consul_cluster, service_name,
                               consul_hostname=None):
    """
    Return (0, hostnames) of the prefered host for a service
    """
    config = service_get_config(log, consul_cluster, service_name,
                                consul_hostname=consul_hostname)
    if config is None:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return None

    return config.lsc_prefered_hostnames


def service_host_check_prefered(log, consul_cluster, service_name, hostname,
                                consul_hostname=None):
    """
    Return 1 if the service is prefered to be mounted on host with hostname.
    0 if not. Negative on error.
    """
    config = service_get_config(log, consul_cluster, service_name,
                                consul_hostname=consul_hostname)
    if config is None:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return -1

    if hostname in config.lsc_prefered_hostnames:
        return 1
    return 0


class ClownfHostConfig():
    """
    The config of host on Consul.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, hostname, autostart=False):
        # The name of the host
        self.chc_hostname = hostname
        # Whether this host needs to be autostarted.
        self.chc_autostart = autostart

    def chc_set_config(self, log, workspace, local_host, consul_cluster, hostname):
        """
        Set the host config.
        """
        config_string = """#
# Run-time config of the host
#
# The config format is YAML.
#
# autostart: [true|false]
# Setting this to true will enable the autostart of the host.
# Whenever the host is down for any reason, autostart mechanism will start
# the host automatically.
#
"""
        config = {}
        config[clownf_constant.CLOWNF_STR_AUTOSTART] = self.chc_autostart
        config_string += yaml.dump(config, Dumper=coral_yaml.YamlDumper,
                                   default_flow_style=False)
        key = ("%s/%s/%s" %
                (clownf_constant.CLOWNF_CONSUL_HOST_PATH,
                hostname, clownf_constant.CLOWNF_CONSUL_CONFIG_KEY))
        return consul_cluster.cclr_set_config(log, workspace, local_host, key,
                                              config_string)


def host_get_config(log, consul_cluster, hostname, consul_hostname=None):
    """
    Get the host config.
    If not exist, return ClownfHostConfig.
    """
    key_path = ("%s/%s/%s" %
                (clownf_constant.CLOWNF_CONSUL_HOST_PATH,
                 hostname,
                 clownf_constant.CLOWNF_CONSUL_CONFIG_KEY))
    ret, config = consul_cluster.cclr_get_config(log, key_path,
                                                 consul_hostname=consul_hostname)
    if ret:
        return None

    if config is not None:
        autostart = utils.config_value(config, clownf_constant.CLOWNF_STR_AUTOSTART)
        autostart = bool(autostart)
    else:
        autostart = False

    return ClownfHostConfig(hostname, autostart=autostart)


def host_autostart_enable_disable(log, workspace, local_host, consul_cluster,
                                  hostname, enable=True):
    """
    Enable or disable the autotest of a service
    """
    config = host_get_config(log, consul_cluster, hostname)
    if config is None:
        log.cl_error("failed to get the config of host [%s]", hostname)
        return -1

    config.chc_autostart = enable
    ret = config.chc_set_config(log, workspace, local_host, consul_cluster,
                                hostname)
    if ret:
        log.cl_error("failed to set the config of host [%s]", hostname)
        return -1
    return 0


def host_autostart_check_enabled(log, consul_cluster, hostname,
                                 consul_hostname=None):
    """
    Return 0 if host is disabled, return 1 if host is enabled. Return
    negative on error.
    """
    config = host_get_config(log, consul_cluster, hostname,
                             consul_hostname=consul_hostname)
    if config is None:
        log.cl_error("failed to get the config of host [%s]", hostname)
        return -1

    if config.chc_autostart:
        return 1
    return 0
