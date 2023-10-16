"""
Consul library
"""
# pylint: disable=too-many-lines
import yaml
from pycoral import coral_yaml
from pycoral import utils
from pyclownf import clownf_constant


CONSUL_AUTOSTART = "autostart"


def service_get_config(log, consul_cluster, service_name, consul_hostname=None):
    """
    Get the service config.
    If not exist, return (0, None). Return (negative, None) on error.
    """
    key_path = ("%s/%s/%s" %
                (clownf_constant.CLF_CONSUL_SERVICE_PATH,
                 service_name,
                 clownf_constant.CLF_CONSUL_CONFIG_KEY))
    return consul_cluster.cclr_get_config(log, key_path,
                                          consul_hostname=consul_hostname)


def service_set_config(log, workspace, local_host, consul_cluster,
                       service_name, config):
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
"""
    config_string += yaml.dump(config, Dumper=coral_yaml.YamlDumper,
                               default_flow_style=False)
    key = ("%s/%s/%s" % (clownf_constant.CLF_CONSUL_SERVICE_PATH,
                         service_name,
                         clownf_constant.CLF_CONSUL_CONFIG_KEY))
    return consul_cluster.cclr_set_config(log, workspace, local_host, key,
                                          config_string)


def service_autostart_check_enabled(log, consul_cluster, service_name,
                                    consul_hostname=None):
    """
    Return 1 if autostart enabled, 0 if not. Negative on error.
    """
    ret, config = service_get_config(log, consul_cluster, service_name,
                                     consul_hostname=consul_hostname)
    if ret:
        return -1

    # By default, autostart is disabled
    if config is None:
        return 0

    autostart = utils.config_value(config, CONSUL_AUTOSTART)
    if autostart is None:
        return 0

    if autostart:
        return 1
    return 0


def service_autostart_enable_disable(log, workspace, local_host, consul_cluster,
                                     service_name, enable=True):
    """
    Enable or disable the autotest of a service
    """
    ret, config = service_get_config(log, consul_cluster, service_name)
    if ret:
        log.cl_error("failed to get the config of service [%s]", service_name)
        return -1

    if config is None:
        config = {}

    config[CONSUL_AUTOSTART] = enable
    ret = service_set_config(log, workspace, local_host, consul_cluster,
                             service_name, config)
    if ret:
        log.cl_error("failed to set the config of service [%s]", service_name)
        return -1
    return 0


def host_get_config(log, consul_cluster, hostname, consul_hostname=None):
    """
    Get the host config.
    If not exist, return (0, None). Return (negative, None) on error.
    """
    key_path = ("%s/%s/%s" %
                (clownf_constant.CLF_CONSUL_HOST_PATH,
                 hostname,
                 clownf_constant.CLF_CONSUL_CONFIG_KEY))
    return consul_cluster.cclr_get_config(log, key_path,
                                          consul_hostname=consul_hostname)


def host_set_config(log, workspace, local_host, consul_cluster, hostname,
                    config):
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
# the service automatically.
#
"""
    config_string += yaml.dump(config, Dumper=coral_yaml.YamlDumper,
                               default_flow_style=False)
    key = ("%s/%s/%s" %
           (clownf_constant.CLF_CONSUL_HOST_PATH,
            hostname, clownf_constant.CLF_CONSUL_CONFIG_KEY))
    return consul_cluster.cclr_set_config(log, workspace, local_host, key,
                                          config_string)


def get_watcher(log, consul_cluster, hostname=None, service_name=None,
                consul_hostname=None):
    """
    Get the watcher hostname of the host or service

    If not exist, return (0, None). Return (negative, None) on error.
    """
    if hostname is not None:
        path = clownf_constant.CLF_CONSUL_HOST_PATH + "/" + hostname
    elif service_name is not None:
        path = clownf_constant.CLF_CONSUL_SERVICE_PATH + "/" + service_name
    else:
        log.cl_error("either hostname or service name should be given")
        return -1, None

    lock_key_path = ("%s/%s" %
                     (path, clownf_constant.CLF_CONSUL_LOCK_KEY))

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


def host_autostart_enable_disable(log, workspace, local_host, consul_cluster,
                                  hostname, enable=True):
    """
    Enable or disable the autotest of a service
    """
    ret, config = host_get_config(log, consul_cluster, hostname)
    if ret:
        log.cl_error("failed to get the config of host [%s]", hostname)
        return -1

    if config is None:
        config = {}

    config[CONSUL_AUTOSTART] = enable
    ret = host_set_config(log, workspace, local_host, consul_cluster,
                          hostname, config)
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
    ret, config = host_get_config(log, consul_cluster, hostname,
                                  consul_hostname=consul_hostname)
    if ret:
        log.cl_error("failed to get the config of host [%s]", hostname)
        return -1

    if config is None:
        return 0

    autostart = utils.config_value(config, CONSUL_AUTOSTART)
    if autostart is None:
        return 0

    if autostart:
        return 1
    return 0


def service_host_check_enabled(log, consul_cluster, service_name, hostname,
                               consul_hostname=None):
    """
    Return 1 if the service can be mounted on host with hostname, 0 if not. Negative on error.
    """
    ret, config = service_get_config(log, consul_cluster, service_name,
                                     consul_hostname=consul_hostname)
    if ret:
        return -1

    # By default, host is enabled
    if config is None:
        return 1

    disabled_hosts = utils.config_value(config, clownf_constant.CLF_DISABLED_HOSTS)
    if disabled_hosts is None:
        return 1

    if hostname in disabled_hosts:
        return 0
    return 1


def service_disabled_hostnames(log, consul_cluster, service_name,
                               consul_hostname=None):
    """
    Return (0, hostnames) of the disabled host for a service
    """
    ret, config = service_get_config(log, consul_cluster, service_name,
                                     consul_hostname=consul_hostname)
    if ret:
        return -1, None

    # By default, host is enabled
    if config is None:
        config = {}

    disabled_hosts = utils.config_value(config, clownf_constant.CLF_DISABLED_HOSTS)
    if disabled_hosts is None:
        return 0, []

    return 0, config[clownf_constant.CLF_DISABLED_HOSTS]


def service_host_enable_disable(log, workspace, local_host, consul_cluster,
                                service_name, hostname, enable=True):
    """
    Do not allow the service to run on host with hostname.
    If hostname is None and enable is False, meaning need to rewrite the
    config. That is useful for initing the config.
    """
    ret, config = service_get_config(log, consul_cluster, service_name)
    if ret:
        return -1

    # By default, host is enabled
    if config is None:
        config = {}

    disabled_hosts = utils.config_value(config, clownf_constant.CLF_DISABLED_HOSTS)
    if disabled_hosts is None:
        if enable:
            return 0
        disabled_hosts = []

    if hostname is None:
        pass
    elif hostname in disabled_hosts:
        if not enable:
            return 0
        disabled_hosts.remove(hostname)
    else:
        if enable:
            return 0
        disabled_hosts.append(hostname)
    config[clownf_constant.CLF_DISABLED_HOSTS] = disabled_hosts

    ret = service_set_config(log, workspace, local_host, consul_cluster,
                             service_name, config)
    if ret:
        log.cl_error("failed to set the service config")
        return -1
    return 0
