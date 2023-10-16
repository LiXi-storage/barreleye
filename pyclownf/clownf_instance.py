"""
Library for Clownfish that manages Lustre
Clownfish is a management system for Lustre
"""
# pylint: disable=too-many-lines
import os
import copy
import toml

# Local libs
from pycoral import utils
from pycoral import parallel
from pycoral import lustre
from pycoral import ssh_host
from pycoral import stonith
from pycoral import constant
from pycoral import install_common
from pycoral import cmd_general
from pycoral import consul
from pyclownf import clownf_consul
from pyclownf import clownf_constant


def lustre_service_instance_on_host(service, local_hostname):
    """
    Return the instance on the host with hostname
    """
    local_instance = None
    for instance in service.ls_instance_dict.values():
        host = instance.lsi_host
        if host.sh_hostname == local_hostname:
            local_instance = instance
            return local_instance
    return local_instance


def hosts_have_same_services(log, host_x, host_y):
    """
    Return true if two host have same services
    """
    service_names_x = []
    service_names_y = []

    for instance in host_x.lsh_instance_dict.values():
        service_name = instance.lsi_service.ls_service_name
        service_names_x.append(service_name)

    for instance in host_y.lsh_instance_dict.values():
        service_name = instance.lsi_service.ls_service_name
        service_names_y.append(service_name)

    for service_name in service_names_x:
        if service_name not in service_names_y:
            log.cl_debug("service [%s] is configured for host [%s], not for "
                         "host [%s]", service_name, host_x.sh_hostname,
                         host_y.sh_hostname)
            return False

    for service_name in service_names_y:
        if service_name not in service_names_x:
            log.cl_debug("service [%s] is configured for host [%s], not for "
                         "host [%s]", service_name, host_y.sh_hostname,
                         host_x.sh_hostname)
            return False

    return True


def host_lustre_prepare(log, workspace, host, lazy_prepare=False):
    """
    wrapper of lsh_lustre_prepare for parrallism
    """
    return host.lsh_lustre_prepare(log, workspace,
                                   lazy_prepare=lazy_prepare)


class ClownfHost(lustre.LustreHost
                 ):
    """
    Host used by clownfish commands
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, hostname, lustre_dist=None, identity_file=None,
                 local=False, is_server=False, is_client=False,
                 ssh_for_local=True, login_name="root"):
        super().__init__(hostname, lustre_dist=lustre_dist,
                         identity_file=identity_file,
                         local=local,
                         is_server=is_server,
                         is_client=is_client,
                         ssh_for_local=ssh_for_local,
                         login_name=login_name)


class ClownfishInstance():
    """
    This instance saves the global Clownfish information
    """
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-public-methods,unused-argument
    def __init__(self, workspace, config, simple_config, config_fpath, host_dict,
                 stonith_dict, mgs_dict, lustre_dict, consul_cluster, local_host,
                 lustre_distribution_dict, logdir_is_default,
                 iso_fpath):
        # pylint: disable=too-many-locals
        # Log to file for debugging
        self.ci_log_to_file = False
        # Whether the workspace is generated under default path
        self.ci_logdir_is_default = logdir_is_default
        # Config content
        self.ci_config = config
        # The config fpath that generates this intance
        self.ci_config_fpath = config_fpath
        # Keys are hostnames
        self.ci_host_dict = host_dict
        # Keys are hostnames
        self.ci_stonith_dict = stonith_dict
        # Keys are the MGS IDs, values are instances of LustreService
        self.ci_mgs_dict = mgs_dict
        # All services including MGS/OST/MDT, type: LustreService
        self.ci_service_dict = mgs_dict.copy()
        # Keys are the fsnames, values are instances of LustreFilesystem
        self.ci_lustre_dict = lustre_dict
        # Key is $HOSTNAME:$MNT, value is LustreClient
        self.ci_client_dict = {}
        for lustrefs in lustre_dict.values():
            for service in lustrefs.lf_service_dict.values():
                service_name = service.ls_service_name
                if service_name in self.ci_lustre_dict:
                    continue
                self.ci_service_dict[service_name] = service
            for client in lustrefs.lf_client_dict.values():
                self.ci_client_dict[client.lc_client_name] = client
        # Workspace to save log
        self.ci_workspace = workspace
        # ConsulCluster
        self.ci_consul_cluster = consul_cluster
        # Local host
        self.ci_local_host = local_host
        # The dir of the ISO
        self.ci_iso_dir = constant.CORAL_ISO_DIR
        # The key is distribution ID, value is LustreDistribution
        self.ci_lustre_distribution_dict = lustre_distribution_dict
        # The cluster is symmetric
        self._ci_is_symmetric = None
        # Simplified config
        self.ci_simple_config = simple_config
        # ISO file path
        self.ci_iso_fpath = iso_fpath

    def ci_mdt_services(self):
        """
        Return a list of MDT services.
        """
        services = []
        for service in self.ci_service_dict.values():
            if service.ls_service_type == lustre.LUSTRE_SERVICE_TYPE_MDT:
                services.append(service)
        return services

    def ci_ost_services(self):
        """
        Return a list of OST services.
        """
        services = []
        for service in self.ci_service_dict.values():
            if service.ls_service_type != lustre.LUSTRE_SERVICE_TYPE_OST:
                continue
            services.append(service)
        return services

    def ci_mdt_ost_services(self):
        """
        Return a list of OST/MDT services.
        """
        services = []
        for service in self.ci_service_dict.values():
            if service.ls_service_type == lustre.LUSTRE_SERVICE_TYPE_MGT:
                continue
            services.append(service)
        return services

    def ci_mgs_services(self):
        """
        Return a list of MGS services (including MDT0).
        """
        mgs_serivces = list(self.ci_mgs_dict.values())
        for lustrefs in self.ci_lustre_dict.values():
            if lustrefs.lf_mgs is not None:
                continue
            mgs_serivces.append(lustrefs.lf_mgs_mdt)
        return mgs_serivces

    def ci_dump_simplified_config(self, log):
        """
        Dump the simplified config to stdout.
        """
        config_string = """#
# Please DO NOT edit this file directly!
#
# Simplified Clownfish config file (TOML format) generated by command
# "clownf simple_config".
#
# The original Clownfish config file might not be trivial for a script to
# parse parse when it has name lists with the format of host[0-100].
# Simplified config files, instead, do not contain any string of name list,
# yet are identical with the original config files.
#
"""
        config_string += toml.dumps(self.ci_simple_config)
        log.cl_stdout(config_string)
        return 0

    def _ci_cleanup_logs(self, log, hosts):
        """
        Cleanup logs if not debug and not specified log dir
        """
        if self.ci_log_to_file:
            return 0
        if not self.ci_logdir_is_default:
            return 0
        for host in hosts:
            command = ("rm -fr %s" % self.ci_workspace)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def ci_fini(self, log):
        """
        The instance is cleaning up
        """
        # pylint: disable=no-self-use
        log.cl_debug("finishing clownfish instance")

    def _ci_consul_config_init(self, log):
        """
        Init the Consul config of hosts/services
        """
        for hostname in self.ci_host_dict:
            ret = self.ci_host_autostart_disable(log, hostname)
            if ret:
                log.cl_error("failed to disable autostart of host [%s]",
                             hostname)
                return -1

        for service_name in self.ci_service_dict:
            ret = self.ci_service_autostart_disable(log, service_name)
            if ret:
                log.cl_error("failed to disable autostart of service [%s]",
                             service_name)
                return -1

            ret = clownf_consul.service_host_enable_disable(log,
                                                            self.ci_workspace,
                                                            self.ci_local_host,
                                                            self.ci_consul_cluster,
                                                            service_name,
                                                            None,
                                                            enable=False)
            if ret:
                log.cl_error("failed to init disabled_hosts config of "
                             "service [%s]", service_name)
                return -1
        return 0

    def ci_consul_restart(self, log, force=False):
        """
        Restart Consul services in the cluster
        """
        return self.ci_consul_cluster.cclr_restart(log, force=force,
                                                   restart=True)

    def ci_consul_start(self, log, force=False):
        """
        Start Consul services in the cluster
        """
        return self.ci_consul_cluster.cclr_restart(log, force=force,
                                                   restart=False)

    def _ci_consul_init(self, log, force=False, install=True):
        """
        Install or reset the Consul cluster
        :param force: continue even hit failures
        """
        rc = 0
        log.cl_info("cleaning up old Consul data in the cluster")
        ret = self.ci_consul_cluster.cclr_cleanup(log, force=force)
        if ret:
            rc = -1
            log.cl_error("failed to cleanup Consul dir in the cluster")
            if not force:
                return -1

        if install:
            log.cl_info("installing Consul in the cluster")
            ret = self.ci_consul_cluster.cclr_install(log, self.ci_workspace,
                                                      self.ci_local_host,
                                                      self.ci_iso_dir)
            if ret:
                rc = -1
                log.cl_error("failed to install Consul in the cluster")
                if not force:
                    return -1

        ret = self.ci_consul_restart(log, force=force)
        if ret:
            rc = -1
            log.cl_error("failed to restart Consul in the cluster")
            if not force:
                return -1

        log.cl_info("initing service/host configs in Consul")
        ret = self._ci_consul_config_init(log)
        if ret:
            rc = -1
            log.cl_error("failed to init Consul configs")
            if not force:
                return -1
        return rc

    def ci_consul_reset(self, log, force=False):
        """
        Cleanup Consul datadir and restart the service
        """
        return self._ci_consul_init(log, force=force, install=False)

    def ci_consul_leader(self, log):
        """
        Return the hostname of Consul leader

        If no leader, return "". If failure return None.
        """
        return self.ci_consul_cluster.cclr_leader(log)

    def _ci_init_install_or_prepare(self, log, iso=None):
        """
        Init using the ISO so we can install or prepare the hosts
        """
        # pylint: disable=too-many-branches,too-many-statements
        if iso is not None:
            ret = install_common.sync_iso_dir(log, self.ci_workspace,
                                              self.ci_local_host, iso,
                                              self.ci_iso_dir)
            if ret:
                log.cl_error("failed to sync ISO files from [%s] to dir "
                             "[%s] on local host [%s]",
                             iso, self.ci_iso_dir,
                             self.ci_local_host.sh_hostname)
                return -1

        lustre_dir = (self.ci_iso_dir + "/" +
                      constant.CORAL_LUSTRE_RELEASE_BASENAME)
        e2fsprogs_dir = (self.ci_iso_dir + "/" +
                         constant.E2FSPROGS_RPM_DIR_BASENAME)
        lustre_dict = lustre.get_lustre_dist(log, self.ci_local_host,
                                             lustre_dir, e2fsprogs_dir)
        if lustre_dict is None:
            log.cl_error("some directories are missing under directory [%s] "
                         "of local host [%s]",
                         self.ci_iso_dir,
                         self.ci_local_host.sh_hostname)
            return -1

        for host in self.ci_host_dict.values():
            if host.lsh_lustre_dist is None:
                log.cl_debug("use default Lustre for host [%s]",
                             host.sh_hostname)
                host.lsh_lustre_dist = lustre_dict
            else:
                log.cl_debug("use specific Lustre for host [%s]",
                             host.sh_hostname)
        return 0

    def _ci_services_autostart_enable_disable(self, log, service_names,
                                              enable=True):
        """
        Enable or disable autostart of services
        """
        for service_name in service_names:
            if enable:
                ret = self.ci_service_autostart_enable(log, service_name)
                if ret:
                    log.cl_error("failed to enable the autostart of service [%s]",
                                 service_name)
                    return -1
            else:
                ret = self.ci_service_autostart_disable(log, service_name)
                if ret:
                    log.cl_error("failed to disable the autostart of service [%s]",
                                 service_name)
                    return -1
        return 0

    def _ci_hosts_autostart_enable_disable(self, log, hostnames,
                                           enable=True):
        """
        Enable or disable autostart of hosts
        """
        for hostname in hostnames:
            if enable:
                ret = self.ci_host_autostart_enable(log, hostname)
                if ret:
                    log.cl_error("failed to enable the autostart of host [%s]",
                                 hostname)
                    return -1
            else:
                ret = self.ci_host_autostart_disable(log, hostname)
                if ret:
                    log.cl_error("failed to disable the autostart of host [%s]",
                                 hostname)
                    return -1
        return 0

    def _ci_cluster_autostart_enable_disable(self, log, enable=True):
        """
        Enable or disable autostart of all services/hosts
        """
        service_names = list(self.ci_service_dict.keys())
        ret = self._ci_services_autostart_enable_disable(log, service_names,
                                                         enable=enable)
        if ret:
            return -1

        hostnames = list(self.ci_host_dict.keys())
        ret = self._ci_hosts_autostart_enable_disable(log, hostnames,
                                                      enable=enable)
        if ret:
            return -1
        return 0

    def ci_cluster_autostart_enable(self, log):
        """
        Enable autostart of all services
        """
        return self._ci_cluster_autostart_enable_disable(log, enable=True)

    def ci_cluster_autostart_disable(self, log):
        """
        Disable autostart of all services/hosts
        """
        return self._ci_cluster_autostart_enable_disable(log, enable=False)

    def _ci_check_umount_autostart_conflict(self, log, service_names, force):
        """
        Check whether umount conflicts with autostart
        """
        conflicts = []
        for service_name in service_names:
            ret = self.ci_service_autostart_check_enabled(log, service_name)
            if ret < 0:
                if force:
                    log.cl_warning("failed to check whether autostart is "
                                   "enabled for service [%s], ignoring",
                                   service_name)
                else:
                    log.cl_error("failed to check whether autostart is "
                                 "enabled for service [%s]", service_name)
                    return -1
            elif ret:
                conflicts.append(service_name)

        if len(conflicts) != 0:
            name = ""
            for service_name in conflicts:
                if name == "":
                    name = service_name
                else:
                    name += ", " + service_name
            if force:
                log.cl_warning("service [%s] would be mounted back by "
                               "autostart, ignoring", name)
            else:
                log.cl_error("service [%s] would be mounted back by "
                             "autostart, use --force to ignore",
                             name)
                return -1
        return 0

    def ci_cluster_umount(self, log, force=False):
        """
        Umount all file system and MGS
        """
        service_names = list(self.ci_service_dict.keys())
        ret = self._ci_check_umount_autostart_conflict(log, service_names,
                                                       force)
        if ret:
            return -1

        for lustrefs in self.ci_lustre_dict.values():
            ret = lustrefs.lf_umount(log)
            if ret:
                log.cl_error("failed to umount file system [%s]",
                             lustrefs.lf_fsname)
                return -1

        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_umount(log)
            if ret:
                log.cl_error("failed to umount MGS [%s]",
                             mgs.ls_service_name)
                return -1

        return 0

    def ci_cluster_mount(self, log):
        """
        Mount all file system and MGS
        """
        for mgs in self.ci_mgs_dict.values():
            ret = self.ci_service_mount(log, mgs, quiet=True)
            if ret:
                log.cl_error("failed to mount MGS [%s]",
                             mgs.ls_service_name)
                return -1

        for lustrefs in self.ci_lustre_dict.values():
            ret = self.ci_lustre_mount(log, lustrefs)
            if ret:
                log.cl_error("failed to mount file system [%s]",
                             lustrefs.lf_fsname)
                return -1

        return 0

    def ci_cluster_format(self, log):
        """
        Format all file system and MGS
        """
        ret = self.ci_cluster_umount(log)
        if ret:
            log.cl_error("failed to umount all Lustre services in the cluster")
            return ret

        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_format(log)
            if ret:
                log.cl_error("failed to format MGS [%s]",
                             mgs.ls_service_name)
                return -1

        for lustrefs in self.ci_lustre_dict.values():
            ret = lustrefs.lf_format(log)
            if ret:
                log.cl_error("failed to format file system [%s]",
                             lustrefs.lf_fsname)
                return -1
        return 0

    def ci_cluster_prepare(self, log, workspace, lazy=True, umount_first=True):
        """
        Prepare all hosts
        """
        # pylint: disable=too-many-locals
        args_array = []
        thread_ids = []
        skipped_host_str = ""
        preparing_host_str = ""
        for host in self.ci_host_dict.values():
            if (not host.lsh_is_server) and (not host.lsh_is_client):
                if skipped_host_str != "":
                    skipped_host_str += ","
                skipped_host_str += host.sh_hostname
                continue
            if preparing_host_str != "":
                preparing_host_str += ","
            preparing_host_str += host.sh_hostname
            args = (host, lazy)
            args_array.append(args)
            thread_id = "prepare_%s" % host.sh_hostname
            thread_ids.append(thread_id)

        message = "starting to prepare hosts [%s]" % preparing_host_str
        if skipped_host_str == "":
            message += " which include all hosts managed by Clownfish"
        else:
            message += ", skipped hosts [%s]" % skipped_host_str

        log.cl_info(message)

        ret = self._ci_init_install_or_prepare(log)
        if ret:
            log.cl_error("failed to init for cluster preparation")
            return -1

        if umount_first:
            ret = self.ci_cluster_umount(log)
            if ret:
                log.cl_warning("failed to umount all Lustre services, continue")

        parallel_execute = parallel.ParallelExecute(workspace,
                                                    "host_prepare",
                                                    host_lustre_prepare,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=8)
        if ret:
            return -1

        # Cleanup logs
        hosts = list(self.ci_host_dict.values())
        if self.ci_local_host not in hosts:
            hosts.append(self.ci_local_host)
        return self._ci_cleanup_logs(log, hosts)

    def _ci_host_migrate_service(self, log, service, hostname):
        """
        Migrate the Lustre service out of the host
        """
        ret = self.ci_service_mount(log, service,
                                    exclude_hostnames=[hostname],
                                    quiet=True)
        if ret:
            log.cl_error("failed to migrate service [%s] out of host [%s]",
                         service.ls_service_name, hostname)
            return -1
        return 0

    def ci_host_migrate_services(self, log, hostname, force=False):
        """
        Migrate all Lustre services out of the host
        :param force: continue migration of other services even hit failures
        """
        # pylint: disable=too-many-branches
        if hostname not in self.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            return -1
        host = self.ci_host_dict[hostname]

        if host.lsh_mgsi is None:
            instances = []
        else:
            instances = [host.lsh_mgsi]
        instances += list(host.lsh_ost_instances.values())
        instances += list(host.lsh_mdt_instances.values())
        for instance in instances[:]:
            service = instance.lsi_service
            service_name = service.ls_service_name

            mounted = instance.lsi_check_mounted(log)
            if mounted < 0:
                log.cl_error("failed to check whether service [%s] is "
                             "mounted on host [%s]", service_name, hostname)
                return -1
            if mounted == 0:
                instances.remove(instance)
                continue

            ret = self.ci_service_autostart_check_enabled(log, service_name)
            if ret < 0:
                log.cl_error("failed to check whether autostart of service "
                             "[%s] is enabled", service_name)
                return -1
            if ret:
                # Autostart enabled, check whether service can be autostart on host
                ret = clownf_consul.service_host_check_enabled(log,
                                                               self.ci_consul_cluster,
                                                               service_name,
                                                               hostname)
                if ret < 0:
                    log.cl_error("failed to check whether service [%s] can "
                                 "be mount on host [%s]",
                                 service_name, hostname)
                    return -1
                if ret:
                    log.cl_error("migration could fail because service [%s] "
                                 "can be autostarted on host [%s]",
                                 service_name, hostname)
                    log.cl_error("please either disable autostart for the "
                                 "service, or disable the host for the service")
                    return -1

        exit_status = 0
        for instance in instances:
            service = instance.lsi_service
            ret = self._ci_host_migrate_service(log, service, hostname)
            if ret:
                if not force:
                    return -1
                exit_status = -1
        return exit_status

    def ci_host_prepare(self, log, workspace, hostname, lazy=True):
        """
        Migrate the services on the host out and prepare the host for Lustre
        service
        """
        host = self.ci_host_dict[hostname]
        if (not host.lsh_is_server) and (not host.lsh_is_client):
            return 0

        ret = self._ci_init_install_or_prepare(log)
        if ret:
            log.cl_error("failed to init for host preparation")
            return -1

        ret = self.ci_host_migrate_services(log, hostname)
        if ret:
            log.cl_error("failed to migrate services out of host [%s]",
                         hostname)
            return ret

        ret = host.lsh_lustre_prepare(log, workspace,
                                      lazy_prepare=lazy)
        if ret:
            log.cl_error("failed to prepare host [%s] to run Lustre service",
                         hostname)
            return -1

        # Cleanup logs
        hosts = [host]
        if self.ci_local_host not in hosts:
            hosts.append(self.ci_local_host)
        return self._ci_cleanup_logs(log, hosts)

    def ci_host_enable_disable_service(self, log, hostname, service_name,
                                       enable=True):
        """
        Enable or disable a Lustre service on the host
        """
        if enable:
            operate = "enable"
        else:
            operate = "disable"
        ret = clownf_consul.service_host_enable_disable(log,
                                                        self.ci_workspace,
                                                        self.ci_local_host,
                                                        self.ci_consul_cluster,
                                                        service_name,
                                                        hostname,
                                                        enable=enable)
        if ret:
            log.cl_error("failed to %s service [%s] on host [%s]",
                         operate, service_name, hostname)
            return -1
        log.cl_info("%sd service [%s] on host [%s]",
                    operate, service_name, hostname)
        return 0

    def _ci_host_enable_disable_services(self, log, hostname, enable=True):
        """
        Enable or disable all Lustre services on the host
        """
        if hostname not in self.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            return -1
        host = self.ci_host_dict[hostname]

        if host.lsh_mgsi is None:
            instances = []
        else:
            instances = [host.lsh_mgsi]
        instances += list(host.lsh_ost_instances.values())
        instances += list(host.lsh_mdt_instances.values())
        for instance in instances:
            service = instance.lsi_service
            service_name = service.ls_service_name
            ret = self.ci_host_enable_disable_service(log, hostname,
                                                      service_name,
                                                      enable=enable)
            if ret:
                return -1
        return 0

    def ci_host_enable_services(self, log, hostname):
        """
        Enable all Lustre services on the host
        """
        return self._ci_host_enable_disable_services(log, hostname,
                                                     enable=True)

    def ci_host_disable_services(self, log, hostname):
        """
        Disable all Lustre services on the host
        """
        return self._ci_host_enable_disable_services(log, hostname,
                                                     enable=False)

    def ci_host_operating(self, log, hostname):
        """
        Return 1 if host is running clownf command. Return 0 if not.
        Return negative on error.
        """
        if hostname not in self.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            return -1
        host = self.ci_host_dict[hostname]
        return self.ci_host_check_operating(log, host)

    def _ci_host_autostart_enable_disable(self, log, hostname, enable=True):
        """
        Enable or disable the autostart of a host
        """
        ret = clownf_consul.host_autostart_enable_disable(log,
                                                          self.ci_workspace,
                                                          self.ci_local_host,
                                                          self.ci_consul_cluster,
                                                          hostname,
                                                          enable=enable)
        if ret:
            return -1
        return 0

    def ci_host_autostart_enable(self, log, hostname):
        """
        Enable the autostart of a host
        """
        return self._ci_host_autostart_enable_disable(log, hostname,
                                                      enable=True)

    def ci_host_autostart_disable(self, log, hostname):
        """
        Disable the autostart of a host
        """
        return self._ci_host_autostart_enable_disable(log, hostname,
                                                      enable=False)

    def ci_host_autostart_check_enabled(self, log, hostname,
                                        consul_hostname=None):
        """
        Return 0 if host is disabled, return 1 if host is enabled. Return
        negative on error.
        """
        return clownf_consul.host_autostart_check_enabled(log,
                                                          self.ci_consul_cluster,
                                                          hostname,
                                                          consul_hostname=consul_hostname)

    def ci_host_shutdown(self, log, hostname, force=False, skip_migrate=False):
        """
        Migrate all the service out and shutdown the host
        """
        if hostname not in self.ci_host_dict:
            log.cl_error("host [%s] is not configured", hostname)
            return -1

        rc = self.ci_host_autostart_check_enabled(log, hostname)
        if rc < 0:
            if force:
                log.cl_warning("failed to get the autostart config of "
                               "host [%s], ignoring", hostname)
            else:
                log.cl_error("failed to get the autostart config of "
                             "host [%s]", hostname)
                log.cl_error("aborting shutdown host [%s]", hostname)
                return -1
        elif rc:
            if force:
                log.cl_warning("shutdown might conflict with autostart of "
                               "host [%s], ignoring", hostname)
            else:
                log.cl_error("shutdown conflicts with autostart of "
                             "host [%s], use --force to ignore",
                             hostname)
                return -1

        if not skip_migrate:
            ret = self.ci_host_migrate_services(log, hostname, force=force)
            if ret:
                if not force:
                    log.cl_error("aborting shutdown host [%s] because not able to migrate services",
                                 hostname)
                    return -1

        # Improvement: if failed to migrate, still do umount to make shutdown easier
        if hostname not in self.ci_stonith_dict:
            log.cl_error("can not shutdown host [%s] because no stonith "
                         "configured for it", hostname)
            return -1

        stonith_host = self.ci_stonith_dict[hostname]
        return stonith_host.sth_stop(log)

    def ci_hosts_start(self, log, hostnames):
        """
        Start the hosts if it is down
        """
        for hostname in hostnames:
            if hostname not in self.ci_host_dict:
                log.cl_error("host [%s] is not configured", hostname)
                return -1

        rc = 0
        already_started = True
        for hostname in hostnames:
            host = self.ci_host_dict[hostname]
            if host.sh_is_up(log):
                continue

            already_started = False
            if hostname not in self.ci_stonith_dict:
                log.cl_error("can not start host [%s] because no stonith "
                             "configured for it", hostname)
                return -1

            stonith_host = self.ci_stonith_dict[hostname]
            ret = stonith_host.sth_start(log)
            if ret:
                rc = -1
        if rc == 0:
            if already_started:
                log.cl_stdout(clownf_constant.CLF_MSG_ALREADY_STARTED[:-1])
            else:
                log.cl_stdout("Started")
        return rc

    def ci_host_watcher(self, log, hostname, consul_hostname=None):
        """
        Get the watcher hostname of the host

        If not exist, return (0, None). Return (negative, None) on error.
        """
        return clownf_consul.host_get_watcher(log, self.ci_consul_cluster,
                                              hostname,
                                              consul_hostname=consul_hostname)

    def _ci_host_watching_or_watcher_candidates_hosts(self, log, hostname,
                                                      is_watcher=False):
        """
        Get the host name list that could be watched by the given host,
        or watching the given host
        """
        sorted_hostnames = list(self.ci_host_dict.keys())
        sorted_hostnames.sort()
        # The list can be got by the reverse process of getting watching
        # candidates
        if is_watcher:
            sorted_hostnames.reverse()
        candidates = []
        found_myself = False
        for name in sorted_hostnames:
            if name == hostname:
                found_myself = True
                continue
            if found_myself:
                candidates.append(name)
                if len(candidates) >= clownf_constant.CLF_MAX_WATCH_HOST:
                    break

        # This is a loop, add the first few hosts to watch list
        if len(candidates) < clownf_constant.CLF_MAX_WATCH_HOST:
            for name in sorted_hostnames:
                if name == hostname:
                    break
                candidates.append(name)
                if len(candidates) >= clownf_constant.CLF_MAX_WATCH_HOST:
                    break
        return candidates

    def ci_host_watcher_candidates(self, log, hostname):
        """
        Get the host name list that could watch the given host
        """
        return self._ci_host_watching_or_watcher_candidates_hosts(log, hostname,
                                                                  is_watcher=True)

    def ci_host_watcher_dict(self, log, hosts):
        """
        Return a dict.
        The key is hostname.
        The value is the hostname that is watching the key host.
        Value could be None, meaning no watcher.

        Return None on error.
        """
        watcher_dict = {}
        for host in hosts:
            watching_hostname = host.sh_hostname
            ret, watcher = \
                clownf_consul.host_get_watcher(log, self.ci_consul_cluster,
                                               watching_hostname)
            if ret:
                log.cl_error("failed to get the watcher of host [%s]",
                             watching_hostname)
                return None
            watcher_dict[watching_hostname] = watcher
        return watcher_dict

    def ci_service_watcher_dict(self, log, services):
        """
        Return a dict.
        The key is service name.
        The value is the hostname that is watching the key service.
        Value could be None, meaning no watcher.

        Return None on error.
        """
        watcher_dict = {}
        for service in services:
            watching_service = service.ls_service_name
            ret, watcher = clownf_consul.service_get_watcher(log,
                                                             self.ci_consul_cluster,
                                                             watching_service)
            if ret:
                log.cl_error("failed to get the watcher of service [%s]",
                             watching_service)
                return None
            watcher_dict[watching_service] = watcher
        return watcher_dict

    def ci_host_watching_hosts(self, log, hostname, consul_hostname=None):
        """
        Get the hostname list being watched by the host

        If not exist, return (0, []). Return (negative, None) on error.
        """
        watching_hostnames = []
        for watching_hostname in self.ci_host_dict:
            ret, watcher = \
                clownf_consul.host_get_watcher(log, self.ci_consul_cluster,
                                               watching_hostname,
                                               consul_hostname=consul_hostname)
            if ret:
                log.cl_error("failed to get the watcher of host [%s]",
                             watching_hostname)
                return -1, None
            if watcher == hostname:
                watching_hostnames.append(watching_hostname)
        return 0, watching_hostnames

    def ci_host_watching_candidate_hosts(self, log, hostname):
        """
        Get the host name list that could be watched by the given host
        """
        return self._ci_host_watching_or_watcher_candidates_hosts(log, hostname,
                                                                  is_watcher=False)

    def ci_host_watching_services(self, log, hostname, consul_hostname=None):
        """
        Get the service name list being watched by the host

        If not exist, return (0, []). Return (negative, None) on error.
        """
        watching_service_names = []
        for watching_servicename in self.ci_service_dict:
            ret, watcher = \
                clownf_consul.service_get_watcher(log,
                                                  self.ci_consul_cluster,
                                                  watching_servicename,
                                                  consul_hostname=consul_hostname)
            if ret:
                log.cl_error("failed to get the watcher of service [%s]",
                             watching_servicename)
                return -1, None
            if watcher == hostname:
                watching_service_names.append(watching_servicename)
        return 0, watching_service_names

    def ci_host_consul_role(self, log, hostname):
        """
        Get the Consul role of the host
        """
        consul_cluster = self.ci_consul_cluster
        if hostname in consul_cluster.cclr_server_dict:
            return clownf_constant.CLOWNF_VALUE_SERVER
        if hostname in consul_cluster.cclr_client_dict:
            return clownf_constant.CLOWNF_VALUE_CLIENT
        return constant.CMD_MSG_NONE

    def ci_service_mount(self, log, service, only_hostnames=None,
                         exclude_hostnames=None, not_select_reason="not selected",
                         exclude_reason="excluded explicitly", quiet=False):
        """
        Mount Lustre service.
        This function will check Consul on the hosts that do not allow to
        mount this service.
        """
        service_name = service.ls_service_name
        exclude_dict = {}
        if exclude_hostnames is not None:
            for hostname in exclude_hostnames:
                exclude_dict[hostname] = exclude_reason

        ret, exclude_hostnames = \
            clownf_consul.service_disabled_hostnames(log,
                                                     self.ci_consul_cluster,
                                                     service_name)
        if ret:
            log.cl_error("failed to get the disabled hosts of service [%s]",
                         service_name)
            return -1

        for hostname in exclude_hostnames:
            exclude_dict[hostname] = "disabled in Consul config"

        selected_hostnames = service.ls_filter_instances(log,
                                                         only_hostnames,
                                                         exclude_dict,
                                                         not_select_reason=not_select_reason)
        if selected_hostnames is None:
            return -1

        ret = service.ls_mount(log, selected_hostnames=selected_hostnames,
                               exclude_dict=exclude_dict, quiet=quiet)
        return ret

    def ci_service_umount(self, log, service, force=False):
        """
        Umount Lustre service.
        This function will check whether the autostart of service is enabled.
        If so, return failure
        """
        service_names = [service.ls_service_name]

        ret = self._ci_check_umount_autostart_conflict(log, service_names,
                                                       force)
        if ret:
            return -1
        return service.ls_umount(log)

    def ci_lustre_mount(self, log, lustrefs, only_service=False):
        """
        Mount services of a Lustre file system.
        This function will check Consul on the hosts that do not allow to
        mount the service. Other things are the same with lf_mount()
        """
        # pylint: disable=too-many-branches
        if only_service:
            log.cl_info("mounting services of file system [%s]",
                        lustrefs.lf_fsname)
        else:
            log.cl_info("mounting services and clients of file system [%s]",
                        lustrefs.lf_fsname)

        if lustrefs.lf_mgs is not None:
            ret = self.ci_service_mount(log, lustrefs.lf_mgs, quiet=True)
            if ret:
                log.cl_error("failed to mount MGS of Lustre file "
                             "system [%s]", lustrefs.lf_fsname)
                return -1

        for service_name, mdt in lustrefs.lf_mdt_dict.items():
            ret = self.ci_service_mount(log, mdt, quiet=True)
            if ret:
                log.cl_error("failed to mount MDT [%s] of Lustre file "
                             "system [%s]", service_name, lustrefs.lf_fsname)
                return -1

        for service_name, ost in lustrefs.lf_ost_dict.items():
            ret = self.ci_service_mount(log, ost, quiet=True)
            if ret:
                log.cl_error("failed to mount OST [%s] of Lustre file "
                             "system [%s]", service_name, lustrefs.lf_fsname)
                return -1

        if not only_service:
            for client_index, client in lustrefs.lf_client_dict.items():
                ret = client.lc_mount(log, quiet=True)
                if ret:
                    log.cl_error("failed to mount client [%s] of Lustre file "
                                 "system [%s]", client_index, lustrefs.lf_fsname)
                    return -1


        return 0

    def ci_lustre_umount(self, log, lustrefs, force=False):
        """
        Umount lustre file system.
        This function will check whether autostart is enabled for any service.
        """
        service_names = list(lustrefs.lf_service_dict.keys())
        ret = self._ci_check_umount_autostart_conflict(log, service_names,
                                                       force)
        if ret:
            return -1

        ret = lustrefs.lf_umount(log, force_umount_mgt=force)
        if ret:
            log.cl_error("failed to umount all services of file system [%s]",
                         lustrefs.lf_fsname)
            return ret
        return 0

    def ci_lustre_format(self, log, lustrefs, force=False):
        """
        Format services of a Lustre file system.
        """
        fsname = lustrefs.lf_fsname
        if ((not force) and (lustrefs.lf_mgs is not None) and
                (len(lustrefs.lf_mgs.lmgs_filesystems) > 1)):
            log.cl_error("abort formatting file system [%s] because MGT [%s] "
                         "is shared by another file system",
                         fsname, lustrefs.lf_mgs.ls_service_name)
            return -1

        if self.ci_lustre_umount(log, lustrefs, force=force):
            return -1

        return lustrefs.lf_format(log, format_mgt=force)

    def ci_lustre_name2service(self, log, service_name):
        """
        Return the service by searching the name
        """
        if service_name in self.ci_mgs_dict:
            service = self.ci_mgs_dict[service_name]
        else:
            fields = service_name.split("-")
            if len(fields) != 2:
                log.cl_error("unexpected service name [%s]", service_name)
                return None

            fsname = fields[0]
            if fsname not in self.ci_lustre_dict:
                log.cl_error("Lustre file system with name [%s] is not configured",
                             fsname)
                return None
            lustrefs = self.ci_lustre_dict[fsname]
            if service_name not in lustrefs.lf_service_dict:
                log.cl_error("service [%s] is not configured for Lustre [%s]",
                             service_name, fsname)
                return None
            service = lustrefs.lf_service_dict[service_name]
        return service

    def ci_lustre_service_mount(self, log, service_name):
        """
        Mount the service
        """
        service = self.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("service [%s] is not configured", service_name)
            return -1

        ret = self.ci_service_mount(log, service)
        if ret:
            log.cl_error("failed to mount service [%s]", service_name)
            return -1
        log.cl_debug("mounted service [%s]", service_name)
        return 0

    def ci_lustre_service_umount(self, log, service_name, force=False):
        """
        Umount the service
        """
        service = self.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("service [%s] is not configured", service_name)
            return -1

        ret = self.ci_service_umount(log, service, force=force)
        if ret:
            return -1
        log.cl_debug("umounted service [%s]", service_name)
        return 0

    def _ci_lustre_autostart_enable_disable(self, log, lustrefs, enable=True):
        """
        Enable or disable autostart of all services/hosts in a Lustre file
        system
        """
        service_names = list(lustrefs.lf_service_dict.keys())
        ret = self._ci_services_autostart_enable_disable(log, service_names,
                                                         enable=enable)
        if ret:
            return -1

        hostnames = list(lustrefs.lf_host_dict().keys())
        ret = self._ci_hosts_autostart_enable_disable(log, hostnames,
                                                      enable=enable)
        if ret:
            return -1
        return 0

    def ci_lustre_autostart_enable(self, log, lustrefs):
        """
        Enable autostart of all services/hosts in a Lustre file system
        """
        return self._ci_lustre_autostart_enable_disable(log, lustrefs,
                                                        enable=True)

    def ci_lustre_autostart_disable(self, log, lustrefs):
        """
        Disable autostart of all services/hosts in a Lustre file system
        """
        return self._ci_lustre_autostart_enable_disable(log, lustrefs,
                                                        enable=False)

    def ci_cluster_install(self, log, iso=None, hosts=None):
        """
        Install Clownfish and Consul on all host (could include localhost)
        Lustre RPMs will not be installed on hosts. Lustre service will not be affected.
        """
        # pylint: disable=too-many-branches
        if hosts is None:
            hosts = list(self.ci_host_dict.values())
        ret = self._ci_init_install_or_prepare(log, iso=iso)
        if ret:
            log.cl_error("failed to prepare to install cluster")
            return -1

        rpms = constant.CORAL_DEPENDENT_RPMS + clownf_constant.CLOWNF_DEPENDENT_RPMS

        ret = install_common.install_rpms_from_iso(log, self.ci_workspace,
                                                   self.ci_local_host,
                                                   self.ci_iso_dir,
                                                   rpms,
                                                   "installation_server")
        if ret:
            log.cl_error("failed to install RPMs on local host [%s] needed for installation",
                         self.ci_local_host.sh_hostname)
            return -1

        send_fpath_dict = {}
        send_fpath_dict[self.ci_config_fpath] = \
            clownf_constant.CLOWNF_CONFIG
        for distribution in self.ci_lustre_distribution_dict.values():
            fpath = distribution.ldis_lustre_rpm_dir
            send_fpath_dict[fpath] = fpath
            fpath = distribution.ldis_e2fsprogs_rpm_dir
            send_fpath_dict[fpath] = fpath
        need_backup_fpaths = []
        install_cluster = \
            install_common.CoralInstallationCluster(self.ci_workspace,
                                                    self.ci_local_host,
                                                    self.ci_iso_dir)
        install_cluster.cic_add_hosts(hosts,
                                      [],
                                      rpms,
                                      send_fpath_dict,
                                      need_backup_fpaths,
                                      coral_reinstall=True)
        ret = install_cluster.cic_install(log)
        if ret:
            log.cl_error("failed to install dependent RPMs on all hosts of "
                         "the cluster")
            return -1

        ret = self._ci_consul_init(log, force=False, install=True)
        if ret:
            log.cl_error("failed to install Consul")
            return -1

        for host in hosts:
            log.cl_info("starting clownf_agent on host [%s]",
                        host.sh_hostname)
            command = ("systemctl daemon-reload")
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            # Need to stop service, otherwise the running agent might not
            # notice that the clownfish.conf has been changed.
            ret = host.sh_service_stop(log, "clownf_agent")
            if ret != 0:
                log.cl_error("failed to stop clownf_agent service on host [%s]",
                             host.sh_hostname)
                return -1

            ret = host.sh_service_start(log, "clownf_agent")
            if ret != 0:
                log.cl_error("failed to start clownf_agent service on host [%s]",
                             host.sh_hostname)
                return -1

            log.cl_info("configuring autostart of clownf_agent on host [%s]",
                        host.sh_hostname)
            command = "systemctl enable clownf_agent"
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        # Cleanup logs
        if self.ci_local_host not in hosts:
            hosts.append(self.ci_local_host)
        return self._ci_cleanup_logs(log, hosts)

    def ci_service_watcher(self, log, service_name, consul_hostname=None):
        """
        Get the watcher hostname of the service

        If not exist, return (0, None). Return (negative, None) on error.
        """
        return clownf_consul.service_get_watcher(log, self.ci_consul_cluster,
                                                 service_name,
                                                 consul_hostname=consul_hostname)

    def ci_service_watcher_candidates(self, log, service):
        """
        Get the candidate watcher hostnames of the service

        If not exist, return (0, None). Return (negative, None) on error.
        """
        # pylint: disable=no-self-use
        hostnames = []
        for instance in service.ls_instance_dict.values():
            host = instance.lsi_host
            hostname = host.sh_hostname
            hostnames.append(hostname)
        return 0, hostnames

    def ci_host_watching_candidate_services(self, log, host):
        """
        Get the candiadte service names that could be watched by the given
        host
        """
        # pylint: disable=no-self-use
        service_names = list(host.lsh_instance_dict.keys())
        return service_names

    def ci_service_autostart_check_enabled(self, log, service_name,
                                           consul_hostname=None):
        """
        Return 1 if autostart enabled, 0 if not. Negative on error.
        """
        service = self.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("Lustre service [%s] does not exist",
                         service_name)
            return -1
        return clownf_consul.service_autostart_check_enabled(log,
                                                             self.ci_consul_cluster,
                                                             service_name,
                                                             consul_hostname=consul_hostname)

    def ci_service_host_check_enabled(self, log, service_name, hostname,
                                      consul_hostname=None):
        """
        Return 1 if the service can be mounted on host, 0 if not.
        Negative on error.
        """
        service = self.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("Lustre service [%s] does not exist",
                         service_name)
            return -1
        return clownf_consul.service_host_check_enabled(log,
                                                        self.ci_consul_cluster,
                                                        service_name,
                                                        hostname,
                                                        consul_hostname=consul_hostname)

    def _ci_service_autostart_enable_disable(self, log, service_name,
                                             enable=True):
        """
        Enable or disable the autostart of a service
        """
        service = self.ci_lustre_name2service(log, service_name)
        if service is None:
            log.cl_error("Lustre service [%s] does not exist",
                         service_name)
            return -1

        ret = clownf_consul.service_autostart_enable_disable(log, self.ci_workspace,
                                                             self.ci_local_host,
                                                             self.ci_consul_cluster,
                                                             service_name,
                                                             enable=enable)
        if ret:
            return -1
        return 0

    def ci_service_autostart_enable(self, log, service_name):
        """
        Enable the autostart of a service
        """
        return self._ci_service_autostart_enable_disable(log, service_name,
                                                         enable=True)

    def ci_service_autostart_disable(self, log, service_name):
        """
        Disable the autostart of a service
        """
        return self._ci_service_autostart_enable_disable(log, service_name,
                                                         enable=False)

    def _ci_service_enabled_disabled_hostnames(self, log, service, enable=True,
                                               consul_hostname=None):
        """
        Return hostnames that enable or disable mounting of the service
        """
        service_name = service.ls_service_name

        ret, exclude_hostnames = \
            clownf_consul.service_disabled_hostnames(log,
                                                     self.ci_consul_cluster,
                                                     service_name,
                                                     consul_hostname=consul_hostname)
        if ret:
            log.cl_error("failed to get the disabled hosts of service [%s]",
                         service_name)
            return None
        hostnames = []
        for instance in service.ls_instance_dict.values():
            hostname = instance.lsi_host.sh_hostname
            if enable:
                if hostname in exclude_hostnames:
                    continue
            else:
                if hostname not in exclude_hostnames:
                    continue
            if hostname in hostnames:
                continue
            hostnames.append(hostname)
        return hostnames

    def ci_service_enabled_hostnames(self, log, service):
        """
        Return hostnames that enable mounting of the service
        """
        return self._ci_service_enabled_disabled_hostnames(log, service,
                                                           enable=True)

    def ci_service_disabled_hostnames(self, log, service):
        """
        Return hostnames that disable mounting of the service
        """
        return self._ci_service_enabled_disabled_hostnames(log, service,
                                                           enable=False)

    def ci_host_check_operating(self, log, host):
        """
        Whether the host is running operation commands (not include clownf_agent)
        If true, return 1, if false, return 0. Return negative on error.
        """
        # pylint: disable=no-self-use,too-many-branches
        command = "pgrep clownf"
        retval = host.sh_run(log, command)
        if (retval.cr_exit_status == 1 and
                retval.cr_stdout == ""):
            return 0

        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, host.sh_hostname)
            return -1

        clownf_pids = retval.cr_stdout.splitlines()

        clownf_agent_pids = []
        command = "pgrep clownf_agent"
        retval = host.sh_run(log, command)
        if (retval.cr_exit_status == 1 and
                retval.cr_stdout == ""):
            pass
        elif retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, host.sh_hostname)
            return -1
        else:
            clownf_agent_pids = retval.cr_stdout.splitlines()

        suspicious_clownf_pids = []
        for clownf_pid in clownf_pids:
            if clownf_pid not in clownf_agent_pids:
                suspicious_clownf_pids.append(clownf_pid)

        if len(suspicious_clownf_pids) == 0:
            return 0

        if not host.sh_is_localhost():
            return 1

        process_id = os.getpid()
        parent_id = os.getppid()

        for clownf_pid in suspicious_clownf_pids:
            if clownf_pid in [str(process_id), str(parent_id)]:
                continue
            return 1
        return 0

    def ci_host_check_running_agent(self, log, host):
        """
        Whether the host is running clownf_agent
        If true, return 1, if false, return 0. Return negative on error.
        """
        # pylint: disable=no-self-use
        command = "ps aux | grep clownf_agent | grep -v grep"
        retval = host.sh_run(log, command)
        if (retval.cr_exit_status == 1 and
                retval.cr_stdout == ""):
            return 0
        if retval.cr_exit_status:
            return -1
        return 1


    def ci_host_collect(self, log, logdir, host, path):
        """
        Collect file/dir
        """
        local_host = self.ci_local_host

        command = "mkdir -p %s" % logdir
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, local_host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if host.sh_is_localhost():
            command = "cp -a %s %s" % (path, logdir)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        else:
            ret = host.sh_get_file(log, path, logdir)
            if ret:
                log.cl_error("failed to get file [%s] on host [%s] to dir "
                             "[%s] on local host [%s]", path,
                             host.sh_hostname, logdir,
                             local_host.sh_hostname)
                return -1

        return 0

    def ci_hosts_collect(self, log, hosts, path):
        """
        Collect file/dir from hosts
        """
        log.cl_info("collecting file [%s] from hosts to dir [%s]",
                    path, self.ci_workspace)
        args_array = []
        thread_ids = []
        for host in hosts:
            args = (host, path)
            args_array.append(args)
            thread_id = host.sh_hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(self.ci_workspace,
                                                    "collect",
                                                    self.ci_host_collect,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, quit_on_error=False,
                                      parallelism=10)

        log.cl_info("log saved to [%s] on local host [%s]", self.ci_workspace,
                    self.ci_local_host.sh_hostname)
        return ret

    def ci_clownf_agent_stop(self, log):
        """
        Stop all clownf_agents in the whole cluster
        """
        for host in self.ci_host_dict.values():
            command = "systemctl stop clownf_agent"
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def ci_is_symmetric(self, log):
        """
        A cluster is symmetric iff:

        1) If two hosts can provide a same service, then any service provided
           by one host should be able to be provided by the other host.

        2) If two hosts can provide a same service, then these two hosts should
           (be considered to) have the same capability of providing services.

        3) If a host can provide multiple services, then the loads of all these
           services should be (considered as) the same.

        A cluster is considered as well-balanced iff the following value is
        minimum in all possible layouts of services:

        SumHost((Load.i / Cap.i - SumHost(Load.i) / SumHost(Cap.i)) ^ 2)

        SumHost($value): For each host [i], sum all $value.
        Load.i: The load on host [i]. Load of each service could be different.
        Cap.i: The capability of host [i]. Capability of each host could be
               different.

        It is much easier to well-balance the load in a symmetric cluster. The
        simplest way is: always choose the host with lowest load when mounting
        a service. Thus, when building a cluster, always try to make it
        symmetric.
        """
        if self._ci_is_symmetric is not None:
            return self._ci_is_symmetric
        for host in self.ci_host_dict.values():
            symmetric_hosts = {}
            symmetric_hosts[host.sh_hostname] = host
            for instance in host.lsh_instance_dict.values():
                service = instance.lsi_service
                for service_instance in service.ls_instance_dict.values():
                    remote_host = service_instance.lsi_host
                    if remote_host in symmetric_hosts:
                        continue
                    if hosts_have_same_services(log, host, remote_host):
                        symmetric_hosts[remote_host.sh_hostname] = remote_host
                        continue
                    self._ci_is_symmetric = False
                    return False
        self._ci_is_symmetric = True
        return True

    def ci_load_balanced(self, log):
        """
        Return 1 if the cluster is balanced. Return 0 if not. Return negative
        on error.

        A symmetric cluster is balanced iff:

        Every host provides the mean load in the symmetric host group.

        A symmetric host group in a symmetric cluster includes all the hosts
        that can provide the same services.
        """
        if not self.ci_is_symmetric(log):
            log.cl_error("unable to check whether load balanced on "
                         "asymmetric cluster")
            return -1

        for host in self.ci_host_dict.values():
            load_status = host.lsh_get_load_status(log)
            if load_status is None:
                return -1
            if load_status != 0:
                return 0
        return 1



def parse_consul_server_config(log, config_fpath, consul_config, host_dict,
                               simple_config):
    """
    Parse the config of Consul server
    """
    # pylint: disable=too-many-locals
    consul_servers = {}
    consul_server_configs = utils.config_value(consul_config,
                                               clownf_constant.CLF_SERVERS)
    if consul_server_configs is None:
        log.cl_error("no [%s] is configured in Consul section",
                     clownf_constant.CLF_SERVERS)
        return None
    # CLF_SERVERS of CLF_CONSUL is nontrivial. CLF_HOSTNAME and CLF_BIND_ADDR
    # could be lists.
    simple_consul_server_configs = []
    simple_consul_config = simple_config[clownf_constant.CLF_CONSUL]
    simple_consul_config[clownf_constant.CLF_SERVERS] = simple_consul_server_configs

    for consul_server_config in consul_server_configs:
        hostname_config = utils.config_value(consul_server_config,
                                             clownf_constant.CLF_HOSTNAME)
        if hostname_config is None:
            log.cl_error("no [%s] is configured in one of Consul servers",
                         clownf_constant.CLF_HOSTNAME)
            return None

        hostnames = cmd_general.parse_list_string(log, hostname_config)
        if hostnames is None:
            log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                         hostname_config,
                         clownf_constant.CLF_HOSTNAME, config_fpath)
            return None

        bind_addr_config = utils.config_value(consul_server_config,
                                              clownf_constant.CLF_BIND_ADDR)
        if bind_addr_config is None:
            log.cl_error("no [%s] is configured in one of Consul servers",
                         clownf_constant.CLF_BIND_ADDR)
            return None

        bind_addrs = cmd_general.parse_list_string(log, bind_addr_config)
        if bind_addrs is None:
            log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                         bind_addr_config,
                         clownf_constant.CLF_BIND_ADDR, config_fpath)
            return None

        if len(bind_addrs) != len(hostnames):
            log.cl_error("the number of [%s] should be equal to the "
                         "number of [%s]",
                         clownf_constant.CLF_BIND_ADDR,
                         clownf_constant.CLF_HOSTNAME)
            return None

        index_number = 0
        for hostname in hostnames:
            if hostname not in host_dict:
                log.cl_error("host [%s] is configured as Consul server, "
                             "but not as a non-standalone host, please "
                             "correct file [%s]",
                             hostname, config_fpath)
                return None

            if hostname in consul_servers:
                log.cl_error("multiple configurations for host [%s] as Consul server, "
                             "please correct file [%s]",
                             hostname, config_fpath)
                return None

            host = host_dict[hostname]
            bind_addr = bind_addrs[index_number]
            consul_servers[hostname] = consul.ConsulServer(host, bind_addr)
            index_number += 1

            simple_consul_server_config = copy.deepcopy(consul_server_config)
            simple_consul_server_config[clownf_constant.CLF_HOSTNAME] = hostname
            simple_consul_server_config[clownf_constant.CLF_BIND_ADDR] = bind_addr
            simple_consul_server_configs.append(simple_consul_server_config)
    return consul_servers


def parse_consul_client_config(log, config_fpath, consul_config, host_dict,
                               consul_server_dict, simple_config):
    """
    Parse the config of Consul client
    """
    # pylint: disable=too-many-locals
    consul_clients = {}

    consul_client_configs = utils.config_value(consul_config, clownf_constant.CLF_CLIENTS)
    if consul_client_configs is None:
        consul_client_configs = []
        log.cl_debug("no [%s] is configured in Consul section",
                     clownf_constant.CLF_CLIENTS)
    # CLF_CLIENTS of CLF_CONSUL is nontrivial. CLF_HOSTNAME and CLF_BIND_ADDR
    # could be lists.
    simple_consul_client_configs = []
    simple_consul_config = simple_config[clownf_constant.CLF_CONSUL]
    simple_consul_config[clownf_constant.CLF_CLIENTS] = simple_consul_client_configs

    for consul_client_config in consul_client_configs:
        hostname_config = utils.config_value(consul_client_config,
                                             clownf_constant.CLF_HOSTNAME)
        if hostname_config is None:
            log.cl_error("no [%s] is configured in one of Consul clients",
                         clownf_constant.CLF_HOSTNAME)
            return None

        hostnames = cmd_general.parse_list_string(log, hostname_config)
        if hostnames is None:
            log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                         hostname_config,
                         clownf_constant.CLF_HOSTNAME, config_fpath)
            return None

        bind_addr_config = utils.config_value(consul_client_config,
                                              clownf_constant.CLF_BIND_ADDR)
        if bind_addr_config is None:
            log.cl_error("no [%s] is configured in one of Consul clients",
                         clownf_constant.CLF_BIND_ADDR)
            return None

        bind_addrs = cmd_general.parse_list_string(log, bind_addr_config)
        if bind_addrs is None:
            log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                         bind_addr_config,
                         clownf_constant.CLF_BIND_ADDR, config_fpath)
            return None

        if len(bind_addrs) != len(hostnames):
            log.cl_error("the number of [%s] should be equal to the "
                         "number of [%s]",
                         clownf_constant.CLF_BIND_ADDR,
                         clownf_constant.CLF_HOSTNAME)
            return None

        index_number = 0
        for hostname in hostnames:
            if hostname not in host_dict:
                log.cl_error("host [%s] is configured as Consul client, "
                             "but not as a non-standalone host, please "
                             "correct file [%s]",
                             hostname, config_fpath)
                return None

            if hostname in consul_server_dict:
                log.cl_error("multiple configurations for host with ID [%s] "
                             "is configured as Consul server/client, "
                             "please correct file [%s]",
                             hostname, config_fpath)
                return None

            if hostname in consul_clients:
                log.cl_error("multiple configurations for host [%s] as Consul client, "
                             "please correct file [%s]",
                             hostname, config_fpath)
                return None

            host = host_dict[hostname]
            bind_addr = bind_addrs[index_number]
            consul_clients[hostname] = consul.ConsulClient(host, bind_addr)
            index_number += 1

            simple_consul_client_config = copy.deepcopy(consul_client_config)
            simple_consul_client_config[clownf_constant.CLF_HOSTNAME] = hostname
            simple_consul_client_config[clownf_constant.CLF_BIND_ADDR] = bind_addr
            simple_consul_client_configs.append(simple_consul_client_config)
    return consul_clients


def parse_consul_config(log, config_fpath, consul_config, host_dict,
                        simple_config):
    """
    Parse the config of Consul
    """
    consul_server_dict = parse_consul_server_config(log, config_fpath,
                                                    consul_config, host_dict,
                                                    simple_config)
    if consul_server_dict is None:
        log.cl_error("failed to parse Consul server section of config")
        return None

    if len(consul_server_dict) < 3:
        log.cl_error("too few Consul servers [%s], need at least 3",
                     len(consul_server_dict))
        return None

    if len(consul_server_dict) // 2 * 2 == len(consul_server_dict):
        log.cl_error("Consul servers with even number [%s] might not able to elect a leader",
                     len(consul_server_dict))
        return None

    consul_client_dict = parse_consul_client_config(log, config_fpath,
                                                    consul_config, host_dict,
                                                    consul_server_dict,
                                                    simple_config)
    if consul_client_dict is None:
        log.cl_error("failed to parse Consul client section of config")
        return None

    datacenter = utils.config_value(consul_config,
                                    clownf_constant.CLF_DATACENTER)
    if datacenter is None:
        log.cl_error("no [%s] is configured in Consul section",
                     clownf_constant.CLF_DATACENTER)
        return None

    encrypt_key = utils.config_value(consul_config,
                                     clownf_constant.CLF_ENCRYPT_KEY)
    if encrypt_key is None:
        log.cl_debug("no [%s] is configured in Consul section",
                     clownf_constant.CLF_ENCRYPT_KEY)

    consul_cluster = consul.ConsulCluster(datacenter, consul_server_dict,
                                          consul_client_dict,
                                          encrypt_key=encrypt_key)
    return consul_cluster


def clownf_parse_libvirt_config(log, config_fpath, libvirt_config,
                                hostname_config, hostnames,
                                libvirt_server_dict, domain_name_dict):
    """
    Parse libvirt config of host
    """
    libvirt_server_hostname = \
        utils.config_value(libvirt_config,
                           clownf_constant.CLF_LIBVIRT_SERVER)
    if libvirt_server_hostname is None:
        log.cl_error("no [%s] in [%s] config of host [%s]",
                     clownf_constant.CLF_LIBVIRT_SERVER,
                     clownf_constant.CLF_LIBVIRT,
                     hostname_config)
        return -1

    for hostname in hostnames:
        libvirt_server_dict[hostname] = libvirt_server_hostname

    domain_name_config = utils.config_value(libvirt_config,
                                            clownf_constant.CLF_DOMAIN_NAME)
    if domain_name_config is None:
        for hostname in hostnames:
            domain_name_dict[hostname] = hostname
        return 0

    if libvirt_server_hostname is None:
        log.cl_error("host [%s] has [%s] configured as [%s], but "
                     "no [%s] configured",
                     hostname_config, clownf_constant.CLF_DOMAIN_NAME,
                     domain_name_config,
                     clownf_constant.CLF_LIBVIRT_SERVER)
        return -1

    domain_names = cmd_general.parse_list_string(log,
                                                 domain_name_config)
    if domain_names is None:
        log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                     domain_name_config,
                     clownf_constant.CLF_DOMAIN_NAME, config_fpath)
        return -1

    if len(domain_names) != len(hostnames):
        log.cl_error("the number of [%s] should be equal to the "
                     "number of [%s]",
                     clownf_constant.CLF_DOMAIN_NAME,
                     clownf_constant.CLF_HOSTNAME)
        return -1

    index_number = 0
    for hostname in hostnames:
        domain_name = domain_names[index_number]
        domain_name_dict[hostname] = domain_name
        index_number += 1
    return 0


def clownf_parse_ipmi_config(log, local_host, config_fpath, ipmi_config,
                             hostname_config, hostnames, ipmi_stonith_dict):
    """
    Parse ipmi config of host
    """
    # pylint: disable=too-many-locals
    ipmi_interface = \
        utils.config_value(ipmi_config,
                           clownf_constant.CLF_IPMI_INTERFACE)
    if ipmi_interface is None:
        log.cl_error("no [%s] in [%s] config of host [%s]",
                     clownf_constant.CLF_IPMI_INTERFACE,
                     clownf_constant.CLF_IPMI,
                     hostname_config)
        return -1

    ipmi_username = \
        utils.config_value(ipmi_config,
                           clownf_constant.CLF_IPMI_USERNAME)
    if ipmi_username is None:
        log.cl_error("no [%s] in [%s] config of host [%s]",
                     clownf_constant.CLF_IPMI_USERNAME,
                     clownf_constant.CLF_IPMI,
                     hostname_config)
        return -1

    ipmi_password = \
        utils.config_value(ipmi_config,
                           clownf_constant.CLF_IPMI_PASSWORD)
    if ipmi_password is None:
        log.cl_error("no [%s] in [%s] config of host [%s]",
                     clownf_constant.CLF_IPMI_PASSWORD,
                     clownf_constant.CLF_IPMI,
                     hostname_config)
        return -1

    ipmi_hostname_config = \
        utils.config_value(ipmi_config,
                           clownf_constant.CLF_IPMI_HOSTNAME)
    if ipmi_hostname_config is None:
        log.cl_error("no [%s] in [%s] config of host [%s]",
                     clownf_constant.CLF_IPMI_HOSTNAME,
                     clownf_constant.CLF_IPMI,
                     hostname_config)
        return -1

    ipmi_hostnames = cmd_general.parse_list_string(log, ipmi_hostname_config)
    if ipmi_hostnames is None:
        log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                     ipmi_hostname_config,
                     clownf_constant.CLF_IPMI_HOSTNAME, config_fpath)
        return -1

    ipmi_port_config = \
        utils.config_value(ipmi_config,
                           clownf_constant.CLF_IPMI_PORT)
    if ipmi_port_config is None:
        log.cl_error("no [%s] in [%s] config of host [%s]",
                     clownf_constant.CLF_IPMI_PORT,
                     clownf_constant.CLF_IPMI,
                     hostname_config)
        return -1
    ipmi_port_config = str(ipmi_port_config)

    ipmi_ports = cmd_general.parse_list_string(log, ipmi_port_config)
    if ipmi_ports is None:
        log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                     ipmi_port_config,
                     clownf_constant.CLF_IPMI_PORT, config_fpath)
        return -1

    if len(ipmi_hostnames) > 1 and len(ipmi_ports) > 1:
        log.cl_error("both [%s] with value [%s] and [%s] with value [%s] are "
                     "configured as lists",
                     clownf_constant.CLF_IPMI_HOSTNAME,
                     ipmi_hostname_config,
                     clownf_constant.CLF_IPMI_PORT,
                     ipmi_port_config)
        return -1

    if len(hostnames) != len(ipmi_hostnames) * len(ipmi_ports):
        log.cl_error("number of [%s] with value [%s] and [%s] with value [%s] "
                     "does not match with hostname [%s]",
                     clownf_constant.CLF_IPMI_HOSTNAME,
                     ipmi_hostname_config,
                     clownf_constant.CLF_IPMI_PORT,
                     ipmi_port_config, hostname_config)
        return -1

    ipmi_hostname_index = 0
    ipmi_port_index = 0
    for hostname in hostnames:
        ipmi_hostname = ipmi_hostnames[ipmi_hostname_index]
        ipmi_port = ipmi_ports[ipmi_port_index]
        ipmi_stonith_dict[hostname] = \
            stonith.IMPIStonithHost(ipmi_interface, ipmi_username,
                                    ipmi_password, ipmi_hostname,
                                    ipmi_port, None, local_host)
        if len(ipmi_hostnames) > 1:
            ipmi_hostname_index += 1
        else:
            ipmi_port_index += 1

    return 0


def clownf_init_instance(log, workspace, config, config_fpath,
                         logdir_is_default, iso,
                         mdt_instance_type=lustre.LustreMDTInstance):
    """
    Parse the config and init the instance
    """
    # pylint: disable=too-many-locals,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    # pylint: disable=bad-option-value,consider-using-get
    simple_config = copy.deepcopy(config)
    local_host = ssh_host.get_local_host()

    # CLF_FS_DISTRIBUTIONS is trivial
    dist_configs = utils.config_value(config,
                                      clownf_constant.CLF_FS_DISTRIBUTIONS)
    if dist_configs is None:
        log.cl_debug("no [%s] is configured in the config file [%s]",
                     clownf_constant.CLF_FS_DISTRIBUTIONS, config_fpath)
        dist_configs = []

    # Keys are the distribution IDs, values are LustreDistribution
    lustre_distribution_dict = {}
    for dist_config in dist_configs:
        lustre_distribution_id = utils.config_value(dist_config,
                                                    clownf_constant.CLF_FS_DISTRIBUTION_ID)
        if lustre_distribution_id is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         clownf_constant.CLF_FS_DISTRIBUTION_ID, config_fpath)
            return None

        if lustre_distribution_id in lustre_distribution_dict:
            log.cl_error("multiple distributions with ID [%s] is "
                         "configured, please correct file [%s]",
                         lustre_distribution_id, config_fpath)
            return None

        lustre_rpm_dir = utils.config_value(dist_config,
                                            clownf_constant.CLF_FS_RPM_DIR)
        if lustre_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         clownf_constant.CLF_FS_RPM_DIR, config_fpath)
            return None

        lustre_rpm_dir = lustre_rpm_dir.rstrip("/")

        e2fsprogs_rpm_dir = utils.config_value(dist_config,
                                               clownf_constant.CLF_E2FSPROGS_RPM_DIR)
        if e2fsprogs_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         clownf_constant.CLF_E2FSPROGS_RPM_DIR, config_fpath)
            return None

        e2fsprogs_rpm_dir = e2fsprogs_rpm_dir.rstrip("/")

        lustre_dist = lustre.LustreDistribution(local_host, lustre_rpm_dir,
                                                e2fsprogs_rpm_dir)
        ret = lustre_dist.ldis_prepare(log)
        if ret:
            log.cl_info("failed to prepare Lustre RPMs, installation will fail")

        lustre_distribution_dict[lustre_distribution_id] = lustre_dist

    ssh_host_configs = utils.config_value(config, clownf_constant.CLF_HOSTS)
    if ssh_host_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     clownf_constant.CLF_HOSTS, config_fpath)
        return None
    # CLF_HOSTS is nontrivial. CLF_HOSTNAME and CLF_DOMAIN_NAME can be
    # lists.
    simple_ssh_host_configs = []
    simple_config[clownf_constant.CLF_HOSTS] = simple_ssh_host_configs

    host_dict = {}
    # Key is the hostname of VM, Value is the hostname of Libvirt server.
    libvirt_server_dict = {}
    standalone_host_dict = {}
    # Key is VM's hostname, value is its domain name on the VM server
    domain_name_dict = {}
    # Key is the hostname of VM. Value is IPMIStonithHost
    ipmi_stonith_dict = {}
    for host_config in ssh_host_configs:
        simple_ssh_host_config = copy.deepcopy(host_config)
        hostname_config = utils.config_value(host_config,
                                             clownf_constant.CLF_HOSTNAME)
        if hostname_config is None:
            log.cl_error("can NOT find [%s] in the config of SSH host "
                         "[%s], please correct file [%s]",
                         clownf_constant.CLF_HOSTNAME, hostname_config,
                         config_fpath)
            return None

        hostnames = cmd_general.parse_list_string(log, hostname_config)
        if hostnames is None:
            log.cl_error("[%s] as [%s] is invalid in the config file [%s]",
                         hostname_config,
                         clownf_constant.CLF_HOSTNAME, config_fpath)
            return None

        for hostname in hostnames:
            if hostname in standalone_host_dict or hostname in host_dict:
                log.cl_error("hostname [%s] is configured for multiple times",
                             hostname)
                return None

        lustre_distribution_id = utils.config_value(host_config,
                                                    clownf_constant.CLF_FS_DISTRIBUTION_ID)
        if lustre_distribution_id is None:
            log.cl_debug("no [%s] is configured for host [%s], using Lustre "
                         "RPMs in the ISO",
                         clownf_constant.CLF_FS_DISTRIBUTION_ID,
                         hostname_config)
            lustre_distribution = None
        else:
            if lustre_distribution_id not in lustre_distribution_dict:
                log.cl_error("no Lustre distributions with ID [%s] is "
                             "configured, please correct file [%s]",
                             lustre_distribution_id, config_fpath)
                return None

            lustre_distribution = lustre_distribution_dict[lustre_distribution_id]

        fs_server = utils.config_value(host_config,
                                       clownf_constant.CLF_FS_SERVER)
        if fs_server is None:
            fs_server = False

        fs_client = utils.config_value(host_config,
                                       clownf_constant.CLF_FS_CLIENT)
        if fs_client is None:
            fs_client = False

        libvirt_config = utils.config_value(host_config,
                                            clownf_constant.CLF_LIBVIRT)
        ipmi_config = utils.config_value(host_config,
                                         clownf_constant.CLF_IPMI)
        if libvirt_config is not None and ipmi_config is not None:
            log.cl_error("both [%s] and [%s] are configured for host [%s]",
                         clownf_constant.CLF_LIBVIRT,
                         clownf_constant.CLF_IPMI,
                         hostname_config)
            return None
        if libvirt_config is not None:
            ret = clownf_parse_libvirt_config(log, config_fpath,
                                              libvirt_config,
                                              hostname_config, hostnames,
                                              libvirt_server_dict,
                                              domain_name_dict)
            if ret:
                log.cl_error("failed to parse libvirt config for host [%s]",
                             hostname_config)
                return None
        if ipmi_config is not None:
            ret = clownf_parse_ipmi_config(log, local_host, config_fpath,
                                           ipmi_config,
                                           hostname_config, hostnames,
                                           ipmi_stonith_dict)
            if ret:
                log.cl_error("failed to parse libvirt config for host [%s]",
                             hostname_config)
                return None

        standalone = utils.config_value(host_config, clownf_constant.CLF_STANDALONE)
        if standalone is None:
            standalone = False

        if standalone:
            if fs_server:
                log.cl_error("host [%s] is configured as standalone, but it is "
                             "Lustre server, please correct file [%s]",
                             hostname_config, config_fpath)
                return None
            if fs_client:
                log.cl_error("host [%s] is configured as standalone, but it is "
                             "Lustre client, please correct file [%s]",
                             hostname_config, config_fpath)
                return None

        if standalone and (libvirt_config is not None):
            log.cl_error("host [%s] is configured as standalone, but it has "
                         "[%s] configured, please correct file [%s]",
                         hostname_config, clownf_constant.CLF_LIBVIRT,
                         config_fpath)
            return None

        ssh_identity_file = utils.config_value(host_config,
                                               clownf_constant.CLF_SSH_IDENTITY_FILE)

        for hostname in hostnames:
            local = hostname == local_host.sh_hostname

            host = ClownfHost(hostname,
                              lustre_dist=lustre_distribution,
                              identity_file=ssh_identity_file,
                              local=local,
                              is_server=fs_server,
                              is_client=fs_client)
            if standalone:
                standalone_host_dict[hostname] = host
            else:
                host_dict[hostname] = host

            simple_ssh_host_config = copy.deepcopy(host_config)
            simple_ssh_host_config[clownf_constant.CLF_HOSTNAME] = hostname

            if hostname in libvirt_server_dict:
                simple_libvirt_config = {}
                simple_libvirt_config[clownf_constant.CLF_DOMAIN_NAME] = \
                    domain_name_dict[hostname]
                simple_libvirt_config[clownf_constant.CLF_LIBVIRT_SERVER] = \
                    libvirt_server_dict[hostname]
                simple_ssh_host_config[clownf_constant.CLF_LIBVIRT] = \
                    simple_libvirt_config
            if hostname in ipmi_stonith_dict:
                ipmi_stonith = ipmi_stonith_dict[hostname]
                simple_ipmi_config = {}
                simple_ipmi_config[clownf_constant.CLF_IPMI_INTERFACE] = \
                    ipmi_stonith.ipmish_interface
                simple_ipmi_config[clownf_constant.CLF_IPMI_USERNAME] = \
                    ipmi_stonith.ipmish_username
                simple_ipmi_config[clownf_constant.CLF_IPMI_PASSWORD] = \
                    ipmi_stonith.ipmish_password
                simple_ipmi_config[clownf_constant.CLF_IPMI_HOSTNAME] = \
                    ipmi_stonith.ipmish_hostname
                simple_ipmi_config[clownf_constant.CLF_IPMI_PORT] = \
                    ipmi_stonith.ipmish_port
                simple_ssh_host_config[clownf_constant.CLF_IPMI] = \
                    simple_ipmi_config

            simple_ssh_host_configs.append(simple_ssh_host_config)

    stonith_dict = {}
    for hostname, libvirt_server_hostname in libvirt_server_dict.items():
        if hostname not in domain_name_dict:
            log.cl_error("hostname [%s] does not have domain name")
            return None
        domain_name = domain_name_dict[hostname]
        server_host = None
        if libvirt_server_hostname in host_dict:
            server_host = host_dict[libvirt_server_hostname]
        elif libvirt_server_hostname in standalone_host_dict:
            server_host = standalone_host_dict[libvirt_server_hostname]

        if server_host is None:
            log.cl_error("host [%s] is configured as libvirt server of host "
                         "[%s], but itself is not configured",
                         libvirt_server_hostname, hostname)
            return None
        if hostname not in host_dict:
            log.cl_error("host [%s] is not configured in the cluster",
                         hostname)
            return None
        host = host_dict[hostname]
        stonith_dict[hostname] = stonith.LibvirtStonithHost(server_host, host,
                                                            domain_name=domain_name)

    # Init the sth_host correctly for IMPIStonithHost. It was inited as None.
    for hostname, ipmi_stonith_host in ipmi_stonith_dict.items():
        if hostname not in host_dict:
            log.cl_error("host [%s] is not configured in the cluster",
                         hostname)
            return None
        host = host_dict[hostname]
        ipmi_stonith_host.sth_host = host
        stonith_dict[hostname] = ipmi_stonith_host

    lustre_configs = utils.config_value(config, clownf_constant.CLF_FILESYSTEMS)
    if lustre_configs is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     clownf_constant.CLF_FILESYSTEMS, config_fpath)
        return None

    mgs_configs = utils.config_value(config, clownf_constant.CLF_MGS_LIST)
    if mgs_configs is None:
        log.cl_debug("no [%s] is configured", clownf_constant.CLF_MGS_LIST)
        mgs_configs = []

    mgs_dict = {}
    for mgs_config in mgs_configs:
        # Parse MGS configs
        mgs_id = utils.config_value(mgs_config, clownf_constant.CLF_MGS_ID)
        if mgs_id is None:
            log.cl_error("no [%s] is configured for a MGS, please correct "
                         "file [%s]",
                         clownf_constant.CLF_MGS_ID, config_fpath)
            return None
        ret = lustre.check_mgs_id(log, mgs_id)
        if ret:
            return None

        if mgs_id in mgs_dict:
            log.cl_error("multiple configurations for MGS [%s], please "
                         "correct file [%s]",
                         mgs_id, config_fpath)
            return None

        backfstype = utils.config_value(mgs_config, clownf_constant.CLF_BACKFSTYPE)
        if backfstype is None:
            log.cl_debug("no [%s] is configured for MGS [%s], using [%s] as "
                         "default value", clownf_constant.CLF_BACKFSTYPE, mgs_id,
                         lustre.BACKFSTYPE_LDISKFS)
            backfstype = lustre.BACKFSTYPE_LDISKFS

        zpool_name = None
        if backfstype == lustre.BACKFSTYPE_ZFS:
            zpool_name = utils.config_value(mgs_config, clownf_constant.CLF_ZPOOL_NAME)
            if zpool_name is None:
                log.cl_error("no [%s] is configured for MGS [%s]",
                             clownf_constant.CLF_ZPOOL_NAME)
                return None

        mgs = lustre.LustreMGS(mgs_id, backfstype, zpool_name=zpool_name)
        mgs_dict[mgs_id] = mgs

        instance_configs = utils.config_value(mgs_config, clownf_constant.CLF_INSTANCES)
        if instance_configs is None:
            log.cl_error("no [%s] is configured for MGS [%s], please correct "
                         "file [%s]",
                         clownf_constant.CLF_INSTANCES, mgs_id, config_fpath)
            return None

        for instance_config in instance_configs:
            hostname = utils.config_value(instance_config, clownf_constant.CLF_HOSTNAME)
            if hostname is None:
                log.cl_error("no host [%s] is configured for instance of MGS "
                             "[%s], please correct file [%s]",
                             clownf_constant.CLF_HOSTNAME, mgs_id, config_fpath)
                return None

            if hostname not in host_dict:
                log.cl_error("no host [%s] is configured in hosts, "
                             "please correct file [%s]",
                             hostname, config_fpath)
                return None
            lustre_host = host_dict[hostname]
            lustre_host.lsh_is_server = True

            device = utils.config_value(instance_config, clownf_constant.CLF_DEVICE)
            if device is None:
                log.cl_error("no [%s] is configured for instance of "
                             "MGS [%s], please correct file [%s]",
                             clownf_constant.CLF_DEVICE, mgs_id, config_fpath)
                return None

            if backfstype == lustre.BACKFSTYPE_ZFS:
                if device.startswith("/"):
                    log.cl_error("device [%s] configured for instance of MGS "
                                 "[%s] with ZFS type should not be an "
                                 "absolute path, please correct file [%s]",
                                 device, mgs_id, config_fpath)
                    return None
            else:
                if not device.startswith("/"):
                    log.cl_error("device [%s] configured for instance of MGS "
                                 "[%s] with Ldiskfs type should be an "
                                 "absolute path, please correct file [%s]",
                                 device, mgs_id, config_fpath)
                    return None

            nid = utils.config_value(instance_config, clownf_constant.CLF_NID)
            if nid is None:
                log.cl_error("no [%s] is configured for instance of "
                             "MGS [%s], please correct file [%s]",
                             clownf_constant.CLF_NID, mgs_id, config_fpath)
                return None

            zpool_create = None
            if backfstype == lustre.BACKFSTYPE_ZFS:
                zpool_create = utils.config_value(instance_config,
                                                  clownf_constant.CLF_ZPOOL_CREATE)
                if zpool_create is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MGS [%s], please correct file [%s]",
                                 clownf_constant.CLF_ZPOOL_CREATE, mgs_id, config_fpath)
                    return None

            mnt = utils.config_value(instance_config, clownf_constant.CLF_MNT)
            if mnt is None:
                log.cl_error("no [%s] is configured for instance of "
                             "MGS [%s], please correct file [%s]",
                             clownf_constant.CLF_MNT, mgs_id, config_fpath)
                return None

            mgsi = lustre.init_mgs_instance(log, mgs, lustre_host, device,
                                            mnt, nid, add_to_host=True)
            if mgsi is None:
                return None

    lustre_dict = {}
    for lustre_config in lustre_configs:
        # Parse general configs of Lustre file system
        fsname = utils.config_value(lustre_config, clownf_constant.CLF_FSNAME)
        if fsname is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         clownf_constant.CLF_FSNAME, config_fpath)
            return None

        if fsname in lustre_dict:
            log.cl_error("file system [%s] is configured for multiple times, "
                         "please correct file [%s]",
                         fsname, config_fpath)
            return None

        lustre_fs = lustre.LustreFilesystem(fsname)

        lustre_dict[fsname] = lustre_fs

        mgs_configured = False

        # Parse MGS config
        mgs_id = utils.config_value(lustre_config, clownf_constant.CLF_MGS_ID)
        if mgs_id is not None:
            log.cl_debug("[%s] is configured for file system [%s]",
                         clownf_constant.CLF_MGS_ID, fsname)

            if mgs_id not in mgs_dict:
                log.cl_error("no MGS with ID [%s] is configured, please "
                             "correct file [%s]",
                             mgs_id, config_fpath)
                return None

            mgs = mgs_dict[mgs_id]

            ret = mgs.lmgs_add_fs(log, lustre_fs)
            if ret:
                log.cl_error("failed to add file system [%s] to MGS [%s], "
                             "please correct file [%s]",
                             fsname, mgs_id, config_fpath)
                return None

            mgs_configured = True

        # Parse MDT configs
        mdt_configs = utils.config_value(lustre_config, clownf_constant.CLF_MDTS)
        if mdt_configs is None:
            log.cl_error("no [%s] is configured for file system [%s], please "
                         "correct file [%s]",
                         clownf_constant.CLF_MDTS,
                         fsname, config_fpath)
            return None

        for mdt_config in mdt_configs:
            mdt_index = utils.config_value(mdt_config, clownf_constant.CLF_INDEX)
            if mdt_index is None:
                log.cl_error("no [%s] is configured for a MDT of file system "
                             "[%s], please correct file [%s]",
                             clownf_constant.CLF_INDEX, fsname, config_fpath)
                return None

            is_mgs = utils.config_value(mdt_config, clownf_constant.CLF_IS_MGS)
            if is_mgs is None:
                log.cl_debug("no [%s] is configured for MDT with index [%s] "
                             "of file system [%s], using default value [False]",
                             clownf_constant.CLF_IS_MGS, mdt_index, fsname)
                is_mgs = False

            if is_mgs:
                if mgs_configured:
                    log.cl_error("multiple MGS are configured for file "
                                 "system [%s], please correct file [%s]",
                                 fsname, config_fpath)
                    return None
                mgs_configured = True

            backfstype = utils.config_value(mdt_config, clownf_constant.CLF_BACKFSTYPE)
            if backfstype is None:
                log.cl_debug("no [%s] is configured for MDT with index [%s] "
                             "of file system [%s], using [%s] as the default "
                             "value", clownf_constant.CLF_BACKFSTYPE, mdt_index, fsname,
                             lustre.BACKFSTYPE_LDISKFS)
                backfstype = lustre.BACKFSTYPE_LDISKFS

            zpool_name = None
            if backfstype == lustre.BACKFSTYPE_ZFS:
                zpool_name = utils.config_value(mdt_config,
                                                clownf_constant.CLF_ZPOOL_NAME)
                if zpool_name is None:
                    log.cl_error("no [%s] is configured for MDT with index "
                                 "[%s] of file system [%s]",
                                 clownf_constant.CLF_ZPOOL_NAME, mdt_index,
                                 fsname)
                    return None

            mdt = lustre.init_mdt(log, lustre_fs, mdt_index, backfstype,
                                  is_mgs=is_mgs, zpool_name=zpool_name)
            if mdt is None:
                return None

            instance_configs = utils.config_value(mdt_config, clownf_constant.CLF_INSTANCES)
            if instance_configs is None:
                log.cl_error("no [%s] is configured for MDT with index "
                             "[%s] of file system [%s], please correct "
                             "file [%s]",
                             clownf_constant.CLF_INSTANCES,
                             mdt_index,
                             fsname,
                             config_fpath)
                return None

            for instance_config in instance_configs:
                hostname = utils.config_value(instance_config, clownf_constant.CLF_HOSTNAME)
                if hostname is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 clownf_constant.CLF_HOSTNAME, mdt_index,
                                 fsname, config_fpath)
                    return None

                if hostname not in host_dict:
                    log.cl_error("host [%s] used for an instance of MDT "
                                 "with index [%s] of file system [%s] has "
                                 "not been configured, please correct file [%s]",
                                 hostname,
                                 mdt_index,
                                 fsname,
                                 config_fpath)
                    return None
                lustre_host = host_dict[hostname]
                lustre_host.lsh_is_server = True

                device = utils.config_value(instance_config, clownf_constant.CLF_DEVICE)
                if device is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 clownf_constant.CLF_DEVICE, mdt_index, fsname,
                                 config_fpath)
                    return None

                if backfstype == lustre.BACKFSTYPE_ZFS:
                    if device.startswith("/"):
                        log.cl_error("device [%s] configured for an instance "
                                     "of MDT with index [%s] of file system "
                                     "[%s] with ZFS type should not be an "
                                     "absolute path, please correct file [%s]",
                                     device, mdt_index, fsname,
                                     config_fpath)
                        return None
                else:
                    if not device.startswith("/"):
                        log.cl_error("device [%s] configured for an instance "
                                     "of MDT with index [%s] of file system "
                                     "[%s] with Ldiskfs type should be an "
                                     "absolute path, please correct file [%s]",
                                     device, mdt_index, fsname,
                                     config_fpath)
                        return None

                nid = utils.config_value(instance_config, clownf_constant.CLF_NID)
                if nid is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 clownf_constant.CLF_NID, mdt_index, fsname,
                                 config_fpath)
                    return None

                zpool_create = None
                if backfstype == lustre.BACKFSTYPE_ZFS:
                    zpool_create = utils.config_value(instance_config,
                                                      clownf_constant.CLF_ZPOOL_CREATE)
                    if zpool_create is None:
                        log.cl_error("no [%s] is configured for an instance of "
                                     "MDT with index [%s] of file system [%s], "
                                     "please correct file [%s]",
                                     clownf_constant.CLF_ZPOOL_CREATE,
                                     mdt_index, fsname,
                                     config_fpath)
                        return None

                mnt = utils.config_value(instance_config, clownf_constant.CLF_MNT)
                if mnt is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 clownf_constant.CLF_MNT, mdt_index, fsname,
                                 config_fpath)
                    return None

                mdti = lustre.init_mdt_instance(log, mdt, lustre_host, device,
                                                mnt, nid, add_to_host=True,
                                                zpool_create=zpool_create,
                                                mdt_instance_type=mdt_instance_type)
                if mdti is None:
                    return None

        if not mgs_configured:
            log.cl_error("no MGS is configured in the system, please correct file [%s]",
                         config_fpath)
            return None

        # Parse OST configs
        ost_configs = utils.config_value(lustre_config, clownf_constant.CLF_OSTS)
        if ost_configs is None:
            log.cl_error("no [%s] is configured for file system [%s], "
                         "please correct file [%s]",
                         clownf_constant.CLF_OSTS,
                         fsname,
                         config_fpath)
            return None

        for ost_config in ost_configs:
            ost_index = utils.config_value(ost_config, clownf_constant.CLF_INDEX)
            if ost_index is None:
                log.cl_error("no [%s] is configured for an OST of file "
                             "system [%s], please correct file [%s]",
                             clownf_constant.CLF_INDEX, fsname,
                             config_fpath)
                return None

            backfstype = utils.config_value(ost_config, clownf_constant.CLF_BACKFSTYPE)
            if backfstype is None:
                log.cl_debug("no [%s] is configured for OST with index [%s] "
                             "of file system [%s], using [%s] as default",
                             clownf_constant.CLF_BACKFSTYPE, ost_index, fsname,
                             lustre.BACKFSTYPE_LDISKFS)
                backfstype = lustre.BACKFSTYPE_LDISKFS

            zpool_name = None
            if backfstype == lustre.BACKFSTYPE_ZFS:
                zpool_name = utils.config_value(ost_config, clownf_constant.CLF_ZPOOL_NAME)
                if zpool_name is None:
                    log.cl_error("no [%s] is configured for OST with index "
                                 "[%s] of file system [%s]",
                                 clownf_constant.CLF_ZPOOL_NAME, ost_index, fsname)
                    return None

            ost = lustre.init_ost(log, lustre_fs, ost_index, backfstype,
                                  zpool_name=zpool_name)
            if ost is None:
                return None

            instance_configs = utils.config_value(ost_config, clownf_constant.CLF_INSTANCES)
            if instance_configs is None:
                log.cl_error("no [%s] is configured for OST with index [%s] "
                             "of file system [%s], please correct file [%s]",
                             clownf_constant.CLF_INSTANCES, ost_index, fsname,
                             config_fpath)
                return None

            for instance_config in instance_configs:
                hostname = utils.config_value(instance_config, clownf_constant.CLF_HOSTNAME)
                if hostname is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system, please "
                                 "correct file [%s]",
                                 clownf_constant.CLF_HOSTNAME, ost_index, fsname, config_fpath)
                    return None

                if hostname not in host_dict:
                    log.cl_error("no host [%s] is configured in hosts, "
                                 "please correct file [%s]",
                                 hostname, config_fpath)
                    return None
                lustre_host = host_dict[hostname]
                lustre_host.lsh_is_server = True

                device = utils.config_value(instance_config, clownf_constant.CLF_DEVICE)
                if device is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 clownf_constant.CLF_DEVICE, ost_index, fsname,
                                 config_fpath)
                    return None

                if backfstype == lustre.BACKFSTYPE_ZFS:
                    if device.startswith("/"):
                        log.cl_error("device [%s] configured for an instance "
                                     "of OST with index [%s] of file system "
                                     "[%s] with ZFS type should not be an "
                                     "absolute path, please correct file [%s]",
                                     device, ost_index, fsname,
                                     config_fpath)
                        return None
                else:
                    if not device.startswith("/"):
                        log.cl_error("device [%s] configured for an instance "
                                     "of OST with index [%s] of file system "
                                     "[%s] with Ldiskfs type should be an "
                                     "absolute path, please correct file [%s]",
                                     device, ost_index, fsname,
                                     config_fpath)
                        return None

                nid = utils.config_value(instance_config, clownf_constant.CLF_NID)
                if nid is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 clownf_constant.CLF_NID, ost_index, fsname,
                                 config_fpath)
                    return None

                zpool_create = None
                if backfstype == lustre.BACKFSTYPE_ZFS:
                    zpool_create = utils.config_value(instance_config, clownf_constant.CLF_ZPOOL_CREATE)
                    if zpool_create is None:
                        log.cl_error("no [%s] is configured for an instance of "
                                     "OST with index [%s] of file system [%s], "
                                     "please correct file [%s]",
                                     clownf_constant.CLF_ZPOOL_CREATE, ost_index, fsname,
                                     config_fpath)
                        return None

                mnt = utils.config_value(instance_config, clownf_constant.CLF_MNT)
                if mnt is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 clownf_constant.CLF_MNT, ost_index, fsname,
                                 config_fpath)
                    return None

                osti = lustre.init_ost_instance(log, ost, lustre_host, device,
                                                mnt, nid, add_to_host=True,
                                                zpool_create=zpool_create)
                if osti is None:
                    return None
        # Parse client configs
        client_configs = utils.config_value(lustre_config,
                                            clownf_constant.CLF_CLIENTS)
        if client_configs is None:
            log.cl_debug("no [%s] is configured for a Lustre file system",
                         clownf_constant.CLF_CLIENTS)
            client_configs = []

        for client_config in client_configs:
            hostname = utils.config_value(client_config, clownf_constant.CLF_HOSTNAME)
            if hostname is None:
                log.cl_error("no [%s] is configured for a Lustre client, "
                             "please correct file [%s]",
                             clownf_constant.CLF_HOSTNAME, config_fpath)
                return None

            if hostname not in host_dict:
                log.cl_error("no host with [%s] is configured in hosts, "
                             "please correct file [%s]",
                             hostname, config_fpath)
                return None

            lustre_host = host_dict[hostname]
            lustre_host.lsh_is_client = True

            mnt = utils.config_value(client_config, clownf_constant.CLF_MNT)
            if mnt is None:
                log.cl_error("no [%s] is configured for a client on host [%s], "
                             "please correct file [%s]",
                             clownf_constant.CLF_MNT, hostname,
                             config_fpath)
                return None

            client = lustre.init_client(log, lustre_fs, lustre_host, mnt,
                                        add_to_host=True)
            if client is None:
                return None

    consul_config = utils.config_value(config, clownf_constant.CLF_CONSUL)
    if consul_config is None:
        log.cl_error("no [%s] section is configured",
                     clownf_constant.CLF_CONSUL)
        return None
    consul_servers = {}
    consul_clients = {}
    consul_cluster = parse_consul_config(log, config_fpath, consul_config,
                                         host_dict, simple_config)
    if consul_cluster is None:
        log.cl_error("failed to parse [%s] section of config",
                     clownf_constant.CLF_CONSUL)
        return None

    # Key is host ID
    consul_servers = consul_cluster.cclr_server_dict
    consul_clients = consul_cluster.cclr_client_dict

    # Check that all server hosts has Consul installed
    for hostname, host in host_dict.items():
        if (not host.lsh_is_server) and (not host.lsh_is_client):
            continue
        if hostname not in consul_servers and hostname not in consul_clients:
            log.cl_error("Lustre host [%s] is not configured as Consul "
                         "server or client",
                         hostname)
            return None

    instance = ClownfishInstance(workspace, config, simple_config, config_fpath,
                                 host_dict, stonith_dict,
                                 mgs_dict, lustre_dict, consul_cluster,
                                 local_host,
                                 lustre_distribution_dict,
                                 logdir_is_default,
                                 iso)
    return instance
