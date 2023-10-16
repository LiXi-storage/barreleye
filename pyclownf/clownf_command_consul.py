"""
Consul commands of Clownf
"""
from pycoral import cmd_general
from pycoral import clog
from pycoral import parallel
from pyclownf import clownf_constant
from pyclownf import clownf_command_common


class ConsulStatusCache():
    """
    This object saves temporary status of a Lustre file system.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, clownfish_instance, leader_hostname, consul_host):
        # ConsulHost
        self.csc_consul_host = consul_host
        self.csc_clownfish_instance = clownfish_instance
        # Any failure when getting this cached status
        self.csc_failed = False
        # Whether the consul service is running on the host.
        # If is active, 1. If inactive, 0. -1 on error.
        self.csc_active = None
        # Hostname of leader
        self.csc_leader_hostname = leader_hostname

    def _csc_init_active(self, log):
        """
        Init active status
        """
        consul_host = self.csc_consul_host
        self.csc_active = consul_host.ch_service_is_active(log)
        if self.csc_active < 0:
            self.csc_failed = True

    def csc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names
        """
        # pylint: disable=too-many-branches
        for field in field_names:
            if field == clownf_constant.CLOWNF_FIELD_HOST:
                continue
            if field == clownf_constant.CLOWNF_FIELD_SERVER:
                continue
            if field == clownf_constant.CLOWNF_FIELD_LEADER:
                continue
            if field == clownf_constant.CLOWNF_FIELD_ACTIVE:
                self._csc_init_active(log)
            else:
                log.cl_error("unknown field [%s]", field)
                return -1
        return 0

    def csc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        consul_host = self.csc_consul_host
        hostname = consul_host.cs_host.sh_hostname
        ret = 0
        if field_name == clownf_constant.CLOWNF_FIELD_HOST:
            result = hostname
        elif field_name == clownf_constant.CLOWNF_FIELD_SERVER:
            result = consul_host.cs_service_name
        elif field_name == clownf_constant.CLOWNF_FIELD_ACTIVE:
            if self.csc_active is None:
                log.cl_error("the active status of Consul on host [%s] is "
                             "not inited", hostname)
                ret = -1
                result = clog.ERROR_MSG
            else:
                if self.csc_active:
                    color = clog.COLOR_GREEN
                    result = clog.colorful_message(color,
                                                   clownf_constant.CLOWNF_VALUE_ACTIVE)
                else:
                    color = clog.COLOR_RED
                    result = clog.colorful_message(color,
                                                   clownf_constant.CLOWNF_VALUE_INACTIVE)
        elif field_name == clownf_constant.CLOWNF_FIELD_LEADER:
            if self.csc_leader_hostname == hostname:
                color = clog.COLOR_GREEN
                result = clog.colorful_message(color,
                                               clownf_constant.CLOWNF_VALUE_LEADER)
            else:
                result = clownf_constant.CLOWNF_VALUE_MEMBER
        else:
            log.cl_error("unknown field [%s] of Lustre file system",
                         field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.csc_failed:
            ret = -1
        return ret, result

    def csc_can_skip_init_fields(self, field_names):
        """
        Whether lfsc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        if clownf_constant.CLOWNF_FIELD_HOST in fields:
            fields.remove(clownf_constant.CLOWNF_FIELD_HOST)
        if clownf_constant.CLOWNF_FIELD_SERVER in fields:
            fields.remove(clownf_constant.CLOWNF_FIELD_SERVER)
        if clownf_constant.CLOWNF_FIELD_LEADER in fields:
            fields.remove(clownf_constant.CLOWNF_FIELD_LEADER)
        if len(fields) == 0:
            return True
        return False


def consul_status_init(log, workspace, consul_status, field_names):
    """
    Init status of a consul host
    """
    # pylint: disable=unused-argument
    return consul_status.csc_init_fields(log, field_names)


def consul_status_field(log, consul_status, field_name):
    """
    Return (0, result) for a field of ConsulStatusCache
    """
    # pylint: disable=unused-argument
    return consul_status.csc_field_result(log, field_name)


def print_consul_hosts(log, clownfish_instance, consul_hosts, status=False,
                       print_table=True, field_string=None):
    """
    Print table of ConsulHost
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if not print_table and len(consul_hosts) > 1:
        log.cl_error("failed to print non-table output with multiple "
                     "Consul hosts")
        return -1

    quick_fields = [clownf_constant.CLOWNF_FIELD_HOST,
                    clownf_constant.CLOWNF_FIELD_SERVER]
    slow_fields = [clownf_constant.CLOWNF_FIELD_ACTIVE,
                   clownf_constant.CLOWNF_FIELD_LEADER]
    none_table_fields = []
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for Consul host",
                     field_string)
        return -1

    leader_hostname = None
    if clownf_constant.CLOWNF_FIELD_LEADER in field_names:
        consul_cluster = clownfish_instance.ci_consul_cluster
        leader_hostname = consul_cluster.cclr_leader(log)

    consul_status_list = []
    for consul_host in consul_hosts:
        consul_status = ConsulStatusCache(clownfish_instance, leader_hostname,
                                          consul_host)
        consul_status_list.append(consul_status)

    if (len(consul_status_list) > 0 and
            not consul_status_list[0].csc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for consul_status in consul_status_list:
            args = (consul_status, field_names)
            args_array.append(args)
            consul_host = consul_status.csc_consul_host
            hostname = consul_host.cs_host.sh_hostname
            thread_id = "consul_status_%s" % hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(clownfish_instance.ci_workspace,
                                                    "consul_status",
                                                    consul_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for services",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, consul_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                consul_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    return rc


class ConsulCommand():
    """
    Commands to manage the Consul in a Clownfish cluster.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, config, logdir, log_to_file, iso):
        # pylint: disable=attribute-defined-outside-init
        self._cc_config_fpath = config
        self._cc_logdir = logdir
        self._cc_log_to_file = log_to_file
        self._cc_iso = iso

    def status(self):
        """
        Print the status of Consul agents in the cluster.
        :param status: print the status of Consul services, default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        consul_cluster = clownfish_instance.ci_consul_cluster
        consul_hosts = list(consul_cluster.cclr_server_dict.values())
        consul_hosts += list(consul_cluster.cclr_client_dict.values())
        rc = print_consul_hosts(log, clownfish_instance,
                                consul_hosts, status=True)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def members(self, status=False):
        """
        Print the members of Consul agents in the cluster.
        :param status: print the status of Consul services, default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "status", status)
        consul_cluster = clownfish_instance.ci_consul_cluster
        consul_hosts = list(consul_cluster.cclr_server_dict.values())
        consul_hosts += list(consul_cluster.cclr_client_dict.values())
        rc = print_consul_hosts(log, clownfish_instance,
                                consul_hosts, status=status)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def start(self, force=False):
        """
        Start Consul services in the whole cluster.
        :param force: continue even hit failures. Default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "force", force)
        if force:
            log.cl_info("--force is specified, will continue even hit failure")
        rc = clownfish_instance.ci_consul_start(log, force=force)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def restart(self, force=False):
        """
        Restart Consul services in the whole cluster.

        After reinstalling Coral RPMs, you might want to run this command to
        restart the Consul services because these services will not be
        automatically restarted to get the possible update of Consul binary.
        If Consul is not updated to a newer version in Coral ISO, running this
        command is most likely unncessary.
        :param force: continue even hit failures. Default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "force", force)
        if force:
            log.cl_info("--force is specified, will continue even hit failure")
        rc = clownfish_instance.ci_consul_restart(log, force=force)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def reset(self, force=False):
        """
        Clean the datadir of Consul and restart Consul service.

        This is useful when Consul is not able to select a leader. The
        configurations of Consul itself won't be changed. But the modified
        Lustre configurations saved in Consul will be all inited to default
        values.
        :param force: continue even hit failures. Default: False.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        cmd_general.check_argument_bool(log, "force", force)
        if force:
            log.cl_info("--force is specified, will continue even hit failure")
        rc = clownfish_instance.ci_consul_reset(log, force=force)
        clownf_command_common.exit_env(log, clownfish_instance, rc)

    def leader(self):
        """
        Print the hostname of Consul leader.
        """
        log, clownfish_instance = \
            clownf_command_common.init_env(self._cc_config_fpath,
                                           self._cc_logdir,
                                           self._cc_log_to_file,
                                           self._cc_iso)
        leader_hostname = clownfish_instance.ci_consul_leader(log)
        rc = 0
        if leader_hostname is None:
            log.cl_error("failed to get the leader of Consul")
            rc = -1
        elif leader_hostname == "":
            log.cl_error("Consul currently has no leader")
            rc = -1
        else:
            log.cl_stdout(leader_hostname)
        clownf_command_common.exit_env(log, clownfish_instance, rc)
