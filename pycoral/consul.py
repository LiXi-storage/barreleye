"""
Consul library
"""
# pylint: disable=too-many-lines
import time
import traceback
import json
import yaml
from pycoral import cmd_general

CONSUL_BIN_NAME = "consul"
CONSUL_BIN_DIR = "/usr/bin"
CONSUL_CONFIG_DIR = "/etc/consul"
CONSUL_DATA_DIR = "/var/lib/consul"
CONSUL_NO_KEY_STRING = "Error! No key exists at:"


class ConsulHost():
    """
    Each Consul server/client has an object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, host, bind_addr, is_server=True):
        self.cs_host = host
        self.cs_bind_addr = bind_addr
        self.cs_is_server = is_server
        if is_server:
            self.cs_service_name = "server"
        else:
            self.cs_service_name = "client"

    def ch_cleanup(self, log):
        """
        Stop the service and cleanup the datadir
        """
        # Stop the current service if any
        ret = self.cs_host.sh_service_stop(log, "consul")
        if ret:
            log.cl_info("failed to stop Consul service on host [%s]",
                        self.cs_host.sh_hostname)
            return -1

        # Cleanup the data if any
        command = ("rm -fr %s" % CONSUL_DATA_DIR)
        retval = self.cs_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.cs_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def ch_install(self, log, workspace, datacenter, sever_ips,
                   encrypt_key):
        """
        Install and config
        """
        ret = self.ch_cleanup(log)
        if ret:
            log.cl_error("failed to cleanup Consul datadir on host [%s]",
                         self.cs_host.sh_hostname)
            return -1

        fpath = workspace + "/" + CONSUL_BIN_NAME
        ret = self.cs_host.sh_send_file(log, fpath, CONSUL_BIN_DIR)
        if ret:
            log.cl_error("failed to send file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         fpath, CONSUL_BIN_DIR,
                         self.cs_host.sh_hostname)
            return -1

        consul_config = {}
        consul_config["datacenter"] = datacenter
        consul_config["data_dir"] = CONSUL_DATA_DIR
        consul_config["node_name"] = self.cs_host.sh_hostname
        consul_config["server"] = self.cs_is_server
        consul_config["ui"] = True
        consul_config["bind_addr"] = self.cs_bind_addr
        consul_config["client_addr"] = "0.0.0.0"
        consul_config["retry_join"] = sever_ips
        consul_config["rejoin_after_leave"] = True
        consul_config["start_join"] = sever_ips
        consul_config["enable_syslog"] = True
        consul_config["disable_update_check"] = True
        # Enable gossip encryption to avoid problems of leader election
        # affected by nodes from another datacenter
        consul_config["encrypt"] = encrypt_key

        if self.cs_is_server:
            consul_config["bootstrap_expect"] = len(sever_ips)

        json_fname = workspace + "/" + "config.json"
        with open(json_fname, 'w', encoding='utf-8') as json_file:
            json.dump(consul_config, json_file, indent=4, sort_keys=True)

        ret = self.cs_host.sh_mkdir(log, CONSUL_CONFIG_DIR)
        if ret:
            log.cl_error("failed to mkdir [%s] on host [%s]",
                         CONSUL_CONFIG_DIR,
                         self.cs_host.sh_hostname)
            return -1

        ret = self.cs_host.sh_send_file(log, json_fname, CONSUL_CONFIG_DIR)
        if ret:
            log.cl_error("failed to send file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         json_fname, CONSUL_CONFIG_DIR,
                         self.cs_host.sh_hostname)
            return -1

        log.cl_info("installed Consul %s on host [%s]",
                    self.cs_service_name, self.cs_host.sh_hostname)
        return 0

    def ch_restart(self, log, restart=True):
        """
        Restart the service
        """
        if restart:
            ret = self.cs_host.sh_service_stop(log, "consul")
            if ret:
                log.cl_error("failed to stop service [%s] on host [%s]",
                             "consul",
                             self.cs_host.sh_hostname)
                return -1

        command = ("systemctl daemon-reload")
        retval = self.cs_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.cs_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret = self.cs_host.sh_service_start(log, "consul")
        if ret:
            log.cl_info("failed to start Consul service on host [%s]",
                        self.cs_host.sh_hostname)
            return -1

        log.cl_debug("configuring autostart of Consul on host [%s]",
                     self.cs_host.sh_hostname)
        command = "systemctl enable consul"
        retval = self.cs_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.cs_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        log.cl_debug("started Consul %s on host [%s]",
                     self.cs_service_name, self.cs_host.sh_hostname)
        return 0

    def ch_service_is_active(self, log):
        """
        If is active, return 1. If inactive, return 0. Return -1 on error.
        """
        return self.cs_host.sh_service_is_active(log, "consul")


class ConsulServer(ConsulHost):
    """
    Each Consul server has an object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, host, bind_addr):
        super().__init__(host, bind_addr, is_server=True)


class ConsulClient(ConsulHost):
    """
    Each Consul client has an object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, host, bind_addr):
        super().__init__(host, bind_addr, is_server=False)


def get_zip_fpath(log, host, iso_dir):
    """
    Find iso path in current work directory
    """
    command = ("ls %s/consul_*.zip" % (iso_dir))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None

    paths = retval.cr_stdout.split()
    if len(paths) != 1:
        log.cl_error("found unexpected file name %s under directory [%s]",
                     paths, iso_dir)
        return None

    return paths[0]


class ConsulCluster():
    """
    Console cluster with all servers and clients
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, datacenter, server_dict, client_dict,
                 encrypt_key=None):
        # Datacenter name
        self.cclr_datacenter = datacenter
        # Key is hostname, value is either ConsulServer or ConsulClient
        self.cclr_agent_dict = {}
        for hostname, agent in server_dict.items():
            self.cclr_agent_dict[hostname] = agent
        for hostname, agent in client_dict.items():
            self.cclr_agent_dict[hostname] = agent
        # Key is hostname, value is ConsulServer
        self.cclr_server_dict = server_dict
        # Key is hostname, value is ConsulClient
        self.cclr_client_dict = client_dict
        self.cclr_server_ips = []
        for server in self.cclr_server_dict.values():
            self.cclr_server_ips.append(server.cs_bind_addr)
        # Whether only have client
        self.cclr_client_only = (len(server_dict) == 0)
        # The encrypt key
        self.cclr_encrypt_key = encrypt_key

    def cclr_leader(self, log, quiet=False):
        """
        Return the hostname of Consul leader

        If no leader, return "". If failure return None.
        """
        # pylint: disable=too-many-branches
        server = self.cclr_alive_agent(log)
        if server is None:
            log.cl_error("no Consul agent is up in the system")
            return None
        host = server.cs_host

        command = "curl http://127.0.0.1:8500/v1/status/leader"
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None
        leader_url = retval.cr_stdout
        if not leader_url.startswith("\"") or not leader_url.endswith("\""):
            if not quiet:
                log.cl_error("unexpected output of command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None

        leader_url = leader_url[1:-1]
        if leader_url == "":
            if not quiet:
                log.cl_error("empty output of command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None
        if not leader_url.endswith(":8300"):
            if not quiet:
                log.cl_error("unexpected port in the output of command [%s] "
                             "on host [%s], ret = [%d], stdout = [%s], "
                             "stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None
        ip = leader_url[:-5]
        for server in self.cclr_server_dict.values():
            if server.cs_bind_addr == ip:
                return server.cs_host.sh_hostname
        if not quiet:
            log.cl_error("IP [%s] does not belong to a Consul server in the output of "
                         "command [%s] on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         ip,
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
        return None

    def cclr_alive_agent(self, log):
        """
        Return alive agent, either Consul server or client
        """
        servers = list(self.cclr_agent_dict.values())
        for server in servers:
            if not server.cs_host.sh_is_up(log):
                continue

            command = "consul members"
            retval = server.cs_host.sh_run(log, command)
            if retval.cr_exit_status:
                continue
            return server
        return None

    def cclr_install(self, log, workspace, local_host, iso_dir):
        """
        Install all servers/clients on the cluster
        """
        if iso_dir is None:
            log.cl_error("no ISO dir to install Consul")
            return -1
        zip_fpath = get_zip_fpath(log, local_host, iso_dir)
        if zip_fpath is None:
            log.cl_error("failed to get zip file of Consul under [%s]",
                         iso_dir)
            return -1

        fpath = workspace + "/" + CONSUL_BIN_NAME
        exists = local_host.sh_path_exists(log, fpath)
        if exists < 0:
            log.cl_error("failed to check whether Consul command exists or not")
            return -1
        if exists == 0:
            command = ("mkdir -p %s && cd %s && unzip %s" %
                       (workspace, workspace, zip_fpath))
            retval = local_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             local_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        encrypt_key = self.cclr_encrypt_key
        if encrypt_key is None:
            command = "%s keygen" % fpath
            retval = local_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             local_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            encrypt_key = retval.cr_stdout.strip()

        for server in self.cclr_server_dict.values():
            ret = server.ch_install(log, workspace, self.cclr_datacenter,
                                    self.cclr_server_ips, encrypt_key)
            if ret:
                log.cl_error("failed to install Consul server on host [%s]",
                             server.cs_host.sh_hostname)
                return ret

        for client in self.cclr_client_dict.values():
            ret = client.ch_install(log, workspace, self.cclr_datacenter,
                                    self.cclr_server_ips, encrypt_key)
            if ret:
                log.cl_error("failed to install Consul client on host [%s]",
                             client.cs_host.sh_hostname)
                return ret

        return 0

    def cclr_cleanup(self, log, force=False):
        """
        Cleanup all Consul servers/clients datadir on the cluster
        """
        rc = 0
        for client in self.cclr_client_dict.values():
            ret = client.ch_cleanup(log)
            if ret:
                rc = ret
                log.cl_error("failed to cleanup Consul client on host [%s]",
                             client.cs_host.sh_hostname)
                if not force:
                    return ret

        for server in self.cclr_server_dict.values():
            ret = server.ch_cleanup(log)
            if ret:
                rc = ret
                log.cl_error("failed to cleanup Consul server on host [%s]",
                             server.cs_host.sh_hostname)
                if not force:
                    return ret
        return rc

    def cclr_server_restart(self, log, force=False, restart=True):
        """
        Restart all Consul servers on the cluster
        """
        if restart:
            log.cl_info("restarting Consul servers")
        else:
            log.cl_info("starting Consul servers")

        rc = 0
        for server in self.cclr_server_dict.values():
            ret = server.ch_restart(log, restart=restart)
            if ret:
                rc = ret
                log.cl_error("failed to start Consul server on host [%s]",
                             server.cs_host.sh_hostname)
                if not force:
                    return ret

        log.cl_info("waiting until Consul has a leader")
        time_start = time.time()
        sleep_interval = 1
        while True:
            leader = self.cclr_leader(log, quiet=True)
            if leader is not None:
                log.cl_info("current Consul leader is [%s]", leader)
                break
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed < 60:
                time.sleep(sleep_interval)
                continue
            log.cl_error("electing Consul leader timeouts after [%f] seconds",
                         elapsed)
            rc = -1
            if not force:
                return rc
        return rc

    def cclr_restart(self, log, force=False, restart=True):
        """
        Restart all Consul servers/clients on the cluster
        """
        # pylint: disable=too-many-branches
        rc = 0
        if not self.cclr_client_only:
            rc = self.cclr_server_restart(log, force=force, restart=restart)
            if rc and not force:
                return -1

        if restart:
            log.cl_info("restarting Consul clients")
        else:
            log.cl_info("starting Consul clients")
        for client in self.cclr_client_dict.values():
            ret = client.ch_restart(log, restart=restart)
            if ret:
                rc = ret
                log.cl_error("failed to start Consul client on host [%s]",
                             client.cs_host.sh_hostname)
                if not force:
                    return ret
        if rc:
            with_failure = " with failure"
        else:
            with_failure = ""
        if restart:
            log.cl_info("finished restarting Consul" + with_failure)
        else:
            log.cl_info("finished starting Consul" + with_failure)
        return rc

    def cclr_set_config(self, log, workspace, local_host, key,
                        config_string):
        """
        Set the host config.
        """
        # pylint: disable=unused-argument
        agent = self.cclr_alive_agent(log)
        if agent is None:
            log.cl_error("no Consul agent is up in the system")
            return -1
        host = agent.cs_host

        tmpdir = "/tmp"
        fpath = tmpdir + "/" + cmd_general.get_identity() + ".yaml"
        with open(fpath, "w", encoding='utf-8') as local_file:
            local_file.write(config_string)

        if not host.sh_is_localhost():
            ret = host.sh_send_file(log, fpath, tmpdir)
            if ret:
                log.cl_error("failed to send dir [%s] on local host to "
                             "directory [%s] on host [%s]",
                             fpath, tmpdir,
                             host.sh_hostname)
                return -1

        command = ("consul kv put %s @%s" %
                   (key,
                    fpath))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if not host.sh_is_localhost():
            command = "rm -f %s" % fpath
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        command = "rm -f %s" % fpath
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, local_host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        return 0

    def cclr_get_config(self, log, key_path, consul_hostname=None):
        """
        Get the yaml config from a key path.
        If not exist, return (0, None). Return (negative, None) on error.
        """
        if consul_hostname is None:
            agent = self.cclr_alive_agent(log)
            if agent is None:
                log.cl_error("no Consul agent is up in the system")
                return -1, None
        else:
            if consul_hostname not in self.cclr_agent_dict:
                log.cl_error("host [%s] is not Consul server/client",
                             consul_hostname)
                return -1, None
            agent = self.cclr_agent_dict[consul_hostname]
        host = agent.cs_host

        command = ("consul kv get %s" % (key_path))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            if CONSUL_NO_KEY_STRING in retval.cr_stderr:
                return 0, None
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None

        try:
            config = yaml.load(retval.cr_stdout, Loader=yaml.FullLoader)
        except:
            log.cl_error("not able to load [%s] outputed by command [%s] on "
                         "host [%s] as yaml file: %s",
                         retval.cr_stdout, command, host.sh_hostname,
                         traceback.format_exc())
            return -1, None

        return 0, config

    def cclr_get_watcher(self, log, lock_key_path, consul_hostname=None):
        """
        Get the watcher of a lock key.

        If not exist, return (0, None). Return (negative, None) on error.
        """
        # pylint: disable=too-many-branches,too-many-locals
        if consul_hostname is None:
            agent = self.cclr_alive_agent(log)
            if agent is None:
                log.cl_error("no Consul agent is up in the system")
                return -1, None
        else:
            if consul_hostname not in self.cclr_agent_dict:
                log.cl_error("host [%s] is not Consul server",
                             consul_hostname)
                return -1, None
            agent = self.cclr_agent_dict[consul_hostname]
        host = agent.cs_host

        command = ("consul kv get -detailed %s" % (lock_key_path))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            if CONSUL_NO_KEY_STRING in retval.cr_stderr:
                return 0, None
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None

        kv_dict = {}
        lines = retval.cr_stdout.splitlines()
        for line in lines:
            fields = line.split()
            if len(fields) != 2:
                log.cl_error("unexpected field number of command output [%s] on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1, None
            key = fields[0]
            value = fields[1]
            kv_dict[key] = value

        if "Session" not in kv_dict:
            return 0, None

        session = kv_dict["Session"]
        if session == "-":
            return 0, None

        command = ("curl http://127.0.0.1:8500/v1/session/info/%s" %
                   (session))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None

        try:
            json_output = json.loads(retval.cr_stdout)
        except:
            log.cl_error("failed to parse json output of command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s], exception: %s",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr,
                         traceback.format_exc())
            return -1, None

        if not isinstance(json_output, list):
            log.cl_error("json output is not a list for command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s], exception: %s",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None

        if len(json_output) == 0:
            return 0, None

        if len(json_output) != 1:
            log.cl_error("unexpect list size for output of command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s], exception: %s",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None

        json_dict = json_output[0]
        if "Node" not in json_dict:
            log.cl_error("cannot find [Node] in json output of command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None

        return 0, json_dict["Node"]


# Consul is alive
CONSUL_STATUS_ALIVE = "alive"
# Consul service is down
CONSUL_STATUS_INACVITE = "inactive"
# Consul is disconnected to others
CONSUL_STATUS_DISCONNECTED = "disconnected"
# Unexpected error
CONSUL_STATUS_ERROR = "error"
# Host is not in consul cluster
CONSUL_STATUS_INVALID = "invalid"
# Host is down
CONSUL_STATUS_HOST_DOWN = "down"


def host_consul_status(log, host):
    """
    Return status of Consul
    """
    # pylint: disable=no-self-use
    command = ("systemctl status consul")
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("Consul service is not up on host [%s]",
                     host.sh_hostname)
        return CONSUL_STATUS_INACVITE

    command = ("consul members")
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("cannot connect to Consul cluster on host [%s]",
                     host.sh_hostname)
        return CONSUL_STATUS_DISCONNECTED

    lines = retval.cr_stdout.splitlines()
    if len(lines) < 2:
        log.cl_error("unexpected output of [consul members] command on "
                     "host [%s]", host.sh_hostname)
        return CONSUL_STATUS_ERROR

    lines = lines[1:]
    for line in lines:
        fields = line.split()
        if len(fields) != 8:
            log.cl_error("unexpected output of [consul members] command on "
                         "host [%s]", host.sh_hostname)
            return CONSUL_STATUS_ERROR

        if fields[0] != host.sh_hostname:
            continue

        return fields[2]

    log.cl_error("host [%s] is not outputed by [consul members] command",
                 host.sh_hostname)
    return CONSUL_STATUS_INVALID
