"""
Consul library
"""
# pylint: disable=too-many-lines
import socket
import base64
import time
import traceback
import json
from http import HTTPStatus
import yaml
import requests

CONSUL_BIN_NAME = "consul"
CONSUL_BIN_DIR = "/usr/bin"
CONSUL_CONFIG_DIR = "/etc/consul"
CONSUL_DATA_DIR = "/var/lib/consul"
CONSUL_NO_KEY_STRING = "Error! No key exists at:"
CONSUL_KV = "kv"

CONSUL_KV_STR_LOCK_INDEX = "LockIndex"
CONSUL_KV_STR_KEY = "Key"
CONSUL_KV_STR_FLAGS = "Flags"
CONSUL_KV_STR_VALUE = "Value"
CONSUL_KV_STR_SESSION = "Session"
CONSUL_KV_STR_CREATE_INDEX = "CreateIndex"
CONSUL_KV_STR_MODIFY_INDEX = "ModifyIndex"
CONSUL_KV_FIELDS = [CONSUL_KV_STR_LOCK_INDEX,
                    CONSUL_KV_STR_KEY,
                    CONSUL_KV_STR_FLAGS,
                    CONSUL_KV_STR_VALUE,
                    CONSUL_KV_STR_CREATE_INDEX,
                    CONSUL_KV_STR_MODIFY_INDEX]


CONSUL_SESSION_INFO_ID = "ID"
CONSUL_SESSION_INFO_NAME = "Name"
CONSUL_SESSION_INFO_NODE = "Node"
CONSUL_SESSION_INFO_LOCKDELAY = "LockDelay"
CONSUL_SESSION_INFO_BEHAVIOR = "Behavior"
CONSUL_SESSION_INFO_TTL = "TTL"
CONSUL_SESSION_INFO_NODECHECKS = "NodeChecks"
CONSUL_SESSION_INFO_SERVICECHECKS = "ServiceChecks"
CONSUL_SESSION_INFO_CREATEINDEX = "CreateIndex"
CONSUL_SESSION_INFO_MODIFYINDEX = "ModifyIndex"
CONSUL_SESSION_INFO_FIELDS = [CONSUL_SESSION_INFO_ID,
                              CONSUL_SESSION_INFO_NAME,
                              CONSUL_SESSION_INFO_NODE,
                              CONSUL_SESSION_INFO_LOCKDELAY,
                              CONSUL_SESSION_INFO_BEHAVIOR,
                              CONSUL_SESSION_INFO_TTL,
                              CONSUL_SESSION_INFO_NODECHECKS,
                              CONSUL_SESSION_INFO_SERVICECHECKS,
                              CONSUL_SESSION_INFO_CREATEINDEX,
                              CONSUL_SESSION_INFO_MODIFYINDEX]

def consul_kv_path(key):
    """
    Return the relative path of a KV from the key.
    """
    return CONSUL_KV + "/" + key


def consul_session_info_path(session_uuid):
    """
    Return the relative path of a KV from the key.
    """
    return "session/info/" + session_uuid


class ConsulAgent():
    """
    Each Consul server/client has an object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, host, bind_addr, is_server=True):
        # The SSHHost
        self.csa_host = host
        # IP like "10.0.0.1"
        self.csa_bind_addr = bind_addr
        # The port, usually 8500
        self.csa_port = "8500"
        # Whether this agent is Consul server.
        self.csa_is_server = is_server
        if is_server:
            self.cs_service_name = "server"
        else:
            self.cs_service_name = "client"
        # Example: "http://10.0.0.1:8500/v1/"
        self.csa_url_v1 = "http://" + bind_addr + ":" + self.csa_port + "/v1/"
        # Example: "http://10.0.0.1:8500/v1/kv/"
        self.csa_url_kv = self.csa_url_v1 + CONSUL_KV + "/"

    def _csa_url(self, path):
        """
        Return the full Consul URL from a relatvie path.
        """
        return self.csa_url_v1 + path

    def _csa_get_raw(self, log, path, quiet=False):
        """
        Key is something like "status/leader".
        Return the response, including the status.
        """
        url = self._csa_url(path)
        try:
            response = requests.get(url)
        except:
            if not quiet:
                log.cl_error("failed to get reply from URL [%s]: %s",
                             url, traceback.format_exc())
            return None
        return response

    def _csa_get(self, log, path, quiet=False):
        """
        Key is something like "status/leader".
        Return the json result
        """
        response = self._csa_get_raw(log, path, quiet=quiet)
        if response is None:
            return None
        if response.status_code != HTTPStatus.OK:
            if not quiet:
                log.cl_error("got status [%s] of Consul path [%s]",
                             response.status_code, path)
            return None
        return response.json()

    def csa_put_kv(self, log, key, value):
        """
        Key is something like "kv/path2key".
        Return the json result.
        """
        url = self._csa_url(consul_kv_path(key))
        try:
            response = requests.put(url, data=value)
        except:
            log.cl_error("failed to put data to URL [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("failed to put data to URL [%s], status [%s]",
                         url, response.status_code)
            return -1
        result = response.json()
        if not isinstance(result, bool):
            log.cl_error("unexpected type for response of putting key [%s]", key)
            return -1
        if not result:
            log.cl_error("failure reply of putting key [%s]", key)
            return -1
        return 0

    def _csa_get_kv_dict(self, log, key):
        """
        The response of the request is something like:
        [{
            "LockIndex": 5,
            "Key": "clownf_host/server0/lock",
            "Flags": 3304740253564472344,
            "Value": "NTQ3Nj2kNDMtN2RjNS1jN2NlLTRjM2QtNjdlMTc4ZjJmNmEz",
            "Session": "ac9d8c70-b8ff-504c-d33f-8e4177e3200d"
            "CreateIndex": 87,
            "ModifyIndex": 305188
        }]
        "Session" can be missing if no lock is held on the lock.

        If the key does not exist, return (0, None)
        """
        response = self._csa_get_raw(log, consul_kv_path(key))
        if response is None:
            return -1, None

        if response.status_code == HTTPStatus.NOT_FOUND:
            return 0, None

        if response.status_code != HTTPStatus.OK:
            log.cl_error("got status [%s] from Consul key [%s]",
                         response.status_code, key)
            return -1, None

        result = response.json()
        if not isinstance(result, list):
            log.cl_error("unexpected type for response of key [%s]", key)
            return -1, None
        if len(result) != 1:
            log.cl_error("unexpected length for response of key [%s]", key)
            return -1, None
        reply_dict = result[0]
        for field in CONSUL_KV_FIELDS:
            if field not in reply_dict:
                log.cl_error("missing [%s] in response of key [%s]",
                             field, key)
                return -1, reply_dict

        return 0, reply_dict

    def csa_get_kv_value(self, log, key):
        """
        Get the key of a KV. The key will be decoded out of base64-encoded
        blob. If the key does not exist, return (0, None)
        """
        ret, reply_dict = self._csa_get_kv_dict(log, key)
        if ret:
            return -1, None
        if reply_dict is None:
            return 0, None
        encoded_value = reply_dict[CONSUL_KV_STR_VALUE]
        try:
            value_bytes = base64.b64decode(encoded_value)
            value_string = value_bytes.decode('ascii')
        except:
            log.cl_error("failed to decode value of key [%s]: %s",
                         key, traceback.format_exc())
            return -1, None
        return 0, value_string

    def csa_get_agent_self_status(self, log, quiet=False):
        """
        Returns configuration of the local agent and member information
        got from "status/self" path.
        Example:
        {
            "Config": {
                "Datacenter": "dc1",
                "NodeName": "foobar",
                "NodeID": "9d754d17-d864-b1d3-e758-f3fe25a9874f",
                "Server": true,
                "Revision": "deadbeef",
                "Version": "1.0.0"
            },
            "DebugConfig": {
                ... full runtime configuration ...
                ... format subject to change ...
            },
            "Coord": {
                "Adjustment": 0,
                "Error": 1.5,
                "Vec": [0,0,0,0,0,0,0,0]
            },
            "Member": {
                "Name": "foobar",
                "Addr": "10.1.10.12",
                "Port": 8301,
                "Tags": {
                "bootstrap": "1",
                "dc": "dc1",
                "id": "40e4a748-2192-161a-0510-9bf59fe950b5",
                "port": "8300",
                "role": "consul",
                "vsn": "1",
                "vsn_max": "1",
                "vsn_min": "1"
                },
                "Status": 1,
                "ProtocolMin": 1,
                "ProtocolMax": 2,
                "ProtocolCur": 2,
                "DelegateMin": 2,
                "DelegateMax": 4,
                "DelegateCur": 4
            },
            "Meta": {
                "instance_type": "i2.xlarge",
                "os_version": "ubuntu_16.04"
            }
        }
        """
        path = "agent/self"
        result = self._csa_get(log, path, quiet=quiet)
        if result is None:
            if not quiet:
                log.cl_error("failed to get the Consul value of path [%s]", path)
            return None
        if not isinstance(result, dict):
            if not quiet:
                log.cl_error("unexpected type for Consul value of path [%s]", path)
            return None
        return result

    def csa_check_connectable(self, log):
        """
        Check the agent is connectable through HTTP API.
        """
        self_status = self.csa_get_agent_self_status(log, quiet=True)
        if self_status is None:
            return -1
        return 0

    def csa_get_leader(self, log):
        """
        Get the lead from "status/leader" key.
        Result is a string like "10.0.0.1:8300"
        """
        path = "status/leader"
        result = self._csa_get(log, path)
        if result is None:
            log.cl_error("failed to get the value of path [%s]", path)
            return None
        if not isinstance(result, str):
            log.cl_error("unexpected type for value of path [%s]", path)
            return None
        return result

    def csa_get_lock_session(self, log, lock_key):
        """
        Get the session name from the lock key.
        If no lock is held on the key, return (0, None)
        """
        ret, reply_dict = self._csa_get_kv_dict(log, lock_key)
        if ret:
            return -1, None
        if reply_dict is None:
            return 0, None
        if CONSUL_KV_STR_SESSION not in reply_dict:
            return 0, None
        session_uuid = reply_dict[CONSUL_KV_STR_SESSION]
        return 0, session_uuid

    def _csa_get_session_info(self, log, session_uuid):
        """
        Get the session info.

        The response of the request is something like:
        [{
            "ID": "adf4238a-882b-9ddc-4a9d-5b6758e4159e",
            "Name": "test-session",
            "Node": "raja-laptop-02",
            "LockDelay": 15000000000,
            "Behavior": "release",
            "TTL": "30s",
            "NodeChecks": [
            "serfHealth"
            ],
            "ServiceChecks": null,
            "CreateIndex": 1086449,
            "ModifyIndex": 1086449
        }]
        """
        result = self._csa_get(log, consul_session_info_path(session_uuid))
        if result is None:
            log.cl_error("failed to get the info of session [%s]", session_uuid)
            return -1, None
        if not isinstance(result, list):
            log.cl_error("unexpected type for info of session [%s]", session_uuid)
            return -1, None
        if len(result) != 1:
            log.cl_error("unexpected length for info of session [%s]", session_uuid)
            return -1, None
        reply_dict = result[0]
        for field in CONSUL_SESSION_INFO_FIELDS:
            if field not in reply_dict:
                log.cl_error("missing [%s] in info of session [%s]",
                             field, session_uuid)
                return -1, None

        return 0, reply_dict

    def csa_get_session_node(self, log, session_uuid):
        """
        Get the session node.
        """
        ret, reply_dict = self._csa_get_session_info(log, session_uuid)
        if ret or reply_dict is None:
            log.cl_info("failed to get info of session [%s]", session_uuid)
            return None
        return reply_dict[CONSUL_SESSION_INFO_NODE]

    def csa_cleanup(self, log):
        """
        Stop the service and cleanup the datadir
        """
        # Stop the current service if any.
        ret = self.csa_host.sh_service_stop(log, "consul")
        if ret:
            log.cl_info("failed to stop Consul service on host [%s]",
                        self.csa_host.sh_hostname)
            return -1

        # Cleanup the data if any
        command = ("rm -fr %s" % CONSUL_DATA_DIR)
        retval = self.csa_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.csa_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def csa_install(self, log, workspace, datacenter, sever_ips,
                    encrypt_key):
        """
        Install and config
        """
        ret = self.csa_cleanup(log)
        if ret:
            log.cl_error("failed to cleanup Consul datadir on host [%s]",
                         self.csa_host.sh_hostname)
            return -1

        fpath = workspace + "/" + CONSUL_BIN_NAME
        ret = self.csa_host.sh_send_file(log, fpath, CONSUL_BIN_DIR)
        if ret:
            log.cl_error("failed to send file [%s] on local host [%s] to "
                         "directory [%s] on host [%s]",
                         fpath, socket.gethostname(), CONSUL_BIN_DIR,
                         self.csa_host.sh_hostname)
            return -1

        consul_config = {}
        consul_config["datacenter"] = datacenter
        consul_config["data_dir"] = CONSUL_DATA_DIR
        consul_config["node_name"] = self.csa_host.sh_hostname
        consul_config["server"] = self.csa_is_server
        consul_config["ui"] = True
        consul_config["bind_addr"] = self.csa_bind_addr
        consul_config["client_addr"] = "0.0.0.0"
        consul_config["retry_join"] = sever_ips
        consul_config["rejoin_after_leave"] = True
        consul_config["start_join"] = sever_ips
        consul_config["enable_syslog"] = True
        consul_config["disable_update_check"] = True
        # Enable gossip encryption to avoid problems of leader election
        # affected by nodes from another datacenter
        consul_config["encrypt"] = encrypt_key

        if self.csa_is_server:
            consul_config["bootstrap_expect"] = len(sever_ips)

        json_fname = workspace + "/" + "config.json"
        with open(json_fname, 'w', encoding='utf-8') as json_file:
            json.dump(consul_config, json_file, indent=4, sort_keys=True)

        command = "mkdir -p " + CONSUL_CONFIG_DIR
        retval = self.csa_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.csa_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret = self.csa_host.sh_send_file(log, json_fname, CONSUL_CONFIG_DIR)
        if ret:
            log.cl_error("failed to send file [%s] on local host [%s] to "
                         "directory [%s] on host [%s]",
                         json_fname, socket.gethostname(), CONSUL_CONFIG_DIR,
                         self.csa_host.sh_hostname)
            return -1

        log.cl_info("installed Consul %s on host [%s]",
                    self.cs_service_name, self.csa_host.sh_hostname)
        return 0

    def csa_restart(self, log, restart=True):
        """
        Restart the service
        """
        if restart:
            ret = self.csa_host.sh_service_stop(log, "consul")
            if ret:
                log.cl_error("failed to stop service [%s] on host [%s]",
                             "consul",
                             self.csa_host.sh_hostname)
                return -1

        command = ("systemctl daemon-reload")
        retval = self.csa_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.csa_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret = self.csa_host.sh_service_start(log, "consul")
        if ret:
            log.cl_info("failed to start Consul service on host [%s]",
                        self.csa_host.sh_hostname)
            return -1

        log.cl_debug("configuring autostart of Consul on host [%s]",
                     self.csa_host.sh_hostname)
        command = "systemctl enable consul"
        retval = self.csa_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.csa_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        log.cl_debug("started Consul %s on host [%s]",
                     self.cs_service_name, self.csa_host.sh_hostname)
        return 0

    def ch_service_is_active(self, log):
        """
        If is active, return 1. If inactive, return 0. Return -1 on error.
        """
        host = self.csa_host
        if not host.sh_is_up(log):
            log.cl_error("host [%s] is down", host.sh_hostname)
            return -1
        return host.sh_service_is_active(log, "consul")



class ConsulServer(ConsulAgent):
    """
    Each Consul server has an object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, host, bind_addr):
        super().__init__(host, bind_addr, is_server=True)


class ConsulClient(ConsulAgent):
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
    # pylint: disable=too-many-instance-attributes
    def __init__(self, local_hostname, datacenter, server_dict, client_dict,
                 encrypt_key=None):
        # Local hostname
        self.cclr_local_hostname = local_hostname
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
            self.cclr_server_ips.append(server.csa_bind_addr)
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
        agent = self.cclr_alive_agent(log)
        if agent is None:
            log.cl_error("no Consul agent is up in the system")
            return None
        host = agent.csa_host

        leader = agent.csa_get_leader(log)
        if leader is None:
            log.cl_error("failed to get leader from agent [%s]",
                         host.sh_hostname)
            return None

        if not leader.endswith(":8300"):
            if not quiet:
                log.cl_error("unexpected port of Consul leader [%s]",
                             leader)
            return None
        leader_ip = leader[:-5]
        for server in self.cclr_server_dict.values():
            if server.csa_bind_addr == leader_ip:
                return server.csa_host.sh_hostname
        if not quiet:
            log.cl_error("IP of Consul leader [%s] does not belong to a Consul server",
                         leader)
        return None

    def cclr_alive_agent(self, log):
        """
        Return alive agent, either Consul server or client
        """
        agents = []
        # Connect through local agent if possible.
        if self.cclr_local_hostname in self.cclr_agent_dict:
            agent = self.cclr_agent_dict[self.cclr_local_hostname]
            agents.append(agent)
        for agent in self.cclr_agent_dict.values():
            if agent in agents:
                continue
            agents.append(agent)

        for agent in agents:
            ret = agent.csa_check_connectable(log)
            if ret:
                continue
            return agent
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
            ret = server.csa_install(log, workspace, self.cclr_datacenter,
                                     self.cclr_server_ips, encrypt_key)
            if ret:
                log.cl_error("failed to install Consul server on host [%s]",
                             server.csa_host.sh_hostname)
                return ret

        for client in self.cclr_client_dict.values():
            ret = client.csa_install(log, workspace, self.cclr_datacenter,
                                     self.cclr_server_ips, encrypt_key)
            if ret:
                log.cl_error("failed to install Consul client on host [%s]",
                             client.csa_host.sh_hostname)
                return ret

        return 0

    def cclr_cleanup(self, log, force=False):
        """
        Cleanup all Consul servers/clients datadir on the cluster
        """
        rc = 0
        for client in self.cclr_client_dict.values():
            ret = client.csa_cleanup(log)
            if ret:
                rc = ret
                log.cl_error("failed to cleanup Consul client on host [%s]",
                             client.csa_host.sh_hostname)
                if not force:
                    return ret

        for server in self.cclr_server_dict.values():
            ret = server.csa_cleanup(log)
            if ret:
                rc = ret
                log.cl_error("failed to cleanup Consul server on host [%s]",
                             server.csa_host.sh_hostname)
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
            ret = server.csa_restart(log, restart=restart)
            if ret:
                rc = ret
                log.cl_error("failed to start Consul server on host [%s]",
                             server.csa_host.sh_hostname)
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
            ret = client.csa_restart(log, restart=restart)
            if ret:
                rc = ret
                log.cl_error("failed to start Consul client on host [%s]",
                             client.csa_host.sh_hostname)
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

        ret = agent.csa_put_kv(log, key, config_string)
        if ret:
            log.cl_error("failed to put value of key [%s]", key)
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

        ret, value = agent.csa_get_kv_value(log, key_path)
        if ret:
            log.cl_error("failed to get value of key [%s]", key_path)
            return -1, None
        if value is None:
            return 0, None

        try:
            config = yaml.load(value, Loader=yaml.FullLoader)
        except:
            log.cl_error("failed to load [%s] of key [%s] on as yaml file: %s",
                         value, key_path,
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

        ret, session = agent.csa_get_lock_session(log, lock_key_path)
        if ret is None:
            log.cl_error("failed to get session of lock [%s]",
                         lock_key_path)
            return -1, None
        if session is None:
            return 0, None

        node = agent.csa_get_session_node(log, session)
        if node is None:
            log.cl_error("failed to get node of session [%s] for lock",
                         session, lock_key_path)
            return -1, None
        return 0, node


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
