"""
Library for Barreleye server.
Barreleye is a performance monitoring system for Lustre.
"""
# pylint: disable=too-many-lines
import os
import traceback
import json
from http import HTTPStatus
from slugify import slugify
import requests
from pycoral import utils
from pybarrele import barrele_constant
from pybarrele import barrele_influxdb


# The Influxdb config fpath
INFLUXDB_CONFIG_FPATH = "/etc/influxdb/influxdb.conf"
# Diff file to change influxdb.conf
INFLUXDB_CONFIG_DIFF = "influxdb.conf.diff"
# The Influxdb config fname
INFLUXDB_CONFIG_FNAME = os.path.basename(INFLUXDB_CONFIG_FPATH)
# The backuped Influxdb config fpath
INFLUXDB_CONFIG_BACKUP_FPATH = (barrele_constant.BARRELE_DIR + "/" +
                                INFLUXDB_CONFIG_FNAME)
# The common prefix of Influxdb continuous query
INFLUXDB_CQ_PREFIX = "cq_"
# The common prefix of Influxdb continuous query measurement
INFLUXDB_CQ_MEASUREMENT_PREFIX = "cqm_"
# Data source name of Influxdb on Grafana
GRAFANA_DATASOURCE_NAME = "barreleye_datasource"
# The dir of Grafana plugins
GRAFANA_PLUGIN_DIR = "/var/lib/grafana/plugins"
# All the dashboards. Key is the name of the dashboards. Value is the fname.
GRAFANA_DASHBOARDS = {}
GRAFANA_DASHBOARDS["Cluster Status"] = "cluster_status.json"
GRAFANA_DASHBOARDS["Lustre MDT"] = "lustre_mdt.json"
GRAFANA_DASHBOARDS["Lustre Client"] = "lustre_client.json"
GRAFANA_DASHBOARDS["Lustre MDS"] = "lustre_mds.json"
GRAFANA_DASHBOARDS["Lustre OSS"] = "lustre_oss.json"
GRAFANA_DASHBOARDS["Lustre OST"] = "lustre_ost.json"
GRAFANA_DASHBOARDS["Lustre Statistics"] = "lustre_statistics.json"
DASHBOARD_NAME_LUSTRE_USER = "Lustre User"
GRAFANA_DASHBOARDS[DASHBOARD_NAME_LUSTRE_USER] = "lustre_user.json"
DASHBOARD_NAME_LUSTRE_GROUP = "Lustre Group"
GRAFANA_DASHBOARDS[DASHBOARD_NAME_LUSTRE_GROUP] = "lustre_group.json"
GRAFANA_DASHBOARDS["Server Statistics"] = "server_statistics.json"
DASHBOARD_NAME_SFA_PHYSICAL = "SFA Physical Disk"
GRAFANA_DASHBOARDS[DASHBOARD_NAME_SFA_PHYSICAL] = "SFA_physical_disk.json"
DASHBOARD_NAME_SFA_VIRTUAL = "SFA Virtual Disk"
GRAFANA_DASHBOARDS[DASHBOARD_NAME_SFA_VIRTUAL] = "SFA_virtual_disk.json"
DASHBOARD_NAME_SFAS = [DASHBOARD_NAME_SFA_PHYSICAL, DASHBOARD_NAME_SFA_VIRTUAL]
GRAFANA_DASHBOARD_DIR = (barrele_constant.BARRELE_DIR + "/" +
                         "grafana_dashboards")
# The key string to replace to collect interval in Grafana dashboard templates
TEMPLATE_COLLECT_INTERVAL = "$BARRELEYE_COLLECT_INTERVAL"
# The key string to replace to collect interval in Grafana dashboard templates
TEMPLATE_DATASOURCE_NAME = "$BARRELEYE_DATASOURCE_NAME"
# Disabled Grafana folder
GRAFANA_FOLDER_DISABLED = "Disabled"
# Grafana folders
GRAFANA_FOLDERS = [GRAFANA_FOLDER_DISABLED]


def sed_replacement_escape(path):
    """
    Escape the '/' so "sed s///" can use it for replacement
    """
    return path.replace("/", r"\/")


def grafana_dashboard_check(log, title_name, dashboard):
    """
    Check whether the dashboard is legal or not
    """
    if dashboard["id"] is not None:
        log.cl_error("Grafana dashabord [%s] is invalid, expected [id] to be "
                     "[null], but got [%s]",
                     title_name, dashboard["id"])
        return -1
    if dashboard["title"] != title_name:
        log.cl_error("Grafana dashabord [%s] is invalid, expected [title] to be "
                     "[%s], but got [%s]",
                     title_name, title_name, dashboard["title"])
        return -1
    return 0


class BarreleServer():
    """
    Barreleye server object
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, host, data_path):
        # Host to run commands.
        self.bes_server_host = host
        # Dir to save monitoring data.
        self.bes_data_path = data_path
        # Influxdb client to run queries.
        self.bes_influxdb_client = \
            barrele_influxdb.BarreleInfluxdbClient(host.sh_hostname,
                                                   barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME)
        # Got fatal when connecting to Grafana server
        self.bes_grafana_fatal = False
        # Login of Grafana viewer user
        self.bes_grafana_viewer_login = "viewer"
        # Password of Grafana viewer user
        self.bes_grafana_viewer_password = "viewer"
        # Login of Grafana admin user
        self.bes_grafana_admin_login = "admin"
        # Password of Grafana admin user
        self.bes_grafana_admin_password = "admin"
        # Port of Grafana
        self.bes_grafana_port = "3000"
        # The folder id (not uid) of disabled
        self.bes_disabled_folder_id = None

    def _bes_erase_influxdb(self, log):
        """
        Only remove the Influxdb subdirs not the directory itself. This will
        prevent disaster when influxdb_path is set to a improper directory.
        """
        influxdb_subdirs = ["data", "meta", "wal"]
        for subdir in influxdb_subdirs:
            command = ('rm %s/%s -fr' % (self.bes_data_path, subdir))
            retval = self.bes_server_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.bes_server_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def _bes_is_influxdb_origin_config(self, log, fpath):
        """
        Check whether the influxdb.conf is patch by Barreleye before.
        """
        host = self.bes_server_host
        command = "grep barreleye %s" % fpath
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            # Not patched
            return True
        return False

    def _bes_backup_influxdb_config(self, log):
        """
        Save /etc/influxdb/influxdb.conf to /etc/coral/barreleye
        """
        host = self.bes_server_host
        log.cl_info("backuping origin Influxdb config file on host [%s]",
                    host.sh_hostname)
        ret = host.sh_path_exists(log, INFLUXDB_CONFIG_BACKUP_FPATH)
        if ret < 0:
            log.cl_error("failed to check whether file [%s] exists on host "
                         "[%s]", INFLUXDB_CONFIG_BACKUP_FPATH,
                         host.sh_hostname)
            return -1

        if ret == 0:
            command = ("cp %s %s" %
                       (INFLUXDB_CONFIG_FPATH,
                        barrele_constant.BARRELE_DIR))
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

        ret = host.sh_files_equal(log, INFLUXDB_CONFIG_FPATH,
                                  INFLUXDB_CONFIG_BACKUP_FPATH)
        if ret < 0:
            log.cl_error("failed to check whether files [%s] and [%s] are "
                         "equal on host [%s]", INFLUXDB_CONFIG_FPATH,
                         INFLUXDB_CONFIG_BACKUP_FPATH,
                         host.sh_hostname)
            return -1

        if ret == 0:
            log.cl_error("files [%s] and [%s] are not equal on host [%s]",
                         INFLUXDB_CONFIG_FPATH,
                         INFLUXDB_CONFIG_BACKUP_FPATH,
                         host.sh_hostname)
            log.cl_error("to fix the problem, please remove file [%s] "
                         "on host [%s]",
                         INFLUXDB_CONFIG_BACKUP_FPATH,
                         host.sh_hostname)
            return -1
        return 0

    def _bes_config_influxdb(self, log, barreleye_instance):
        """
        Configure Influxdb
        """
        # Copy the diff file to workspace to edit
        host = self.bes_server_host
        workspace = barreleye_instance.bei_workspace
        config_diff = (barrele_constant.BARRELE_DIR + "/" +
                       INFLUXDB_CONFIG_DIFF)

        if self._bes_is_influxdb_origin_config(log, INFLUXDB_CONFIG_FPATH):
            ret = self._bes_backup_influxdb_config(log)
            if ret:
                log.cl_error("failed to save original config file of "
                             "Influxdb")
                return -1
        else:
            log.cl_info("Influxdb config [%s] on host [%s] was "
                        "changed by Barreleye before, using backup",
                        INFLUXDB_CONFIG_FPATH, host.sh_hostname)

        final_diff = (workspace + "/" + INFLUXDB_CONFIG_DIFF)
        command = ("cp %s %s" % (config_diff, workspace))
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

        # Replace BARRELE_INFLUXDB_PATH to the configured path
        command = ("sed -i 's/BARRELE_INFLUXDB_PATH/" +
                   sed_replacement_escape(self.bes_data_path) + "'" + '/g ' +
                   final_diff)
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

        # Replace INFLUXDB_PATH to the configured path
        command = ("sed -i 's/BARRELE_INFLUXDB_DATABASE_NAME/" +
                   sed_replacement_escape(barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME) +
                   "'" + '/g ' + final_diff)
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

        command = ("/usr/bin/cp -f %s %s" %
                   (INFLUXDB_CONFIG_BACKUP_FPATH,
                    INFLUXDB_CONFIG_FPATH))
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

        command = ("patch -i %s %s" % (final_diff, INFLUXDB_CONFIG_FPATH))
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

    def _bes_influxdb_drop_database(self, log):
        """
        Drop database
        """
        host = self.bes_server_host
        log.cl_info("dropping existing Influxdb database [%s] on host [%s]",
                    barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME,
                    host.sh_hostname)
        command = ('influx -execute "DROP DATABASE %s"' %
                   barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME)
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

    def _bes_influxdb_create_database(self, log, drop_database=False):
        """
        Create Barreleye database of Influxdb
        """
        host = self.bes_server_host
        if drop_database:
            ret = self._bes_influxdb_drop_database(log)
            if ret:
                return -1

        command = ('influx -execute "CREATE DATABASE %s"' %
                   barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME)
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_debug("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _bes_influxdb_check_service(self, log, drop_database=False):
        """
        Check whether Influxdb is still up when creating database.
        """
        # pylint: disable=unused-argument
        host = self.bes_server_host
        service_name = "influxdb"

        ret = host.sh_service_is_active(log, service_name)
        if ret < 0:
            log.cl_error("failed to check whether service [%s] is active "
                         "on host [%s]", service_name, host.sh_hostname)
            return -1

        if ret == 0:
            log.cl_error("service [%s] is NOT active on host [%s]",
                         service_name, host.sh_hostname)
            return -1

        return 0

    def _bes_influxdb_service_start_enable(self, log):
        """
        Start and enable the Influxdb service
        """
        host = self.bes_server_host
        service_name = "influxdb"
        ret = host.sh_service_start_enable(log, service_name)
        if ret:
            log.cl_error("failed to start and enable service [%s] on "
                         "host [%s]", service_name, host.sh_hostnmae)
            return -1
        return 0

    def bes_grafana_url(self):
        """
        Return full Grafana URL
        """
        return ("http://%s:%s" %
                (self.bes_server_host.sh_hostname,
                 self.bes_grafana_port))

    def bes_grafana_viewer_url(self):
        """
        Return full Grafana URL
        """
        return ("http://%s:%s@%s:%s" %
                (self.bes_grafana_viewer_login, self.bes_grafana_viewer_password,
                 self.bes_server_host.sh_hostname,
                 self.bes_grafana_port))

    def bes_grafana_admin_url(self, api_path=""):
        """
        Return full Grafana URL
        """
        return ("http://%s:%s@%s:%s%s" %
                (self.bes_grafana_admin_login,
                 self.bes_grafana_admin_password,
                 self.bes_server_host.sh_hostname,
                 self.bes_grafana_port,
                 api_path))

    def _bes_grafana_try_connect(self, log):
        """
        Check whether we can connect to Grafana
        """
        url = self.bes_grafana_admin_url("")
        try:
            response = requests.get(url)
        except:
            log.cl_debug("got exception when connecting to [%s]: %s", url,
                         traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got status [%s] when acessing Grafana url [%s]",
                         response.status_code, url)
            # Quick way to abort without waiting
            self.bes_grafana_fatal = True
            return 0
        return 0

    def _bes_grafana_service_restart_and_enable(self, log):
        """
        Reinstall Grafana
        """
        host = self.bes_server_host
        service_name = "grafana-server"
        ret = host.sh_service_restart(log, service_name)
        if ret:
            log.cl_error("failed to restart service [%s] on host [%s]",
                         service_name, host.sh_hostname)
            return -1

        ret = host.sh_service_enable(log, service_name)
        if ret:
            log.cl_error("failed to start service [%s] on host [%s]",
                         service_name, host.sh_hostname)
            return -1

        ret = utils.wait_condition(log, self._bes_grafana_try_connect,
                                   ())
        if ret:
            log.cl_error("failed to connect to Grafana server")
            return -1

        if self.bes_grafana_fatal:
            log.cl_error("got fatal when connecting to Grafana server")
            return -1
        return 0

    def _bes_grafana_has_influxdb_datasource(self, log):
        """
        Return 1 if has influxdb datasource, return 0 if not, return -1 if
        error
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/datasources/name/%s" %
                                         GRAFANA_DATASOURCE_NAME)
        try:
            response = requests.get(url, headers=headers)
        except:
            log.cl_error("failed to get data source through URL [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code == HTTPStatus.OK:
            return 1
        if response.status_code == HTTPStatus.NOT_FOUND:
            return 0
        log.cl_error("got grafana status [%d] when get datasource of influxdb",
                     response.status_code)
        return -1

    def _bes_grafana_influxdb_datasource_delete(self, log):
        """
        Delete influxdb datasource from Grafana
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/datasources/name/%s" %
                                         GRAFANA_DATASOURCE_NAME)
        try:
            response = requests.delete(url, headers=headers)
        except:
            log.cl_error("not able to delete data source through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got grafana status [%d] when deleting datasource",
                         response.status_code)
            return -1
        return 0

    def _bes_grafana_influxdb_datasource_add(self, log):
        """
        Add Influxdb datasource to Grafana
        """
        # pylint: disable=bare-except
        influxdb_url = "http://%s:8086" % self.bes_server_host.sh_hostname
        data = {
            "name": GRAFANA_DATASOURCE_NAME,
            "isDefault": True,
            "type": "influxdb",
            "url": influxdb_url,
            "access": "proxy",
            "database": barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME,
            "basicAuth": False,
        }

        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/datasources")
        try:
            response = requests.post(url, json=data, headers=headers)
        except:
            log.cl_error("not able to create data source through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got Grafana status [%d] when creating datasource",
                         response.status_code)
            return -1
        return 0

    def _bes_grafana_influxdb_datasource_remove_and_add(self, log):
        """
        Add Influxdb datasource to Grafana.
        If the Influxdb datasource already exists, remove it first.
        """
        log.cl_info("adding Influxdb data source to Grafana on host [%s]",
                    self.bes_server_host.sh_hostname)
        ret = self._bes_grafana_has_influxdb_datasource(log)
        if ret < 0:
            return -1
        if ret:
            ret = self._bes_grafana_influxdb_datasource_delete(log)
            if ret:
                return -1

        ret = self._bes_grafana_influxdb_datasource_add(log)
        if ret:
            return ret
        return 0

    def _bes_grafana_install_plugin(self, log, barreleye_instance,
                                    panel_name):
        """
        Install a Grafana plugin
        """
        host = self.bes_server_host
        plugin_dir = GRAFANA_PLUGIN_DIR + "/" + panel_name
        command = ("rm -fr %s" % (plugin_dir))
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

        new_plugin_dir = barreleye_instance.bei_iso_dir + "/" + panel_name
        command = ("cp -a %s %s" % (new_plugin_dir, GRAFANA_PLUGIN_DIR))
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

    def _bes_grafana_install_plugins(self, log, barreleye_instance):
        """
        Install Grafana plugins
        """
        host = self.bes_server_host
        log.cl_info("installing Grafana plugins on host [%s]",
                    host.sh_hostname)
        for plugin in barrele_constant.GRAFANA_PLUGINS:
            ret = self._bes_grafana_install_plugin(log, barreleye_instance,
                                                   plugin)
            if ret:
                log.cl_error("failed to install Grafana plugin [%s]", plugin)
                return -1
        return 0

    def _bes_grafana_get_folders(self, log):
        """
        Get all folders
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/folders")
        try:
            response = requests.get(url, headers=headers)
        except:
            log.cl_error("failed to get all Grafana folders through [%s]: %s",
                         url, traceback.format_exc())
            return None
        if response.status_code == HTTPStatus.OK:
            return response.json()
        log.cl_error("got Grafana status [%d] when get all folders",
                     response.status_code)
        return None

    def _bes_grafana_folder_delete_uid(self, log, uid):
        """
        Delete folder with a uid from Grafana
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/folders/%s" %
                                         uid)
        try:
            response = requests.delete(url, headers=headers)
        except:
            log.cl_error("failed to delete Grafana folder with uid [%s] "
                         "through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got Grafana status [%d] when deleting folder "
                         "with uid [%s]",
                         response.status_code)
            return -1
        return 0

    def _bes_grafana_delete_folder(self, log, title_name):
        """
        Delete folders with a title from Grafana
        """
        json_obj = self._bes_grafana_get_folders(log)
        if json_obj is None:
            log.cl_error("failed to get all bashboards and folders")
            return -1
        for item in json_obj:
            if item['title'] != title_name:
                continue
            uid = item['uid']
            ret = self._bes_grafana_folder_delete_uid(log, uid)
            if ret:
                log.cl_error("failed to delete folder with uid [%s] and "                         "title [%s]", uid, title_name)
                return -1
        return 0

    def _bes_grafana_create_folder(self, log, title_name):
        """
        Create a Grafana folder with a title
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        data = {
            "title": title_name,
        }

        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/folders")
        try:
            response = requests.post(url, json=data, headers=headers)
        except:
            log.cl_error("failed to create Grafana folder through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got status [%d] when adding folder [%s] to "
                         "Grafana",
                         response.status_code, title_name)
            return -1

        if title_name == GRAFANA_FOLDER_DISABLED:
            self.bes_disabled_folder_id = response.json()["id"]
        return 0

    def _bes_grafana_recreate_folder(self, log, title_name):
        """
        Recreate a Grafana folder with a title
        """
        ret = self._bes_grafana_delete_folder(log, title_name)
        if ret:
            log.cl_error("failed to delete folders with title [%s]",
                         title_name)
            return -1

        ret = self._bes_grafana_create_folder(log, title_name)
        if ret:
            log.cl_error("failed to create folder with title [%s]",
                         title_name)
            return -1
        return 0

    def _bes_grafana_recreate_folders(self, log):
        """
        Recreate Grafana dashboards
        """
        # pylint: disable=too-many-locals
        host = self.bes_server_host

        log.cl_info("recreating Grafana folders on host [%s]",
                    host.sh_hostname)
        for folder_title in GRAFANA_FOLDERS:
            ret = self._bes_grafana_recreate_folder(log, folder_title)
            if ret:
                log.cl_error("failed to replace Grafana folder with title "
                             "[%s] on host [%s]",
                             folder_title, host.sh_hostname)
                return -1
        return 0

    def _bes_grafana_search_all(self, log):
        """
        Retrieving all folders and dashboards.
        Return the json object of the response.
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/search?query=%")
        try:
            response = requests.get(url, headers=headers)
        except:
            log.cl_error("failed to get all Grafana folders and dashboards "
                         "through [%s]: %s",
                         url, traceback.format_exc())
            return None
        if response.status_code == HTTPStatus.OK:
            return response.json()
        log.clerror("got status [%d] when getting all Grafana "
                    "folders and dashboards",
                    response.status_code)
        return None

    def _bes_grafana_has_dashboard(self, log, title_name):
        """
        Check whether Grafana has dashboard.
        Return 1 if has dashboard, return 0 if not, return -1 if error.
        """
        json_obj = self._bes_grafana_search_all(log)
        if json_obj is None:
            log.cl_error("failed to get all bashboards and folders")
            return -1
        for item in json_obj:
            if item['type'] == 'dash-db' and item['title'] == title_name:
                return 1
        return 0

    def _bes_grafana_dashboard_delete_uid(self, log, uid):
        """
        Delete bashboard with a uid from Grafana
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/dashboards/uid/%s" %
                                         uid)
        try:
            response = requests.delete(url, headers=headers)
        except:
            log.cl_error("failed to delete Grafana dashboard with uid [%s] "
                         "through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got Grafana status [%d] when deleting dashboard "
                         "with uid [%s]",
                         response.status_code)
            return -1
        return 0

    def _bes_grafana_dashboard_delete(self, log, title_name):
        """
        Delete bashboards with a title from Grafana
        """
        json_obj = self._bes_grafana_search_all(log)
        if json_obj is None:
            log.cl_error("failed to get all bashboards and folders")
            return -1
        for item in json_obj:
            if item['type'] != 'dash-db':
                continue
            if item['title'] != title_name:
                continue
            uid = item['uid']
            ret = self._bes_grafana_dashboard_delete_uid(log, uid)
            if ret:
                log.cl_error("failed to delete dashboard with uid [%s] and "                      "title [%s]", uid, title_name)
                return -1
        return 0

    def _bes_grafana_create_dashboard(self, log, title_name, dashboard,
                                      overwrite=False, folder_id=0):
        """
        Add dashboard of Grafana
        """
        ret = grafana_dashboard_check(log, title_name, dashboard)
        if ret:
            log.cl_error("Grafana dashboard [%s] is illegal",
                         title_name)
            return -1

        data = {
            "dashboard": dashboard,
            "overwrite": overwrite,
            "folderId": folder_id,
        }

        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/dashboards/db")
        try:
            response = requests.post(url, json=data, headers=headers)
        except:
            log.cl_error("failed to add Grafana bashboard through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got status [%d] when adding dashbard [%s] to "
                         "Grafana, json = [%s]",
                         response.status_code, title_name,
                         response.json())
            return -1
        return 0

    def _bes_grafana_recreate_dashboard(self, log, title_name, dashboard,
                                        folder_id=0):
        """
        Replace a bashboard in Grafana
        """
        host = self.bes_server_host
        ret = self._bes_grafana_dashboard_delete(log, title_name)
        if ret:
            log.cl_error("failed to delete Grafana dashboard with title"
                         "[%s] on host [%s]",
                         title_name, host.sh_hostname)
            return -1

        ret = self._bes_grafana_create_dashboard(log, title_name, dashboard,
                                                 overwrite=False,
                                                 folder_id=folder_id)
        if ret:
            log.cl_error("failed to overwrite Grafana dashboard [%s]",
                         title_name)
            return -1
        return 0

    def _bes_grafana_recreate_dashboards(self, log, barreleye_instance):
        """
        Recreate Grafana dashboards
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        host = self.bes_server_host
        collect_interval = str(barreleye_instance.bei_collect_interval)
        local_host = barreleye_instance.bei_local_host
        workspace = barreleye_instance.bei_workspace
        jobstat_pattern = barreleye_instance.bei_jobstat_pattern

        log.cl_info("recreating Grafana dashboards on host [%s]",
                    host.sh_hostname)
        for name, fname in GRAFANA_DASHBOARDS.items():
            dashboard_json_fpath = GRAFANA_DASHBOARD_DIR + "/" + fname
            dashboard_template_fpath = dashboard_json_fpath + ".template"
            ret = local_host.sh_path_exists(log, dashboard_template_fpath)
            if ret < 0:
                log.cl_error("failed to check whether file [%s] exists on "
                             "host [%s]", dashboard_template_fpath,
                             host.sh_hostname)
                return -1
            if ret:
                dashboard_json_fpath = (workspace + "/" + fname)
                with open(dashboard_template_fpath, 'r', encoding='utf-8') as template_file:
                    lines = template_file.readlines()

                with open(dashboard_json_fpath, 'w', encoding='utf-8') as json_file:
                    for line in lines:
                        line = line.replace(TEMPLATE_COLLECT_INTERVAL,
                                            collect_interval)
                        line = line.replace(TEMPLATE_DATASOURCE_NAME,
                                            GRAFANA_DATASOURCE_NAME)
                        json_file.write(line)
            else:
                ret = local_host.sh_path_exists(log, dashboard_json_fpath)
                if ret < 0:
                    log.cl_error("failed to check whether file [%s] exists on "
                                 "host [%s]", dashboard_json_fpath,
                                 host.sh_hostname)
                    return -1
                if ret == 0:
                    log.cl_error("file [%s] does not on host [%s]",
                                 dashboard_json_fpath,
                                 host.sh_hostname)
                    return -1

            with open(dashboard_json_fpath, "r", encoding='utf-8') as json_file:
                dashboard = json.load(json_file)

            folder_id = 0
            user_patterns = [barrele_constant.BARRELE_JOBSTAT_PATTERN_PROCNAME_UID,
                             barrele_constant.BARRELE_JOBSTAT_PATTERN_UID_GID]
            group_patterns = [barrele_constant.BARRELE_JOBSTAT_PATTERN_UID_GID]
            if name == DASHBOARD_NAME_LUSTRE_USER:
                if jobstat_pattern not in user_patterns:
                    # If jobstat pattern is not recognized, put the
                    # Lustre User dashboard to "Disabled" folder
                    folder_id = self.bes_disabled_folder_id
            elif name == DASHBOARD_NAME_LUSTRE_GROUP:
                if jobstat_pattern not in group_patterns:
                    # If jobstat pattern is not recognized, put the
                    # Lustre Group dashboard to "Disabled" folder
                    folder_id = self.bes_disabled_folder_id
            elif name in DASHBOARD_NAME_SFAS:
                # SFA dashboards is not supported yet.
                folder_id = self.bes_disabled_folder_id

            ret = self._bes_grafana_recreate_dashboard(log, name, dashboard,
                                                       folder_id)
            if ret:
                log.cl_error("failed to replace Grafana dashboard "
                             "[%s] on host [%s]",
                             name, host.sh_hostname)
                return -1
        return 0

    def _bes_grafana_user_delete(self, log, user_id):
        """
        Delete a user from Grafana
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/admin/users/%s" % user_id)
        try:
            response = requests.delete(url, headers=headers)
        except:
            log.cl_error("not able to delete users through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code == HTTPStatus.OK:
            return 0
        log.cl_error("got status [%d] when deleting user with ID [%s] "
                     "from user [%s]", user_id,
                     response.status_code)
        return -1

    def _bes_grafana_user_add(self, log, name, email_address, login,
                              password):
        """
        Adding user of Grafana
        """
        host = self.bes_server_host
        log.cl_info("adding user/password [%s/%s] to Grafana on "
                    "host [%s]", login, password, host.sh_hostname)
        data = {
            "name": name,
            "email": email_address,
            "login": login,
            "password": password,
        }

        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/admin/users")
        try:
            response = requests.post(url, json=data, headers=headers)
        except:
            log.cl_error("not able to add user through [%s]: %s",
                         url, traceback.format_exc())
            return -1
        if response.status_code != HTTPStatus.OK:
            log.cl_error("got status [%d] when adding user [%s] to Grafana",
                         response.status_code, name)
            return -1
        return 0

    def _bes_grafana_user_info(self, log, name):
        """
        Add viewer user
        """
        # pylint: disable=bare-except
        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        url = self.bes_grafana_admin_url("/api/users/lookup?loginOrEmail=%s" %
                                         (slugify(name)))
        try:
            response = requests.get(url, headers=headers)
        except:
            log.cl_error("not able to get users through [%s]: %s",
                         url, traceback.format_exc())
            return -1, None
        if response.status_code == HTTPStatus.OK:
            return 1, response.json()
        if response.status_code == HTTPStatus.NOT_FOUND:
            return 0, None
        log.cl_error("got status [%d] when getting user info from Grafana",
                     response.status_code)
        return -1, None

    def _bes_grafana_user_recreate(self, log, name, email_address, login,
                                   password):
        """
        If user doesn't exist, add the user.
        If user exists, remove it first.
        """
        ret, json_info = self._bes_grafana_user_info(log, "viewer")
        if ret < 0:
            return -1
        if ret == 1:
            user_id = json_info["id"]
            log.cl_debug("Grafana user [%s] exists with id [%d], deleting it",
                         name, user_id)
            ret = self._bes_grafana_user_delete(log, user_id)
            if ret:
                return ret

        ret = self._bes_grafana_user_add(log, name, email_address, login,
                                         password)
        if ret:
            return ret
        log.cl_debug("added user [%s] to Grafana", name)
        return 0

    def _bes_grafana_reinstall(self, log, barreleye_instance):
        """
        Reinstall Grafana
        """
        host = self.bes_server_host
        service_name = "grafana-server"
        log.cl_info("restarting and enabling service [%s] on host [%s]",
                    service_name, host.sh_hostname)
        ret = self._bes_grafana_service_restart_and_enable(log)
        if ret:
            log.cl_error("failed to restart or enable service [%s] on "
                         "host [%s]", service_name,
                         host.sh_hostname)
            return -1

        ret = self._bes_grafana_install_plugins(log, barreleye_instance)
        if ret:
            log.cl_error("failed to install Grafana plugins on host [%s]",
                         host.sh_hostname)
            return ret

        ret = self._bes_grafana_recreate_folders(log)
        if ret:
            log.cl_error("failed to recreate Grafana folders on host [%s]",
                         host.sh_hostname)
            return ret

        ret = self._bes_grafana_recreate_dashboards(log, barreleye_instance)
        if ret:
            log.cl_error("failed to recreate Grafana dashboards on host [%s]",
                         host.sh_hostname)
            return ret

        ret = self._bes_grafana_influxdb_datasource_remove_and_add(log)
        if ret:
            log.cl_error("failed to add Influxdb data source to Grafana on "
                         "host [%s]", host.sh_hostname)
            return -1

        ret = self._bes_grafana_user_recreate(log, "Viewer",
                                              "viewer@localhost",
                                              self.bes_grafana_viewer_login,
                                              self.bes_grafana_viewer_password)
        if ret:
            log.cl_error("failed to add user/password [%s/%s] to Grafana on "
                         "host [%s]", self.bes_grafana_viewer_login,
                         self.bes_grafana_viewer_password, host.sh_hostname)
            return -1
        return 0

    def bes_server_reinstall(self, log, barreleye_instance,
                             erase_influxdb=False,
                             drop_database=False):
        """
        Reinstall Barreleye server
        """
        host = self.bes_server_host
        service_name = "influxdb"

        ret = host.sh_service_stop(log, service_name)
        if ret:
            log.cl_error("failed to stop service [%s] on host [%s]",
                         service_name, host.sh_hostname)
            return -1

        if erase_influxdb:
            log.cl_info("erasing data and metadata of Influxdb")
            ret = self._bes_erase_influxdb(log)
            if ret:
                log.cl_error("failed to erase data and metadata of Influxdb")
                return -1

        command = ('mkdir -p %s && chown influxdb %s && chgrp influxdb %s' %
                   (self.bes_data_path, self.bes_data_path,
                    self.bes_data_path))
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

        ret = self._bes_config_influxdb(log, barreleye_instance)
        if ret:
            log.cl_error("failed to configure Influxdb")
            return -1

        log.cl_info("starting and enabling service [%s] on host [%s]",
                    service_name, host.sh_hostname)
        ret = self._bes_influxdb_service_start_enable(log)
        if ret:
            log.cl_error("failed to starting or enabling service [%s] on "
                         "host [%s]", service_name, host.sh_hostname)
            return -1

        log.cl_info("waiting until Influxdb runs well on host [%s]",
                    host.sh_hostname)
        ret = utils.wait_condition(log, self._bes_influxdb_create_database,
                                   (drop_database,),
                                   timeout=600,
                                   quit_func=self._bes_influxdb_check_service)
        if ret:
            log.cl_error("failed to create Influxdb database")
            return -1

        ret = self._bes_influxdb_recreate_cqs(log, barreleye_instance)
        if ret:
            log.cl_error("failed to recreate continuous queries of Influxdb")
            return -1

        ret = self._bes_grafana_reinstall(log, barreleye_instance)
        if ret:
            log.cl_error("failed to reinstall Grafana on host [%s]",
                         host.sh_hostname)
            return -1
        return 0

    def _bes_influxdb_cq_create(self, log, barreleye_instance,
                                measurement, groups, where=""):
        """
        Create continuous query in influxdb
        """
        # pylint: disable=bare-except
        collect_interval = barreleye_instance.bei_collect_interval
        continuous_query_periods = barreleye_instance.bei_continuous_query_periods
        cq_query = INFLUXDB_CQ_PREFIX + measurement
        group_string = ""
        cq_measurement = INFLUXDB_CQ_MEASUREMENT_PREFIX + measurement
        for group in groups:
            group_string += ', "%s"' % group
            cq_query += "_%s" % group
            cq_measurement += "-%s" % group

        cq_time = int(collect_interval) * int(continuous_query_periods)
        query = ('CREATE CONTINUOUS QUERY %s ON "%s" \n'
                 'BEGIN SELECT sum("value") / %s INTO "%s" \n'
                 '    FROM "%s" %s GROUP BY time(%ds)%s \n'
                 'END;' %
                 (cq_query, barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME,
                  continuous_query_periods, cq_measurement,
                  measurement, where, cq_time, group_string))
        response = self.bes_influxdb_client.bic_query(log, query)
        if response is None:
            log.cl_error("failed to create continuous query with query [%s]",
                         query)
            return -1

        if response.status_code != HTTPStatus.OK:
            log.cl_error("got InfluxDB status [%d] when creating "
                         "continuous query with query [%s]",
                         response.status_code, query)
            return -1
        return 0

    def bes_influxdb_cq_delete(self, log, measurement, groups):
        """
        Delete continuous query in influxdb
        """
        # pylint: disable=bare-except
        cq_query = INFLUXDB_CQ_PREFIX + measurement
        for group in groups:
            cq_query += "_%s" % group
        query = ('DROP CONTINUOUS QUERY %s ON "%s";' %
                 (cq_query, barrele_constant.BARRELE_INFLUXDB_DATABASE_NAME))
        response = self.bes_influxdb_client.bic_query(log, query)
        if response is None:
            log.cl_error("failed to drop continuous query with query [%s]",
                         query)
            return -1

        if response.status_code != HTTPStatus.OK:
            log.cl_error("got InfluxDB status [%d] when droping "
                         "continuous query with query [%s]",
                         response.status_code, query)
            return -1
        return 0

    def _bes_influxdb_cq_recreate(self, log, barreleye_instance, measurement,
                                  groups, where=""):
        """
        Create continuous query in influxdb, delete one first if necesary
        """
        # Sort the groups so that we will get a unique cq name for the same groups
        groups.sort()
        ret = self._bes_influxdb_cq_create(log, barreleye_instance,
                                           measurement, groups, where=where)
        if ret == 0:
            return 0

        ret = self.bes_influxdb_cq_delete(log, measurement, groups)
        if ret:
            return ret

        ret = self._bes_influxdb_cq_create(log, barreleye_instance,
                                           measurement, groups, where=where)
        if ret:
            log.cl_error("failed to create continuous query for measurement [%s]",
                         measurement)
        return ret

    def _bes_influxdb_recreate_cqs(self, log, barreleye_instance):
        """
        Create all the continuous queries of Influxdb
        """
        # pylint: disable=too-many-statements
        log.cl_info("recreating continuous queries of Influxdb on host [%s]",
                    self.bes_server_host.sh_hostname)
        continuous_queries = []
        InfluxdbContinuousQuery = barrele_influxdb.InfluxdbContinuousQuery
        continuous_query = InfluxdbContinuousQuery("mdt_acctuser_samples",
                                                   ["fs_name", "optype",
                                                    "user_id"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("mdt_acctgroup_samples",
                                                   ["fs_name", "group_id",
                                                    "optype"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("mdt_acctproject_samples",
                                                   ["fs_name", "optype",
                                                    "project_id"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_acctuser_samples",
                                                   ["fs_name", "optype",
                                                    "user_id"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_acctgroup_samples",
                                                   ["fs_name", "optype",
                                                    "group_id"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_acctproject_samples",
                                                   ["fs_name", "optype",
                                                    "project_id"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("exp_md_stats",
                                                   ["exp_client", "fs_name"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("mdt_jobstats_samples",
                                                   ["fs_name", "job_id"])
        continuous_queries.append(continuous_query)

        if (barreleye_instance.bei_jobstat_pattern ==
                barrele_constant.BARRELE_JOBSTAT_PATTERN_PROCNAME_UID):
            continuous_query = InfluxdbContinuousQuery("mdt_jobstats_samples",
                                                       ["fs_name", "uid"])
            continuous_queries.append(continuous_query)
        elif (barreleye_instance.bei_jobstat_pattern ==
              barrele_constant.BARRELE_JOBSTAT_PATTERN_UID_GID):
            continuous_query = InfluxdbContinuousQuery("mdt_jobstats_samples",
                                                       ["fs_name", "uid"])
            continuous_queries.append(continuous_query)
            continuous_query = InfluxdbContinuousQuery("mdt_jobstats_samples",
                                                       ["fs_name", "gid"])
            continuous_queries.append(continuous_query)
        elif (barreleye_instance.bei_jobstat_pattern !=
              barrele_constant.BARRELE_JOBSTAT_PATTERN_UNKNOWN):
            log.cl_error("unknown jobstat pattern [%s] when creating "
                         "continuous queries for MDT",
                         barreleye_instance.bei_jobstat_pattern)
            return -1
        continuous_query = InfluxdbContinuousQuery("ost_stats_bytes",
                                                   ["fs_name", "optype",
                                                    "fqdn"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_stats_bytes",
                                                   ["fs_name", "ost_index"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_stats_bytes",
                                                   ["fs_name", "fqdn"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_stats_bytes",
                                                   ["fs_name", "optype"])
        continuous_queries.append(continuous_query)
        measure_meants = ["ost_brw_stats_page_discontiguous_rpc_samples",
                          "ost_brw_stats_block_discontiguous_rpc_samples",
                          "ost_brw_stats_fragmented_io_samples",
                          "ost_brw_stats_io_in_flight_samples",
                          "ost_brw_stats_io_time_samples",
                          "ost_brw_stats_io_size_samples"]
        for measure_meant in measure_meants:
            continuous_query = \
                InfluxdbContinuousQuery(measure_meant,
                                        ["field", "fs_name", "size"])
            continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                   ["fs_name", "job_id", "optype"])
        continuous_queries.append(continuous_query)
        where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
        continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                   ["fs_name", "job_id"],
                                                   where=where)
        continuous_queries.append(continuous_query)
        where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
        continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                   ["fs_name", "job_id",
                                                    "ost_index"],
                                                   where=where)
        if (barreleye_instance.bei_jobstat_pattern ==
                barrele_constant.BARRELE_JOBSTAT_PATTERN_PROCNAME_UID):
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "uid",
                                                        "optype"])
            continuous_queries.append(continuous_query)
            where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "uid"],
                                                       where=where)
            continuous_queries.append(continuous_query)
            where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "uid",
                                                        "ost_index"],
                                                       where=where)
            continuous_queries.append(continuous_query)
        elif (barreleye_instance.bei_jobstat_pattern ==
              barrele_constant.BARRELE_JOBSTAT_PATTERN_UID_GID):
            # UID
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "uid",
                                                        "optype"])
            continuous_queries.append(continuous_query)
            where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "uid"],
                                                       where=where)
            continuous_queries.append(continuous_query)
            where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "uid",
                                                        "ost_index"],
                                                       where=where)
            continuous_queries.append(continuous_query)

            # GID
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "gid",
                                                        "optype"])
            continuous_queries.append(continuous_query)
            where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "gid"],
                                                       where=where)
            continuous_queries.append(continuous_query)
            where = "WHERE optype = 'sum_read_bytes' OR optype = 'sum_write_bytes'"
            continuous_query = InfluxdbContinuousQuery("ost_jobstats_bytes",
                                                       ["fs_name", "gid",
                                                        "ost_index"],
                                                       where=where)
            continuous_queries.append(continuous_query)
        elif (barreleye_instance.bei_jobstat_pattern !=
              barrele_constant.BARRELE_JOBSTAT_PATTERN_UNKNOWN):
            log.cl_error("unknown jobstat pattern [%s] when creating "
                         "continuous queries for OST",
                         barreleye_instance.bei_jobstat_pattern)
            return -1
        continuous_query = InfluxdbContinuousQuery("ost_brw_stats_rpc_bulk_samples",
                                                   ["field", "fs_name",
                                                    "size"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("exp_ost_stats_bytes",
                                                   ["fs_name", "exp_client",
                                                    "optype"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("md_stats",
                                                   ["fs_name"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("md_stats",
                                                   ["fs_name", "mdt_index"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("md_stats",
                                                   ["fs_name", "optype"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("mdt_filesinfo_free",
                                                   ["fs_name"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("mdt_filesinfo_used",
                                                   ["fs_name"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_kbytesinfo_free",
                                                   ["fs_name"])
        continuous_queries.append(continuous_query)
        continuous_query = InfluxdbContinuousQuery("ost_kbytesinfo_used",
                                                   ["fs_name"])
        continuous_queries.append(continuous_query)

        for continuous_query in continuous_queries:
            ret = self._bes_influxdb_cq_recreate(log, barreleye_instance,
                                                 continuous_query.icq_measurement,
                                                 continuous_query.icq_groups,
                                                 where=continuous_query.icq_where)
            if ret:
                log.cl_error("failed to create continuous query of "
                             "measurement [%s]",
                             continuous_query.icq_measurement)
                return -1
        return 0

    def bes_grafana_running(self, log):
        """
        Check whether the grafana is running.
        Return 1 if running. Return -1 if failure.
        """
        command = "systemctl is-active grafana-server"
        retval = self.bes_server_host.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 1
        if retval.cr_stdout == "unknown\n":
            return 0
        if retval.cr_stdout == "inactive\n":
            return 0
        log.cl_error("unexpected stdout of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.bes_server_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def bes_influxdb_running(self, log):
        """
        Check whether the influxdb is running.
        Return 1 if running. Return -1 if failure.
        """
        command = "systemctl is-active influxdb"
        retval = self.bes_server_host.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 1
        if retval.cr_stdout == "unknown\n":
            return 0
        if retval.cr_stdout == "inactive\n":
            return 0
        log.cl_error("unexpected stdout of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.bes_server_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def bes_influxdb_version(self, log):
        """
        Return the Influxdb version, e.g. 1.8.4-1.x86_64
        """
        host = self.bes_server_host
        ret, version = host.sh_rpm_version(log, "influxdb-")
        if ret or version is None:
            log.cl_error("failed to get the Influxdb RPM version on host [%s]",
                         host.sh_hostname)
        return version

    def bes_grafana_version(self, log):
        """
        Return the Grafana version, e.g. 7.3.7-1.x86_64
        """
        host = self.bes_server_host
        ret, version = host.sh_rpm_version(log, "grafana-")
        if ret or version is None:
            log.cl_error("failed to get the Grafana RPM version on host [%s]",
                         host.sh_hostname)
        return version
