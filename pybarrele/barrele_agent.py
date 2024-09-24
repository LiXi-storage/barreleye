"""
Library for Barreleye agent.
Barreleye is a performance monitoring system for Lustre.
"""
import json
from pycoral import utils
from pycoral import os_distro
from pybarrele import barrele_collectd
from pybarrele import barrele_constant


class BarreleAgent():
    """
    Each agent has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, host, barreleye_server,
                 enable_disk=False, enable_lustre_oss=True,
                 enable_lustre_mds=True, enable_lustre_client=False,
                 enable_infiniband=False):
        # Barreleye server with type of BarreleServer
        self.bea_barreleye_server = barreleye_server
        # Host to run commands.
        self.bea_host = host
        # Whether to collect disk metrics from this agent.
        self.bea_enable_disk = enable_disk
        # Whether to collect Lustre OSS metrics from this agent.
        self.bea_enable_lustre_oss = enable_lustre_oss
        # Whether to collect Lustre MDS metrics from this agent.
        self.bea_enable_lustre_mds = enable_lustre_mds
        # Whether to collect Lustre client metrics from this agent.
        self.bea_enable_lustre_client = enable_lustre_client
        # Whether to collect Infiniband metrics from this agent.
        self.bea_enable_infiniband = enable_infiniband
        # Lustre version on this host.
        self.bea_lustre_version = None
        # Collectd RPMs needed to be installed in this agent.
        self.bea_needed_collectd_package_types = \
            [barrele_collectd.LIBCOLLECTDCLIENT_TYPE_NAME,
             barrele_collectd.COLLECTD_TYPE_NAME]
        # The last timestamp when a measurement has been found to be updated.
        self.bea_influxdb_update_time = None
        # Collectd config for test. Type: CollectdConfig
        self.bea_collectd_config_for_test = None
        # Collectd config for production. Type: CollectdConfig
        self.bea_collectd_config_for_production = None
        # BarreleInstance
        self.bea_instance = None

    def _bea_check_connection_with_server(self, log):
        # The client might has problem to access Barreyele server, find the
        # problem as early as possible.
        barreleye_server = self.bea_barreleye_server
        command = ("ping -c 1 %s" % barreleye_server.bes_server_host.sh_hostname)
        retval = self.bea_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.bea_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _bea_sanity_check(self, log):
        """
        Sanity check of the host before installation
        """
        ret = self._bea_check_connection_with_server(log)
        if ret:
            log.cl_error("failed to check the connection of Barreleye agent "
                         "[%s] with server",
                         self.bea_host.sh_hostname)
            return -1

        distro = self.bea_host.sh_distro(log)
        if distro not in (os_distro.DISTRO_RHEL7,
                          os_distro.DISTRO_RHEL8,
                          os_distro.DISTRO_UBUNTU2004,
                          os_distro.DISTRO_UBUNTU2204):
            log.cl_error("host [%s] has unsupported distro [%s]",
                         self.bea_host.sh_hostname, distro)
            return -1

        cpu_target = self.bea_host.sh_target_cpu(log)
        if cpu_target is None:
            log.cl_error("failed to get target cpu on host [%s]",
                         self.bea_host.sh_hostname)
            return -1

        if cpu_target != "x86_64":
            log.cl_error("host [%s] has unsupported CPU type [%s]",
                         self.bea_host.sh_hostname, cpu_target)
            return -1

        command = ("hostname")
        retval = self.bea_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.bea_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        # If the hostname is inconsistent with the configured hostname,
        # fqdn tag of the data points will be unexpected.
        hostname = retval.cr_stdout.strip()
        if hostname != self.bea_host.sh_hostname:
            log.cl_error("inconsistent hostname [%s] of Barreleye agent "
                         "host [%s]", hostname, self.bea_host.sh_hostname)
            return -1
        return 0

    def _bea_check_lustre_version_rpm(self, log, lustre_fallback_version):
        """
        Check the Lustre version according to the installed RPMs
        """
        # pylint: disable=too-many-return-statements,too-many-branches

        # Old Lustre kernel RPM might not be uninstalled ye, so ignore
        # kernel RPMs.
        command = ("rpm -qa | grep lustre | grep -v kernel")
        retval = self.bea_host.sh_run(log, command)
        if (retval.cr_exit_status == 1 and retval.cr_stdout == "" and
                retval.cr_stderr == ""):
            log.cl_info("Lustre RPM is not installed on host [%s], "
                        "using default [%s]",
                        self.bea_host.sh_hostname,
                        lustre_fallback_version.lv_version_name)
            self.bea_lustre_version = lustre_fallback_version
            return 0
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.bea_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        rpm_names = retval.cr_stdout.split()
        rpm_fnames = []
        for rpm_name in rpm_names:
            rpm_fnames.append(rpm_name + ".rpm")

        instance = self.bea_instance
        version_db = instance.bei_lustre_version_db

        version, _ = version_db.lvd_match_version_from_rpms(log,
                                                            rpm_fnames,
                                                            skip_kernel=True,
                                                            skip_test=True)
        if version is None:
            version, _ = version_db.lvd_match_version_from_rpms(log,
                                                                rpm_fnames,
                                                                client=True)
            if version is None:
                log.cl_warning("failed to match Lustre version according to RPM "
                               "names on host [%s], using default [%s]",
                               self.bea_host.sh_hostname,
                               lustre_fallback_version.lv_version_name)
                self.bea_lustre_version = lustre_fallback_version
        if version is not None:
            log.cl_info("detected Lustre version [%s] on host [%s]",
                        version.lv_version_name,
                        self.bea_host.sh_hostname)
            self.bea_lustre_version = version
        return 0

    def _bea_check_lustre_version_deb(self, log, lustre_fallback_version):
        """
        Check the Lustre version according to the installed debs
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        command = ("apt list --installed | grep lustre-client-modules")
        retval = self.bea_host.sh_run(log, command)
        if (retval.cr_exit_status == 1 and retval.cr_stdout == ""):
            log.cl_info("Lustre deb is not installed on host [%s], "
                        "using default [%s]",
                        self.bea_host.sh_hostname,
                        lustre_fallback_version.lv_version_name)
            self.bea_lustre_version = lustre_fallback_version
            return 0
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.bea_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        deb_lines = retval.cr_stdout.splitlines()
        if len(deb_lines) != 1:
            log.cl_error("multiple lines outputed by command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.bea_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        deb_line = deb_lines[0]
        fields = deb_line.split()
        if len(fields) != 4:
            log.cl_error("unexpected field number outputed by command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.bea_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        version = fields[1]
        instance = self.bea_instance
        version_db = instance.bei_lustre_version_db
        lversion = version_db.lvd_match_version_from_deb(log, version)
        if lversion is None:
            log.cl_error("failed to detect Lustre version on host [%s]",
                         self.bea_host.sh_hostname)
            return -1

        log.cl_info("Lustre version [%s] detected on host [%s]",
                    lversion.lv_version_name,
                    self.bea_host.sh_hostname)
        self.bea_lustre_version = lversion
        return 0

    def _bea_check_lustre_version(self, log, lustre_fallback_version):
        """
        Check the Lustre version according to the installed RPMs or debs
        """
        host = self.bea_host
        distro = host.sh_distro(log)

        if distro in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8):
            return self._bea_check_lustre_version_rpm(log, lustre_fallback_version)
        if distro in (os_distro.DISTRO_UBUNTU2004, os_distro.DISTRO_UBUNTU2204):
            return self._bea_check_lustre_version_deb(log, lustre_fallback_version)

        log.cl_error("distro [%s] of host [%s] is not supported",
                     distro, host.sh_hostname)
        return -1

    def _bea_generate_collectd_config(self, log,
                                      collectd_test=False):
        """
        Generate Collectd config
        """
        instance = self.bea_instance
        if collectd_test:
            interval = barrele_collectd.COLLECTD_INTERVAL_TEST
        else:
            interval = instance.bei_collect_interval
        collectd_config = \
            barrele_collectd.CollectdConfig(self, interval,
                                            instance.bei_jobstat_pattern)
        if (self.bea_enable_lustre_oss or self.bea_enable_lustre_mds or
                self.bea_enable_lustre_client):
            ret = collectd_config.cdc_plugin_lustre(log,
                                                    self.bea_lustre_version,
                                                    enable_lustre_oss=self.bea_enable_lustre_oss,
                                                    enable_lustre_mds=self.bea_enable_lustre_mds,
                                                    enable_lustre_client=self.bea_enable_lustre_client,
                                                    enable_lustre_exp_ost=instance.bei_enable_lustre_exp_ost,
                                                    enable_lustre_exp_mdt=instance.bei_enable_lustre_exp_mdt)
            if ret:
                log.cl_error("failed to config Lustre plugin of Collectd")
                return None

        if self.bea_enable_infiniband:
            collectd_config.cdc_plugin_infiniband()
        return collectd_config

    def bea_generate_configs(self, log):
        """
        Steps before configuring Barreleye agent
        """
        instance = self.bea_instance
        ret = self._bea_sanity_check(log)
        if ret:
            log.cl_error("Barreleye agent host [%s] is insane",
                         self.bea_host.sh_hostname)
            return -1

        ret = self._bea_check_lustre_version(log,
                                             instance.bei_lustre_fallback_version)
        if ret:
            log.cl_error("failed to check the Lustre version on Barreleye "
                         "agent [%s]",
                         self.bea_host.sh_hostname)
            return -1

        collectd_config = self._bea_generate_collectd_config(log,
                                                             collectd_test=True)
        if collectd_config is None:
            log.cl_error("failed to generate Collectd config for test")
            return -1

        self.bea_collectd_config_for_test = collectd_config

        collectd_config = self._bea_generate_collectd_config(log,
                                                             collectd_test=False)
        if collectd_config is None:
            log.cl_error("failed to generate Collectd config for production "
                         "usage")
            return -1
        self.bea_collectd_config_for_production = collectd_config
        return 0

    def _bea_influxdb_measurement_check(self, log, measurement_name, tags):
        # pylint: disable=bare-except,too-many-return-statements
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """
        Check whether the datapoint is recieved by InfluxDB
        """
        tag_string = ""
        for key, value in tags.items():
            if tag_string != "":
                tag_string += " AND"
            else:
                tag_string = " WHERE"
            tag_string += (" %s = '%s'" % (key, value))
        query = ('SELECT * FROM "%s"%s ORDER BY time DESC LIMIT 1;' %
                 (measurement_name, tag_string))
        influxdb_client = self.bea_barreleye_server.bes_influxdb_client
        serie = influxdb_client.bic_query_serie(log, query, quiet=True)
        if serie is None:
            return -1

        json_string = json.dumps(serie, indent=4, separators=(',', ': '))
        if barrele_constant.INFLUX_COLUMNS not in serie:
            log.cl_debug("missing [%s] in serie result of influxdb: %s",
                         json_string, barrele_constant.INFLUX_COLUMNS)
            return -1
        columns = serie[barrele_constant.INFLUX_COLUMNS]

        if barrele_constant.INFLUX_VALUES not in serie:
            log.cl_debug("missing [%s] in serie result of influxdb: %s",
                         barrele_constant.INFLUX_VALUES, json_string)
            return -1
        serie_values = serie[barrele_constant.INFLUX_VALUES]

        if len(serie_values) != 1:
            log.cl_debug("invalid [%s] in serie result of influxdb: %s",
                         barrele_constant.INFLUX_VALUES, json_string)
            return -1
        value = serie_values[0]

        time_index = -1
        i = 0
        for column in columns:
            if column == "time":
                time_index = i
                break
            i += 1

        if time_index == -1:
            log.cl_debug("got wrong InfluxDB data [%s], no [time] in "
                         "the columns", json_string)
            return -1

        timestamp = int(value[time_index])

        if self.bea_influxdb_update_time is None:
            self.bea_influxdb_update_time = timestamp
        elif timestamp > self.bea_influxdb_update_time:
            return 0
        log.cl_debug("timestamp [%d] is not updated with query [%s]",
                     timestamp, query)
        return -1

    def bea_influxdb_measurement_check(self, log, measurement_name, **tags):
        """
        Check whether influxdb has datapoint
        """
        if "fqdn" not in tags:
            tags["fqdn"] = self.bea_host.sh_hostname
        ret = utils.wait_condition(log, self._bea_influxdb_measurement_check,
                                   (measurement_name, tags))
        if ret:
            log.cl_error("Influxdb gets no data point for measurement [%s] "
                         "from agent [%s]", measurement_name,
                         self.bea_host.sh_hostname)
            return -1
        return 0

    def bea_collectd_send_config(self, log,
                                 test_config=False):
        """
        Dump and send the collectd.conf to the agent host
        """
        host = self.bea_host
        instance = self.bea_instance
        local_host = instance.bei_local_host
        command = "mkdir -p %s" % instance.bei_workspace
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

        fpath = instance.bei_workspace + "/"
        if test_config:
            fpath += barrele_collectd.COLLECTD_CONFIG_TEST_FNAME
            collectd_config = self.bea_collectd_config_for_test
        else:
            fpath += barrele_collectd.COLLECTD_CONFIG_FINAL_FNAME
            collectd_config = self.bea_collectd_config_for_production
        fpath += "." + host.sh_hostname

        collectd_config.cdc_dump(fpath)

        distro = self.bea_host.sh_distro(log)
        if distro in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8):
            etc_path = "/etc/collectd.conf"
        elif distro in (os_distro.DISTRO_UBUNTU2004,
                        os_distro.DISTRO_UBUNTU2204):
            etc_path = "/etc/collectd/collectd.conf"
        else:
            log.cl_error("unsupported OS distro [%s]",
                         distro)
            return -1
        ret = host.sh_send_file(log, fpath, etc_path)
        if ret:
            log.cl_error("failed to send file [%s] on local host [%s] to "
                         "directory [%s] on host [%s]",
                         fpath, etc_path,
                         instance.bei_local_host.sh_hostname,
                         host.sh_hostname)
            return -1
        return 0

    def bea_config_agent(self, log):
        """
        Configure agent
        """
        host = self.bea_host
        log.cl_info("configuring Collectd on host [%s]",
                    host.sh_hostname)
        ret = self.bea_collectd_send_config(log,
                                            test_config=True)
        if ret:
            log.cl_error("failed to send test config to Barreleye agent "
                         "on host [%s]",
                         self.bea_host.sh_hostname)
            return -1

        service_name = "collectd"
        ret = host.sh_service_restart(log, service_name)
        if ret:
            log.cl_error("failed to restart Collectd service on host [%s]",
                         host.sh_hostname)
            return -1

        log.cl_info("checking whether Influxdb can get data points from "
                    "agent [%s]", host.sh_hostname)
        ret = self.bea_collectd_config_for_test.cdc_check(log)
        if ret:
            log.cl_error("Influxdb doesn't have expected data points from "
                         "agent [%s]",
                         host.sh_hostname)
            return -1

        ret = self.bea_collectd_send_config(log,
                                            test_config=False)
        if ret:
            log.cl_error("failed to send final Collectd config to Barreleye "
                         "agent on host [%s]",
                         host.sh_hostname)
            return -1

        ret = host.sh_service_restart(log, service_name)
        if ret:
            log.cl_error("failed to restart Barreleye agent on host [%s]",
                         host.sh_hostname)
            return -1

        ret = host.sh_service_enable(log, service_name)
        if ret:
            log.cl_error("failed to enable service [%s] on host [%s]",
                         service_name, host.sh_hostname)
            return -1

        return 0

    def bea_collectd_running(self, log):
        """
        Check whether the Collectd is running.
        Return 1 if running. Return -1 if failure.
        """
        command = "systemctl is-active collectd"
        retval = self.bea_host.sh_run(log, command)
        if retval.cr_stdout == "active\n":
            return 1
        if retval.cr_stdout == "unknown\n":
            return 0
        if retval.cr_stdout == "inactive\n":
            return 0
        log.cl_error("unexpected stdout of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.bea_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    def bea_collectd_stop(self, log):
        """
        Stop Collectd service.
        """
        service_name = "collectd"
        host = self.bea_host
        ret = host.sh_service_stop(log, service_name)
        if ret:
            log.cl_error("failed to stop [%s] service on agent host [%s]",
                         service_name, host.sh_hostname)
            return -1
        return 0

    def bea_collectd_start(self, log):
        """
        Start Collectd service.
        """
        service_name = "collectd"
        host = self.bea_host
        ret = host.sh_service_start(log, service_name)
        if ret:
            log.cl_error("failed to start [%s] service on agent host [%s]",
                         service_name, host.sh_hostname)
            return -1
        return 0

    def bea_collectd_version(self, log):
        """
        Return the Collectd version, e.g. 5.12.0.barreleye0-1.el7.x86_64
        """
        host = self.bea_host
        ret, version = host.sh_rpm_version(log, "collectd-")
        if ret or version is None:
            log.cl_error("failed to get the Collectd RPM version on host [%s]",
                         host.sh_hostname)
        return version
