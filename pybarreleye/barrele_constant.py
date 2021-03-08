"""
Constants used in Barreleye
"""
from pycoral import constant

# The Influxdb database name.
BARRELE_INFLUXDB_DATABASE_NAME = "barreleye_database"
# Config file of Barreleye
BARRELE_CONFIG_FNAME = "barreleye.conf"
# The dir to save original influxdb.conf and also the patch for it
BARRELE_CONFIG_DIR = constant.ETC_CORAL_DIR + "/barreleye"
# The dir for XML files
BARRELE_XML_DIR = constant.ETC_CORAL_DIR + "/barreleye-xmls"
# Config file path of barrelelye.conf
BARRELE_CONFIG = constant.ETC_CORAL_DIR + "/" + BARRELE_CONFIG_FNAME
BARRELE_LOG_DIR = constant.CORAL_LOG_DIR + "/barrele"
# RPMS needed by Barreleye agents.
BARRELE_AGENT_DEPENDENT_RPMS = ["libmnl",  # Needed by collectd
                                # Needed by collectd-sensors
                                "lm_sensors-libs",
                                # Needed by collectd-ssh
                                "openpgm",
                                "rsync",
                                # Needed by collectd
                                "yajl",
                                # Needed by zeromq3
                                "zeromq3"]
# RPMS needed by Barreleye servers.
BARRELE_SERVER_DEPENDENT_RPMS = ["influxdb", "grafana",
                                 "patch",  # Needed when patching influxdb.conf
                                 "urw-base35-fonts"]  # Needed by Grafana
BARRELE_DEPENDENT_RPMS = BARRELE_AGENT_DEPENDENT_RPMS + BARRELE_SERVER_DEPENDENT_RPMS
BARRELE_TEST_LOG_DIR_BASENAME = "barrele_test"


BRL_AGENTS = "agents"
BRL_CONTINUOUS_QUERY_PERIODS = "continuous_query_periods"
BRL_COLLECT_INTERVAL = "collect_interval"
BRL_DATA_PATH = "data_path"
BRL_ENABLE_DISK = "enable_disk"
BRL_ENABLE_IME = "enable_ime"
BRL_ENABLE_INFINIBAND = "enable_infiniband"
BRL_ENABLE_LUSTRE_CLIENT = "enable_lustre_client"
BRL_ENABLE_LUSTRE_MDS = "enable_lustre_mds"
BRL_ENABLE_LUSTRE_OSS = "enable_lustre_oss"
BRL_ENABLE_LUSTRE_EXP_MDT = "enable_lustre_exp_mdt"
BRL_ENABLE_LUSTRE_EXP_OST = "enable_lustre_exp_ost"
BRL_HOSTNAME = "hostname"
BRL_JOBSTAT_PATTERN = "jobstat_pattern"
BRL_LUSTRE_FALLBACK_VERSION = "lustre_fallback_version"
BRL_SERVER = "server"
BRL_SSH_IDENTITY_FILE = "ssh_identity_file"

GRAFANA_STATUS_PANEL = "Grafana_Status_panel"
GRAFANA_PIECHART_PANEL = "grafana-piechart-panel"
# All plugin names
GRAFANA_PLUGINS = [GRAFANA_STATUS_PANEL,
                   GRAFANA_PIECHART_PANEL]

# The jobstat pattern configured in Lustre is unknown.
BARRELE_JOBSTAT_PATTERN_UNKNOWN = "unknown"
# The jobstat configured in Lustre is "procname_uid". Some metrics of users
# will be enabled.
BARRELE_JOBSTAT_PATTERN_PROCNAME_UID = "procname_uid"
BARRELE_JOBSTAT_PATTERNS = [BARRELE_JOBSTAT_PATTERN_UNKNOWN,
                            BARRELE_JOBSTAT_PATTERN_PROCNAME_UID]
