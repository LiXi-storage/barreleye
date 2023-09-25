"""
Constants used in Barreleye
"""
from pycoral import constant

# The Influxdb database name.
BARRELE_INFLUXDB_DATABASE_NAME = "barreleye_database"
# Config file of Barreleye
BARRELE_CONFIG_FNAME = "barreleye.conf"
BARRELE_DIR = constant.CORAL_DIR + "/barrele"
# The dir for XML files
BARRELE_XML_DIR = BARRELE_DIR + "/xmls"
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
                                # Needed by collectd-ssh
                                "zeromq"]
# RPMs needed to download for server
BARRELE_SERVER_DOWNLOAD_DEPENDENT_RPMS = ["patch",  # Needed when patching influxdb.conf
                                          "urw-base35-fonts"]  # Needed by Grafana
# RPMs needed to download for Barreleye server and agent
BARRELE_DOWNLOAD_DEPENDENT_RPMS = (BARRELE_SERVER_DOWNLOAD_DEPENDENT_RPMS +
                                   BARRELE_AGENT_DEPENDENT_RPMS)
# Debs needed to download for Barreleye server and agent (Ubuntu)
BARRELE_DOWNLOAD_DEPENDENT_DEBS = ["librrd8"]
# RPMS needed by Barreleye servers.
BARRELE_SERVER_DEPENDENT_RPMS = ["influxdb", "grafana"]
BARRELE_SERVER_DEPENDENT_RPMS += BARRELE_DOWNLOAD_DEPENDENT_RPMS
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
# The jobid_name needs to be configured as "u%u.g%g".
BARRELE_JOBSTAT_PATTERN_UID_GID = "uid_gid"
BARRELE_JOBSTAT_PATTERNS = [BARRELE_JOBSTAT_PATTERN_UNKNOWN,
                            BARRELE_JOBSTAT_PATTERN_PROCNAME_UID,
                            BARRELE_JOBSTAT_PATTERN_UID_GID]

# The Collectd/Influxdb service is active
BARRELE_AGENT_ACTIVE = "active"
# The Collectd/Influxdb service is inactive
BARRELE_AGENT_INACTIVE = "inactive"

# The host of agent is up
BARRELE_AGENT_UP = "up"
# The Collectd service is down
BARRELE_AGENT_DOWN = "down"

# The Grafana service is running
BARRELE_AGENT_RUNNING = "running"
# The Grafana service is stopped
BARRELE_AGENT_STOPPED = "stopped"

# The agent hostname
BARRELE_FIELD_HOST = "Host"
# Lustre version name
BARRELE_FIELD_LUSTRE_VERSION = "Lustre Version"
# The status of agent up
BARRELE_FIELD_UP = "Up"
# The status of agent collectd
BARRELE_FIELD_COLLECTD = "Collectd"
# The version of agent collectd
BARRELE_FIELD_COLLECTD_VERSION = "Collectd Version"
# The status of Grafana service
BARRELE_FIELD_GRAFANA = "Grafana"
# The version of Grafana
BARRELE_FIELD_GRAFANA_VERSION = "Grafana Version"
# The status of Influxdb service
BARRELE_FIELD_INFLUXDB = "Influxdb"
# The version of Influxdb
BARRELE_FIELD_INFLUXDB_VERSION = "Influxdb Version"
