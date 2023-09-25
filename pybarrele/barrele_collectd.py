"""
Library for generating collectd config
"""
# pylint: disable=too-many-lines
import re
import collections
from pycoral import lustre_version
from pybarrele import barrele_constant

LIBCOLLECTDCLIENT_TYPE_NAME = "libcollectdclient"
COLLECTD_TYPE_NAME = "collectd"

COLLECTD_CONFIG_TEST_FNAME = "collectd.conf.test"
COLLECTD_CONFIG_FINAL_FNAME = "collectd.conf.final"
# The collection interval of testing Collectd
COLLECTD_INTERVAL_TEST = 1
# ES2 of version ddn18 added support for used inode/space in the future
ES2_HAS_USED_INODE_SPACE_SUPPORT = False
# ES4 will add support for used inode/space in the future
ES4_HAS_USED_INODE_SPACE_SUPPORT = True

XML_FNAME_ES2 = "lustre-ieel-2.5_definition.xml"
XML_FNAME_ES3 = "lustre-ieel-2.7_definition.xml"
XML_FNAME_ES4 = "lustre-es4-2.10.xml"
XML_FNAME_2_12 = "lustre-2.12.xml"
XML_FNAME_ES5_1 = "lustre-b_es5_1.xml"
XML_FNAME_ES5_2 = "lustre-b_es5_2.xml"
XML_FNAME_ES6_0 = "lustre-b_es6_0.xml"
XML_FNAME_ES6_1 = "lustre-b_es6_1.xml"
XML_FNAME_2_13 = "lustre-2.13.xml"
XML_FNAME_IME_1_1 = "ime-1.1.xml"
XML_FNAME_IME_1_2 = "ime-1.2.xml"
# The Lustre XML files that supports ZFS.
SUPPORTED_ZFS_XML_FNAMES = [XML_FNAME_ES3, XML_FNAME_ES4,
                            XML_FNAME_2_12, XML_FNAME_ES5_1,
                            XML_FNAME_ES5_2, XML_FNAME_2_13,
                            XML_FNAME_ES6_0, XML_FNAME_ES6_1]


def lustre_version_xml_fname(log, version, quiet=False):
    """
    Return the XML file of this Lustre version
    """
    if version.lv_name == lustre_version.LUSTRE_VERSION_NAME_2_12:
        xml_fname = XML_FNAME_2_12
    elif version.lv_name == lustre_version.LUSTRE_VERSION_NAME_ES5_1:
        xml_fname = XML_FNAME_ES5_1
    elif version.lv_name == lustre_version.LUSTRE_VERSION_NAME_ES5_2:
        xml_fname = XML_FNAME_ES5_2
    elif version.lv_name == lustre_version.LUSTRE_VERSION_NAME_ES6_0:
        xml_fname = XML_FNAME_ES6_0
    elif version.lv_name == lustre_version.LUSTRE_VERSION_NAME_ES6_1:
        xml_fname = XML_FNAME_ES6_1
    else:
        if not quiet:
            log.cl_error("unsupported Lustre version of [%s]",
                         version.lv_name)
        return None
    return xml_fname


def support_zfs(xml_fname):
    """
    Whether this XML file supports zfs
    """
    if xml_fname in SUPPORTED_ZFS_XML_FNAMES:
        return True
    return False


def support_acctgroup_acctproject(version):
    """
    Whether this Lustre version supports acctgroup and acctproject
    """
    if version.lv_name == "es2":
        return False
    return True


def support_lustre_client(version):
    """
    Whether this Lustre version supports client_stats_*
    """
    if version.lv_name in ["es2", lustre_version.LUSTRE_VERSION_NAME_2_12]:
        return False
    return True


class CollectdConfig():
    """
    Each collectd config has an object of this type
    """
    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    def __init__(self, barreleye_agent, collect_internal, jobstat_pattern):
        self.cdc_configs = collections.OrderedDict()
        self.cdc_plugins = collections.OrderedDict()
        self.cdc_filedatas = collections.OrderedDict()
        self.cdc_aggregations = collections.OrderedDict()
        self.cdc_post_cache_chain_rules = collections.OrderedDict()
        self.cdc_sfas = collections.OrderedDict()
        self.cdc_checks = []
        self.cdc_jobstat_pattern = jobstat_pattern
        # On some hosts, Collectd might get an hostname that differs from the
        # output of command "hostname". Thus, fix the hostname in Collectd
        # by configuring it.
        self.cdc_configs["Hostname"] = \
            '"' + barreleye_agent.bea_host.sh_hostname + '"'
        self.cdc_configs["Interval"] = collect_internal
        self.cdc_configs["WriteQueueLimitHigh"] = 1000000
        self.cdc_configs["WriteQueueLimitLow"] = 800000
        self.cdc_plugin_syslog("err")
        self.cdc_plugin_memory()
        self.cdc_plugin_cpu()
        self.cdc_barreleye_agent = barreleye_agent
        self.cdc_plugin_write_tsdb()
        self.cdc_plugin_df()
        self.cdc_plugin_load()
        self.cdc_plugin_sensors()
        self.cdc_plugin_uptime()
        self.cdc_plugin_users()

    def cdc_dump(self, fpath):
        """
        Dump the config to file
        """
        # pylint: disable=too-many-statements,too-many-locals,too-many-branches
        with open(fpath, "wt", encoding='utf-8') as fout:
            fout.write("# Collectd config file generated automatcially by "
                       "Barreleye\n\n")
            for config_name, config in self.cdc_configs.items():
                text = '%s %s\n' % (config_name, config)
                fout.write(text)
            fout.write("\n")

            if any(self.cdc_aggregations):
                config = """LoadPlugin aggregation
<Plugin "aggregation">
"""
                fout.write(config)
                for config in self.cdc_aggregations.values():
                    fout.write(config)
                config = """
</Plugin>

"""
                fout.write(config)

            if any(self.cdc_post_cache_chain_rules):
                config = """LoadPlugin match_regex
PostCacheChain "PostCache"
# Don't send "cpu-X" stats
<Chain "PostCache">
"""
                fout.write(config)
                for config in self.cdc_post_cache_chain_rules.values():
                    fout.write(config)
                config = """
    Target "write"
</Chain>

"""
                fout.write(config)

            if any(self.cdc_sfas):
                config = 'LoadPlugin ssh\n'
                fout.write(config)
                template_prefix = """<Plugin "ssh">
    <Common>
        DefinitionFile "/etc/%s"
        Extra_tags "extrahost=%s"
"""
                template_controller0 = """
        <ServerHost>
            HostName "%s"
            UserName "user"
            UserPassword "user"
            SshTerminator "%sRAID[0]$ "
            IpcDir "/tmp"
            #KnownhostsFile "/root/.ssh/known_hosts"
            #PublicKeyfile "/root/.ssh/id_dsa.pub"
            #PrivateKeyfile "/root/.ssh/id_dsa"
            #SshKeyPassphrase "passphrase"
        </ServerHost>
"""
                template_controller1 = """
        <ServerHost>
            HostName "%s"
            UserName "user"
            UserPassword "user"
            SshTerminator "%sRAID[1]$ "
            IpcDir "/tmp"
            #KnownhostsFile "/root/.ssh/known_hosts"
            #PublicKeyfile "/root/.ssh/id_dsa.pub"
            #PrivateKeyfile "/root/.ssh/id_dsa"
            #SshKeyPassphrase "passphrase"
        </ServerHost>
"""
                template_postfix = """
    </Common>
    <Item>
        Type "vd_c_rates"
    </Item>
    <Item>
        Type "vd_read_latency"
    </Item>
    <Item>
        Type "vd_write_latency"
    </Item>
    <Item>
        Type "vd_read_iosize"
    </Item>
    <Item>
        Type "vd_write_iosize"
    </Item>
    <Item>
        Type "pd_c_rates"
    </Item>
    <Item>
        Type "pd_read_latency"
    </Item>
    <Item>
        Type "pd_write_latency"
    </Item>
    <Item>
        Type "pd_read_iosize"
    </Item>
    <Item>
        Type "pd_write_iosize"
    </Item>
</Plugin>

"""
                for sfa in self.cdc_sfas.values():
                    if sfa.esfa_subsystem_name == "":
                        name = ""
                    else:
                        name = sfa.esfa_subsystem_name + " "

                    controller0 = sfa.esfa_index2controller(controller0=True)
                    controller1 = sfa.esfa_index2controller(controller0=False)
                    config = (template_prefix % (sfa.esfa_xml_fname,
                                                 sfa.esfa_name))
                    if controller0 is not None:
                        config = config + (template_controller0 %
                                           (controller0, name))
                    if controller1 is not None:
                        config = config + (template_controller1 %
                                           (controller1, name))
                    config = config + template_postfix
                    fout.write(config)

            if any(self.cdc_filedatas):
                config = """LoadPlugin filedata
"""
                fout.write(config)
                for config in self.cdc_filedatas.values():
                    fout.write(config)

            for plugin_name, plugin_config in self.cdc_plugins.items():
                text = 'LoadPlugin %s\n' % plugin_name
                text += plugin_config + '\n'
                fout.write(text)

    def cdc_check(self, log):
        """
        Check the config to file
        """
        for check in self.cdc_checks:
            ret = check(log)
            if ret:
                return ret
        return 0

    def cdc_plugin_syslog(self, log_level):
        """
        Config the syslog plugin
        """
        if log_level not in ("err", "info", "debug"):
            return -1
        config = ('<Plugin "syslog">\n'
                  '    LogLevel %s\n'
                  '</Plugin>\n' % log_level)
        self.cdc_plugins["syslog"] = config
        return 0

    def cdc_plugin_memory_check(self, log):
        """
        Check the memory plugin
        """
        name = "memory.buffered.memory"
        return self.cdc_barreleye_agent.bea_influxdb_measurement_check(log, name)

    def cdc_plugin_memory(self):
        """
        Config the memory plugin
        """
        self.cdc_plugins["memory"] = ""
        if self.cdc_plugin_memory_check not in self.cdc_checks:
            self.cdc_checks.append(self.cdc_plugin_memory_check)
        return 0

    def cdc_plugin_write_tsdb(self):
        """
        Config the write TSDB plugin
        """
        barreleye_server = self.cdc_barreleye_agent.bea_barreleye_server
        host = barreleye_server.bes_server_host.sh_hostname
        config = ('<Plugin "write_tsdb">\n'
                  '    <Node>\n'
                  '        Host "%s"\n'
                  '        Port "4242"\n'
                  '        DeriveRate true\n'
                  '    </Node>\n'
                  '</Plugin>\n' % host)
        self.cdc_plugins["write_tsdb"] = config
        return 0

    def cdc_plugin_cpu_check(self, log):
        """
        Check the CPU plugin
        """
        barreleye_agent = self.cdc_barreleye_agent
        measurement = "aggregation.cpu-average.cpu.system"
        return barreleye_agent.bea_influxdb_measurement_check(log, measurement)

    def cdc_plugin_cpu(self):
        """
        Config the cpu plugin
        """
        self.cdc_aggregations["cpu"] = """    <Aggregation>
        Plugin "cpu"
        Type "cpu"
        GroupBy "Host"
        GroupBy "TypeInstance"
        CalculateAverage true
    </Aggregation>
"""
        self.cdc_post_cache_chain_rules["cpu"] = """    <Rule>
        <Match regex>
            Plugin "^cpu$"
            PluginInstance "^[0-9]+$"
        </Match>
        <Target write>
            Plugin "aggregation"
        </Target>
        Target stop
    </Rule>
"""
        self.cdc_plugins["cpu"] = ""
        if self.cdc_plugin_cpu_check not in self.cdc_checks:
            self.cdc_checks.append(self.cdc_plugin_cpu_check)
        return 0

    def cdc_plugin_lustre(self, log, version, enable_lustre_oss=False,
                          enable_lustre_mds=False, enable_lustre_client=False,
                          enable_lustre_exp_ost=False, enable_lustre_exp_mdt=False):
        # pylint: disable=too-many-branches,too-many-statements
        """
        Config the Lustre plugin
        """
        xml_fname = lustre_version_xml_fname(log, version)
        if xml_fname is None:
            return -1
        xml_fpath = barrele_constant.BARRELE_XML_DIR + "/" + xml_fname

        enable_zfs = support_zfs(xml_fname)

        config = """<Plugin "filedata">
    <Common>
        DefinitionFile \""""
        config += xml_fpath + '"'
        config += """
    </Common>
"""
        config += """
    <Item>
        Type "ldlm_canceld_stats_req_waittime"
    </Item>
    <Item>
        Type "ldlm_canceld_stats_req_qdepth"
    </Item>
    <Item>
        Type "ldlm_canceld_stats_req_active"
    </Item>
    <Item>
        Type "ldlm_canceld_stats_req_timeout"
    </Item>
    <Item>
        Type "ldlm_canceld_stats_reqbuf_avail"
    </Item>

    <Item>
        Type "ldlm_cbd_stats_req_waittime"
    </Item>
    <Item>
        Type "ldlm_cbd_stats_req_qdepth"
    </Item>
    <Item>
        Type "ldlm_cbd_stats_req_active"
    </Item>
    <Item>
        Type "ldlm_cbd_stats_req_timeout"
    </Item>
    <Item>
        Type "ldlm_cbd_stats_reqbuf_avail"
    </Item>
"""
        if enable_lustre_oss:
            config += """
    # OST stats
    <Item>
        Type "ost_acctuser"
    </Item>"""
            if enable_zfs:
                config += """
    <Item>
        Type "zfs_ost_acctuser"
    </Item>"""
            if support_acctgroup_acctproject(version):
                config += """
    <Item>
        Type "ost_acctgroup"
    </Item>
    <Item>
        Type "ost_acctproject"
    </Item>"""
                if enable_zfs:
                    config += """
    <Item>
        Type "zfs_ost_acctgroup"
    </Item>
    <Item>
        Type "zfs_ost_acctproject"
    </Item>
"""
            config += """
    <Item>
        Type "ost_brw_stats_rpc_bulk"
    </Item>
    <Item>
        Type "ost_brw_stats_page_discontiguous_rpc"
    </Item>
    <Item>
        Type "ost_brw_stats_block_discontiguous_rpc"
    </Item>
    <Item>
        Type "ost_brw_stats_fragmented_io"
    </Item>
    <Item>
        Type "ost_brw_stats_io_in_flight"
    </Item>
    <Item>
        Type "ost_brw_stats_io_time"
    </Item>
    <Item>
        Type "ost_brw_stats_io_size"
    </Item>

    <Item>
        Type "ost_stats_write"
    </Item>
    <Item>
        Type "ost_stats_read"
    </Item>
    <Item>
        Type "ost_stats_statfs"
    </Item>

    <Item>
        Type "ost_jobstats"
#        <Rule>
#            Field "job_id"
#            Match "[[:digit:]]+"
#        </Rule>
    </Item>
#   <ItemType>
#       Type "ost_jobstats"
#       <ExtendedParse>
#           # Parse the field job_id
#           Field "job_id"
#           # Match the pattern
#           Pattern "u([[:digit:]]+)[.]g([[:digit:]]+)[.]j([[:digit:]]+)"
#           <ExtendedField>
#               Index 1
#               Name slurm_job_uid
#           </ExtendedField>
#           <ExtendedField>
#               Index 2
#               Name slurm_job_gid
#           </ExtendedField>
#           <ExtendedField>
#               Index 3
#               Name slurm_job_id
#           </ExtendedField>
#       </ExtendedParse>
#       TsdbTags "slurm_job_uid=${extendfield:slurm_job_uid} slurm_job_gid=${extendfield:slurm_job_gid} slurm_job_id=${extendfield:slurm_job_id}"
#   </ItemType>

    <Item>
        Type "ost_kbytestotal"
    </Item>
    <Item>
        Type "ost_kbytesfree"
    </Item>
    <Item>
        Type "ost_filestotal"
    </Item>
    <Item>
        Type "ost_filesfree"
    </Item>"""
            config += """

    # Items of ost_threads_* are not enabled

    # Items of ost_io_stats_* are not enabled because in order to get meaningful
    # value, need to, for example:
    # ost_io_stats_usec_sum / ost_io_stats_usec_samples

    # Items of ost_io_threads_* are not enabled

    # Item ost_ldlm_stats is not enabled, because min/max/sum/stddev is not so
    # useful for none-rate metrics.

    <Item>
        Type "ost_stats_req_waittime"
    </Item>
    <Item>
        Type "ost_stats_req_qdepth"
    </Item>
    <Item>
        Type "ost_stats_req_active"
    </Item>
    <Item>
        Type "ost_stats_req_timeout"
    </Item>
    <Item>
        Type "ost_stats_reqbuf_avail"
    </Item>

    <Item>
        Type "ost_io_stats_req_waittime"
    </Item>
    <Item>
        Type "ost_io_stats_req_qdepth"
    </Item>
    <Item>
        Type "ost_io_stats_req_active"
    </Item>
    <Item>
        Type "ost_io_stats_req_timeout"
    </Item>
    <Item>
        Type "ost_io_stats_reqbuf_avail"
    </Item>
    <Item>
        Type "ost_io_stats_ost_read"
    </Item>
    <Item>
        Type "ost_io_stats_ost_write"
    </Item>
    <Item>
        Type "ost_io_stats_ost_punch"
    </Item>

    <Item>
        Type "ost_create_stats_req_waittime"
    </Item>
    <Item>
        Type "ost_create_stats_req_qdepth"
    </Item>
    <Item>
        Type "ost_create_stats_req_active"
    </Item>
    <Item>
        Type "ost_create_stats_req_timeout"
    </Item>
    <Item>
        Type "ost_create_stats_reqbuf_avail"
    </Item>

    # Currently do not enable:
    # ost_seq_stats_[req_waittime|req_qdepth|req_active|req_timeout|reqbuf_avail]

    <Item>
        Type "ost_lock_count"
    </Item>
    <Item>
        Type "ost_lock_timeouts"
    </Item>

    # Currently do not enable:
    # ost_recovery_status_[recovery_start|recovery_duration|replayed_requests|
    # last_transno|time_remaining|req_replay_clients|lock_replay_clients|
    # queued_requests|next_transno]
    #
    # Whenever enabling completed_clients or connected_clients, need to enable
    # them both, because when recovery under different status (COMPLETE|RECOVERING),
    # /proc prints the same variables but with different leading words:
    #
    # When status is COMPLETE:
    #
    # completed_clients: $finished_clients/$recoverable_clients
    #
    # When status is RECOVERING:
    #
    # connected_clients: $finished_clients/$recoverable_clients
    #
    # evicted_clients will be printed only during RECOVERING, thus is a good sign
    # to show that recovery is in process.
    #
    <Item>
        Type "ost_recovery_status_completed_clients"
    </Item>
    <Item>
        Type "ost_recovery_status_connected_clients"
    </Item>
    <Item>
        Type "ost_recovery_status_evicted_clients"
    </Item>
"""
            if (self.cdc_jobstat_pattern ==
                    barrele_constant.BARRELE_JOBSTAT_PATTERN_PROCNAME_UID):
                config += """
    <ItemType>
        Type "ost_jobstats"
        <ExtendedParse>
            # Parse the field job_id when jobid is configured to "procname_uid".
            Field "job_id"
            # Match the pattern
            Pattern "(.+)[.]([[:digit:]]+)"
            <ExtendedField>
                Index 1
                Name procname
            </ExtendedField>
            <ExtendedField>
                Index 2
                Name uid
            </ExtendedField>
        </ExtendedParse>
        TsdbTags "procname=${extendfield:procname} uid=${extendfield:uid}"
    </ItemType>
"""
            elif (self.cdc_jobstat_pattern ==
                  barrele_constant.BARRELE_JOBSTAT_PATTERN_UID_GID):
                config += """
    <ItemType>
        Type "ost_jobstats"
        <ExtendedParse>
            # Parse the field job_id when jobid_name is configured as "u%u.g%g".
            Field "job_id"
            # Match the pattern
            Pattern "u([[:digit:]]+)[.]g([[:digit:]]+)"
            <ExtendedField>
                Index 1
                Name uid
            </ExtendedField>
            <ExtendedField>
                Index 2
                Name gid
            </ExtendedField>
        </ExtendedParse>
        TsdbTags "uid=${extendfield:uid} gid=${extendfield:gid}"
    </ItemType>
"""
            elif (self.cdc_jobstat_pattern !=
                  barrele_constant.BARRELE_JOBSTAT_PATTERN_UNKNOWN):
                log.cl_error("unknown jobstat pattern [%s] when configuring "
                             "job ID parser for OST",
                             self.cdc_jobstat_pattern)
                return -1
        if enable_lustre_exp_ost:
            config += """
    <Item>
        Type "exp_ost_stats_read"
    </Item>
    <Item>
        Type "exp_ost_stats_write"
    </Item>
    # The other exp_ost_stats_* items are not enabled here
"""
        if enable_lustre_client and support_lustre_client(version):
            config += """
    # Client stats
    <Item>
        Type "client_stats_read"
    </Item>
    <Item>
        Type "client_stats_write"
    </Item>
    <Item>
        Type "client_stats_read_bytes"
    </Item>
    <Item>
        Type "client_stats_write_bytes"
    </Item>
    <Item>
        Type "client_stats_ioctl"
    </Item>
    <Item>
        Type "client_stats_open"
    </Item>
    <Item>
        Type "client_stats_close"
    </Item>
    <Item>
        Type "client_stats_mmap"
    </Item>
    <Item>
        Type "client_stats_page_fault"
    </Item>
    <Item>
        Type "client_stats_page_mkwrite"
    </Item>
    <Item>
        Type "client_stats_seek"
    </Item>
    <Item>
        Type "client_stats_fsync"
    </Item>
    <Item>
        Type "client_stats_readdir"
    </Item>
    <Item>
        Type "client_stats_setattr"
    </Item>
    <Item>
        Type "client_stats_truncate"
    </Item>
    <Item>
        Type "client_stats_flock"
    </Item>
    <Item>
        Type "client_stats_getattr"
    </Item>
    <Item>
        Type "client_stats_fallocate"
    </Item>
    <Item>
        Type "client_stats_create"
    </Item>
    <Item>
        Type "client_stats_open"
    </Item>
    <Item>
        Type "client_stats_link"
    </Item>
    <Item>
        Type "client_stats_unlink"
    </Item>
    <Item>
        Type "client_stats_symlink"
    </Item>
    <Item>
        Type "client_stats_mkdir"
    </Item>
    <Item>
        Type "client_stats_rmdir"
    </Item>
    <Item>
        Type "client_stats_mknod"
    </Item>
    <Item>
        Type "client_stats_rename"
    </Item>
    <Item>
        Type "client_stats_statfs"
    </Item>
    <Item>
        Type "client_stats_setxattr"
    </Item>
    <Item>
        Type "client_stats_getxattr"
    </Item>
    <Item>
        Type "client_stats_getxattr_hits"
    </Item>
    <Item>
        Type "client_stats_listxattr"
    </Item>
    <Item>
        Type "client_stats_removexattr"
    </Item>
    <Item>
        Type "client_stats_inode_permission"
    </Item>
"""
        if enable_lustre_mds:
            config += """
    # MDT stats
    <Item>
        Type "mdt_acctuser"
    </Item>"""
            if enable_zfs:
                config += """
    <Item>
        Type "zfs_mdt_acctuser"
    </Item>"""
            if support_acctgroup_acctproject(version):
                config += """
    <Item>
        Type "mdt_acctgroup"
    </Item>
    <Item>
        Type "mdt_acctproject"
    </Item>"""
                if enable_zfs:
                    config += """
    <Item>
        Type "zfs_mdt_acctgroup"
    </Item>
    <Item>
        Type "zfs_mdt_acctproject"
    </Item>"""
            config += """
    <Item>
        Type "md_stats_open"
    </Item>
    <Item>
        Type "md_stats_close"
    </Item>
    <Item>
        Type "md_stats_mknod"
    </Item>
    <Item>
        Type "md_stats_unlink"
    </Item>
    <Item>
        Type "md_stats_mkdir"
    </Item>
    <Item>
        Type "md_stats_rmdir"
    </Item>
    <Item>
        Type "md_stats_rename"
    </Item>
    <Item>
        Type "md_stats_getattr"
    </Item>
    <Item>
        Type "md_stats_setattr"
    </Item>
    <Item>
        Type "md_stats_getxattr"
    </Item>
    <Item>
        Type "md_stats_setxattr"
    </Item>
    <Item>
        Type "md_stats_statfs"
    </Item>
    <Item>
        Type "md_stats_sync"
    </Item>

    <Item>
        Type "mdt_jobstats"
#       <Rule>
#           Field "job_id"
#           Match "[[:digit:]]+"
#       </Rule>
    </Item>
#   <ItemType>
#       Type "mdt_jobstats"
#       <ExtendedParse>
#           # Parse the field job_id
#           Field "job_id"
#           # Match the pattern
#           Pattern "u([[:digit:]]+)[.]g([[:digit:]]+)[.]j([[:digit:]]+)"
#           <ExtendedField>
#               Index 1
#               Name slurm_job_uid
#           </ExtendedField>
#           <ExtendedField>
#               Index 2
#               Name slurm_job_gid
#           </ExtendedField>
#           <ExtendedField>
#               Index 3
#               Name slurm_job_id
#           </ExtendedField>
#       </ExtendedParse>
#       TsdbTags "slurm_job_uid=${extendfield:slurm_job_uid} slurm_job_gid=${extendfield:slurm_job_gid} slurm_job_id=${extendfield:slurm_job_id}"
#   </ItemType>

    <Item>
        Type "mdt_filestotal"
    </Item>
    <Item>
        Type "mdt_filesfree"
    </Item>"""

            if (self.cdc_jobstat_pattern ==
                    barrele_constant.BARRELE_JOBSTAT_PATTERN_PROCNAME_UID):
                config += """
    <ItemType>
        Type "mdt_jobstats"
        <ExtendedParse>
            # Parse the field job_id when jobid is configured to "procname_uid".
            Field "job_id"
            # Match the pattern
            Pattern "(.+)[.]([[:digit:]]+)"
            <ExtendedField>
                Index 1
                Name procname
            </ExtendedField>
            <ExtendedField>
                Index 2
                Name uid
            </ExtendedField>
        </ExtendedParse>
        TsdbTags "procname=${extendfield:procname} uid=${extendfield:uid}"
    </ItemType>
"""
            elif (self.cdc_jobstat_pattern ==
                  barrele_constant.BARRELE_JOBSTAT_PATTERN_UID_GID):
                config += """
    <ItemType>
        Type "mdt_jobstats"
        <ExtendedParse>
            # Parse the field job_id when jobid_name is configured as "u%u.g%g".
            Field "job_id"
            # Match the pattern
            Pattern "u([[:digit:]]+)[.]g([[:digit:]]+)"
            <ExtendedField>
                Index 1
                Name uid
            </ExtendedField>
            <ExtendedField>
                Index 2
                Name gid
            </ExtendedField>
        </ExtendedParse>
        TsdbTags "uid=${extendfield:uid} gid=${extendfield:gid}"
    </ItemType>
"""
            elif (self.cdc_jobstat_pattern !=
                  barrele_constant.BARRELE_JOBSTAT_PATTERN_UNKNOWN):
                log.cl_error("unknown jobstat pattern [%s] when configuring "
                             "job ID parser for MDT",
                             self.cdc_jobstat_pattern)
                return -1

            config += """
    <Item>
        Type "mdt_stats_req_waittime"
    </Item>
    <Item>
        Type "mdt_stats_req_qdepth"
    </Item>
    <Item>
        Type "mdt_stats_req_active"
    </Item>
    <Item>
        Type "mdt_stats_req_timeout"
    </Item>
    <Item>
        Type "mdt_stats_reqbuf_avail"
    </Item>
    <Item>
        Type "mdt_stats_ldlm_ibits_enqueue"
    </Item>
    <Item>
        Type "mdt_stats_mds_getattr"
    </Item>
    <Item>
        Type "mdt_stats_mds_connect"
    </Item>
    <Item>
        Type "mdt_stats_mds_get_root"
    </Item>
    <Item>
        Type "mdt_stats_mds_statfs"
    </Item>
    <Item>
        Type "mdt_stats_mds_getxattr"
    </Item>
    <Item>
        Type "mdt_stats_obd_ping"
    </Item>

    <Item>
        Type "mdt_readpage_stats_req_waittime"
    </Item>
    <Item>
        Type "mdt_readpage_stats_req_qdepth"
    </Item>
    <Item>
        Type "mdt_readpage_stats_req_active"
    </Item>
    <Item>
        Type "mdt_readpage_stats_req_timeout"
    </Item>
    <Item>
        Type "mdt_readpage_stats_reqbuf_avail"
    </Item>
    <Item>
        Type "mdt_readpage_stats_mds_close"
    </Item>
    <Item>
        Type "mdt_readpage_stats_mds_readpage"
    </Item>

    # Currently do not enable:
    # mdt_setattr_stats_[req_waittime|req_qdepth|req_active|req_timeout|
    # reqbuf_avail], because Lustre doesn't use it yet.

    <Item>
        Type "mdt_lock_count"
    </Item>
    <Item>
        Type "mdt_lock_timeouts"
    </Item>

    # Currently do not enable:
    # mdt_recovery_status_[recovery_start|recovery_duration|replayed_requests|
    # last_transno|time_remaining|req_replay_clients|lock_replay_clients|
    # queued_requests|next_transno]
    #
    # Whenever enabling completed_clients or connected_clients, need to enable
    # them both, because when recovery under different status (COMPLETE|RECOVERING),
    # /proc prints the same variables but with different leading words:
    #
    # When status is COMPLETE:
    #
    # completed_clients: $finished_clients/$recoverable_clients
    #
    # When status is RECOVERING:
    #
    # connected_clients: $finished_clients/$recoverable_clients
    #
    # evicted_clients will be printed only during RECOVERING, thus is a good sign
    # to show that recovery is in process.
    #
    <Item>
        Type "mdt_recovery_status_completed_clients"
    </Item>
    <Item>
        Type "mdt_recovery_status_connected_clients"
    </Item>
    <Item>
        Type "mdt_recovery_status_evicted_clients"
    </Item>
"""

        if enable_lustre_exp_mdt:
            config += """
    <Item>
        Type "exp_md_stats_open"
    </Item>
    <Item>
        Type "exp_md_stats_close"
    </Item>
    <Item>
        Type "exp_md_stats_mknod"
    </Item>
    <Item>
        Type "exp_md_stats_link"
    </Item>
    <Item>
        Type "exp_md_stats_unlink"
    </Item>
    <Item>
        Type "exp_md_stats_mkdir"
    </Item>
    <Item>
        Type "exp_md_stats_rmdir"
    </Item>
    <Item>
        Type "exp_md_stats_rename"
    </Item>
    <Item>
        Type "exp_md_stats_getattr"
    </Item>
    <Item>
        Type "exp_md_stats_setattr"
    </Item>
    <Item>
        Type "exp_md_stats_getxattr"
    </Item>
    <Item>
        Type "exp_md_stats_setxattr"
    </Item>
    <Item>
        Type "exp_md_stats_statfs"
    </Item>
    <Item>
        Type "exp_md_stats_sync"
    </Item>
"""

        # Client support, e.g. max_rpcs_in_flight of mdc could be added
        config += "</Plugin>\n\n"
        self.cdc_filedatas["lustre"] = config
        barreleye_agent = self.cdc_barreleye_agent
        rpm_name = "collectd-filedata"
        if rpm_name not in barreleye_agent.bea_needed_collectd_rpm_types:
            barreleye_agent.bea_needed_collectd_rpm_types.append(rpm_name)
        return 0

    def cdc_plugin_df_check(self, log):
        """
        Check the df plugin
        """
        barreleye_agent = self.cdc_barreleye_agent
        measurement = "df.root.df_complex.free"
        return barreleye_agent.bea_influxdb_measurement_check(log, measurement)

    def cdc_plugin_df(self):
        """
        Config the df plugin on /
        """
        self.cdc_plugins["df"] = """<Plugin "df">
    MountPoint "/"
</Plugin>

"""
        if self.cdc_plugin_df_check not in self.cdc_checks:
            self.cdc_checks.append(self.cdc_plugin_df_check)
        return 0

    def cdc_plugin_load_check(self, log):
        """
        Check the load plugin
        """
        barreleye_agent = self.cdc_barreleye_agent
        measurement = "load.load.shortterm"
        return barreleye_agent.bea_influxdb_measurement_check(log, measurement)

    def cdc_plugin_load(self):
        """
        Config the load plugin
        """
        self.cdc_plugins["load"] = ""
        if self.cdc_plugin_load_check not in self.cdc_checks:
            self.cdc_checks.append(self.cdc_plugin_load_check)
        return 0

    def cdc_plugin_sensors_check(self, log):
        """
        Check the sensors plugin
        """
        barreleye_agent = self.cdc_barreleye_agent
        host = barreleye_agent.bea_host
        measurement = "aggregation.sensors-max.temperature"

        command = "sensors | grep temp"
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_debug("failed to run command [%s] on host [%s], there "
                         "might be no sensor, skip checking measurement "
                         "[%s]",
                         command,
                         host.sh_hostname,
                         measurement)
            return 0
        return barreleye_agent.bea_influxdb_measurement_check(log, measurement)

    def cdc_plugin_sensors(self):
        """
        Config the sensors plugin
        """
        self.cdc_aggregations["sensors"] = """    <Aggregation>
        Plugin "sensors"
        Type "temperature"
        GroupBy "Host"
        CalculateMaximum true
    </Aggregation>
"""
        self.cdc_post_cache_chain_rules["sensors"] = """    <Rule>
        <Match regex>
            Plugin "^sensors$"
            Type "^temperature$"
        </Match>
        <Target write>
            Plugin "aggregation"
        </Target>
    </Rule>
"""
        self.cdc_plugins["sensors"] = ""
        if self.cdc_plugin_sensors_check not in self.cdc_checks:
            self.cdc_checks.append(self.cdc_plugin_sensors_check)

        barreleye_agent = self.cdc_barreleye_agent
        rpm_name = "collectd-sensors"
        if rpm_name not in barreleye_agent.bea_needed_collectd_rpm_types:
            barreleye_agent.bea_needed_collectd_rpm_types.append(rpm_name)
        return 0

    def cdc_plugin_disk(self):
        """
        Config the disk plugin
        """
        self.cdc_plugins["disk"] = ""

        barreleye_agent = self.cdc_barreleye_agent
        rpm_name = "collectd-disk"
        if rpm_name not in barreleye_agent.bea_needed_collectd_rpm_types:
            barreleye_agent.bea_needed_collectd_rpm_types.append(rpm_name)
        return 0

    def cdc_plugin_uptime_check(self, log):
        """
        Check the uptime plugin
        """
        barreleye_agent = self.cdc_barreleye_agent
        measurement = "uptime.uptime"
        return barreleye_agent.bea_influxdb_measurement_check(log, measurement)

    def cdc_plugin_uptime(self):
        """
        Config the uptime plugin
        """
        self.cdc_plugins["uptime"] = ""
        if self.cdc_plugin_uptime_check not in self.cdc_checks:
            self.cdc_checks.append(self.cdc_plugin_uptime_check)
        return 0

    def cdc_plugin_users_check(self, log):
        """
        Check the users plugin
        """
        barreleye_agent = self.cdc_barreleye_agent
        measurement = "users.users"
        return barreleye_agent.bea_influxdb_measurement_check(log, measurement)

    def cdc_plugin_users(self):
        """
        Config the users plugin
        """
        self.cdc_plugins["users"] = ""
        if self.cdc_plugin_users_check not in self.cdc_checks:
            self.cdc_checks.append(self.cdc_plugin_users_check)
        return 0

    def cdc_plugin_infiniband(self):
        """
        Add IB configuration
        """
        config = """<Plugin "filedata">
    <Common>
        DefinitionFile "/etc/infiniband-0.1_definition.xml"
    </Common>
    <Item>
        Type "excessive_buffer_overrun_errors"
    </Item>
    <Item>
        Type "link_downed"
    </Item>
    <Item>
        Type "link_error_recovery"
    </Item>
    <Item>
        Type "local_link_integrity_errors"
    </Item>
    <Item>
        Type "port_rcv_constraint_errors"
    </Item>
    <Item>
        Type "port_rcv_data"
    </Item>
    <Item>
        Type "port_rcv_errors"
    </Item>
    <Item>
        Type "port_rcv_packets"
    </Item>
    <Item>
        Type "port_rcv_remote_physical_errors"
    </Item>
    <Item>
        Type "port_xmit_constraint_errors"
    </Item>
    <Item>
        Type "port_xmit_data"
    </Item>
    <Item>
        Type "port_xmit_discards"
    </Item>
    <Item>
        Type "port_xmit_packets"
    </Item>
    <Item>
        Type "symbol_error"
    </Item>
    <Item>
        Type "VL15_dropped"
    </Item>
    <Item>
        Type "port_rcv_switch_relay_errors"
    </Item>
</Plugin>

"""
        self.cdc_filedatas["infiniband"] = config
        barreleye_agent = self.cdc_barreleye_agent
        rpm_name = "collectd-filedata"
        if rpm_name not in barreleye_agent.bea_needed_collectd_rpm_types:
            barreleye_agent.bea_needed_collectd_rpm_types.append(rpm_name)


def collectd_package_type_from_name(log, name):
    """
    Return Collectd type from full RPM/deb name or RPM/deb fname
    The Collectd RPM/deb types. The RPM type is the minimum string
    that yum could understand and find the RPM.
    For example:
    libcollectdclient-5.11.0...rpm has a type of libcollectdclient;
    collectd-5.11.0...rpm has a type of collectd;
    collectd-disk-5.11.0...rpm has a type of collectd-disk.

    Example debs:
    collectd_5.12.0.brl3_amd64.deb
    collectd-core_5.12.0.brl3_amd64.deb
    collectd-dev_5.12.0.brl3_all.deb
    collectd-utils_5.12.0.brl3_amd64.deb
    libcollectdclient1_5.12.0.brl3_amd64.deb
    libcollectdclient-dev_5.12.0.brl3_amd64.deb
    """
    if ((not name.startswith("collectd")) and
            (not name.startswith("libcollectdclient"))):
        return None
    collectd_pattern = (r"^(?P<type>\S+)[-_](\d+)\.(\d+).+")
    collectd_regular = re.compile(collectd_pattern)
    match = collectd_regular.match(name)
    if match is None:
        log.cl_error("name [%s] starts with [collectd] but does not match "
                     "the package pattern", name)
        return None
    return match.group("type")


def get_collectd_package_type_dict(log, host, packages_dir):
    """
    Return a dict. Key is the RPM/deb type, value is the file name.

    The RPM type is the minimum string that yum could understand and
    find the RPM.

    For example:
    libcollectdclient-5.11.0...rpm has a type of libcollectdclient;
    collectd-5.11.0...rpm has a type of collectd;
    collectd-disk-5.11.0...rpm has a type of collectd-disk.
    """
    fnames = host.sh_get_dir_fnames(log, packages_dir)
    if fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on "
                     "host [%s]", packages_dir, host.sh_hostname)
        return None
    collectd_package_type_dict = {}
    for fname in fnames:
        if ((not fname.startswith("collectd")) and
            (not fname.startswith("libcollectdclient"))):
            continue
        package_type = collectd_package_type_from_name(log, fname)
        if package_type is None:
            log.cl_error("failed to get the package type from name [%s]",
                         fname)
            return None
        if package_type in collectd_package_type_dict:
            log.cl_error("both Collectd packages [%s] and [%s] matches "
                         "type [%s]", fname,
                         collectd_package_type_dict[package_type],
                         package_type)
            return None

        collectd_package_type_dict[package_type] = fname
        log.cl_debug("Collectd package [%s] is found under dir [%s] on local "
                     "host [%s]", package_type, packages_dir,
                     host.sh_hostname)
    return collectd_package_type_dict


def collectd_debs_install(log, host, packages_dir):
    """
    Install all the Collectd debs under the package dir
    """
    # Remove the existing collectd configuration to avoid failure of starting
    # collectd service when installing Collectd deb file.
    command = ("rm -f /etc/collectd/collectd.conf")
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

    command = ("dpkg -i %s/collectd*.deb %s/libcollectdclient*.deb" %
               (packages_dir, packages_dir))
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
