include(`lustre.m4')dnl
HEAD(Lustre-es6_2)
<definition>
	<version>es6_2</version>
	CLIENT_STATS_MEAN(1, read, usecs)
	CLIENT_STATS_MEAN(1, write, usecs)
	CLIENT_STATS_MEAN(1, open, usecs)
	CLIENT_STATS_MEAN(1, close, usecs)
	CLIENT_STATS_MEAN(1, mmap, usecs)
	CLIENT_STATS_MEAN(1, page_fault, usecs)
	CLIENT_STATS_MEAN(1, page_mkwrite, usecs)
	CLIENT_STATS_MEAN(1, seek, usecs)
	CLIENT_STATS_MEAN(1, fsync, usecs)
	CLIENT_STATS_MEAN(1, readdir, usecs)
	CLIENT_STATS_MEAN(1, setattr, usecs)
	CLIENT_STATS_MEAN(1, truncate, usecs)
	CLIENT_STATS_MEAN(1, flock, usecs)
	CLIENT_STATS_MEAN(1, getattr, usecs)
	CLIENT_STATS_MEAN(1, fallocate, usecs)
	CLIENT_STATS_MEAN(1, create, usecs)
	CLIENT_STATS_MEAN(1, link, usecs)
	CLIENT_STATS_MEAN(1, unlink, usecs)
	CLIENT_STATS_MEAN(1, symlink, usecs)
	CLIENT_STATS_MEAN(1, mkdir, usecs)
	CLIENT_STATS_MEAN(1, rmdir, usecs)
	CLIENT_STATS_MEAN(1, mknod, usecs)
	CLIENT_STATS_MEAN(1, rename, usecs)
	CLIENT_STATS_MEAN(1, statfs, usecs)
	CLIENT_STATS_MEAN(1, setxattr, usecs)
	CLIENT_STATS_MEAN(1, getxattr, usecs)
	CLIENT_STATS_MEAN(1, listxattr, usecs)
	CLIENT_STATS_MEAN(1, removexattr, usecs)
	CLIENT_STATS_MEAN(1, inode_permission, usecs)
	MATH_ENTRY(1, mdt_filesinfo_total, -, mdt_filesinfo_free, mdt_filesinfo_used, filesused, 1)
	MATH_ENTRY(1, mdt_kbytesinfo_total, -, mdt_kbytesinfo_free, mdt_kbytesinfo_used, kbytesused, 1)
	MATH_ENTRY(1, ost_filesinfo_total, -, ost_filesinfo_free, ost_filesinfo_used, filesused, 1)
	MATH_ENTRY(1, ost_kbytesinfo_total, -, ost_kbytesinfo_free, ost_kbytesinfo_used, kbytesused, 1)
	SERVICE_STATS_MEAN(1, mdt, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, mdt, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, mdt, req_active, reqs)
	SERVICE_STATS_MEAN(1, mdt, req_timeout, sec)
	SERVICE_STATS_MEAN(1, mdt, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, mdt, ldlm_ibits_enqueue, reqs)
	SERVICE_STATS_MEAN(1, mdt, mds_getattr, usecs)
	SERVICE_STATS_MEAN(1, mdt, mds_connect, usecs)
	SERVICE_STATS_MEAN(1, mdt, mds_get_root, usecs)
	SERVICE_STATS_MEAN(1, mdt, mds_statfs, usecs)
	SERVICE_STATS_MEAN(1, mdt, mds_getxattr, usecs)
	SERVICE_STATS_MEAN(1, mdt, obd_ping, usecs)
	SERVICE_STATS_MEAN(1, mdt_readpage, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, mdt_readpage, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, mdt_readpage, req_active, reqs)
	SERVICE_STATS_MEAN(1, mdt_readpage, req_timeout, sec)
	SERVICE_STATS_MEAN(1, mdt_readpage, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, mdt_readpage, mds_close, usecs)
	SERVICE_STATS_MEAN(1, mdt_readpage, mds_readpage, usecs)
	SERVICE_STATS_MEAN(1, mdt_setattr, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, mdt_setattr, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, mdt_setattr, req_active, reqs)
	SERVICE_STATS_MEAN(1, mdt_setattr, req_timeout, sec)
	SERVICE_STATS_MEAN(1, mdt_setattr, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, mdt_fld, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, mdt_fld, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, mdt_fld, req_active, reqs)
	SERVICE_STATS_MEAN(1, mdt_fld, req_timeout, sec)
	SERVICE_STATS_MEAN(1, mdt_fld, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, mdt_out, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, mdt_out, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, mdt_out, req_active, reqs)
	SERVICE_STATS_MEAN(1, mdt_out, req_timeout, sec)
	SERVICE_STATS_MEAN(1, mdt_out, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, mdt_seqm, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, mdt_seqm, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, mdt_seqm, req_active, reqs)
	SERVICE_STATS_MEAN(1, mdt_seqm, req_timeout, sec)
	SERVICE_STATS_MEAN(1, mdt_seqm, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, mdt_seqs, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, mdt_seqs, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, mdt_seqs, req_active, reqs)
	SERVICE_STATS_MEAN(1, mdt_seqs, req_timeout, sec)
	SERVICE_STATS_MEAN(1, mdt_seqs, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, ost, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, ost, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, ost, req_active, reqs)
	SERVICE_STATS_MEAN(1, ost, req_timeout, sec)
	SERVICE_STATS_MEAN(1, ost, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, ost_io, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, ost_io, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, ost_io, req_active, reqs)
	SERVICE_STATS_MEAN(1, ost_io, req_timeout, sec)
	SERVICE_STATS_MEAN(1, ost_io, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, ost_io, ost_read, usecs)
	SERVICE_STATS_MEAN(1, ost_io, ost_write, usecs)
	SERVICE_STATS_MEAN(1, ost_io, ost_punch, usecs)
	SERVICE_STATS_MEAN(1, ost_create, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, ost_create, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, ost_create, req_active, reqs)
	SERVICE_STATS_MEAN(1, ost_create, req_timeout, sec)
	SERVICE_STATS_MEAN(1, ost_create, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, ost_seq, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, ost_seq, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, ost_seq, req_active, reqs)
	SERVICE_STATS_MEAN(1, ost_seq, req_timeout, sec)
	SERVICE_STATS_MEAN(1, ost_seq, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, ldlm_canceld, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, ldlm_canceld, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, ldlm_canceld, req_active, reqs)
	SERVICE_STATS_MEAN(1, ldlm_canceld, req_timeout, sec)
	SERVICE_STATS_MEAN(1, ldlm_canceld, reqbuf_avail, bufs)
	SERVICE_STATS_MEAN(1, ldlm_cbd, req_waittime, usecs)
	SERVICE_STATS_MEAN(1, ldlm_cbd, req_qdepth, reqs)
	SERVICE_STATS_MEAN(1, ldlm_cbd, req_active, reqs)
	SERVICE_STATS_MEAN(1, ldlm_cbd, req_timeout, sec)
	SERVICE_STATS_MEAN(1, ldlm_cbd, reqbuf_avail, bufs)
	<entry>
		<subpath>
			<subpath_type>constant</subpath_type>
			<path>/proc/fs/lustre</path>
		</subpath>
		<mode>directory</mode>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>osd-ldiskfs</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(MDT[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>mdt_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>quota_slave</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						SUBPATH(6, constant, acct_user, 1)
						MODE(6, file, 1)
						<item>
							<name>mdt_acctuser</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							MDT_ACCTUSER_FIELD(7, 1, id, string, gauge, 1)
							MDT_ACCTUSER_FIELD(7, 2, usage_inodes, number, gauge, 1)
							MDT_ACCTUSER_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_group, 1)
						MODE(6, file, 1)
						<item>
							<name>mdt_acctgroup</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							MDT_ACCTGROUP_FIELD(7, 1, id, string, gauge, 1)
							MDT_ACCTGROUP_FIELD(7, 2, usage_inodes, number, gauge, 1)
							MDT_ACCTGROUP_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_project, 1)
						MODE(6, file, 1)
						<item>
							<name>mdt_acctproject</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							MDT_ACCTPROJECT_FIELD(7, 1, id, string, gauge, 1)
							MDT_ACCTPROJECT_FIELD(7, 2, usage_inodes, number, gauge, 1)
							MDT_ACCTPROJECT_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
				</entry>
			</entry>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(OST[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>ost_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>brw_stats</path>
					</subpath>
					<mode>file</mode>
					OST_BRW_STATS_ITEM(5, rpc_bulk, ^pages per bulk .+
(.+
)*$, [[:digit:]]+[KM]?, pages, 1)
					OST_BRW_STATS_ITEM(5, page_discontiguous_rpc, ^discontiguous pages .+
(.+
)*$, [[:digit:]]+[KM]?, pages, 1)
					OST_BRW_STATS_ITEM(5, block_discontiguous_rpc, ^discontiguous blocks .+
(.+
)*$, [[:digit:]]+[KM]?, blocks, 1)
					OST_BRW_STATS_ITEM(5, fragmented_io, ^disk fragmented .+
(.+
)*$, [[:digit:]]+[KM]?, fragments, 1)
					OST_BRW_STATS_ITEM(5, io_in_flight, ^disk I/Os .+
(.+
)*$, [[:digit:]]+[KM]?, ios, 1)
					OST_BRW_STATS_ITEM(5, io_time, ^I/O time .+
(.+
)*$, [[:digit:]]+[KM]?, milliseconds, 1)
					OST_BRW_STATS_ITEM(5, io_size, ^disk I/O size .+
(.+
)*$, [[:digit:]]+[KM]?, Bytes, 1)
				</entry>
				<entry>
					SUBPATH(5, constant, quota_slave, 1)
					MODE(5, directory, 1)
					<entry>
						SUBPATH(6, constant, acct_user, 1)
						MODE(6, file, 1)
						<item>
							<name>ost_acctuser</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							OST_ACCTUSER_FIELD(7, 1, id, string, gauge, 1)
							OST_ACCTUSER_FIELD(7, 2, usage_inodes, number, gauge, 1)
							OST_ACCTUSER_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_group, 1)
						MODE(6, file, 1)
						<item>
							<name>ost_acctgroup</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							OST_ACCTGROUP_FIELD(7, 1, id, string, gauge, 1)
							OST_ACCTGROUP_FIELD(7, 2, usage_inodes, number, gauge, 1)
							OST_ACCTGROUP_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_project, 1)
						MODE(6, file, 1)
						<item>
							<name>ost_acctproject</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							OST_ACCTPROJECT_FIELD(7, 1, id, string, gauge, 1)
							OST_ACCTPROJECT_FIELD(7, 2, usage_inodes, number, gauge, 1)
							OST_ACCTPROJECT_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
				</entry>
			</entry>
		</entry>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>osd-zfs</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(MDT[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>mdt_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>quota_slave</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						SUBPATH(6, constant, acct_user, 1)
						MODE(6, file, 1)
						<item>
							<name>zfs_mdt_acctuser</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							MDT_ACCTUSER_FIELD(7, 1, id, string, gauge, 1)
							MDT_ACCTUSER_FIELD(7, 2, usage_inodes, number, gauge, 1)
							MDT_ACCTUSER_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_group, 1)
						MODE(6, file, 1)
						<item>
							<name>zfs_mdt_acctgroup</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							MDT_ACCTGROUP_FIELD(7, 1, id, string, gauge, 1)
							MDT_ACCTGROUP_FIELD(7, 2, usage_inodes, number, gauge, 1)
							MDT_ACCTGROUP_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_project, 1)
						MODE(6, file, 1)
						<item>
							<name>zfs_mdt_acctproject</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							MDT_ACCTPROJECT_FIELD(7, 1, id, string, gauge, 1)
							MDT_ACCTPROJECT_FIELD(7, 2, usage_inodes, number, gauge, 1)
							MDT_ACCTPROJECT_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
				</entry>
			</entry>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(OST[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>ost_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				<entry>
					SUBPATH(5, constant, quota_slave, 1)
					MODE(5, directory, 1)
					<entry>
						SUBPATH(6, constant, acct_user, 1)
						MODE(6, file, 1)
						<item>
							<name>zfs_ost_acctuser</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							OST_ACCTUSER_FIELD(7, 1, id, string, gauge, 1)
							OST_ACCTUSER_FIELD(7, 2, usage_inodes, number, gauge, 1)
							OST_ACCTUSER_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_group, 1)
						MODE(6, file, 1)
						<item>
							<name>zfs_ost_acctgroup</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							OST_ACCTGROUP_FIELD(7, 1, id, string, gauge, 1)
							OST_ACCTGROUP_FIELD(7, 2, usage_inodes, number, gauge, 1)
							OST_ACCTGROUP_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
					<entry>
						SUBPATH(6, constant, acct_project, 1)
						MODE(6, file, 1)
						<item>
							<name>zfs_ost_acctproject</name>
							<pattern>- +id: +(.+)
  usage: +\{ inodes: +([[:digit:]]+), kbytes: +([[:digit:]]+).+</pattern>
							OST_ACCTPROJECT_FIELD(7, 1, id, string, gauge, 1)
							OST_ACCTPROJECT_FIELD(7, 2, usage_inodes, number, gauge, 1)
							OST_ACCTPROJECT_FIELD(7, 3, usage_kbytes, number, gauge, 1)
						</item>
					</entry>
				</entry>
			</entry>
		</entry>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>mdt</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(MDT[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>mdt_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>recovery_status</path>
					</subpath>
					<mode>file</mode>
					RECOVERY_STATUS_ITEM(5, recovery_start, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, recovery_duration, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_CONNECTED_ITEM(5, completed_clients, mdt, 1)
					RECOVERY_STATUS_ITEM(5, replayed_requests, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, last_transno, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, time_remaining, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_CONNECTED_ITEM(5, connected_clients, mdt, 1)
					RECOVERY_STATUS_ITEM(5, req_replay_clients, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, lock_replay_clients, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, evicted_clients, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, queued_requests, mdt, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, next_transno, mdt, ([[:digit:]]+), number, 1)
				</entry>
				<entry>
					<!-- mds_stats_counter_init() -->
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>md_stats</path>
					</subpath>
					<mode>file</mode>
					MD_STATS_ITEM_V2(5, open, 1)
					MD_STATS_ITEM_V2(5, close, 1)
					MD_STATS_ITEM_V2(5, mknod, 1)
					MD_STATS_ITEM_V2(5, link, 1)
					MD_STATS_ITEM_V2(5, unlink, 1)
					MD_STATS_ITEM_V2(5, mkdir, 1)
					MD_STATS_ITEM_V2(5, rmdir, 1)
					MD_STATS_ITEM_V2(5, rename, 1)
					MD_STATS_ITEM_V2(5, getattr, 1)
					MD_STATS_ITEM_V2(5, setattr, 1)
					MD_STATS_ITEM_V2(5, getxattr, 1)
					MD_STATS_ITEM_V2(5, setxattr, 1)
					MD_STATS_ITEM_V2(5, statfs, 1)
					MD_STATS_ITEM_V2(5, sync, 1)
				</entry>
				<entry>
					SUBPATH(5, constant, exports, 1)
					MODE(5, directory, 1)
					<entry>
						TWO_FIELD_SUBPATH(6, regular_expression, (.+)@(.+), mdt_exp_client, mdt_exp_type, 1)
						MODE(6, directory, 1)
						EXPORT_MD_STATS_ENTRY_V2(6, , 1)
					</entry>
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>job_stats</path>
					</subpath>
					<mode>file</mode>
					<item>
						<name>mdt_jobstats</name>
						<pattern>- +job_id: +(.+)
 +snapshot_time: +.+
.*\n?.*\n?  open: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  close: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  mknod: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  link: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  unlink: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  mkdir: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  rmdir: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  rename: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  getattr: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  setattr: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  getxattr: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  setxattr: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  statfs: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  sync: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  samedir_rename: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  parallel_rename_file: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  parallel_rename_dir: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  crossdir_rename: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  read: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  write: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  read_bytes: +\{ samples: +([[:digit:]]+), unit: bytes, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  write_bytes: +\{ samples: +([[:digit:]]+), unit: bytes, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  punch: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  migrate: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }</pattern>
						JOBSTAT_FIELD(6, 1, job_id, string, derive, mdt, jobid, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 2, open, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 7, close, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 12, mknod, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 17, link, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 22, unlink, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 27, mkdir, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 32, rmdir, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 37, rename, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 42, getattr, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 47, setattr, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 52, getxattr, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 57, setxattr, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 62, statfs, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 67, sync, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 72, samedir_rename, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 77, parallel_rename_file, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 82, parallel_rename_dir, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 87, crossdir_rename, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 92, read, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 97, write, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 102, crossdir_rename, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 107, read_bytes, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 112, punch, number, derive, mdt, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 117, migrate, number, derive, mdt, 1)
					</item>
				</entry>
			</entry>
		</entry>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>obdfilter</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(OST[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>ost_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>recovery_status</path>
					</subpath>
					<mode>file</mode>
					RECOVERY_STATUS_ITEM(5, recovery_start, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, recovery_duration, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_CONNECTED_ITEM(5, completed_clients, ost, 1)
					RECOVERY_STATUS_ITEM(5, replayed_requests, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, last_transno, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, time_remaining, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_CONNECTED_ITEM(5, connected_clients, ost, 1)
					RECOVERY_STATUS_ITEM(5, req_replay_clients, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, lock_replay_clients, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, evicted_clients, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, queued_requests, ost, ([[:digit:]]+), number, 1)
					RECOVERY_STATUS_ITEM(5, next_transno, ost, ([[:digit:]]+), number, 1)
				</entry>
				<entry>
					<!-- filter_setup().
					     There are a lot of counter, only defined part of them here
					-->
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>stats</path>
					</subpath>
					<mode>file</mode>
					OST_STATS_ITEM_RW(5, read, 1)
					OST_STATS_ITEM_RW(5, write, 1)
					OST_STATS_ITEM_V2(5, getattr, usecs, 1)
					OST_STATS_ITEM_V2(5, setattr, usecs, 1)
					OST_STATS_ITEM_V2(5, punch, usecs, 1)
					OST_STATS_ITEM_V2(5, sync, usecs, 1)
					OST_STATS_ITEM_V2(5, destroy, usecs, 1)
					OST_STATS_ITEM_V2(5, create, usecs, 1)
					OST_STATS_ITEM_V2(5, statfs, usecs, 1)
					OST_STATS_ITEM_V2(5, get_info, usecs, 1)
					OST_STATS_ITEM_V2(5, set_info_async, usecs, 1)
					OST_STATS_ITEM_V2(5, quotactl, usecs, 1)
				</entry>
				<entry>
					SUBPATH(5, constant, exports, 1)
					MODE(5, directory, 1)
					<entry>
						TWO_FIELD_SUBPATH(6, regular_expression, (.+)@(.+), ost_exp_client, ost_exp_type, 1)
						MODE(6, directory, 1)
						EXPORT_OST_STATS_ENTRY_V2(6, , 1)
					</entry>
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>job_stats</path>
					</subpath>
					<mode>file</mode>
					<item>
						<name>ost_jobstats</name>
						<pattern>- +job_id: +(.+)
 +snapshot_time: +.+
.*\n?.*\n?  read_bytes: +\{ samples: +([[:digit:]]+), unit: bytes, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+).+ }
  write_bytes: +\{ samples: +([[:digit:]]+), unit: bytes, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+).+ }
  read: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  write: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  getattr: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  setattr: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  punch: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  sync: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  destroy: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  create: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  statfs: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  get_info: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  set_info: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  quotactl: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }
  prealloc: +\{ samples: +([[:digit:]]+), unit: usecs, min: *([[:digit:]]+), max: *([[:digit:]]+), sum: *([[:digit:]]+), sumsq: *([[:digit:]]+) }</pattern>
						JOBSTAT_FIELD(6, 1, job_id, string, derive, ost, jobid, 1)
						OST_JOBSTAT_FIELD(6, 2, read_samples, number, derive, 1)
						OST_JOBSTAT_FIELD_BYTES(6, 3, min_read_bytes, number, derive, 1)
						OST_JOBSTAT_FIELD_BYTES(6, 4, max_read_bytes, number, derive, 1)
						OST_JOBSTAT_FIELD_BYTES(6, 5, sum_read_bytes, number, derive, 1)
						OST_JOBSTAT_FIELD(6, 6, write_samples, number, derive, 1)
						OST_JOBSTAT_FIELD_BYTES(6, 7, min_write_bytes, number, derive, 1)
						OST_JOBSTAT_FIELD_BYTES(6, 8, max_write_bytes, number, derive, 1)
						OST_JOBSTAT_FIELD_BYTES(6, 9, sum_write_bytes, number, derive, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 10, read, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 15, write, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 20, getattr, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 25, setattr, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 30, punch, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 35, sync, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 40, destroy, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 45, create, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 50, statfs, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 55, get_info, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 60, set_info, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 65, quotactl, number, derive, ost, 1)
						JOBSTAT_FIELD_META_OPERATIONS(6, 70, prealloc, number, derive, ost, 1)
					</item>
				</entry>
			</entry>
		</entry>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>mdc</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(MDT.)+-(mdc.+)$</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>mdt_index</name>
					</subpath_field>
					<subpath_field>
						<index>3</index>
						<name>mdc_tag</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				MDC_MDT_CONSTANT_FILE_ENTRY(4, max_rpcs_in_flight, (.+), mdc_rpcs, gauge, max_rpcs_in_flight, max_rpcs_in_flight, 1)
			</entry>
		</entry>
	</entry>
	<entry>
		<subpath>
			<subpath_type>constant</subpath_type>
			<path>/sys/fs/lustre</path>
		</subpath>
		<mode>directory</mode>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>osd-ldiskfs</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(MDT[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>mdt_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				FILES_KBYTES_INFO_ENTRIES(4, mdt, ${subpath:fs_name}-${subpath:mdt_index}, 1)
			</entry>
			<entry>
				<subpath>
					<subpath_type>regular_expression</subpath_type>
					<path>(^.+)-(OST[0-9a-fA-F]+$)</path>
					<subpath_field>
						<index>1</index>
						<name>fs_name</name>
					</subpath_field>
					<subpath_field>
						<index>2</index>
						<name>ost_index</name>
					</subpath_field>
				</subpath>
				<mode>directory</mode>
				FILES_KBYTES_INFO_ENTRIES(4, ost, ${subpath:fs_name}-${subpath:ost_index}, 1)
			</entry>
		</entry>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>ldlm</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>constant</subpath_type>
					<path>namespaces</path>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>regular_expression</subpath_type>
						<path>^filter-(.+)-(OST[0-9a-fA-F]+)_UUID$</path>
						<subpath_field>
							<index>1</index>
							<name>fs_name</name>
						</subpath_field>
						<subpath_field>
							<index>2</index>
							<name>ost_index</name>
						</subpath_field>
					</subpath>
					<mode>directory</mode>
					LDLM_LOCK_INFO_ENTRIES(5, ost, ${subpath:fs_name}-${subpath:ost_index}, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>regular_expression</subpath_type>
						<path>^mdt-(.+)-(MDT[0-9a-fA-F]+)_UUID$</path>
						<subpath_field>
							<index>1</index>
							<name>fs_name</name>
						</subpath_field>
						<subpath_field>
							<index>2</index>
							<name>mdt_index</name>
						</subpath_field>
					</subpath>
					<mode>directory</mode>
					LDLM_LOCK_INFO_ENTRIES(5, mdt, ${subpath:fs_name}-${subpath:mdt_index}, 1)
				</entry>
			</entry>
		</entry>
	</entry>
	<entry>
		<subpath>
			<subpath_type>constant</subpath_type>
			<path>/sys/kernel/debug/lustre</path>
		</subpath>
		<mode>directory</mode>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>mds</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>constant</subpath_type>
					<path>MDS</path>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>mdt</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, mdt, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, mdt, reqbuf_avail, bufs, 1)
						SERVICE_STATS_ITEM(6, mdt, ldlm_ibits_enqueue, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt, mds_getattr, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt, mds_connect, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt, mds_get_root, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt, mds_statfs, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt, mds_getxattr, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt, obd_ping, usecs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, mds, mds, normal_metadata_ops, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>mdt_readpage</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, mdt_readpage, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt_readpage, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_readpage, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_readpage, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, mdt_readpage, reqbuf_avail, bufs, 1)
						SERVICE_STATS_ITEM(6, mdt_readpage, mds_close, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt_readpage, mds_readpage, usecs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, mds, mds, readpage, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>mdt_setattr</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, mdt_setattr, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt_setattr, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_setattr, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_setattr, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, mdt_setattr, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, mds, mds, setattr_service, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>mdt_fld</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, mdt_fld, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt_fld, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_fld, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_fld, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, mdt_fld, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, mds, mds, fld_service, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>mdt_out</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, mdt_out, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt_out, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_out, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_out, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, mdt_out, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, mds, mds, metadata_out_service, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>mdt_seqm</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, mdt_seqm, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt_seqm, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_seqm, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_seqm, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, mdt_seqm, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, mds, mds, metadata_seqm_service, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>mdt_seqs</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, mdt_seqs, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, mdt_seqs, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_seqs, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, mdt_seqs, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, mdt_seqs, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, mds, mds, metadata_seqs_service, gauge, 1)
				</entry>
			</entry>
		</entry>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>ost</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>constant</subpath_type>
					<path>OSS</path>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>ost</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, ost, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, ost, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, ost, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, ost, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, ost, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, ost, ost, normal_data, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>ost_io</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, ost_io, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, ost_io, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, ost_io, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, ost_io, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, ost_io, reqbuf_avail, bufs, 1)
						SERVICE_STATS_ITEM(6, ost_io, ost_read, usecs, 1)
						SERVICE_STATS_ITEM(6, ost_io, ost_write, usecs, 1)
						SERVICE_STATS_ITEM(6, ost_io, ost_punch, usecs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, ost_io, ost, bulk_data_IO, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>ost_create</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, ost_create, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, ost_create, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, ost_create, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, ost_create, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, ost_create, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, ost_create, ost, obj_pre-creation_service, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>ost_seq</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, ost_seq, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, ost_seq, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, ost_seq, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, ost_seq, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, ost_seq, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, ost_seq, ost, seq_service, gauge, 1)
				</entry>
			</entry>
		</entry>
		<entry>
			<subpath>
				<subpath_type>constant</subpath_type>
				<path>ldlm</path>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>constant</subpath_type>
					<path>services</path>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>ldlm_canceld</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, ldlm_canceld, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, ldlm_canceld, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, ldlm_canceld, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, ldlm_canceld, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, ldlm_canceld, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, ldlm_cancel, ldlm_service, lock_cancel, gauge, 1)
				</entry>
				<entry>
					<subpath>
						<subpath_type>constant</subpath_type>
						<path>ldlm_cbd</path>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>stats</path>
						</subpath>
						<mode>file</mode>
						<write_after_read>0</write_after_read>
						SERVICE_STATS_ITEM(6, ldlm_cbd, req_waittime, usecs, 1)
						SERVICE_STATS_ITEM(6, ldlm_cbd, req_qdepth, reqs, 1)
						SERVICE_STATS_ITEM(6, ldlm_cbd, req_active, reqs, 1)
						SERVICE_STATS_ITEM(6, ldlm_cbd, req_timeout, sec, 1)
						SERVICE_STATS_ITEM(6, ldlm_cbd, reqbuf_avail, bufs, 1)
					</entry>
					THREAD_INFO_ENTRIES(5, ldlm_cbd, ldlm_service, lock_grant, gauge, 1)
				</entry>
			</entry>
			<entry>
				<subpath>
					<subpath_type>constant</subpath_type>
					<path>namespaces</path>
				</subpath>
				<mode>directory</mode>
				<entry>
					<subpath>
						<subpath_type>regular_expression</subpath_type>
						<path>^filter-(.+)-(OST[0-9a-fA-F]+)_UUID$</path>
						<subpath_field>
							<index>1</index>
							<name>fs_name</name>
						</subpath_field>
						<subpath_field>
							<index>2</index>
							<name>ost_index</name>
						</subpath_field>
					</subpath>
					<mode>directory</mode>
					<entry>
						<subpath>
							<subpath_type>constant</subpath_type>
							<path>pool</path>
						</subpath>
						<mode>directory</mode>
						<entry>
							<!-- ldlm_stats_counter_init() -->
							<subpath>
								<subpath_type>constant</subpath_type>
								<path>stats</path>
							</subpath>
							<mode>file</mode>
							<item>
								<name>ost_ldlm_stats</name>
								<pattern>granted                   +[[:digit:]]+ samples \[locks\] +([[:digit:]]+).+
grant                     +[[:digit:]]+ samples \[locks\] +([[:digit:]]+).+
cancel                    +[[:digit:]]+ samples \[locks\] +([[:digit:]]+).+
grant_rate                +[[:digit:]]+ samples \[locks\/s\] +([[:digit:]]+).+
cancel_rate               +[[:digit:]]+ samples \[locks\/s\] +([[:digit:]]+).+
grant_plan                +[[:digit:]]+ samples \[locks\/s\] +([[:digit:]]+).+
slv                       +[[:digit:]]+ samples \[slv\] +([[:digit:]]+).+
recalc_freed              +[[:digit:]]+ samples \[locks\] +([[:digit:]]+).+
recalc_timing             +[[:digit:]]+ samples \[sec\] +([[:digit:]]+).+</pattern>
								LDLM_STATS_FIELD(8, 1, granted, number, gauge)
								LDLM_STATS_FIELD(8, 2, grant, number, gauge)
								LDLM_STATS_FIELD(8, 3, cancel, number, gauge)
								LDLM_STATS_FIELD(8, 4, grant_rate, number, gauge)
								LDLM_STATS_FIELD(8, 5, cancel_rate, number, gauge)
								LDLM_STATS_FIELD(8, 6, grant_plan, number, gauge)
								LDLM_STATS_FIELD(8, 7, slv, number, gauge)
								LDLM_STATS_FIELD(8, 8, recalc_freed, number, gauge)
								LDLM_STATS_FIELD(8, 9, recalc_timing, number, gauge)
							</item>
						</entry>
					</entry>
				</entry>
			</entry>
		</entry>
	</entry>
	<entry>
		<subpath>
			<subpath_type>constant</subpath_type>
			<path>/sys/kernel/debug/lustre/llite</path>
		</subpath>
		<mode>directory</mode>
		<entry>
			<subpath>
				<subpath_type>regular_expression</subpath_type>
				<path>(^.+)-([0-9a-fA-F]+$)</path>
				<subpath_field>
					<index>1</index>
					<name>fs_name</name>
				</subpath_field>
				<subpath_field>
					<index>2</index>
					<name>client_uuid</name>
				</subpath_field>
			</subpath>
			<mode>directory</mode>
			<entry>
				<subpath>
					<subpath_type>constant</subpath_type>
					<path>stats</path>
				</subpath>
				<mode>file</mode>
				CLIENT_STATS_ITEM_FOUR_BYTES(4, read_bytes, bytes, derive)
				CLIENT_STATS_ITEM_FOUR_BYTES(4, write_bytes, bytes, derive)
				CLIENT_STATS_ITEM_FOUR(4, read, usecs)
				CLIENT_STATS_ITEM_FOUR(4, write, usecs)
				CLIENT_STATS_ITEM_ONE(4, ioctl, reqs)
				CLIENT_STATS_ITEM_FOUR(4, open, usecs)
				CLIENT_STATS_ITEM_FOUR(4, close, usecs)
				CLIENT_STATS_ITEM_FOUR(4, mmap, usecs)
				CLIENT_STATS_ITEM_FOUR(4, page_fault, usecs)
				CLIENT_STATS_ITEM_FOUR(4, page_mkwrite, usecs)
				CLIENT_STATS_ITEM_FOUR(4, seek, usecs)
				CLIENT_STATS_ITEM_FOUR(4, fsync, usecs)
				CLIENT_STATS_ITEM_FOUR(4, readdir, usecs)
				CLIENT_STATS_ITEM_FOUR(4, setattr, usecs)
				CLIENT_STATS_ITEM_FOUR(4, truncate, usecs)
				CLIENT_STATS_ITEM_FOUR(4, flock, usecs)
				CLIENT_STATS_ITEM_FOUR(4, getattr, usecs)
				CLIENT_STATS_ITEM_FOUR(4, fallocate, usecs)
				CLIENT_STATS_ITEM_FOUR(4, create, usecs)
				CLIENT_STATS_ITEM_FOUR(4, link, usecs)
				CLIENT_STATS_ITEM_FOUR(4, unlink, usecs)
				CLIENT_STATS_ITEM_FOUR(4, symlink, usecs)
				CLIENT_STATS_ITEM_FOUR(4, mkdir, usecs)
				CLIENT_STATS_ITEM_FOUR(4, rmdir, usecs)
				CLIENT_STATS_ITEM_FOUR(4, mknod, usecs)
				CLIENT_STATS_ITEM_FOUR(4, rename, usecs)
				CLIENT_STATS_ITEM_FOUR(4, statfs, usecs)
				CLIENT_STATS_ITEM_FOUR(4, setxattr, usecs)
				CLIENT_STATS_ITEM_FOUR(4, getxattr, usecs)
				CLIENT_STATS_ITEM_ONE(4, getxattr_hits, reqs)
				CLIENT_STATS_ITEM_FOUR(4, listxattr, usecs)
				CLIENT_STATS_ITEM_FOUR(4, removexattr, usecs)
				CLIENT_STATS_ITEM_FOUR(4, inode_permission, usecs)
			</entry>
		</entry>
	</entry>
</definition>

