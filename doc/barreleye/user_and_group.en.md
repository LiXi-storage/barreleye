This document describes how to configure Barreleye properly to monitor bandwidth and IOPS for each user and group.

1. Modify the`/etc/coral/barreleye.conf` configuration and change the configuration of `jobstat_pattern` to `uid_gid`.
2. Run the following command to install and configure Barreleye:
```
# barrele cluster install
```

After running the command successfully, Barreleye should be configured. Barreleye is now able to be accessed through the Grafana web page. However, since the jobstat feature of Lustre might have not been configured yet, there will be no bandwidth and IOPS data points for each user and group. Thus no data will be shown in the corresponding panel of the Grafana dashboard.

To configure the Jobstat feature of Lustre, run the following commands:

1. Run the following command on MGS:

 ```
 # lctl set_param jobid_var="nodelocal" -P
 ```
2. Run `lctl get_param jobid_var` on the client to check whether the configuration on MGS has been enforced:

```
# lctl get_param jobid_var
jobid_var=nodelocal
```
3. Run the following command on MGS:
```
# lctl set_param jobid_name="u%u.g%g" -P
```
4. Run `lctl get_param jobid_name` on the client to check if the output is as follows:
```
# lctl get_param jobid_name
jobid_name=u%u.g%g
```
After the enforcement of Barreleye and Lustre configurations, Barreleye can now collect and display bandwidth and IOPS for each user and group.

If no data is shown on Grafana, you can troubleshoot by taking the following steps.

1. Run the following command on the client to write some data:
```
# export LUSTRE_MNT=/mnt/lustre0
# dd if=/dev/zero of=$LUSTRE_MNT/test_file bs=1048576 count=1000
```
2. Run the following command on OSS to check if the data of jobstats is contained and if it is in the expected format:

```
# lctl get_param obdfilter.*.job_stats
obdfilter.lustre0-OST0000.job_stats=
job_stats:

- job_id:          u0.g0
  snapshot_time:   1624873755
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         250, unit: bytes, min: 4194304, max: 4194304, sum:      1048576000 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }
```
3. Run the following command on MDS to check if the data of jobstats is contained and if it is in the expected format:

```
# lctl get_param mdt.*.job_stats
mdt.lustre0-MDT0000.job_stats=
job_stats:
- job_id:          u0.g0
  snapshot_time:   1624873835
  open:            { samples:           1, unit:  reqs }
  close:           { samples:           1, unit:  reqs }
  mknod:           { samples:           1, unit:  reqs }
  link:            { samples:           0, unit:  reqs }
  unlink:          { samples:           0, unit:  reqs }
  mkdir:           { samples:           0, unit:  reqs }
  rmdir:           { samples:           0, unit:  reqs }
  rename:          { samples:           0, unit:  reqs }
  getattr:         { samples:           1, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  getxattr:        { samples:           0, unit:  reqs }
  setxattr:        { samples:           0, unit:  reqs }
  statfs:          { samples:           2, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  samedir_rename:  { samples:           0, unit:  reqs }
  crossdir_rename: { samples:           0, unit:  reqs }
  read_bytes:      { samples:           0, unit:  reqs, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:           0, unit:  reqs, min:       0, max:       0, sum:               0 }
  punch:           { samples:           0, unit:  reqs }
```

