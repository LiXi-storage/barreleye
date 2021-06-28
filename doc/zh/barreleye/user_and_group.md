本文介绍如何采用Barreleye来监控每个用户和组的带宽和IOPS。

1. 修改`/etc/coral/barreleye.conf`配置，将`jobstat_pattern`配置改为`uid_gid`
2. 运行下列命令，在整个系统中安装和配置Barreleye：
```
# barrele cluster install
```

成功运行完命令后，Barreleye应该已经配置好，可以通过Grafana图形界面访问了。但此时，因为Lustre的jobstat功能还未配置，无法获得每个用户和组的带宽和IOPS信息，Grafana界面上会显示无数据。

为了配置Lustre的jobstat功能，运行下列命令：

1. 在MGS运行：

 ```
 # lctl set_param jobid_var="nodelocal" -P
 ```
2. 在客户端运行lctl get_param jobid_var查看是否输出如下：

```
# lctl get_param jobid_var
jobid_var=nodelocal
```
3. 在MGS运行：
```
# lctl set_param jobid_name="u%u.g%g" -P
```
4. 在客户端运行lctl get_param jobid_name查看是否输出如下：
```
# lctl get_param jobid_name
jobid_name=u%u.g%g
```
至此，包括Barreleye和Lustre的配置都已生效，Barreleye应该可以收集并显示每个用户和组的带宽和IOPS信息。

如果发现Grafana上仍然没有数据，可通过如下步骤进行查错：

1. 在客户端运行下面类似命令，写入一些数据
```
# export LUSTRE_MNT=/mnt/lustre0
# dd if=/dev/zero of=$LUSTRE_MNT/test_file bs=1048576 count=1000
```
2. 在OSS运行下面的命令，查看是否有jobstats的数据，数据的格式是否是预期：

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
3. 在MDS运行下面的命令，查看是否有jobstats的数据，数据的格式是否是预期：

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

