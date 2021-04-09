# Overview

:tropical_fish:Barreleye is a performance monitoring system for Lustre file system. It has the following features:

* Collects almost all Lustre performance monitoring statistics that worths to be analized;
* Uses deeply customized Collectd as the light-weighted agent to collect system status of Lustre;
* Uses Influxdb as the time series database;
* Uses Grafana as the graphical user interface to display metrics;
* Easy to install. With the configuration file written properly, the installation can be finished on the whole cluster with a single command which provides the login linkage to Grafana;
* Tunable data collection interval. The minimal interval is one data point per second;
* High scalability. It has been running on multiple Lustre storage systems each with capacity of more than 10 PB;
* Has a lot of plugins that enable monitoring of many metrics from the system softwares or applications in addition with Lustre;
* Supports off-line installation. Even the cluster is disconnected from Internet, installation can be finished with a single comamnd;

Lustre statistics collected by Barreleye includes but is not limited to: 

* The used and total space/inodes of the Lustre file system;
* The used and total space/inodes of each Lustre OST/MDT;
* The used space/inodes of each user/group;
* The read/write throughput and IOPS of the Lustre file system;
* The read/write throughput and IOPS of each OST/MDT;
* The read/write throughpu  and IOPS of each client;
* The read/write throughput and IOPS of each job;
* The read/write RPC size distribution and I/O fragmentation;
* Size and duration distribution of read/write I/O on disk;
* Status of servers in the cluster, such as CPU load, free memory and free disk space.

The Linux distributions supported by Barreleye are: 

* CentOS7/RHEL7;
* CentOS8/RHEL8.

Currently, Barreleye supports the architecture of x86_64.

Barreleye has been fully tested on distribution Lustre-2.12 and supports the following versions:

* Lustre-1.8.9 (Old Long Term Support release with expired support. Upgrade recommended);
* Lustre-2.1.6 (Old Lustre LTS release with expired support. Upgrade recommended);
* Lustre-2.4.2 (Lustre non-LTS release. Not recommended to use);
* Lustre-2.5 (Old Lustre LTS release with expired support. Upgrade recommended);
* Lustre-2.7 (Old Lustre LTS release with expired support. Upgrade recommended);
* Lustre-2.10 (Old Lustre LTS release. Upgrade recommended);
* Lustre-2.12 (Stable LTS release. Recommended to use).
* Lustre-2.13 (Lustre non-LTS release. Not recommended to use).

Support for Lustre-2.14 (the latest LTS release, stability to be tested) is under development.

# Building

To build Barreleye, please run the following command under the source code directory:

`# ./coral build`

Please note that:

* This command will automatically install/update dependency packages such as compilation tools, thus it will change the system environment of the compilation host;
* This command will download and compile the dependency packages, which may cost a long time;
* In order to speed up this command and avoid any failure due to timeout, please run this command on a host with fast Internet connection;
* After a successful compilation, this command will be accelerated in the next round due to cache.

This command will generate Barreleye ISO file under the source directory, with file name such like `coral-2.0.0_dev_3_g086ccf5.x86_64.iso`. The ISO can be used for off-line installation.

If the dependencies (e.g. the source code tarball of Collectd) are slow to download, they can be manually put into the cache directory. The path of the cache directory is `/var/log/coral/build_cache/open/`:

```
# ls /var/log/coral/build_cache/open/ -l
total 13300
-rw-r--r-- 1 root root  401036 Feb 24 00:21 Grafana_Status_panel.zip
-rw-r--r-- 1 root root 3596102 Feb 25 14:58 PyInstaller-4.2.tar.gz
-rw-r--r-- 1 root root 1979362 Mar 21 23:44 collectd-5.12.0.barreleye1.tar.bz2
-rw-r--r-- 1 root root  274360 Sep 17  2020 grafana-piechart-panel-1.6.1.zip
drwxr-xr-x 6 root root    4096 Mar 24 10:21 iso_cache
drwxr-xr-x 2 root root    4096 Mar 21 20:56 pip
```

The download URLs of dependencies can be got from command `./coral barrele urls`.

# Installation

The installation steps are as follows:

* Install the RPM package for Barreleye:

```
# mount coral-2.0.0_dev_3_g086ccf5.x86_64.iso /mnt/
# rpm -ivh /mnt/Packages/coral-barreleye-2.0.0_dev_3_g086ccf5-1.el7.x86_64.rpm
Preparing...                          ################################# [100%]
Updating / installing...
   1:coral-barrelee-2.0.0_dev_3_g086c ################################# [100%]
# umount /mnt
```

* Modify the Barreleye configuration file:

```
# cp /etc/coral/barreleye.conf.example /etc/coral/barreleye.conf
# vi /etc/coral/barreleye.conf
```

* Install and configure Barreleye on the cluster:

```
# barrele cluster install --iso coral-2.0.0_dev_3_g086ccf5.x86_64.iso
INFO: syncing ISO to [/var/log/coral/iso] on host [server1]
INFO: detected Lustre version [2.12] on host [agent1]
INFO: detected Lustre version [2.12] on host [agent2]
INFO: detected Lustre version [2.12] on host [agent3]
INFO: detected Lustre version [2.12] on host [agent4]
INFO: failed to match Lustre version according to RPM names on host [server1], using default [2.12]
INFO: updating [/etc/rsyslog.conf] on host [agent1] to avoid flood of messages
INFO: updating [/etc/rsyslog.conf] on host [agent2] to avoid flood of messages
INFO: updating [/etc/rsyslog.conf] on host [agent3] to avoid flood of messages
INFO: updating [/etc/rsyslog.conf] on host [agent4] to avoid flood of messages
INFO: updating [/etc/rsyslog.conf] on host [server1] to avoid flood of messages
INFO: disabling SELinux on host [server1]
INFO: disabling SELinux on host [agent3]
INFO: disabling SELinux on host [agent4]
INFO: disabling SELinux on host [agent1]
INFO: disabling SELinux on host [agent2]
INFO: disabling firewalld on host [server1]
INFO: disabling firewalld on host [agent3]
INFO: disabling firewalld on host [agent4]
INFO: disabling firewalld on host [agent1]
INFO: disabling firewalld on host [agent2]
INFO: syncing ISO dir from local host [server1] to host [agent3]
INFO: syncing ISO dir from local host [server1] to host [agent4]
INFO: syncing ISO dir from local host [server1] to host [agent1]
INFO: syncing ISO dir from local host [server1] to host [agent2]
INFO: reinstalling Coral RPMs on host [server1]
INFO: reinstalling Coral RPMs on host [agent3]
INFO: reinstalling Coral RPMs on host [agent4]
INFO: reinstalling Coral RPMs on host [agent1]
INFO: reinstalling Coral RPMs on host [agent2]
INFO: Influxdb config [/etc/influxdb/influxdb.conf] on host [server1] was changed by Barreleye before, using backup
INFO: starting and enabling service [influxdb] on host [server1]
INFO: recreating continuous queries of Influxdb on host [server1]
INFO: restarting and enabling service [grafana-server] on host [server1]
INFO: installing Grafana plugins on host [server1]
INFO: recreating Grafana folders on host [server1]
INFO: recreating Grafana dashboards on host [server1]
INFO: adding Influxdb data source to Grafana on host [server1]
INFO: adding user/password [viewer/viewer] to Grafana on host [server1]
INFO: configuring Collectd on host [agent1]
INFO: checking whether Influxdb can get data points from agent [agent1]
INFO: configuring Collectd on host [agent2]
INFO: checking whether Influxdb can get data points from agent [agent2]
INFO: configuring Collectd on host [agent3]
INFO: checking whether Influxdb can get data points from agent [agent3]
INFO: configuring Collectd on host [agent4]
INFO: checking whether Influxdb can get data points from agent [agent4]
INFO: configuring Collectd on host [server1]
INFO: checking whether Influxdb can get data points from agent [server1]
INFO: URL of the dashboards is [http://server1:3000]
INFO: please login by [viewer:viewer] for viewing
INFO: please login by [admin:admin] for administrating
```

Now the installation of the monitoring system is completed, and all the metrics can be viewed through the Grafana web interface.

[MIT © Li Xi](./LICENSE)
