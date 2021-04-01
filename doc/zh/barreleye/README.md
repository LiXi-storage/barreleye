# 概述

:tropical_fish:Barreleye是一套Lustre文件系统的性能监控系统。具有以下特点：

* 收集了几乎全部有分析价值的Lustre性能监控信息；
* 采用深度定制的Collectd作为轻量级代理收集Lustre的系统状态数据；
* 采用Influxdb存储时序数据；
* 采用Grafana作为可视化监控指标展示工具；
* 安装简单。修改配置文件后，一键完成软件在Lustre集群上的安装、配置，可获得Grafana的登录链接；
* 收集的时间间隔可自由定制，最短时间间隔为一秒一次；
* 具有良好的可扩展性，曾成功使用于10PB量级的Lustre存储系统上；
* 具有丰富的插件，可监控除Lustre之外的诸多系统和应用程序的性能指标；
* 支持离线安装，可在与互联网隔离的集群上一键安装。

Barreleye收集和统计的Lustre信息包含但不局限于：

* Lustre文件系统的空间及文件的用量及余量；
* 各OST/MDT的空间及文件的用量及余量；
* 各用户/组的空间及文件的用量；
* Lustre文件系统的读写吞吐率及IOPS；
* 各OST/MDT的读写吞吐率及IOPS；
* 各客户端的读写吞吐率及IOPS；
* 各作业的读写吞吐率及IOPS；
* 读写RPC的粒度分布情况、I/O碎片情况；
* 磁盘读写I/O粒度、时长；
* 集群中服务器的状态信息，如CPU负载、空闲内存、空闲磁盘空间。

Barreleye当前支持的Linux发行版为：

* CentOS7/RHEL7；
* CentOS8/RHEL8。

Barreleye当前支持的体系结构为x86_64。

Barreleye在发行版Lustre-2.12上经过充分测试，共支持以下版本：

* Lustre-1.8.9（旧Lustre长线支持版，已无支持，建议升级）；
* Lustre-2.1.6（旧Lustre长线支持版，已无支持，建议升级）；
* Lustre-2.4.2（不推荐使用的非Lustre长线支持版）；
* Lustre-2.5（旧Lustre长线支持版，已无支持，建议升级）；
* Lustre-2.7（旧Lustre长线支持版，已无支持，建议升级）；
* Lustre-2.10（旧Lustre长线支持版，建议升级）;
* Lustre-2.12（稳定的Lustre长线支持版，建议使用）；
* Lustre-2.13（不推荐使用的非Lustre长线支持版）。

对最新（但稳定性待测）的长线支持版本Lustre-2.14的支持正在开发中。

# 编译

在源码目录中运行命令：

`# ./coral build`

注意：

* 该命令会自动安装或更新包含编译工具在内的各类依赖包，因此会改变编译环境；
* 该命令会下载和编译依赖包，因此可能会持续较长时间；
* 为加速和避免失败，请在网络连接良好的机器上运行该命令；
* 首次编译成功后，缓存会加速下次编译过程。

命令会在源码目录中会产生ISO文件，如`coral-2.0.0_dev_3_g086ccf5.x86_64.iso`，该文件可用来离线安装。

如果依赖包（如collectd的源码包）的下载速度比较慢，可以手动将依赖包放在编译缓存中。编译缓存的路径为`/var/log/coral/build_cache/open/`:

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

依赖包的下载地址可以通过`./coral barrele urls`命令获得。

# 安装

安装步骤如下：

* 安装Barreleye的RPM包，运行命令：

```
# mount coral-2.0.0_dev_3_g086ccf5.x86_64.iso /mnt/
# rpm -ivh /mnt/Packages/coral-barreleye-2.0.0_dev_3_g086ccf5-1.el7.x86_64.rpm
Preparing...                          ################################# [100%]
Updating / installing...
   1:coral-barreleye-2.0.0_dev_3_g086c################################# [100%]
# umount /mnt
```

* 修改Barreleye配置文件：

```
# cp /etc/coral/barreleye.conf.example /etc/coral/barreleye.conf
# vi /etc/coral/barreleye.conf
```

* 运行下列命令，在整个集群中安装和配置Barreleye：

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

至此，监控系统安装完成，后续可通过Grafana界面查看各指标。