"""
Library for building Barreleye
"""
import os
import stat
from pycoral import constant
from pycoral import ssh_host
from pycoral import clog
from pycoral import cmd_general
from pybuild import build_common
from pybarrele import barrele_constant

PACAKGE_URL_DICT = {}
# The URL of Collectd tarball
COLLECTD_URL = ("https://github.com/LiXi-storage/collectd/releases/download/"
                "collectd-5.12.0.brl2/collectd-5.12.0.brl2.tar.bz2")
# The sha1sum of Collectd tarball. Need to update together with
# COLLECTD_URL
COLLECTD_SHA1SUM = "9fb8be9d7c0bf7c84b93ef5bf441d393b081e7d9"
PACAKGE_URL_DICT["collectd"] = COLLECTD_URL

# The RPM names of Collectd to check
COLLECTD_RPM_NAMES = ["collectd", "collectd-disk", "collectd-filedata",
                      "collectd-sensors", "collectd-ssh",
                      "libcollectdclient"]
# The URL of Influxdb RPM for x86_64
INFLUXDB_RPM_URL_X86_64 = "https://dl.influxdata.com/influxdb/releases/influxdb-1.8.4.x86_64.rpm"
# The sha1sum of Influxdb RPM for x86_64. Need to update together with
# INFLUXDB_RPM_URL_X86_64
INFLUXDB_RPM_SHA1SUM_X86_64 = "26832ce558b8a79beb3bfda1799eb7394d5c13cd"
PACAKGE_URL_DICT["influxdb"] = INFLUXDB_RPM_URL_X86_64
# The URL of Grafana RPM for x86_64
GRAFANA_RPM_URL_X86_64 = "https://dl.grafana.com/oss/release/grafana-7.3.7-1.x86_64.rpm"
# The sha1sum of Grafana RPM for x86_64. Need to update together with
# GRAFANA_RPM_URL_X86_64
GRAFANA_RPM_SHA1SUM_X86_64 = "bf5814ac0c756fbe85611bd355acce49ff8d5f44"
# The URL of Grafana RPM for x86_64
GRAFANA_RPM_URL_AARCH64 = "https://dl.grafana.com/oss/release/grafana-7.3.7-1.aarch64.rpm"
# The sha1sum of Grafana RPM for ARM. Need to update together with
# GRAFANA_RPM_URL_X86_64
GRAFANA_RPM_SHA1SUM_AARCH64 = "8356a324acefcd788cb0711b9bc5bd8689774be1"
PACAKGE_URL_DICT["grafana"] = GRAFANA_RPM_URL_X86_64

# The URL of Grafana status panel plugin
GRAFANA_STATUS_PANEL_URL = ("https://github.com/Vonage/Grafana_Status_panel/"
                            "archive/v1.0.10.zip")
# URL does not have expected fname, specify explictly here.
GRAFANA_STATUS_PANEL_FNAME = barrele_constant.GRAFANA_STATUS_PANEL + ".zip"
# The unzipped file name is not GRAFANA_STATUS_PANEL, need to rename to
# GRAFANA_STATUS_PANEL
GRAFANA_STATUS_PANEL_UNZIPPED_FNAME = "Grafana_Status_panel-1.0.10"
# The sha1sum of Grafana status panel plugin. Need to update together with
# GRAFANA_STATUS_PANEL_URL
GRAFANASTATUS_PANEL_SHA1SUM = "e46fb5cb8f30d9ea743e0fda67402c0504a39690"
PACAKGE_URL_DICT["grafana_status_panel"] = GRAFANA_STATUS_PANEL_URL
# The URL of Grafana piechart panel plugin
GRAFANA_PIECHART_PANEL_URL = ("https://github.com/grafana/piechart-panel/"
                              "releases/download/v1.6.1/"
                              "grafana-piechart-panel-1.6.1.zip")
# The sha1sum of Grafana piechart panel plugin. Need to update together with
# GRAFANA_PIECHART_PANEL_URL
GRAFANA_PIECHART_PANEL_SHA1SUM = "2b3c33afd865af4575d87a83e3d45e61acf8273a"
PACAKGE_URL_DICT["grafana_piechart_panel"] = GRAFANA_PIECHART_PANEL_URL
# RPMs needed by building collectd, both for RHEL7 and RHEL8
COLLECTD_BUILD_DEPENDENT_COMMON_RPMS = ["libcurl-devel",
                                        "ganglia-devel",
                                        "gtk2-devel",
                                        "iptables-devel",
                                        "iproute-devel",
                                        "libatasmart-devel",
                                        "libdbi-devel",
                                        "libcap-devel",
                                        "libesmtp-devel",
                                        "libgcrypt-devel",
                                        "libmemcached-devel",
                                        "libmicrohttpd-devel",
                                        "libmnl-devel",
                                        "libnotify-devel",
                                        "libpcap-devel",
                                        "libssh2-devel",
                                        "libxml2-devel",
                                        "libvirt-devel",
                                        "lm_sensors-devel",
                                        "lua-devel",
                                        "mosquitto-devel",
                                        "net-snmp-devel",
                                        "OpenIPMI-devel",
                                        "openldap-devel",
                                        "perl-ExtUtils-Embed",
                                        "qpid-proton-c-devel",
                                        "riemann-c-client-devel",
                                        "rrdtool-devel",
                                        "systemd-devel",  # libudev.h
                                        "uthash-devel",
                                        "xfsprogs-devel",
                                        "yajl-devel",
                                        "zeromq-devel"]

# RPMs needed by building barreleye
BARRELEYE_BUILD_DEPENDENT_COMMON_RPMS = COLLECTD_BUILD_DEPENDENT_COMMON_RPMS
COLLECTD_BUILD_DEPENDENT_RHEL7_RPMS = (BARRELEYE_BUILD_DEPENDENT_COMMON_RPMS +
                                       ["postgresql-devel", "python-devel"])
COLLECTD_BUILD_DEPENDENT_RHEL8_RPMS = (BARRELEYE_BUILD_DEPENDENT_COMMON_RPMS +
                                       ["libpq-devel", "python36-devel"])
BARRELEYE_BUILD_DEPENDENT_PIPS = ["requests", "python-slugify"]


def get_collectd_rpm_suffix(distro_number, target_cpu,
                            collectd_version_release):
    """
    Return the suffix of Collectd RPMs.
    The suffix starts from "-", e.g.
    "-5.11.0.gadf6f83.lustre-1.el7.x86_64.rpm"
    """
    return ("-%s.el%s.%s.rpm" %
            (collectd_version_release, distro_number, target_cpu))


def check_collectd_rpms_integrity(log, rpm_fnames, distro_number, target_cpu,
                                  collectd_version_release, quiet=True):
    """
    Check whether the existing RPMs has all expected Collectd RPMs
    """
    suffix = get_collectd_rpm_suffix(distro_number, target_cpu,
                                     collectd_version_release)
    for collect_rpm_name in COLLECTD_RPM_NAMES:
        collect_rpm_full = collect_rpm_name + suffix
        if collect_rpm_full not in rpm_fnames:
            if not quiet:
                log.cl_error("RPM [%s] does not exist",
                             collect_rpm_full)
            else:
                log.cl_debug("RPM [%s] does not exist",
                             collect_rpm_full)
            return -1
    return 0


def get_and_clean_collectd_rpms(log, host, packages_dir,
                                rpm_fnames, distro_number, target_cpu,
                                collectd_version_release,
                                expect_clean=False):
    """
    Return a list of Collectd RPMs under a directory.
    If there are other version of Collectd RPMs, remove them.
    """
    # pylint: disable=too-many-locals
    suffix = get_collectd_rpm_suffix(distro_number, target_cpu,
                                     collectd_version_release)
    prefixes = ["collectd", "libcollectdclient"]
    collectd_fnames = []
    for rpm_fname in rpm_fnames:
        found = False
        for prefix in prefixes:
            if rpm_fname.startswith(prefix):
                found = True
        if not found:
            continue

        if not rpm_fname.endswith(suffix):
            if expect_clean:
                log.cl_error("Collectd RPM [%s] has different suffix, "
                             "expected [%s]",
                             rpm_fname, suffix)
                return None

            log.cl_info("Collectd RPM [%s] has different suffix, "
                        "expected [%s], removing",
                        rpm_fname, suffix)
            fpath = packages_dir + "/" + rpm_fname
            command = ("rm -f %s" % (fpath))
            retval = host.sh_run(command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return None
            continue
        collectd_fnames.append(rpm_fname)
    return collectd_fnames


def remove_collectd_rpms(log, host, packages_dir):
    """
    Remove old Collectd RPMs
    """
    patterns = ["collectd-*", "libcollectdclient-*"]
    for pattern in patterns:
        command = "rm -f %s/%s" % (packages_dir, pattern)
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


def get_collectd_version(log, host, collectd_src_dir):
    """
    Return the Collectd version.
    This assumes the collectd.spec has following line:

    Version:        {?rev}
    """
    command = (r"cd %s && grep Version contrib/redhat/collectd.spec | "
               r"grep -v \# | awk '{print $2}'" %
               collectd_src_dir)
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None
    collectd_version_string = retval.cr_stdout.strip()
    if collectd_version_string != "%{?rev}":
        log.cl_error("version string [%s] in dir [%s] on host [%s] is "
                     "unexpected", collectd_version_string, collectd_src_dir,
                     host.sh_hostname)
        log.cl_error("dir [%s] on host is not a Barreleye release of Collectd")
        return None

    command = "cd %s && ./version-gen.sh" % collectd_src_dir
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None
    collectd_version = retval.cr_stdout.strip()
    return collectd_version


def get_collectd_version_release(log, host, collectd_src_dir,
                                 collectd_version):
    """
    Return the Collectd version
    """
    command = (r"cd %s && grep Release contrib/redhat/collectd.spec | "
               r"grep -v \# | awk '{print $2}'" %
               collectd_src_dir)
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None
    collectd_release_string = retval.cr_stdout.strip()
    collectd_release = collectd_release_string.replace('%{?dist}', '')
    collectd_version_release = collectd_version + "-" + collectd_release
    return collectd_version_release


def build_collectd_rpms(log, host, target_cpu, packages_dir,
                        collectd_src_dir, tarball_fpath, distro_number,
                        collectd_version):
    """
    Build Collectd RPMs on a host
    """
    command = ("cd %s && mkdir {BUILD,RPMS,SOURCES,SRPMS} && "
               "cp %s SOURCES" %
               (collectd_src_dir, tarball_fpath))
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

    command = ('cd %s && '
               'rpmbuild -ba --with write_tsdb --with nfs --without java '
               '--without amqp --without gmond --without nut --without pinba '
               '--without ping --without varnish --without dpdkstat '
               '--without turbostat --without redis --without write_redis '
               '--without gps --without lvm --without modbus --without mysql '
               '--without ime '
               '--define "_topdir %s" '
               '--define="rev %s" '
               '--define="dist .el%s" '
               'contrib/redhat/collectd.spec' %
               (collectd_src_dir, collectd_src_dir,
                collectd_version, distro_number))
    log.cl_info("running command [%s] on host [%s]",
                command, host.sh_hostname)
    # This command is time consuming.
    retval = host.sh_run(log, command, timeout=None)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    generated_collectd_rpm_dir = ("%s/RPMS/%s" %
                                  (collectd_src_dir, target_cpu))

    command = ("mv %s/* %s" %
               (generated_collectd_rpm_dir, packages_dir))
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


def collectd_build_and_check(log, host, target_cpu, packages_dir,
                             collectd_src_dir, collectd_version,
                             collectd_version_release,
                             tarball_fpath, extra_package_fnames):
    """
    Check and build Collectd RPMs
    """
    existing_rpm_fnames = host.sh_get_dir_fnames(log, packages_dir)
    if existing_rpm_fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                     packages_dir,
                     host.sh_hostname)
        return -1

    distro = host.sh_distro(log)
    if distro == ssh_host.DISTRO_RHEL7:
        distro_number = "7"
    elif distro == ssh_host.DISTRO_RHEL8:
        distro_number = "8"
    else:
        log.cl_error("build on distro [%s] is not supported yet", distro)
        return -1

    ret = check_collectd_rpms_integrity(log, existing_rpm_fnames,
                                        distro_number, target_cpu,
                                        collectd_version_release)
    if ret == 0:
        log.cl_debug("Collectd RPMs already exist")
        collectd_rpm_fnames = get_and_clean_collectd_rpms(log, host,
                                                          packages_dir,
                                                          existing_rpm_fnames,
                                                          distro_number,
                                                          target_cpu,
                                                          collectd_version_release)
        if collectd_rpm_fnames is None:
            log.cl_error("failed to get the Collectd RPM names")
            return -1
        extra_package_fnames += collectd_rpm_fnames
        return 0

    log.cl_debug("building Collectd RPMs")
    ret = remove_collectd_rpms(log, host, packages_dir)
    if ret:
        log.cl_error("failed to remove old Collectd RPMs")
        return -1

    ret = build_collectd_rpms(log, host, target_cpu, packages_dir,
                              collectd_src_dir, tarball_fpath, distro_number,
                              collectd_version)
    if ret:
        log.cl_error("failed to build Collectd RPMs from src [%s]",
                     collectd_src_dir)
        return -1

    existing_rpm_fnames = host.sh_get_dir_fnames(log, packages_dir)
    if existing_rpm_fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                     packages_dir,
                     host.sh_hostname)
        return -1

    ret = check_collectd_rpms_integrity(log, existing_rpm_fnames,
                                        distro_number, target_cpu,
                                        collectd_version_release,
                                        quiet=False)
    if ret:
        log.cl_error("generated Collectd RPMs is not complete")
        return -1

    collectd_rpm_fnames = get_and_clean_collectd_rpms(log, host,
                                                      packages_dir,
                                                      existing_rpm_fnames,
                                                      distro_number,
                                                      target_cpu,
                                                      collectd_version_release,
                                                      expect_clean=True)
    if collectd_rpm_fnames is None:
        log.cl_error("failed to get the Collectd RPM names")
        return -1
    extra_package_fnames += collectd_rpm_fnames
    return 0


def build_collectd_tarball(log, workspace, host, target_cpu, packages_dir,
                           tarball_fpath, extra_package_fnames,
                           known_collectd_version=None):
    """
    Untar the tarball, check the target version, check the cache, and build
    if cache is invalid.
    """
    # pylint: disable=too-many-locals
    if not tarball_fpath.endswith(".tar.bz2"):
        log.cl_error("Collectd tarball [%s] does not end with [.tar.bz2]",
                     tarball_fpath)
        return -1
    fname = os.path.basename(tarball_fpath)
    dirname = fname[:-8]

    collectd_build_dir = workspace + "/collectd_build"
    command = "mkdir -p %s" % collectd_build_dir
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

    command = "tar xfj %s -C %s" % (tarball_fpath, collectd_build_dir)
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

    collectd_src_dir = collectd_build_dir + "/" + dirname
    ret = host.sh_path_isdir(log, collectd_src_dir)
    if ret < 0:
        log.cl_error("failed to check whether path [%s] is a dir on host [%s]",
                     collectd_src_dir, host.sh_hostname)
        return -1
    if ret == 0:
        log.cl_error("path [%s] is not a Collectd source dir on host [%s]",
                     collectd_src_dir, host.sh_hostname)
        return -1

    if known_collectd_version is None:
        collectd_version = get_collectd_version(log, host, collectd_src_dir)
        if collectd_version is None:
            log.cl_error("failed to get Collectd version of src dir [%s] on "
                         "host [%s]", collectd_src_dir, host.sh_hostname)
            return -1
    else:
        collectd_version = known_collectd_version

    collectd_version_release = get_collectd_version_release(log, host,
                                                            collectd_src_dir,
                                                            collectd_version)
    if collectd_version_release is None:
        log.cl_error("failed to get Collectd version release from Collectd "
                     "source dir")
        return -1

    ret = collectd_build_and_check(log, host, target_cpu, packages_dir,
                                   collectd_src_dir, collectd_version,
                                   collectd_version_release,
                                   tarball_fpath, extra_package_fnames)
    if ret:
        log.cl_error("failed to build and check Collectd RPMs")
        return -1
    return 0


def download_and_build_collectd(log, workspace, host, type_cache, target_cpu,
                                packages_dir, collectd_url, expected_sha1sum,
                                extra_package_fnames):
    """
    Download Collectd source code tarball and build
    """
    log.cl_info("building Collectd RPMs from URL [%s] on host [%s]",
                collectd_url, host.sh_hostname)
    tarball_fname = os.path.basename(collectd_url)
    tarball_fpath = type_cache + "/" + tarball_fname
    ret = host.sh_download_file(log, collectd_url, tarball_fpath,
                                expected_sha1sum)
    if ret:
        log.cl_error("failed to download Collectd sourcecode tarball")
        return -1

    ret = build_collectd_tarball(log, workspace, host, target_cpu,
                                 packages_dir, tarball_fpath,
                                 extra_package_fnames)
    if ret:
        log.cl_error("failed to build Collectd tarball [%s]",
                     tarball_fpath)
        return -1
    return 0


def build_collectd_dir(log, workspace, host, target_cpu, packages_dir,
                       origin_collectd_dir, extra_package_fnames):
    """
    Build Collectd from src dir
    """
    # pylint: disable=too-many-locals
    basename = os.path.basename(origin_collectd_dir)
    collectd_dir = workspace + "/" + basename
    # Get the Collectd version from the origin Collectd dir. If we get it
    # from the tarball generated later, some info like Git tag might be lost.
    collectd_version = get_collectd_version(log, host, origin_collectd_dir)
    if collectd_version is None:
        log.cl_error("failed to get Collectd version of src dir [%s] on "
                     "host [%s]", origin_collectd_dir, host.sh_hostname)
        return -1

    command = "cp -a %s %s" % (origin_collectd_dir, workspace)
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

    command = "rm -f %s/collectd-*.tar.bz2" % (collectd_dir)
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

    command = ("cd %s && mkdir -p libltdl/config && sh ./build.sh && "
               "./configure && make dist-bzip2" % (collectd_dir))
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

    command = ("cd %s && ls collectd-*.tar.bz2" % (collectd_dir))
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

    collectd_tarballs = retval.cr_stdout.split()
    if len(collectd_tarballs) != 1:
        log.cl_error("unexpected output of Collectd tarball: [%s]",
                     retval.cr_stdout)
        return -1

    collectd_tarball_fname = collectd_tarballs[0]

    if (not collectd_tarball_fname.endswith(".tar.bz2") or
            len(collectd_tarball_fname) <= 8):
        log.cl_error("unexpected Collectd tarball fname [%s] generated "
                     "from [%s]", collectd_tarball_fname,
                     origin_collectd_dir)
        return -1

    collectd_tarball_fpath = collectd_dir + "/" + collectd_tarball_fname
    ret = build_collectd_tarball(log, workspace, host, target_cpu, packages_dir,
                                 collectd_tarball_fpath, extra_package_fnames,
                                 known_collectd_version=collectd_version)
    if ret:
        log.cl_error("failed to build Collectd tarball [%s] on host",
                     collectd_tarball_fpath)
        return -1
    return 0


def build_collectd(log, workspace, host, type_cache, target_cpu, packages_dir,
                   collectd, extra_package_fnames):
    """
    Build Collectd
    """
    if collectd is None:
        return download_and_build_collectd(log, workspace, host, type_cache,
                                           target_cpu, packages_dir, COLLECTD_URL,
                                           COLLECTD_SHA1SUM,
                                           extra_package_fnames)

    stat_result = host.sh_stat(log, collectd)
    if stat_result is not None:
        if stat.S_ISREG(stat_result.st_mode):
            log.cl_info("building Collectd RPMs from tarball [%s] on host [%s]",
                        collectd, host.sh_hostname)
            ret = build_collectd_tarball(log, workspace, host, target_cpu,
                                         packages_dir, collectd,
                                         extra_package_fnames)
            if ret:
                log.cl_error("failed to build Collectd tarball [%s] on host",
                             collectd)
                return -1
        elif stat.S_ISDIR(stat_result.st_mode):
            log.cl_info("building Collectd RPMs from dir [%s] on host [%s]",
                        collectd, host.sh_hostname)
            ret = build_collectd_dir(log, workspace, host, target_cpu,
                                     packages_dir, collectd,
                                     extra_package_fnames)
            if ret:
                log.cl_error("failed to build Collectd dir [%s] on host [%s]",
                             collectd, host.sh_hostname)
                return -1
        else:
            log.cl_error("unexpected file type of Collectd [%s] on host [%s]",
                         collectd, host.sh_hostname)
            return -1
        return 0

    return download_and_build_collectd(log, workspace, host, type_cache,
                                       target_cpu, packages_dir, collectd, None,
                                       extra_package_fnames)


def download_influxdb_x86_64(log, host, packages_dir, extra_package_fnames):
    """
    Build Influxdb for x86_64 platform
    """
    url = INFLUXDB_RPM_URL_X86_64
    expected_sha1sum = INFLUXDB_RPM_SHA1SUM_X86_64

    fname = os.path.basename(url)
    fpath = packages_dir + "/" + fname
    log.cl_info("downloading Influxdb RPM")
    ret = host.sh_download_file(log, url, fpath, expected_sha1sum)
    if ret:
        log.cl_error("failed to download RPM of Influxdb")
        return -1
    extra_package_fnames.append(fname)
    return 0


def build_influxdb(log, host, target_cpu, packages_dir, extra_package_fnames):
    """
    Build Influxdb
    """
    if target_cpu == "x86_64":
        rc = download_influxdb_x86_64(log, host, packages_dir,
                                      extra_package_fnames)
        if rc:
            return rc
    else:
        log.cl_error("unsupported CPU type [%s]", target_cpu)
        return -1

    return 0


def build_grafana(log, host, target_cpu, packages_dir, extra_package_fnames):
    """
    Build Grafana
    """
    if target_cpu == "x86_64":
        url = GRAFANA_RPM_URL_X86_64
        expected_sha1sum = GRAFANA_RPM_SHA1SUM_X86_64
    elif target_cpu == "aarch64":
        url = GRAFANA_RPM_URL_AARCH64
        expected_sha1sum = GRAFANA_RPM_SHA1SUM_AARCH64
    else:
        log.cl_error("unsupported CPU type [%s]", target_cpu)
        return -1
    fname = os.path.basename(url)
    fpath = packages_dir + "/" + fname
    log.cl_info("downloading Grafana RPM")
    ret = host.sh_download_file(log, url, fpath, expected_sha1sum)
    if ret:
        log.cl_error("failed to download RPM of Grafana")
        return -1
    extra_package_fnames.append(fname)
    return 0


def download_grafana_status_panel_plugin(log, host, type_cache, iso_cache,
                                         extra_iso_fnames):
    """
    Download and untar Grafana Status Panel plugin
    """
    tarball_url = GRAFANA_STATUS_PANEL_URL
    tarball_fname = GRAFANA_STATUS_PANEL_FNAME
    tarball_fpath = type_cache + "/" + tarball_fname
    expected_sha1sum = GRAFANASTATUS_PANEL_SHA1SUM

    log.cl_info("downloading Grafana Status Panel plugin")
    ret = host.sh_download_file(log, tarball_url, tarball_fpath,
                                expected_sha1sum,
                                output_fname=tarball_fname)
    if ret:
        log.cl_error("failed to download Grafana Status Panel plugin")
        return -1

    command = ("rm -fr %s/%s && unzip %s -d %s" %
               (iso_cache, GRAFANA_STATUS_PANEL_UNZIPPED_FNAME,
                tarball_fpath, iso_cache))
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

    # Need to rename
    command = ("rm -fr %s/%s && mv %s/%s %s/%s" %
               (iso_cache, barrele_constant.GRAFANA_STATUS_PANEL,
                iso_cache, GRAFANA_STATUS_PANEL_UNZIPPED_FNAME,
                iso_cache, barrele_constant.GRAFANA_STATUS_PANEL))
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
    extra_iso_fnames.append(barrele_constant.GRAFANA_STATUS_PANEL)
    return 0


def download_grafana_piechart_panel_plugin(log, host, type_cache, iso_cache,
                                           extra_iso_fnames):
    """
    Download and untar Grafana Piechart plugin
    """
    tarball_url = GRAFANA_PIECHART_PANEL_URL
    tarball_fname = os.path.basename(tarball_url)
    tarball_fpath = type_cache + "/" + tarball_fname
    expected_sha1sum = GRAFANA_PIECHART_PANEL_SHA1SUM

    log.cl_info("downloading Grafana Piechart panel")
    ret = host.sh_download_file(log, tarball_url, tarball_fpath,
                                expected_sha1sum)
    if ret:
        log.cl_error("failed to download Grafana Piechart panel plugin")
        return -1

    command = ("rm -fr %s/%s && unzip %s -d %s" %
               (iso_cache, barrele_constant.GRAFANA_PIECHART_PANEL,
                tarball_fpath, iso_cache))
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
    extra_iso_fnames.append(barrele_constant.GRAFANA_PIECHART_PANEL)
    return 0


def build_grafana_plugins(log, host, type_cache, iso_cache, extra_iso_fnames):
    """
    Build Grafana plugins
    """
    ret = download_grafana_piechart_panel_plugin(log, host, type_cache,
                                                 iso_cache, extra_iso_fnames)
    if ret:
        log.cl_error("failed to download Grafana Piechart Panel plugin")
        return -1

    ret = download_grafana_status_panel_plugin(log, host, type_cache,
                                               iso_cache, extra_iso_fnames)
    if ret:
        log.cl_error("failed to download Grafana Status Panel plugin")
        return -1
    return 0


def build_barreleye(log, workspace, host, type_cache, target_cpu, iso_cache,
                    packages_dir, collectd, extra_iso_fnames,
                    extra_package_fnames, extra_rpm_names):
    """
    Build barreleye
    """
    rc = build_collectd(log, workspace, host, type_cache, target_cpu,
                        packages_dir, collectd, extra_package_fnames)
    if rc:
        log.cl_error("failed to build Collectd RPMs")
        return -1

    rc = build_grafana_plugins(log, host, type_cache, iso_cache,
                               extra_iso_fnames)
    if rc:
        log.cl_error("failed to download Grafana")
        return -1

    rc = build_grafana(log, host, target_cpu, packages_dir, extra_package_fnames)
    if rc:
        log.cl_error("failed to download Grafana")
        return -1

    rc = build_influxdb(log, host, target_cpu, packages_dir,
                        extra_package_fnames)
    if rc:
        log.cl_error("failed to download Influxdb")
        return -1

    extra_rpm_names += barrele_constant.BARRELE_DOWNLOAD_DEPENDENT_RPMS
    return 0


class CoralBarrelePlugin(build_common.CoralPluginType):
    """
    Barrele Plugin
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        super().__init__("barrele",
                         BARRELEYE_BUILD_DEPENDENT_PIPS,
                         is_devel=False,
                         need_collectd=True)

    def cpt_build_dependent_rpms(self, distro):
        """
        Return the RPMs needed to install before building
        """
        if distro == ssh_host.DISTRO_RHEL7:
            return COLLECTD_BUILD_DEPENDENT_RHEL7_RPMS
        if distro == ssh_host.DISTRO_RHEL8:
            return COLLECTD_BUILD_DEPENDENT_RHEL8_RPMS
        return None

    def cpt_build(self, log, workspace, local_host, source_dir, target_cpu,
                  type_cache, iso_cache, packages_dir, extra_iso_fnames,
                  extra_package_fnames, extra_rpm_names, option_dict):
        """
        Build the plugin
        """
        # pylint: disable=unused-argument,no-self-use
        collectd = option_dict["collectd"]
        ret = build_barreleye(log, workspace, local_host, type_cache,
                              target_cpu, iso_cache, packages_dir, collectd,
                              extra_iso_fnames, extra_package_fnames,
                              extra_rpm_names)
        if ret:
            log.cl_error("failed to build Barreleye")
            return -1
        return 0


class CoralBarreleCommand():
    """
    Commands to build Collectd.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._cbc_log_to_file = log_to_file

    def urls(self):
        """
        Print the URLs to download dependency packages.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        for package, url in PACAKGE_URL_DICT.items():
            log.cl_stdout("%s: %s", package, url)
        cmd_general.cmd_exit(log, 0)


class CoralCollectdCommand():
    """
    Commands for building Barreleye plugin.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._ccc_log_to_file = log_to_file

    def build(self, collectd=None):
        """
        Build Collectd.
        :param collectd: The Collectd source codes.
            Default: https://github.com/LiXi-storage/collectd/releases/$latest.
            A local source dir or .tar.bz2 generated by "make dist" of Collectd
            can be specified if modification to Collectd is needed.
        """
        # pylint: disable=too-many-locals
        source_dir = os.getcwd()
        identity = build_common.get_build_path()
        logdir_is_default = True
        log, workspace = cmd_general.init_env_noconfig(source_dir,
                                                       self._ccc_log_to_file,
                                                       logdir_is_default,
                                                       identity=identity)
        local_host = ssh_host.get_local_host(ssh=False)
        if collectd is not None:
            collectd = cmd_general.check_argument_fpath(log, local_host, collectd)

        shared_cache = constant.CORAL_BUILD_CACHE
        type_fname = constant.CORAL_BUILD_CACHE_TYPE_DEVEL
        # Shared cache for this build type
        shared_type_cache = shared_cache + "/" + type_fname
        type_cache = workspace + "/" + type_fname
        iso_cache = type_cache + "/" + constant.ISO_CACHE_FNAME
        # Directory path of package under ISO cache
        packages_dir = iso_cache + "/" + constant.BUILD_PACKAGES
        # Extra RPM file names under package directory
        extra_package_fnames = []

        command = ("mkdir -p %s" % (packages_dir))
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            cmd_general.cmd_exit(log, -1)

        ret = build_common.get_shared_build_cache(log, local_host, workspace,
                                                  shared_type_cache)
        if ret:
            log.cl_error("failed to get shared build cache")
            cmd_general.cmd_exit(log, -1)

        target_cpu = local_host.sh_target_cpu(log)
        if target_cpu is None:
            log.cl_error("failed to get the target cpu on host [%s]",
                         local_host.sh_hostname)
            cmd_general.cmd_exit(log, -1)

        rc = build_collectd(log, workspace, local_host, type_cache,
                            target_cpu, packages_dir, collectd,
                            extra_package_fnames)
        if rc:
            log.cl_error("failed to build Collectd RPMs")
            cmd_general.cmd_exit(log, -1)
        cmd_general.cmd_exit(log, 0)


build_common.coral_command_register("barrele", CoralBarreleCommand())
build_common.coral_command_register("collectd", CoralCollectdCommand())
build_common.coral_plugin_register(CoralBarrelePlugin())
