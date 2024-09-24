"""
Coral Reaf command entrance
"""
# pylint: disable=too-many-lines
import os
from fire import Fire
from pycoral import os_distro
from pycoral import ssh_host
from pycoral import cmd_general
from pycoral import lustre_version
from pycoral import lustre
from pycoral import release_info
from pycoral import constant
from pycoral import clog
from pyreaf import reaf_constant
from pyreaf import reaf_release_common


def read_release_rinfo(log, local_host, is_local, full_name, short_name,
                       cwd, is_lustre=False):
    """
    Read rinfo from a file.
    """
    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    if is_lustre:
        info_fname = constant.LUSTRE_RELEASE_INFO_FNAME
    else:
        info_fname = constant.E2FSPROGS_RELEASE_INFO_FNAME

    info_fpath = releases_dir + "/" + short_name + "/" + info_fname
    ret = local_host.sh_path_exists(log, info_fpath)
    if ret < 0:
        log.cl_error("failed to check whether file [%s] exists on "
                     "host [%s]", info_fpath, local_host.sh_hostname)
        return None
    if ret == 0:
        log.cl_error("release [%s] does not exist",
                     full_name)
        return None
    rinfo = release_info.rinfo_read_from_local_file(log, local_host, info_fpath)
    if rinfo is None:
        return None

    if is_lustre:
        info_fname = constant.LUSTRE_RELEASE_INFO_FNAME
    else:
        info_fname = constant.E2FSPROGS_RELEASE_INFO_FNAME

    rinfo.rli_extra_relative_fpaths.append(info_fname)
    return rinfo


def lustre_refresh_release_info(log, local_host, release_dir):
    """
    Rerefresh the release info of Lustre.
    """
    return release_info.refresh_release_info(log, local_host, release_dir,
                                             constant.LUSTRE_RELEASE_INFO_FNAME)


def e2fsprogs_refresh_release_info(log, local_host, release_dir):
    """
    Rerefresh the release info of E2fsprogs.
    """
    return release_info.refresh_release_info(log, local_host, release_dir,
                                             constant.E2FSPROGS_RELEASE_INFO_FNAME)


def print_release_files(release, status=False, is_lustre=False):
    """
    Print the files of a release.
    """
    # pylint: disable=too-many-locals
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    is_local, full_name, short_name = \
        reaf_release_common.parse_release_argument(log, release)
    cmd_general.check_argument_bool(log, "status", status)
    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()

    rinfo = read_release_rinfo(log, local_host, is_local, full_name, short_name,
                               cwd, is_lustre=is_lustre)
    if rinfo is None:
        cmd_general.cmd_exit(log, -1)

    if rinfo.rli_file_dict is None:
        log.cl_error("files of release [%s] has not been configured", release)
        cmd_general.cmd_exit(log, -1)
    download_file_status_list = []
    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    release_dir = releases_dir + "/" + short_name
    for download_file in rinfo.rli_file_dict.values():
        download_file_status = \
            reaf_release_common.DownloadFileStatusCache(local_host,
                                                        full_name,
                                                        rinfo,
                                                        release_dir,
                                                        download_file)
        download_file_status_list.append(download_file_status)
    rc = reaf_release_common.print_release_files(log, cwd,
                                                 download_file_status_list,
                                                 status=status)
    cmd_general.cmd_exit(log, rc)


def print_release_list(status=False, is_lustre=True):
    """
    Print release list, either lustre or e2fsprogs.
    """
    # pylint: disable=too-many-locals
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    cmd_general.check_argument_bool(log, "status", status)
    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()
    if is_lustre:
        release_dict = reaf_release_common.get_full_lustre_release_dict(log, local_host, cwd)
    else:
        release_dict = reaf_release_common.get_full_e2fsprogs_release_dict(log, local_host, cwd)
    if release_dict is None:
        cmd_general.cmd_exit(log, -1)
    release_status_list = []
    for release_name in release_dict:
        release = release_dict[release_name]
        is_local, _, short_name = \
            reaf_release_common.parse_full_release_name(log, release_name)
        release_dir = get_release_dir(is_local, short_name, cwd, is_lustre=is_lustre)
        release_status = reaf_release_common.ReleaseStatusCache(local_host,
                                                                release_name,
                                                                release,
                                                                release_dir)
        release_status_list.append(release_status)
    rc = reaf_release_common.print_release_infos(log, cwd,
                                                 release_status_list,
                                                 print_table=True,
                                                 status=status)
    cmd_general.cmd_exit(log, rc)


def print_release_status(release, is_lustre=True):
    """
    Command of printing a status of E2fsprogs/Lustre
    """
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    is_local, full_name, short_name = \
        reaf_release_common.parse_release_argument(log, release)

    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()
    rinfo = read_release_rinfo(log, local_host, is_local, full_name, short_name,
                               cwd, is_lustre=is_lustre)
    if rinfo is None:
        cmd_general.cmd_exit(log, -1)
    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    release_dir = releases_dir + "/" + short_name
    release_status = reaf_release_common.ReleaseStatusCache(local_host, full_name,
                                                            rinfo, release_dir)
    rc = reaf_release_common.print_release_infos(log, cwd, [release_status],
                                                 status=True,
                                                 print_table=False)
    cmd_general.cmd_exit(log, rc)


def release_download(release, is_lustre):
    """
    Download the files of a Lustre release from URLs.
    :param release: Release name of Lustre.
    """
    # pylint: disable=no-self-use
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)

    is_local, full_name, short_name = \
        reaf_release_common.parse_release_argument(log, release)

    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()

    rinfo = read_release_rinfo(log, local_host, is_local, full_name, short_name,
                               cwd, is_lustre=is_lustre)
    if rinfo is None:
        cmd_general.cmd_exit(log, -1)
    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    release_dir = releases_dir + "/" + short_name

    release_dir = releases_dir + "/" + short_name
    rc = rinfo.rli_files_download(log, local_host, release_dir)
    cmd_general.cmd_exit(log, rc)


def copy_release(source, dest, is_lustre=True, prune=False):
    """
    Copy a release.
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()

    source_is_local, source_full_name, source_short_name = \
        reaf_release_common.parse_full_release_name(log, source)
    dest_is_local, dest_full_name, dest_short_name = \
        reaf_release_common.parse_full_release_name(log, dest)
    source_releases_dir = get_releases_dir(source_is_local, cwd, is_lustre=is_lustre)
    dest_releases_dir = get_releases_dir(dest_is_local, cwd, is_lustre=is_lustre)
    source_rinfo = read_release_rinfo(log, local_host, source_is_local,
                                      source_full_name, source_short_name,
                                      cwd, is_lustre=is_lustre)
    if source_rinfo is None:
        cmd_general.cmd_exit(log, -1)

    if not is_lustre:
        if prune:
            log.cl_error("copy with prune is not supported for E2fsprogs")
            cmd_general.cmd_exit(log, -1)

    ret = check_release_exists(log, local_host, dest_is_local,
                               dest_short_name,
                               cwd, is_lustre=is_lustre)
    if ret < 0:
        cmd_general.cmd_exit(log, -1)
    if ret:
        log.cl_error("release [%s] already exists", dest)
        cmd_general.cmd_exit(log, -1)

    tmp_dest_release_dir = ("%s/.%s.%s" %
                            (dest_releases_dir, dest_short_name,
                             cmd_general.get_identity()))
    command = ("mkdir -p %s" % tmp_dest_release_dir)
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on local host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        cmd_general.cmd_exit(log, -1)

    source_release_dir = source_releases_dir + "/" + source_short_name
    dest_release_dir = dest_releases_dir + "/" + dest_short_name
    if prune:
        ret = lustre_release_copy_with_pruning(log, local_host,
                                               source_full_name,
                                               source_rinfo,
                                               source_release_dir,
                                               tmp_dest_release_dir)
    else:
        ret = source_rinfo.rli_release_extract(log, local_host,
                                               source_release_dir,
                                               tmp_dest_release_dir,
                                               ignore_missing=True)
    if ret:
        log.cl_error("failed to copy release [%s] to dir [%s]",
                     source_full_name, tmp_dest_release_dir)
        cmd_general.cmd_exit(log, -1)

    command = ("mv %s %s" % (tmp_dest_release_dir, dest_release_dir))
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on local host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        cmd_general.cmd_exit(log, -1)

    if prune:
        log.cl_info("release [%s] copied to [%s] with pruning",
                    source_full_name, dest_full_name)
    else:
        log.cl_info("release [%s] copied to [%s]",
                    source_full_name, dest_full_name)
    cmd_general.cmd_exit(log, 0)


def create_release(release_name, distro_short, target_cpu, version,
                   is_lustre=True):
    """
    Create a release.
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()
    is_local, _, short_name = \
        reaf_release_common.parse_full_release_name(log, release_name)
    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    if is_lustre:
        release_dict = reaf_release_common.get_lustre_release_dict(log, local_host,
                                                                   releases_dir,
                                                                   is_local=is_local)
    else:
        release_dict = reaf_release_common.get_e2fsprogs_release_dict(log, local_host,
                                                                      releases_dir,
                                                                      is_local=is_local)
    if release_dict is None:
        cmd_general.cmd_exit(log, -1)
    if release_name in release_dict:
        log.cl_error("release [%s] already exists", release_name)
        cmd_general.cmd_exit(log, -1)

    release_dir = releases_dir + "/" + short_name
    tmp_release_dir = ("%s/.%s.%s" %
                       (releases_dir, short_name, cmd_general.get_identity()))
    command = "mkdir %s" % tmp_release_dir
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on local host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        cmd_general.cmd_exit(log, -1)

    distro = os_distro.short_distro2standard(log, distro_short)
    if distro is None:
        cmd_general.cmd_exit(log, -1)

    target_cpu = cmd_general.check_argument_str(log, "target_cpu", target_cpu)
    ret = cmd_general.check_name_is_valid(target_cpu)
    if ret:
        log.cl_error("invalid CPU type [%s]", target_cpu)
        cmd_general.cmd_exit(log, -1)

    version = cmd_general.check_argument_str(log, "version", version)
    ret = cmd_general.check_name_is_valid(version)
    if ret:
        log.cl_error("invalid release version [%s]", version)
        cmd_general.cmd_exit(log, -1)

    if is_lustre:
        info_fname = constant.LUSTRE_RELEASE_INFO_FNAME
    else:
        info_fname = constant.E2FSPROGS_RELEASE_INFO_FNAME

    rinfo = release_info.ReleaseInfo()
    rinfo.rli_extra_relative_fpaths.append(info_fname)
    rinfo.rli_version = version
    rinfo.rli_distro_short = distro_short
    rinfo.rli_target_cpu = target_cpu
    rinfo.rli_file_dict = {}

    info_fpath = tmp_release_dir + "/" + info_fname
    ret = rinfo.rli_save_to_local_file(log, info_fpath)
    if ret:
        log.cl_error("failed to save info file [%s]", info_fpath)
        cmd_general.cmd_exit(log, -1)

    command = ("mv %s %s" % (tmp_release_dir, release_dir))
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on local host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        cmd_general.cmd_exit(log, -1)

    cmd_general.cmd_exit(log, 0)


def update_release(release_name, distro_short=None, target_cpu=None,
                   version=None, is_lustre=True):
    """
    Update a release.
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()
    is_local, full_name, short_name = \
        reaf_release_common.parse_full_release_name(log, release_name)
    rinfo = read_release_rinfo(log, local_host, is_local, full_name, short_name,
                               cwd, is_lustre=is_lustre)
    if rinfo is None:
        cmd_general.cmd_exit(log, -1)
    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    if is_lustre:
        info_fname = constant.LUSTRE_RELEASE_INFO_FNAME
    else:
        info_fname = constant.E2FSPROGS_RELEASE_INFO_FNAME

    release_dir = releases_dir + "/" + short_name
    if (distro_short is None and target_cpu is None and version is None):
        log.cl_error("please specify what to update")
        cmd_general.cmd_exit(log, -1)

    if distro_short is not None:
        distro = os_distro.short_distro2standard(log, distro_short)
        if distro is None:
            cmd_general.cmd_exit(log, -1)
        rinfo.rli_distro_short = distro_short

    if target_cpu is not None:
        target_cpu = cmd_general.check_argument_str(log, "target_cpu", target_cpu)
        ret = cmd_general.check_name_is_valid(target_cpu)
        if ret:
            log.cl_error("invalid CPU type [%s]", target_cpu)
            cmd_general.cmd_exit(log, -1)
        rinfo.rli_target_cpu = target_cpu

    if version is not None:
        version = cmd_general.check_argument_str(log, "version", version)
        ret = cmd_general.check_name_is_valid(version)
        if ret:
            log.cl_error("invalid release version [%s]", version)
            cmd_general.cmd_exit(log, -1)
        rinfo.rli_version = version

    info_fpath = release_dir + "/" + info_fname
    ret = rinfo.rli_save_to_local_file(log, info_fpath)
    if ret:
        log.cl_error("failed to save info file [%s]", info_fpath)
        cmd_general.cmd_exit(log, -1)
    cmd_general.cmd_exit(log, 0)


def get_releases_dir(is_local, cwd, is_lustre=True):
    """
    Return the releases dir.
    """
    if is_lustre:
        if is_local:
            return cwd + "/" + constant.LUSTRE_RELEASES_DIRNAME
        return constant.LUSTRE_RELEASES_DIR
    if is_local:
        return cwd + "/" + constant.E2FSPROGS_RELEASES_DIRNAME
    return constant.E2FSPROGS_RELEASES_DIR


def get_release_dir(is_local, short_name, cwd, is_lustre=True):
    """
    Return the dir of a release.
    """
    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    return releases_dir + "/" + short_name


def check_release_exists(log, local_host, is_local, short_name,
                         cwd, is_lustre=True):
    """
    Check whether release exists. Return -1 on error. Return 0 if not exist.
    Return 1 if exists.
    """
    release_dir = get_release_dir(is_local, short_name, cwd,
                                  is_lustre=is_lustre)
    ret = local_host.sh_path_exists(log, release_dir)
    if ret < 0:
        log.cl_error("failed to check whether dir [%s] exists on "
                     "host [%s]", release_dir, local_host.sh_hostname)
        return -1
    return ret


def remove_release(release_name, is_lustre=True):
    """
    Remove a release.
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()

    is_local, full_name, short_name = \
        reaf_release_common.parse_release_argument(log, release_name)
    ret = check_release_exists(log, local_host, is_local, short_name,
                               cwd, is_lustre=is_lustre)
    if ret < 0:
        cmd_general.cmd_exit(log, -1)
    if ret == 0:
        log.cl_error("release [%s] does not exist", full_name)
        cmd_general.cmd_exit(log, -1)

    release_dir = get_release_dir(is_local, short_name, cwd,
                                  is_lustre=is_lustre)

    command = "rm -fr %s" % release_dir
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on local host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        cmd_general.cmd_exit(log, -1)

    cmd_general.cmd_exit(log, 0)


def lustre_release_copy_with_pruning(log, local_host, source_full_name,
                                     rinfo, source_release_dir, dest_release_dir):
    """
    Return a pruned ReleaseInfo.
    """
    # pylint: disable=too-many-locals,too-many-branches
    if rinfo.rli_distro_short is None:
        log.cl_error("missing [%s] in release info file under [%s]",
                     release_info.RELEASE_INFO_DISTRO_SHORT,
                     source_release_dir)
        return -1

    if rinfo.rli_target_cpu is None:
        log.cl_error("missing [%s] in release info file under [%s]",
                     release_info.RELEASE_INFO_TARGET_CPU,
                     source_release_dir)
        return -1
    target_cpu = rinfo.rli_target_cpu

    relative_rpm_dir = lustre.lustre_get_relative_rpm_dir(target_cpu)

    definition_dir = constant.LUSTRE_VERSION_DEFINITION_DIR
    version_db = lustre_version.load_lustre_version_database(log, local_host,
                                                             definition_dir)
    if version_db is None:
        log.cl_error("failed to load version database")
        return -1

    rpm_files = []
    for relative_fpath, cdf in rinfo.rli_file_dict.items():
        if not relative_fpath.startswith(relative_rpm_dir):
            continue
        rpm_files.append(cdf.cdf_fname)

    lustre_rpm_version, lustre_rpm_dict = \
        version_db.lvd_match_version_from_rpms(log, rpm_files)
    if lustre_rpm_version is None:
        log.cl_error("failed to detect Lustre version from RPMs of release [%s]",
                     source_full_name)
        return -1
    log.cl_info("Lustre version [%s] detected from RPMs of release [%s]",
                lustre_rpm_version.lv_version_name,
                source_full_name)
    rpm_fnames = []
    for rpm_type, fname in lustre_rpm_dict.items():
        if rpm_type in (lustre_version.LVD_LUSTRE_OSD_ZFS,
                        lustre_version.LVD_LUSTRE_OSD_ZFS_MOUNT):
            continue
        rpm_fnames.append(fname)

    mofed_rpms = lustre.filter_mofed_rpm_fnames(log, rpm_files, ignore_missing=True)
    if mofed_rpms is None:
        log.cl_error("failed to filter the mofed rpms of release [%s]",
                     source_full_name)
        return -1
    rpm_fnames += mofed_rpms

    new_file_dict = {}
    for file in rinfo.rli_file_dict.values():
        relative_fpath = file.cdf_relative_fpath
        fname = os.path.basename(relative_fpath)
        if fname not in rpm_fnames:
            continue
        new_file_dict[relative_fpath] = file
    rinfo.rli_file_dict = new_file_dict
    rinfo.rli_extra_relative_fpaths = []
    ret = rinfo.rli_release_extract(log, local_host, source_release_dir,
                                    dest_release_dir,
                                    ignore_missing=True)
    if ret:
        log.cl_error("failed to extract lustre from [%s] to [%s]",
                     source_release_dir, dest_release_dir)
        return -1
    fpath = dest_release_dir + "/" + constant.LUSTRE_RELEASE_INFO_FNAME
    ret = rinfo.rli_save_to_local_file(log, fpath)
    if ret:
        log.cl_error("failed to save rinfo file [%s]",
                     fpath)
        return -1
    return 0


def import_releases(is_lustre=True):
    """
    Init the release.
    """
    local_host = ssh_host.get_local_host(ssh=False)
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    if is_lustre:
        rpm_realeases_fname = constant.LUSTRE_RELEASES_DIRNAME
    else:
        rpm_realeases_fname = constant.E2FSPROGS_RELEASES_DIRNAME
    rpm_releases_dir = reaf_constant.REAF_DIR + "/" + rpm_realeases_fname
    cwd = os.getcwd()

    rpm_release_names = local_host.sh_get_dir_fnames(log, rpm_releases_dir)
    if rpm_release_names is None:
        log.cl_error("failed to get fnames under directory [%s]",
                     rpm_releases_dir)
        return -1

    releases_dir = get_releases_dir(False, # is_local
                                    cwd, is_lustre=True)

    for rpm_release_name in rpm_release_names:
        full_name = release_info.RELEASE_NAME_GLOBAL + "/" + rpm_release_name
        ret = check_release_exists(log, local_host, False, # is_local
                                   rpm_release_name, # short_name
                                   cwd, is_lustre=is_lustre)
        if ret < 0:
            cmd_general.cmd_exit(log, -1)
        if ret:
            log.cl_info("release [%s] already exists, skipping",
                        full_name)
            continue
        command = ("cp -a %s/%s %s" %
                   (rpm_releases_dir, rpm_release_name, releases_dir))
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        log.cl_info("imported release [%s] from RPM", full_name)
    return 0


def add_release_file(release, relative_fpath, file=None, url=None,
                     is_lustre=True):
    """
    Add a file to release.
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    is_local, full_name, short_name = \
        reaf_release_common.parse_release_argument(log, release)

    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()
    rinfo = read_release_rinfo(log, local_host, is_local, full_name, short_name,
                               cwd, is_lustre=is_lustre)
    if rinfo is None:
        cmd_general.cmd_exit(log, -1)

    relative_fpath = cmd_general.check_argument_str(log, "relative_fpath",
                                                    relative_fpath)
    if relative_fpath in rinfo.rli_file_dict:
        log.cl_error("file [%s] already exists for release [%s]",
                     relative_fpath, full_name)
        cmd_general.cmd_exit(log, -1)

    if url is not None:
        url = cmd_general.check_argument_str(log, "url", url)

    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    if is_lustre:
        info_fname = constant.LUSTRE_RELEASE_INFO_FNAME
    else:
        info_fname = constant.E2FSPROGS_RELEASE_INFO_FNAME
    release_dir = releases_dir + "/" + short_name
    info_fpath = release_dir + "/" + info_fname

    if file is None:
        log.cl_error("please specify --file")
        cmd_general.cmd_exit(log, -1)
    if file == "download":
        if url is None:
            log.cl_error("please specify [--url] when using [--file download]")
            cmd_general.cmd_exit(log, -1)
        fpath = release_dir + "/" + relative_fpath
        fname = os.path.basename(fpath)
        target_dir = os.path.dirname(fpath)
        tmp_fpath = target_dir + "/." + fname + "." + cmd_general.get_identity()
        tmp_fname = os.path.basename(tmp_fpath)
        ret = local_host.sh_download_file(log, url, tmp_fpath,
                                          expected_checksum=None,
                                          output_fname=tmp_fname,
                                          download_anyway=True)
        if ret:
            log.cl_error("failed to download file")
            cmd_general.cmd_exit(log, -1)
        command = "mv %s %s" % (tmp_fpath, fpath)
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            cmd_general.cmd_exit(log, -1)
    else:
        file = cmd_general.check_argument_fpath(log, local_host, file)
        real_file = local_host.sh_real_path(log, file)
        if real_file is None:
            cmd_general.cmd_exit(log, -1)

        fpath = release_dir + "/" + relative_fpath
        real_fpath = local_host.sh_real_path(log, fpath)
        if real_fpath is None:
            cmd_general.cmd_exit(log, -1)

        if real_file != real_fpath:
            command = "cp %s %s" % (real_file, real_fpath)
            retval = local_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on local host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             local_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                cmd_general.cmd_exit(log, -1)

    sha1sum = local_host.sh_get_checksum(log, fpath, checksum_command="sha1sum")
    if sha1sum is None:
        log.cl_error("failed to calculate the checksum of file [%s] on "
                     "host [%s]", fpath, local_host.sh_hostname)
        cmd_general.cmd_exit(log, -1)

    download_file = release_info.CoralDownloadFile(relative_fpath,
                                                   sha1sum,
                                                   download_url=url)
    rinfo.rli_file_dict[relative_fpath] = download_file
    ret = rinfo.rli_save_to_local_file(log, info_fpath)
    if ret:
        log.cl_error("failed to save info file [%s]", info_fpath)
        cmd_general.cmd_exit(log, -1)

    log.cl_info("added file [%s] to release [%s]", relative_fpath,
                full_name)
    cmd_general.cmd_exit(log, 0)


def remove_release_file(release, relative_fpath, is_lustre=True):
    """
    Remove a file in a release.
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
    is_local, full_name, short_name = \
        reaf_release_common.parse_release_argument(log, release)

    local_host = ssh_host.get_local_host(ssh=False)
    cwd = os.getcwd()
    rinfo = read_release_rinfo(log, local_host, is_local, full_name, short_name,
                               cwd, is_lustre=is_lustre)
    if rinfo is None:
        cmd_general.cmd_exit(log, -1)

    relative_fpath = cmd_general.check_argument_str(log, "relative_fpath",
                                                    relative_fpath)
    if relative_fpath not in rinfo.rli_file_dict:
        log.cl_error("file [%s] does not exist for release [%s]",
                     relative_fpath, full_name)
        cmd_general.cmd_exit(log, -1)

    releases_dir = get_releases_dir(is_local, cwd, is_lustre=is_lustre)
    if is_lustre:
        info_fname = constant.LUSTRE_RELEASE_INFO_FNAME
    else:
        info_fname = constant.E2FSPROGS_RELEASE_INFO_FNAME

    info_fpath = releases_dir + "/" + short_name + "/" + info_fname

    del rinfo.rli_file_dict[relative_fpath]
    ret = rinfo.rli_save_to_local_file(log, info_fpath)
    if ret:
        log.cl_error("failed to save info file [%s]", info_fpath)
        cmd_general.cmd_exit(log, -1)

    log.cl_info("removed file [%s] from release [%s]", relative_fpath,
                full_name)
    cmd_general.cmd_exit(log, 0)


class CoralLustreCommand():
    """
    Commands to manage Lustre releases.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, logdir, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._clc_logdir = logdir
        self._clc_log_to_file = log_to_file

    def imp(self):
        """
        Import the global Lustre releases provided by Reaf RPM.
        """
        # pylint: disable=no-self-use
        import_releases(is_lustre=True)

    def create(self, release, distro_short, target_cpu, version):
        """
        Create a Lustre release.
        :param release: Name of the Lustre release to create.
        :param distro_short: Short OS distributor, e.a. el7 or el8.
        :param target_cpu: CPU type, e.g. x86_64 or aarch64.
        :param version: The E2fsprogs version.
        """
        # pylint: disable=no-self-use
        create_release(release, distro_short, target_cpu, version,
                       is_lustre=True)

    def update(self, release, distro_short=None, target_cpu=None, version=None):
        """
        Update the metadata of a Lustre release.
        :param release: Name of the Lustre release to create.
        :param distro_short: Short OS distributor to change to, e.a. el7 or el8.
        :param target_cpu: CPU type to change to, e.g. x86_64 or aarch64.
        :param version: The Lustre version to change to.
        """
        # pylint: disable=no-self-use
        update_release(release, distro_short=distro_short,
                       target_cpu=target_cpu, version=version, is_lustre=True)

    def remove(self, release):
        """
        Remove a Lustre release.
        :param release: Name of the Lustre release to remove.
        """
        # pylint: disable=no-self-use
        remove_release(release, is_lustre=True)

    def detect_version(self, directory):
        """
        Detect the Lustre version from RPM names.
        :param directory: Directory path that saves Lustre RPMs.
        """
        logdir_is_default = bool(self._clc_logdir == reaf_constant.REAF_LOG_DIR)
        log, _ = cmd_general.init_env_noconfig(self._clc_logdir,
                                               self._clc_log_to_file,
                                               logdir_is_default)
        local_host = ssh_host.get_local_host(ssh=False)
        fnames = local_host.sh_get_dir_fnames(log, directory)
        if fnames is None:
            log.cl_error("failed to get fnames under directory [%s]",
                         directory)
            cmd_general.cmd_exit(log, -1)
        definition_dir = constant.LUSTRE_VERSION_DEFINITION_DIR
        version_db = lustre_version.load_lustre_version_database(log, local_host,
                                                                 definition_dir)
        if version_db is None:
            log.cl_error("failed to load version database")
            cmd_general.cmd_exit(log, -1)
        version, _ = version_db.lvd_match_version_from_rpms(log,
                                                            fnames,
                                                            skip_kernel=True,
                                                            skip_test=True)
        if version is None:
            version, _ = version_db.lvd_match_version_from_rpms(log,
                                                                fnames,
                                                                client=True)
            if version is None:
                log.cl_error("failed to match Lustre version according to RPM names")
                cmd_general.cmd_exit(log, -1)
            log.cl_stdout("Lustre client: %s", version.lv_version_name)
            cmd_general.cmd_exit(log, 0)
        log.cl_stdout("Lustre server: %s", version.lv_version_name)
        cmd_general.cmd_exit(log, 0)

    def ls(self, status=False):
        """
        Print the list of Lustre releases.
        :param status: Print the status of the agents. Default: False.
        """
        # pylint: disable=no-self-use
        print_release_list(status=status, is_lustre=True)

    def copy(self, source, dest, prune=False):
        """
        Copy a release.
        :param source: Lustre release name to copy from.
        :param dest: Lustre release name to copy to.
        :param prune: Skip copying some files of the release to reduce space usage.
        """
        # pylint: disable=no-self-use
        copy_release(source, dest, is_lustre=True, prune=prune)

    def status(self, release):
        """
        Print the status of a Lustre release.
        :param release: Release name of Lustre.
        """
        # pylint: disable=no-self-use,too-many-locals
        print_release_status(release, is_lustre=True)

    def download(self, release):
        """
        Download the files of a Lustre release from URLs.
        :param release: Release name of Lustre.
        """
        # pylint: disable=no-self-use
        release_download(release, is_lustre=True)

    def files(self, release, status=False):
        """
        Print the file list of a Lustre release.
        :param release: Release name of Lustre.
        :param status: Print the status of the agents. Default: False.
        """
        # pylint: disable=no-self-use
        print_release_files(release, status=status, is_lustre=True)

    def scan(self, dirpath):
        """
        Scan the dir to generate the Lustre release info file.
        :param dirpath: Directory path of Lustre release.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        local_host = ssh_host.get_local_host(ssh=False)
        cmd_general.check_argument_fpath(log, local_host, dirpath)
        dirpath = dirpath.rstrip('/')
        rc = lustre_refresh_release_info(log, local_host, dirpath)
        cmd_general.cmd_exit(log, rc)

    def file_add(self, release, relative_fpath, file=None, url=None):
        """
        Add a file to a Lustre release.
        :param release: Name of the Lustre release to add file to.
        :param relative_fpath: File path to add to the Lustre release.
        """
        # pylint: disable=no-self-use
        add_release_file(release, relative_fpath, file=file, url=url, is_lustre=True)

    def file_remove(self, release, relative_fpath):
        """
        Remove a file from a Lustre release.
        :param release: Name of the Lustre release to add file to.
        :param relative_fpath: File path to add to the Lustre release.
        """
        # pylint: disable=no-self-use
        remove_release_file(release, relative_fpath, is_lustre=True)


class CoralE2fsprogsCommand():
    """
    Commands to manage E2fsprogs releases.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, logdir, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._cec_logdir = logdir
        self._cec_log_to_file = log_to_file

    def create(self, release, distro_short, target_cpu, version):
        """
        Create an E2fsprogs release.
        :param release: Name of the E2fsprogs release to create.
        :param distro_short: Short OS distributor, e.a. el7 or el8.
        :param target_cpu: CPU type, e.g. x86_64 or aarch64.
        :param version: The E2fsprogs version.
        """
        # pylint: disable=no-self-use
        create_release(release, distro_short, target_cpu, version,
                       is_lustre=False)

    def update(self, release, distro_short=None, target_cpu=None, version=None):
        """
        Update information of an E2fsprogs release.
        :param release: Name of the E2fsprogs release to create.
        :param distro_short: Short OS distributor to change to, e.a. el7 or el8.
        :param target_cpu: CPU type to change to, e.g. x86_64 or aarch64.
        :param version: The E2fsprogs version to change to.
        """
        # pylint: disable=no-self-use
        update_release(release, distro_short=distro_short,
                       target_cpu=target_cpu, version=version,
                       is_lustre=False)

    def copy(self, source, dest):
        """
        Copy a release.
        :param source: E2fsprogs release name to copy from.
        :param dest: E2fsprogs release name to copy to.
        """
        # pylint: disable=no-self-use
        copy_release(source, dest, is_lustre=False)

    def imp(self):
        """
        Import the global E2fsprogs releases provided by Reaf RPM.
        """
        # pylint: disable=no-self-use
        import_releases(is_lustre=False)

    def remove(self, release):
        """
        Remove a release.
        :param release: Name of the E2fsprogs release to remove.
        """
        # pylint: disable=no-self-use
        remove_release(release, is_lustre=True)

    def ls(self, status=False):
        """
        Print the list of E2fsprogs releases.
        :param status: Print the status of the agents. Default: False.
        """
        # pylint: disable=no-self-use
        print_release_list(status=status, is_lustre=False)

    def status(self, release):
        """
        Print the status of an E2fsprogs release.
        :param release: Release name of E2fsprogs.
        """
        # pylint: disable=no-self-use
        print_release_status(release, is_lustre=False)

    def download(self, release):
        """
        Download the files of an E2fsprogs release from URLs.
        :param release: Release name of E2fsprogs.
        """
        # pylint: disable=no-self-use
        release_download(release, is_lustre=False)

    def files(self, release, status=False):
        """
        Print the file list of an E2fsprogs release.
        :param release: Release name of Lustre.
        :param status: Print the status of the agents. Default: False.
        """
        # pylint: disable=no-self-use
        print_release_files(release, status=status, is_lustre=False)

    def scan(self, dirpath):
        """
        Scan the dir to generate the E2fsprogs release info file.
        :param dirpath: Directory path of E2fsprogs release.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        local_host = ssh_host.get_local_host(ssh=False)
        cmd_general.check_argument_fpath(log, local_host, dirpath)
        dirpath = dirpath.rstrip('/')
        rc = e2fsprogs_refresh_release_info(log, local_host, dirpath)
        cmd_general.cmd_exit(log, rc)

    def file_add(self, release, relative_fpath, file=None, url=None):
        """
        Add a file to an E2fsprogs release.
        :param release: Name of the E2fsprogs release to add file to.
        :param relative_fpath: File path to add to the E2fsprogs release.
        """
        # pylint: disable=no-self-use
        add_release_file(release, relative_fpath, file=file, url=url, is_lustre=False)

    def file_remove(self, release, relative_fpath):
        """
        Remove a file from an E2fsprogs release.
        :param release: Name of the E2fsprogs release to add file to.
        :param relative_fpath: File path to add to the E2fsprogs release.
        """
        # pylint: disable=no-self-use
        remove_release_file(release, relative_fpath, is_lustre=False)


class CoralReafCommand():
    """
    The command line utility for Coral tools.
    :param log: Log directory. Default: /var/log/coral/ctool/${TIMESTAMP}.
    :param debug: Whether to dump debug logs into files. Default: False.
    :param iso: The ISO tarball to use for installation, default: None.
    """
    # pylint: disable=too-few-public-methods
    lustre = CoralLustreCommand()
    e2fsprogs = CoralE2fsprogsCommand()

    def __init__(self,
                 log=reaf_constant.REAF_LOG_DIR,
                 debug=False, iso=None):
        # pylint: disable=protected-access,unused-argument
        self._ctc_logdir = log
        self._ctc_log_to_file = debug
        self._ctc_iso = iso
        self.lustre._init(log, debug)
        self.e2fsprogs._init(log, debug)


def main():
    """
    Main routine of Coral tool commands
    """
    Fire(CoralReafCommand)
