"""
Commands to download E2fsprogs releases from website.
"""
import os
from pycoral import lustre
from pycoral import clog
from pycoral import cmd_general
from pycoral import ssh_host
from pycoral import constant
from pybuild import build_common
from pyreaf import reaf_release_common

E2FSPROGS_RELEASES_DIRNAME = "e2fsprogs_releases"


class CoralE2fsprogsCommand():
    """
    Commands to download E2fsprogs releases from website.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._cec_log_to_file = log_to_file

    def use(self, release
            ):
        """
        Use a E2fsprogs release to a dir as the build cache.
        """
        # pylint: disable=no-self-use,too-many-locals
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        is_local, full_name, short_name = \
            reaf_release_common.parse_release_argument(log, release)
        local_host = ssh_host.get_local_host(ssh=False)
        source_dir = os.getcwd()
        release_dict = \
            reaf_release_common.get_full_lustre_release_dict(log, local_host, source_dir)
        if release_dict is None:
            cmd_general.cmd_exit(log, -1)
        if full_name not in release_dict:
            log.cl_error("release [%s] does not exists", full_name)
            cmd_general.cmd_exit(log, -1)

        release_obj = release_dict[full_name]
        cache_type = constant.CORAL_BUILD_CACHE_TYPE_OPEN
        cache_type_dir = (constant.CORAL_BUILD_CACHE + "/" + cache_type)
        iso_cache_dir = cache_type_dir + "/" + constant.ISO_CACHE_FNAME
        if is_local:
            source_release_dirs = source_dir + "/" + constant.LUSTRE_RELEASES_DIRNAME
        else:
            source_release_dirs = constant.LUSTRE_RELEASES_DIR
        source_release_dir = source_release_dirs + "/" + short_name
        release_dir = iso_cache_dir + "/" + constant.CORAL_E2FSPROGS_RELEASE_BASENAME
        rc = release_obj.rli_files_send(log, local_host, source_release_dir,
                                        local_host, release_dir)
        if rc == 0:
            log.cl_info("use [%s] as E2fsprogs for [%s]", release, cache_type)
        cmd_general.cmd_exit(log, rc)

    def using(self):
        """
        Print the E2fsprogs release that the build cache is using.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        local_host = ssh_host.get_local_host(ssh=False)
        source_dir = os.getcwd()
        release_status_list = []
        for cache_type in constant.CORAL_BUILD_CACHE_TYPES:
            cache_type_dir = (constant.CORAL_BUILD_CACHE + "/" + cache_type)
            iso_cache_dir = cache_type_dir + "/" + constant.ISO_CACHE_FNAME
            release_dir = iso_cache_dir + "/" + constant.CORAL_E2FSPROGS_RELEASE_BASENAME
            rinfo = lustre.e2fsprogs_read_release_info(log, local_host,
                                                       None,  # workspace
                                                       local_host,
                                                       release_dir)
            release_status = reaf_release_common.ReleaseStatusCache(local_host,
                                                                    cache_type,
                                                                    rinfo,
                                                                    release_dir)
            release_status_list.append(release_status)
        rc = reaf_release_common.print_release_infos(log, source_dir,
                                                     release_status_list, status=True)
        cmd_general.cmd_exit(log, rc)

build_common.coral_command_register("e2fsprogs", CoralE2fsprogsCommand())
