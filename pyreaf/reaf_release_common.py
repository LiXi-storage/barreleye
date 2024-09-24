"""
Common library for release info.
"""
from pycoral import clog
from pycoral import parallel
from pycoral import cmd_general
from pycoral import constant
from pycoral import release_info

RIC_FIELD_COMPLETE = "Complete"
RIC_FIELD_DISTRO_SHROT = "Distro Short"
RIC_FIELD_RELEASE = "Release"
RIC_FIELD_RELATIVE_FPATH = "Path"
RIC_FIELD_SHA1SUM = "Sha1 Checksum"
RIC_FIELD_TARGET_CPU = "Target CPU"
RIC_FIELD_URL = "URL"
RIC_FIELD_VERSION = "Version"

RIC_STATUS_COMPLETE = "complete"
RIC_STATUS_INCOMPLETE = "incomplete"
RIC_STATUS_UNKNOWN = "unknown"


class ReleaseStatusCache():
    """
    This object saves temporary status of a release.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, local_host, release_name, release, release_dir):
        # Host to run command.
        self.rsc_local_host = local_host
        # Any failure when getting this cached status
        self.rsc_failed = False
        # ReleaseInfo. Can be None if failed to read relase info.
        self.rsc_release = release
        # Whether the release has been complete
        self.rsc_complete = None
        # Name of the release
        self.rsc_release_name = release_name
        # Dir of the release
        self.rsc_release_dir = release_dir

    def rsc_init_fields(self, log, field_names):
        """
        Init fields
        """
        for field_name in field_names:
            if field_name == RIC_FIELD_RELEASE:
                continue
            if field_name == RIC_FIELD_VERSION:
                continue
            if field_name == RIC_FIELD_DISTRO_SHROT:
                continue
            if field_name == RIC_FIELD_TARGET_CPU:
                continue
            if field_name == RIC_FIELD_COMPLETE:
                release = self.rsc_release
                if release is None:
                    self.rsc_failed = True
                    self.rsc_complete = -1
                    continue
                if release.rli_file_dict is None or self.rsc_release_dir is None:
                    continue
                complete = release.rli_files_are_complete(log, self.rsc_local_host,
                                                          self.rsc_release_dir)
                if complete < 0:
                    self.rsc_failed = True
                self.rsc_complete = complete
            else:
                log.cl_error("unknown field [%s]", field_name)
                return -1
        return 0

    def rsc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        ret = 0
        release = self.rsc_release
        if field_name == RIC_FIELD_RELEASE:
            result = self.rsc_release_name
        elif field_name == RIC_FIELD_VERSION:
            if release is None or release.rli_version is None:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               constant.CMD_MSG_NONE)
            else:
                result = release.rli_version
        elif field_name == RIC_FIELD_DISTRO_SHROT:
            if release is None or release.rli_distro_short is None:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               constant.CMD_MSG_NONE)
            else:
                result = release.rli_distro_short
        elif field_name == RIC_FIELD_TARGET_CPU:
            if release is None or release.rli_target_cpu is None:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               constant.CMD_MSG_NONE)
            else:
                result = release.rli_target_cpu
        elif field_name == RIC_FIELD_COMPLETE:
            if release is None:
                result = clog.ERROR_MSG
            elif release.rli_file_dict is None or self.rsc_release_dir  is None:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               RIC_STATUS_UNKNOWN)
            elif self.rsc_complete is None:
                log.cl_error("complete status is not inited for release")
                result = clog.ERROR_MSG
                ret = -1
            elif self.rsc_complete < 0:
                result = clog.ERROR_MSG
            elif self.rsc_complete:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               RIC_STATUS_COMPLETE)
            else:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               RIC_STATUS_INCOMPLETE)
        else:
            log.cl_error("unknown field [%s] of release", field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.rsc_failed:
            ret = -1
        return ret, result

    def rsc_can_skip_init_fields(self, field_names):
        """
        Whether rsc_init_fields can be skipped
        """
        # pylint: disable=no-self-use,too-many-branches
        fields = field_names[:]
        if RIC_FIELD_VERSION in fields:
            fields.remove(RIC_FIELD_VERSION)
        if RIC_FIELD_DISTRO_SHROT in fields:
            fields.remove(RIC_FIELD_DISTRO_SHROT)
        if RIC_FIELD_TARGET_CPU in fields:
            fields.remove(RIC_FIELD_TARGET_CPU)
        if len(fields) == 0:
            return True
        return False


def release_status_init(log, workspace, release_status, field_names):
    """
    Init status of a release
    """
    # pylint: disable=unused-argument
    return release_status.rsc_init_fields(log, field_names)


def release_status_field(log, release_status, field_name):
    """
    Return (0, result) for a field of ReleaseStatusCache
    """
    # pylint: disable=unused-argument
    return release_status.rsc_field_result(log, field_name)


def print_release_infos(log, workspace, release_status_list, status=False,
                        print_table=True, field_string=None):
    """
    Print table of release infos
    """
    # pylint: disable=too-many-locals
    if not print_table and len(release_status_list) > 1:
        log.cl_error("failed to print non-table output with multiple releases")
        return -1

    quick_fields = [RIC_FIELD_RELEASE,
                    RIC_FIELD_VERSION]
    slow_fields = [RIC_FIELD_COMPLETE]
    none_table_fields = [RIC_FIELD_DISTRO_SHROT,
                         RIC_FIELD_TARGET_CPU]
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for release",
                     field_string)
        return -1

    if (len(release_status_list) > 0 and
            not release_status_list[0].rsc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        for release_status in release_status_list:
            args = (release_status, field_names)
            args_array.append(args)
            thread_id = "release_status_%s" % release_status.rsc_release_name
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(workspace,
                                                    "release_status",
                                                    release_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for agents",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, release_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                release_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    return rc


class DownloadFileStatusCache():
    """
    This object saves temporary status of a download file.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, local_host, release_name, release, release_dir, download_file):
        # Instance
        self.dfsc_local_host = local_host
        # Any failure when getting this cached status
        self.dfsc_failed = False
        # ReleaseInfo
        self.dfsc_release = release
        # Return value of checking the file has expected checksum.
        self.dfsc_checksum_ret = None
        # Name of the release
        self.dfsc_release_name = release_name
        # CoralDownloadFile
        self.dfsc_download_file = download_file
        # Dir to save the release
        self.dfsc_release_dir = release_dir

    def dfsc_init_fields(self, log, field_names):
        """
        Init fields
        """
        download_dir = self.dfsc_release_dir
        for field_name in field_names:
            if field_name == RIC_FIELD_RELATIVE_FPATH:
                continue
            if field_name == RIC_FIELD_URL:
                continue
            if field_name == RIC_FIELD_COMPLETE:
                ret = self.dfsc_download_file.cdf_check_file(log, self.dfsc_local_host,
                                                             download_dir)
                if ret < 0:
                    self.dfsc_failed = True
                self.dfsc_checksum_ret = ret
            else:
                log.cl_error("unknown field [%s]", field_name)
                return -1
        return 0

    def dfsc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches,too-many-statements
        ret = 0
        download_file = self.dfsc_download_file
        if field_name == RIC_FIELD_RELATIVE_FPATH:
            result = download_file.cdf_relative_fpath
        elif field_name == RIC_FIELD_URL:
            result = download_file.cdf_download_url
        elif field_name == RIC_FIELD_SHA1SUM:
            result = download_file.cdf_sha1sum
        elif field_name == RIC_FIELD_COMPLETE:
            if self.dfsc_checksum_ret is None:
                log.cl_error("complete status is not inited for file")
                result = clog.ERROR_MSG
                ret = -1
            elif self.dfsc_checksum_ret < 0:
                result = clog.colorful_message(clog.COLOR_YELLOW,
                                               RIC_STATUS_INCOMPLETE)
            else:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               RIC_STATUS_COMPLETE)
        else:
            log.cl_error("unknown field [%s] of release", field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.dfsc_failed:
            ret = -1
        return ret, result

    def dfsc_can_skip_init_fields(self, field_names):
        """
        Whether dfsc_init_fields can be skipped
        """
        # pylint: disable=no-self-use,too-many-branches
        fields = field_names[:]

        if RIC_FIELD_RELATIVE_FPATH in fields:
            fields.remove(RIC_FIELD_RELATIVE_FPATH)
        if RIC_FIELD_URL in fields:
            fields.remove(RIC_FIELD_URL)
        if RIC_FIELD_SHA1SUM in fields:
            fields.remove(RIC_FIELD_SHA1SUM)
        if len(fields) == 0:
            return True
        return False


def download_file_status_init(log, workspace, download_file_status, field_names):
    """
    Init status of a download file
    """
    # pylint: disable=unused-argument
    return download_file_status.dfsc_init_fields(log, field_names)


def download_file_status_field(log, download_file_status, field_name):
    """
    Return (0, result) for a field of DownloadFileStatusCache
    """
    # pylint: disable=unused-argument
    return download_file_status.dfsc_field_result(log, field_name)


def print_release_files(log, workspace, download_file_status_list, status=False,
                        print_table=True, field_string=None):
    """
    Print table of download_files
    """
    # pylint: disable=too-many-locals
    if not print_table and len(download_file_status_list) > 1:
        log.cl_error("failed to print non-table output with multiple files")
        return -1

    quick_fields = [RIC_FIELD_RELATIVE_FPATH]
    slow_fields = [RIC_FIELD_COMPLETE]
    none_table_fields = [RIC_FIELD_URL]
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for file",
                     field_string)
        return -1

    if (len(download_file_status_list) > 0 and
            not download_file_status_list[0].dfsc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        index = 0
        for download_file_status in download_file_status_list:
            release_name = download_file_status.dfsc_release_name
            args = (download_file_status, field_names)
            args_array.append(args)
            thread_id = ("download_file_status_%s.%s" %
                         (release_name, index))
            thread_ids.append(thread_id)
            index += 1

        parallel_execute = parallel.ParallelExecute(workspace,
                                                    "download_file_status",
                                                    download_file_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for agents",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, download_file_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                download_file_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    return rc


def get_lustre_release_dict(log, local_host, releases_dir, is_local=False):
    """
    Return a dict of Lustre release info. Key is release name, value is ReleaseInfo().
    """
    return release_info.get_release_info_dict(log, local_host, releases_dir,
                                              constant.LUSTRE_RELEASE_INFO_FNAME,
                                              is_local=is_local)


def get_full_lustre_release_dict(log, local_host, cwd):
    """
    Return global and local release dict
    """
    releases_dir = cwd + "/" + constant.LUSTRE_RELEASES_DIRNAME
    release_dict = get_lustre_release_dict(log, local_host, releases_dir,
                                           is_local=True)
    if release_dict is None:
        log.cl_error("failed to get local Lustre release dict")
        return None
    releases_dir = constant.LUSTRE_RELEASES_DIR
    global_release_dict = get_lustre_release_dict(log, local_host, releases_dir,
                                                  is_local=False)
    if global_release_dict is None:
        log.cl_error("failed to get global Lustre release dict")
        return None
    for name, value in global_release_dict.items():
        release_dict[name] = value
    return release_dict


def parse_full_release_name(log, full_name):
    """
    Parse full release name with local and global.
    """
    fields = full_name.split("/")
    if len(fields) != 2:
        log.cl_error("invalid release name [%s]", full_name)
        cmd_general.cmd_exit(log, -1)
    global_local = fields[0]
    short_name = fields[1]
    if global_local == release_info.RELEASE_NAME_LOCAL:
        is_local = True
    elif global_local == release_info.RELEASE_NAME_GLOBAL:
        is_local = False
    else:
        log.cl_error("missing global/local in release name [%s]", full_name)
        cmd_general.cmd_exit(log, -1)

    ret = cmd_general.check_name_is_valid(short_name)
    if ret:
        log.cl_error("invalid [%s] in release name [%s]",
                     short_name, full_name)
        cmd_general.cmd_exit(log, -1)
    return is_local, full_name, short_name


def parse_release_argument(log, release):
    """
    Parse release argument
    """
    full_name = cmd_general.check_argument_str(log, "release", release)
    return parse_full_release_name(log, full_name)


def get_e2fsprogs_release_dict(log, local_host, releases_dir, is_local=False):
    """
    Return a dict of E2fsprogs release info. Key is release name, value is ReleaseInfo().
    """
    return release_info.get_release_info_dict(log, local_host, releases_dir,
                                              constant.E2FSPROGS_RELEASE_INFO_FNAME,
                                              is_local=is_local)


def get_full_e2fsprogs_release_dict(log, local_host, cwd):
    """
    Return global and local release dict
    """
    releases_dir = cwd + "/" + constant.E2FSPROGS_RELEASES_DIRNAME
    release_dict = get_e2fsprogs_release_dict(log, local_host, releases_dir,
                                              is_local=True)
    if release_dict is None:
        return None
    releases_dir = constant.E2FSPROGS_RELEASES_DIR
    global_release_dict = get_e2fsprogs_release_dict(log, local_host, releases_dir,
                                                     is_local=False)
    if global_release_dict is None:
        return None
    for name, value in global_release_dict.items():
        release_dict[name] = value
    return release_dict
