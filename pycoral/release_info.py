"""
Library for release info file.
"""
# pylint: disable=too-many-lines
import os
import socket
from pycoral import coral_yaml
from pycoral import constant
from pycoral import cmd_general

RELEASE_INFO_DISTRO_SHORT = "distro_short"
RELEASE_INFO_FILES = "files"
RELEASE_INFO_RELATIVE_FPATH = "relative_fpath"
RELEASE_INFO_SHA1SUM = "sha1sum"
RELEASE_INFO_TARGET_CPU = "target_cpu"
RELEASE_INFO_DOWNLOAD_URL = "download_url"
RELEASE_INFO_VERSION = "version"


class CoralDownloadFile():
    """
    Save the information of downloading file from URL
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, relative_fpath, sha1sum, download_url=None):
        # URL to download
        self.cdf_download_url = download_url
        # Relative file path
        self.cdf_relative_fpath = relative_fpath
        # sha1sum of the file
        self.cdf_sha1sum = sha1sum
        # File name
        self.cdf_fname = os.path.basename(relative_fpath)
        # Relative fpath
        self.cdf_relative_dir = os.path.dirname(relative_fpath)

    def _cdf_fpath(self, release_dir):
        """
        Return the fpath
        """
        return release_dir + "/" + self.cdf_relative_fpath

    def cdf_check_file(self, log, host, release_dir, quiet=False):
        """
        Check whether the file is complete.
        """
        fpath = self._cdf_fpath(release_dir)
        return host.sh_check_checksum(log, fpath, self.cdf_sha1sum,
                                      checksum_command="sha1sum",
                                      quiet=quiet)

    def cdf_file_remove(self, log, host, release_dir):
        """
        Remove the file.
        """
        fpath = self._cdf_fpath(release_dir)
        command = "rm -f %s" % fpath
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def cdf_download_file(self, log, host, release_dir):
        """
        Download this file to a (possibly remote) host.
        """
        fpath = self._cdf_fpath(release_dir)
        ret = host.sh_download_file(log, self.cdf_download_url, fpath,
                                    expected_checksum=self.cdf_sha1sum,
                                    checksum_command="sha1sum")
        if ret:
            log.cl_error("failed to download file [%s]",
                         self.cdf_relative_fpath)
            return -1
        return 0

    def cdf_send_file(self, log, host, source_dir, dest_dir):
        """
        Send the file from local host to remote host.
        """
        source_fpath = self._cdf_fpath(source_dir)
        dest_fpath = self._cdf_fpath(dest_dir)
        parent_dir = os.path.dirname(dest_fpath)

        command = ("mkdir -p %s" % (parent_dir))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_info("sending file [%s] on local host [%s] to dir [%s] "
                    "on host [%s]",
                    source_fpath, socket.gethostname(), parent_dir,
                    host.sh_hostname)
        ret = host.sh_send_file(log, source_fpath, parent_dir)
        if ret:
            log.cl_error("failed to send file [%s] from local host to host [%s]",
                         source_fpath,
                         host.sh_hostname)
            return -1
        return 0

    def cdf_dump_config(self):
        """
        Return the config dict.
        """
        file_config = {}
        if self.cdf_download_url is not None:
            file_config[RELEASE_INFO_DOWNLOAD_URL] = self.cdf_download_url
        file_config[RELEASE_INFO_RELATIVE_FPATH] = self.cdf_relative_fpath
        file_config[RELEASE_INFO_SHA1SUM] = self.cdf_sha1sum
        return file_config

    def cdf_copy_or_hardlink(self, log, host, source_dir, dest_dir,
                             hardlink=True, ignore_missing=False):
        """
        Create a hardlink or copy from release dir to the target dir.
        """
        source_fpath = self._cdf_fpath(source_dir)
        dest_fpath = self._cdf_fpath(dest_dir)
        parent_dir = os.path.dirname(dest_fpath)

        ret = host.sh_path_exists(log, source_fpath)
        if ret < 0:
            log.cl_error("failed to check whether file [%s] exists "
                         "on host [%s]",
                         source_fpath,
                         host.sh_hostname)
            return -1
        if not ret:
            if ignore_missing:
                log.cl_info("file [%s] does not exist, ignoring", source_fpath)
                return 0
            log.cl_error("file [%s] does not exist on host [%s]",
                         source_fpath, host.sh_hostname)
            return -1

        command = ("mkdir -p %s" % (parent_dir))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if hardlink:
            command = "ln %s %s" % (source_fpath, parent_dir)
        else:
            command = "cp %s %s" % (source_fpath, parent_dir)
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0


def parse_file_config(log, file_config):
    """
    Parse the file config.
    """
    # pylint: disable=no-self-use
    if RELEASE_INFO_RELATIVE_FPATH not in file_config:
        log.cl_error("missing [%s] in file config",
                     RELEASE_INFO_RELATIVE_FPATH)
        return None
    relative_fpath = file_config[RELEASE_INFO_RELATIVE_FPATH]

    if RELEASE_INFO_DOWNLOAD_URL not in file_config:
        log.cl_debug("no [%s] in config of file [%s]",
                     RELEASE_INFO_DOWNLOAD_URL, relative_fpath)
        download_url = None
    else:
        download_url = file_config[RELEASE_INFO_DOWNLOAD_URL]

    if RELEASE_INFO_SHA1SUM not in file_config:
        log.cl_error("missing [%s] in file config",
                     RELEASE_INFO_SHA1SUM)
        return None
    sha1sum = file_config[RELEASE_INFO_SHA1SUM]
    return CoralDownloadFile(relative_fpath, sha1sum, download_url=download_url)


ACTION_IGNORE = "ignore"
ACTION_REMOVE = "remove"
ACTION_REPORT = "report"

class ReleaseInfo():
    """
    This instance saves the release information of Lustre/Coral/E2fsprogs/Other
    software.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self):
        # Short SHA of git
        self.rli_git_short_sha = None
        # Versio printed by "./cbuild version show"
        self.rli_version = None
        # Short distro, e.g. el7 or el8
        self.rli_distro_short = None
        # Target CPU, e.g. x86_64
        self.rli_target_cpu = None
        # The dict of files to download. Key: cdf_relative_fpath. Value: CoralDownloadFile.
        self.rli_file_dict = None
        # Extra fpaths expected under the release dir.
        self.rli_extra_relative_fpaths = []
        # Fpaths ignored when copying/packing the release dir.
        self.rli_nondist_relative_fpaths = []
        # Dir paths ignored when copying/packing the release dir.
        self.rli_nondist_relative_dir_paths = []

    def rli_release_extract(self, log, host, source_dir, dest_dir, ignore_missing=False):
        """
        Extract the files to a dir on the same host.
        """
        if self.rli_file_dict is None:
            log.cl_error("release files not specified")
            return -1

        same_fs = host.sh_same_filesystem(log, source_dir, dest_dir)
        if same_fs < 0:
            log.cl_error("failed to check whether dir [%s] and [%s] is on "
                         "the same file system on host [%s]",
                         source_dir, dest_dir, host.sh_hostname)
            return -1

        for file in self.rli_file_dict.values():
            ret = file.cdf_copy_or_hardlink(log, host, source_dir, dest_dir,
                                            hardlink=same_fs,
                                            ignore_missing=ignore_missing)
            if ret:
                log.cl_error("failed to copy file [%s] of release",
                             file.cdf_relative_fpath)
                return -1

        for relative_fpath in self.rli_extra_relative_fpaths:
            source_fpath = source_dir + "/" + relative_fpath
            dest_fpath = dest_dir + "/" + relative_fpath
            parent_dir = os.path.dirname(dest_fpath)

            ret = host.sh_path_exists(log, source_fpath)
            if ret < 0:
                log.cl_error("failed to check whether file [%s] exists "
                             "on host [%s]",
                             source_fpath,
                             host.sh_hostname)
                return -1
            if not ret:
                if ignore_missing:
                    log.cl_info("file [%s] does not exist, ignoring",
                                source_fpath)
                    continue
                log.cl_error("file [%s] does not exist on host [%s]",
                             source_fpath, host.sh_hostname)
                return -1

            command = ("mkdir -p %s" % (parent_dir))
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on local host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
            if same_fs:
                command = "ln %s %s" % (source_fpath, parent_dir)
            else:
                command = "cp %s %s" % (source_fpath, parent_dir)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on local host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def rli_files_download(self, log, host, release_dir):
        """
        Download the files.
        """
        if self.rli_file_dict is None:
            log.cl_error("download files not configured for release")
            return -1

        for file in self.rli_file_dict.values():
            ret = file.cdf_download_file(log, host, release_dir)
            if ret:
                log.cl_error("failed to download file")
                return -1
        complete = self.rli_files_are_complete(log, host, release_dir,
                                               ignore_unncessary_files=False)
        if complete < 0:
            return -1
        if complete == 0:
            return -1
        return 0

    def rli_files_send(self, log, local_host, source_dir,
                       host, dest_dir):
        """
        Send the files of the release from local host to remote host.
        """
        if self.rli_file_dict is None:
            log.cl_error("release files not specified")
            return -1

        complete = self.rli_files_are_complete(log, local_host, source_dir,
                                               ignore_unncessary_files=True)
        if complete < 0:
            log.cl_error("failed to check whether release [%s] is complete on "
                         "local host [%s]",
                         source_dir, local_host.sh_hostname)
            return -1
        if complete == 0:
            log.cl_error("incomplete release [%s] on local host [%s]",
                         source_dir, local_host.sh_hostname)
            return -1

        for file in self.rli_file_dict.values():
            ret = file.cdf_send_file(log, host, source_dir, dest_dir)
            if ret:
                log.cl_error("failed to send file [%s] from local host [%s] to host [%s]",
                             file.cdf_relative_fpath,
                             local_host.sh_hostname,
                             host.sh_hostname)
                return -1

        relative_fpaths = self.rli_extra_relative_fpaths
        for relative_fpath in relative_fpaths:
            fpath = source_dir + "/" + relative_fpath
            log.cl_info("sending file [%s] on local host [%s] to dir [%s] "
                        "on host [%s]",
                        fpath, socket.gethostname(), dest_dir,
                        host.sh_hostname)
            ret = host.sh_send_file(log, fpath, dest_dir)
            if ret:
                log.cl_error("failed to send file [%s] from local host [%s] to host [%s]",
                             fpath,
                             local_host.sh_hostname,
                             host.sh_hostname)
                return -1

        ret = self._rli_files_check(log, host, dest_dir,
                                    unncessary_files_action=ACTION_REMOVE)
        if ret < 0:
            log.cl_error("failed to check the files of release")
            return -1
        if ret == 0:
            log.cl_error("incomplete files of release")
            return -1
        return 0

    def rli_files_scan(self, log, host, release_dir):
        """
        Scan the files and fill rli_file_dict
        """
        ret = self._rli_files_check(log, host, release_dir,
                                    update_files=True)
        if ret < 0:
            log.cl_error("failed to check the update file information of release")
            return -1
        if ret == 0:
            log.cl_error("incomplete files of release")
            return -1
        return 0

    def rli_file_update_checksum(self, log, host, release_dir, relative_fpath):
        """
        Update the checksum of a file.
        """
        if relative_fpath not in self.rli_file_dict:
            log.cl_error("file [%s] is not included in release yet")
            return -1
        rfile = self.rli_file_dict[relative_fpath]
        fpath = release_dir + "/" + relative_fpath
        sha1sum = host.sh_get_checksum(log, fpath,
                                       checksum_command="sha1sum")
        if sha1sum is None:
            log.cl_error("failed to calculate checksum of file [%s]",
                         fpath)
            return -1
        rfile.cdf_sha1sum = sha1sum
        return 0

    def _rli_files_check(self, log, host, release_dir, update_files=False,
                         unncessary_files_action=ACTION_REPORT,
                         check_file_content=True):
        """
        Check whether the files are complete under the dir.
        Return 1 if true, return 0 if false.
        Return -1 on error.
        """
        # pylint: disable=no-self-use,too-many-locals,too-many-branches,too-many-statements
        # Files expected under the release directory, include:
        # 1) Extra files;
        # 2) None distributed files;
        # 3) Files saved in "files".
        expected_fpaths = []
        # Files that do not need to have neither checksum nor saved in "files", include:
        # 1) Extra files;
        # 2) None distributed files.
        no_checksum_fpaths = []
        # Demanded files, include:
        # 1) Extra files;
        # 2) Files saved in "files".
        demanded_fpath_dict = {}

        no_checksum_relative_fpaths = (self.rli_extra_relative_fpaths +
                                       self.rli_nondist_relative_fpaths)
        for no_checksum_relative_fpath in no_checksum_relative_fpaths:
            no_checksum_fpath = release_dir + "/" + no_checksum_relative_fpath
            expected_fpaths.append(no_checksum_fpath)
            no_checksum_fpaths.append(no_checksum_fpath)

        nondist_dir_paths = []
        for relative_dir_path in self.rli_nondist_relative_dir_paths:
            nondist_dir_paths.append(release_dir + "/" + relative_dir_path)

        for extra_relative_fpath in self.rli_extra_relative_fpaths:
            fpath = release_dir + "/" + extra_relative_fpath
            demanded_fpath_dict[fpath] = False

        if self.rli_file_dict is not None:
            for file in self.rli_file_dict.values():
                fpath = release_dir + "/" + file.cdf_relative_fpath
                expected_fpaths.append(fpath)
                demanded_fpath_dict[fpath] = False

        command = "find %s -type f" % release_dir
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if update_files:
            self.rli_file_dict = {}

        ret = 1
        release_dir_length = len(release_dir) + 1
        fpaths = retval.cr_stdout.splitlines()
        unexpected_fpaths = []
        for fpath in fpaths:
            under_nondist_dir = False
            for dir_path in nondist_dir_paths:
                dir_path += "/"
                if fpath.startswith(dir_path):
                    under_nondist_dir = True
                    break
            if under_nondist_dir:
                continue

            if fpath in demanded_fpath_dict:
                demanded_fpath_dict[fpath] = True

            if fpath in no_checksum_fpaths:
                continue

            if fpath not in expected_fpaths:
                unexpected_fpaths.append(fpath)
                if not update_files:
                    continue

            if not check_file_content and not update_files:
                continue

            relative_fpath = fpath[release_dir_length:]
            sha1sum = host.sh_get_checksum(log, fpath,
                                           checksum_command="sha1sum")
            if sha1sum is None:
                log.cl_error("failed to calculate checksum of file [%s]",
                             fpath)
                return -1
            if update_files:
                download_file = CoralDownloadFile(relative_fpath,
                                                  sha1sum)
                self.rli_file_dict[relative_fpath] = download_file
                continue
            if relative_fpath not in self.rli_file_dict:
                log.cl_error("failed to find file [%s] in release info",
                             relative_fpath)
                return -1
            download_file = self.rli_file_dict[relative_fpath]
            if download_file.cdf_sha1sum != sha1sum:
                log.cl_info("unxpected sha1sum [%s] of file [%s], expected [%s]",
                            sha1sum, fpath, download_file.cdf_sha1sum)
                ret = 0

        missing_demanded_fpaths = []
        for fpath, found in demanded_fpath_dict.items():
            if not found:
                missing_demanded_fpaths.append(fpath)
        if len(missing_demanded_fpaths) != 0:
            log.cl_info("missing files for release:")
            for missing_demanded_fpath in missing_demanded_fpaths:
                log.cl_info("    " + missing_demanded_fpath)
            ret = 0

        if unncessary_files_action == ACTION_REMOVE:
            for unexpected_fpath in unexpected_fpaths:
                log.cl_info("removing unnecessary file [%s] of release",
                            unexpected_fpath)
                command = "rm -f %s" % unexpected_fpath
                retval = host.sh_run(log, command)
                if retval.cr_exit_status:
                    log.cl_error("failed to run command [%s] on local host [%s], "
                                 "ret = [%d], stdout = [%s], stderr = [%s]",
                                 command,
                                 host.sh_hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1
            rc = host.sh_remove_empty_dirs(log, release_dir)
            if rc:
                log.cl_error("failed remove empty dirs under [%s]",
                             release_dir)
                return rc
            return ret
        if len(unexpected_fpaths) == 0:
            return ret
        if update_files:
            log.cl_info("files found for release:")
            for unexpected_fpath in unexpected_fpaths:
                log.cl_info("    " + unexpected_fpath)
            return ret
        if unncessary_files_action == ACTION_IGNORE:
            return ret
        log.cl_info("unnecessary files found for release:")
        for unexpected_fpath in unexpected_fpaths:
            log.cl_info("    " + unexpected_fpath)
            ret = 0
        return ret

    def rli_files_are_complete(self, log, host, release_dir,
                               ignore_unncessary_files=False,
                               check_file_content=True):
        """
        Check the files are complete.
        Return 1 if true, return 0 if false.
        Return -1 on error.
        """
        if self.rli_file_dict is None:
            log.cl_error("files not configured for release")
            return -1
        if ignore_unncessary_files:
            action = ACTION_IGNORE
        else:
            action = ACTION_REPORT
        ret = self._rli_files_check(log, host, release_dir,
                                    unncessary_files_action=action,
                                    check_file_content=check_file_content)
        return ret

    def rli_release_remove(self, log, host, release_dir):
        """
        Remove all the files.
        If force remove ignoring what are the contents under the release dir.
        """
        ret = self.rli_files_are_complete(log, host, release_dir,
                                          ignore_unncessary_files=False,
                                          check_file_content=False)
        if ret < 0:
            log.cl_error("failed to check whether release [%s] is complete on host [%s]",
                         release_dir, host.sh_hostname)
            return -1
        if ret == 0:
            log.cl_error("release [%s] is NOT complete on host [%s]",
                         release_dir, host.sh_hostname)
            return -1

        for download_file in self.rli_file_dict.values():
            ret = download_file.cdf_file_remove(log, host, release_dir)
            if ret:
                log.cl_error("failed to remove file [%s] of release [%s] on host [%s]",
                             download_file.cdf_fname, release_dir,
                             host.sh_hostname)
                return -1

        for relative_fpath in (self.rli_extra_relative_fpaths +
                               self.rli_nondist_relative_fpaths):
            command = "rm -f %s/%s" % (release_dir, relative_fpath)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on local host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        for dir_path in self.rli_nondist_relative_dir_paths:
            command = "rm -fr %s/%s" % (release_dir, dir_path)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on local host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        command = "rmdir %s" % (release_dir)
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _rli_parse_version(self, log, info):
        """
        Parse the version.
        """
        version = None
        if RELEASE_INFO_VERSION in info:
            version = info[RELEASE_INFO_VERSION]
        if version is not None:
            if self.rli_version is None:
                self.rli_version = version
            elif self.rli_version != version:
                log.cl_error("inconsistent old and new values of field [%s], [%s] vs. [%s]",
                             RELEASE_INFO_VERSION,
                             self.rli_version,
                             version)
                return -1
        return 0

    def _rli_parse_distro_short(self, log, info):
        """
        Parse the distro_short.
        """
        distro_short = None
        if RELEASE_INFO_DISTRO_SHORT in info:
            distro_short = info[RELEASE_INFO_DISTRO_SHORT]
        if distro_short is not None:
            if self.rli_distro_short is None:
                self.rli_distro_short = distro_short
            elif self.rli_distro_short != distro_short:
                log.cl_error("inconsistent old and new values of field [%s], [%s] vs. [%s]",
                             RELEASE_INFO_DISTRO_SHORT,
                             self.rli_distro_short,
                             distro_short)
                return -1
        return 0

    def _rli_parse_target_cpu(self, log, info):
        """
        Parse the target CPU.
        """
        target_cpu = None
        if RELEASE_INFO_TARGET_CPU in info:
            target_cpu = info[RELEASE_INFO_TARGET_CPU]
        if target_cpu is not None:
            if self.rli_target_cpu is None:
                self.rli_target_cpu = target_cpu
            elif self.rli_target_cpu != target_cpu:
                log.cl_error("inconsistent old and new values of field [%s], [%s] vs. [%s]",
                             RELEASE_INFO_TARGET_CPU,
                             self.rli_distro_short,
                             target_cpu)
                return -1
        return 0

    def _rli_parse_files(self, log, info):
        """
        Parse the downloads.
        """
        file_configs = None
        if RELEASE_INFO_FILES in info:
            file_configs = info[RELEASE_INFO_FILES]
        if file_configs is not None:
            if self.rli_file_dict is not None:
                log.cl_error("download files configured for multiple times")
                return -1
            file_path_dict = {}
            for file_config in file_configs:
                download_file = parse_file_config(log, file_config)
                if download_file is None:
                    log.cl_error("failed to parse file config [%s] of a release",
                                 file_config)
                    return -1
                if download_file.cdf_relative_fpath in file_path_dict:
                    log.cl_error("duplicated file path [%s] configured",
                                 download_file.cdf_relative_fpath)
                    return -1
                file_path_dict[download_file.cdf_relative_fpath] = download_file
            self.rli_file_dict = file_path_dict
        return 0

    def rli_init_from_dict(self, log, info):
        """
        Init myself from dictionary.
        """
        # pylint: disable=too-many-branches,too-many-statements
        # pylint: disable=too-many-locals
        ret = self._rli_parse_version(log, info)
        if ret:
            log.cl_error("failed to parse version")
            return -1

        ret = self._rli_parse_distro_short(log, info)
        if ret:
            log.cl_error("failed to parse distro_short")
            return -1

        ret = self._rli_parse_target_cpu(log, info)
        if ret:
            log.cl_error("failed to parse target_cpu")
            return -1

        ret = self._rli_parse_files(log, info)
        if ret:
            log.cl_error("failed to parse config of files")
            return -1
        return 0

    def rli_read_from_file(self, log, fpath):
        """
        Read the info file and init myself.
        """
        info = coral_yaml.read_yaml_file(log, fpath)
        if info is None:
            log.cl_error("invalid yaml file [%s]",
                         fpath)
            return -1
        return self.rli_init_from_dict(log, info)


    def _rli_info_dict_add(self, info_dict, key, value):
        """
        If value is not none, add to the dict
        """
        # pylint: disable=no-self-use
        if value is not None:
            info_dict[key] = value

    def rli_info_dict(self):
        """
        Return dict of release info.
        """
        info_dict = {}
        self._rli_info_dict_add(info_dict, RELEASE_INFO_VERSION,
                                self.rli_version)
        self._rli_info_dict_add(info_dict, RELEASE_INFO_DISTRO_SHORT,
                                self.rli_distro_short)
        self._rli_info_dict_add(info_dict, RELEASE_INFO_TARGET_CPU,
                                self.rli_target_cpu)
        if self.rli_file_dict is not None:
            file_configs = []
            for download_file in self.rli_file_dict.values():
                file_dict = download_file.cdf_dump_config()
                file_configs.append(file_dict)
            self._rli_info_dict_add(info_dict, RELEASE_INFO_FILES,
                                    file_configs)
        return info_dict

    def rli_save_to_local_file(self, log, fpath):
        """
        Save release info to a file.
        """
        info_dict = self.rli_info_dict()
        prefix = """#
# Please DO NOT edit this file directly!
# Release information saved by Coral.
#
"""
        ret = coral_yaml.write_yaml_file(log, prefix, info_dict, fpath)
        if ret:
            log.cl_error("failed to save YAML file of release information")
            return -1
        return 0

    def rli_save_to_file(self, log, local_host, workspace, host, fpath):
        """
        Save the info file into a file.
        """
        if not host.sh_is_localhost():
            local_parent_dir = workspace + "/" + cmd_general.get_identity()
            info_fname = os.path.basename(fpath)
            local_fpath = local_parent_dir + "/" + info_fname
        else:
            local_parent_dir = os.path.dirname(fpath)
            local_fpath = fpath

        command = "mkdir -p %s" % local_parent_dir
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
        ret = self.rli_save_to_local_file(log, local_fpath)
        if ret:
            log.cl_error("failed to save release info to file [%s] on "
                         "local host [%s]", local_fpath,
                         local_host.sh_hostname)
            return -1

        if not host.sh_is_localhost():
            dirpath = os.path.dirname(fpath)
            ret = host.sh_send_file(log, local_fpath, dirpath)
            if ret:
                log.cl_error("failed to send file [%s] from local host [%s] to "
                             "dir [%s] on host [%s]",
                             local_fpath,
                             local_host.sh_hostname,
                             dirpath,
                             host.sh_hostname)
                return -1
        return 0

    def rli_release_name(self, log):
        """
        Return release name from release info
        """
        if self.rli_version is not None:
            return self.rli_version
        if self.rli_git_short_sha is None:
            log.cl_error("git short SHA is not inited")
            return None
        git_identity = "git_" + self.rli_git_short_sha
        return git_identity


def rinfo_read_from_local_file(log, local_host, info_fpath):
    """
    Read the info file
    """
    ret = local_host.sh_path_exists(log, info_fpath)
    if ret < 0:
        log.cl_error("failed to check whether file [%s] exists "
                     "on host [%s]",
                     info_fpath,
                     local_host.sh_hostname)
        return None

    if ret == 0:
        log.cl_error("file [%s] does not exist on host [%s]",
                     info_fpath, local_host.sh_hostname)
        return None

    release_info = ReleaseInfo()
    ret = release_info.rli_read_from_file(log, info_fpath)
    if ret:
        log.cl_error("failed to parse release info file")
        return None
    return release_info


def rinfo_read_from_file(log, local_host, workspace, host, info_fpath):
    """
    Read the info file
    """
    if not host.sh_is_localhost():
        tmp_dir = workspace + "/" + cmd_general.get_identity()
        command = "mkdir -p %s" % tmp_dir
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        ret = host.sh_get_file(log, info_fpath, tmp_dir)
        if ret:
            log.cl_error("failed to get file [%s] from host [%s] to "
                         "dir [%s] on local host [%s]",
                         info_fpath, host.sh_hostname,
                         tmp_dir, local_host.sh_hostname)
            return None
        local_info_fpath = tmp_dir + "/" + os.path.basename(info_fpath)
    else:
        local_info_fpath = info_fpath

    return rinfo_read_from_local_file(log, local_host, local_info_fpath)


def rinfo_read_from_dir(log, local_host, workspace, host, release_dir, info_fname):
    """
    Read release info from dir.
    """
    info_fpath = release_dir + "/" + info_fname
    return rinfo_read_from_file(log, local_host, workspace, host, info_fpath)


def rinfo_read_from_local_dir(log, local_host, release_dir, info_fname):
    """
    Read release info from dir on local host.
    """
    return rinfo_read_from_dir(log, local_host,
                               None,  # workspace,
                               local_host,  # host
                               release_dir, info_fname)


def rinfo_read_from_coral_iso(log, local_host, workspace, host, iso_fpath):
    """
    Read release info from ISO file. Return ReleaseInfo.
    """
    tmp_dir = workspace + "/" + cmd_general.get_identity()
    relative_fpath = constant.CORAL_RELEASE_INFO_FNAME
    info_fpath = host.sh_unpack_file_from_iso(log, iso_fpath, tmp_dir, relative_fpath)
    if info_fpath is None:
        log.cl_error("failed to unpack [%s] from iso [%s] on host [%s]",
                     relative_fpath, iso_fpath, host.sh_hostname)
        return None

    rinfo = rinfo_read_from_file(log, local_host, tmp_dir, host, info_fpath)
    if rinfo is None:
        log.cl_error("failed to read release info [%s] on host [%s]",
                     info_fpath, host.sh_hostname)
        return None
    return rinfo


def rinfo_read_from_local_coral_iso(log, local_host, workspace, iso_fpath):
    """
    Read release info from ISO file on local host. Return ReleaseInfo.
    """
    return rinfo_read_from_coral_iso(log, local_host, workspace,
                                     local_host, # local_host
                                     iso_fpath)


def rinfo_scan_and_save(log, local_host, workspace, host, release_dir, info_fname):
    """
    Scan the release directory and save the info file into the artifact dir.
    """
    fpath = release_dir + "/" + info_fname
    rinfo = ReleaseInfo()
    ret = rinfo.rli_files_scan(log, host, release_dir)
    if ret:
        log.cl_error("failed to scan files of release dir [%s] on host [%s]",
                     release_dir, host.sh_hostname)
        return -1

    ret = rinfo.rli_save_to_file(log, local_host, workspace, host, fpath)
    if ret:
        log.cl_error("failed to save release info of release dir [%s] "
                     "on host [%s]",
                     release_dir, host.sh_hostname)
        return -1
    return 0


def rinfo_scan_and_save_local(log, local_host, release_dir, info_fname):
    """
    Scan the release directory and save the info file into the artifact dir on
    local host.
    """
    return rinfo_scan_and_save(log, local_host,
                               None,  # workspace,
                               local_host,  # host
                               release_dir,
                               info_fname)



def refresh_release_info(log, local_host, release_dir, info_fname):
    """
    Scan the dir to refresh the release info file.
    """
    info_fpath = release_dir + "/" + info_fname
    ret = local_host.sh_path_exists(log, info_fpath)
    if ret < 0:
        log.cl_error("failed to check whether file [%s] exists on "
                     "host [%s]", info_fpath, local_host.sh_hostname)
        return -1
    if ret:
        rinfo = rinfo_read_from_local_dir(log, local_host, release_dir,
                                          info_fname)
        if rinfo is None:
            log.cl_error("failed to read release info from dir [%s]",
                         release_dir)
            return -1
    else:
        rinfo = ReleaseInfo()

    if rinfo.rli_file_dict is not None:
        complete = rinfo.rli_files_are_complete(log, local_host, release_dir)
        if complete < 0:
            log.cl_error("failed to check whether release [%s] is complete",
                         release_dir)
            return -1
        if not complete:
            log.cl_error("incomplete release [%s]",
                         release_dir)
            return -1
    else:
        ret = rinfo.rli_files_scan(log, local_host, release_dir)
        if ret:
            log.cl_error("failed to scan files of release [%s]",
                         release_dir)
            return -1

    info_fpath = release_dir + "/" + info_fname
    ret = rinfo.rli_save_to_local_file(log, info_fpath)
    if ret:
        log.cl_error("failed to save info file [%s]", info_fpath)
        return -1
    log.cl_info("refreshed release info file [%s]", info_fpath)
    return 0


RELEASE_NAME_LOCAL = "local"
RELEASE_NAME_GLOBAL = "global"


def get_release_info_dict(log, local_host, releases_dir, info_fname, is_local=False):
    """
    Return a dict of release info. Key is release name, value is ReleaseInfo().
    """
    release_dict = {}
    ret = local_host.sh_path_exists(log, releases_dir)
    if ret < 0:
        log.cl_error("failed to check whether path [%s] exists on host [%s]",
                     releases_dir, local_host.sh_hostname)
        return None
    if not ret:
        return release_dict

    fnames = local_host.sh_get_dir_fnames(log, releases_dir)
    if fnames is None:
        log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                     releases_dir, local_host.sh_hostname)
        return None

    for release_name in fnames:
        release_dir = releases_dir + "/" + release_name
        rinfo = rinfo_read_from_local_dir(log, local_host, release_dir,
                                          info_fname)
        if rinfo is None:
            log.cl_error("failed to read release info from dir [%s]",
                         release_dir)
            return None
        rinfo.rli_extra_relative_fpaths.append(info_fname)
        if is_local:
            prefix = RELEASE_NAME_LOCAL
        else:
            prefix = RELEASE_NAME_GLOBAL
        full_name = prefix + "/" + release_name
        release_dict[full_name] = rinfo
    return release_dict
