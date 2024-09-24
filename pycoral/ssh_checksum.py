"""
Library for calculating checksum on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import os

class SSHHostChecksumMixin():
    """
    Mixin class of SSHHost for calculating checksum.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostChecksumMixin)
    """
    def sh_get_checksum(self, log, path, checksum_command="sha256sum"):
        """
        Get a file's checksum.
        """
        command = "%s %s" % (checksum_command, path)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command, self.sh_hostname, retval.cr_stdout)
            return None

        fields = lines[0].split()
        if len(fields) != 2:
            log.cl_error("unexpected field number of output of command "
                         "[%s] on host [%s], stdout = [%s]",
                         command, self.sh_hostname, retval.cr_stdout)
            return None

        calculated_checksum = fields[0]
        return calculated_checksum

    def sh_check_checksum(self, log, path, expected_checksum,
                          checksum_command="sha256sum", quiet=False):
        """
        Check whether the file's checksum is expected
        """
        exists = self.sh_path_exists(log, path)
        if exists < 0:
            log.cl_error("failed to check whether file [%s] exists on host [%s]",
                         path, self.sh_hostname)
            return -1
        if exists == 0:
            return -1

        calculated_checksum = self.sh_get_checksum(log, path,
                                                   checksum_command=checksum_command)
        if calculated_checksum is None:
            log.cl_error("failed to calculate the checksum of file [%s] on "
                         "host [%s]",
                         path, self.sh_hostname)
            return -1

        if expected_checksum != calculated_checksum:
            if not quiet:
                log.cl_error("unexpected [%s] checksum [%s] of file [%s] on "
                             "host [%s], expected [%s]",
                             checksum_command, calculated_checksum, path,
                             self.sh_hostname, expected_checksum)
            return -1
        return 0


    def sh_download_file(self, log, url, fpath,
                         expected_checksum=None,
                         output_fname=None,
                         checksum_command="sha1sum",
                         download_anyway=False,
                         use_curl=False,
                         no_check_certificate=False):
        """
        Download file and check checksum after downloading.
        Use existing file if checksum is expected.
        If output_fname exists, need to add -O to wget because sometimes the URL
        does not save the download file with expected fname.
        """
        # pylint: disable=too-many-branches,too-many-locals,too-many-statements
        target_dir = os.path.dirname(fpath)
        fname = os.path.basename(fpath)
        url_fname = os.path.basename(url)
        if output_fname is None:
            if fname != url_fname:
                log.cl_error("url [%s] and fpath [%s] are not consistent",
                             url, fpath)
                return -1
        elif fname != output_fname:
            log.cl_error("fpath [%s] and output fname [%s] are not consistent",
                         fpath, output_fname)
            return -1

        operation = "downloading"
        if not download_anyway:
            if checksum_command is not None and expected_checksum is not None:
                ret = self.sh_check_checksum(log, fpath, expected_checksum,
                                             checksum_command=checksum_command,
                                             quiet=True)
            else:
                ret = self.sh_path_exists(log, fpath)
                if ret < 0:
                    log.cl_error("failed to check whether file [%s] exists on host [%s]",
                                 fpath, self.sh_hostname)
                    return -1
            if ret == 0:
                log.cl_info("reusing cached file [%s]", os.path.basename(fpath))
                return 0
            operation = "re-downloading"

        log.cl_info("%s [%s] to [%s]", operation, url, fpath)
        # Cleanup the file
        command = "rm -f %s" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if output_fname is None:
            if use_curl:
                download_command = "curl -O %s" % url
            else:
                download_command = "wget %s" % url
        else:
            if use_curl:
                download_command = ("curl %s -o %s" %
                                    (url, output_fname))
            else:
                download_command = ("wget %s -O %s" %
                                    (url, output_fname))

        # Curl does not care about certificate
        if not use_curl and no_check_certificate:
            download_command += " --no-check-certificate"

        command = ("mkdir -p %s && cd %s && %s" %
                   (target_dir, target_dir, download_command))
        log.cl_debug("running command [%s] on host [%s]",
                     command, self.sh_hostname)
        retval = self.sh_run(log, command, timeout=None)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if checksum_command is None or expected_checksum is None:
            return 0

        ret = self.sh_check_checksum(log, fpath, expected_checksum,
                                     checksum_command=checksum_command)
        if ret:
            log.cl_error("newly downloaded file [%s] does not have expected "
                         "sha1sum [%s]",
                         fpath, expected_checksum)
            return -1
        return 0
