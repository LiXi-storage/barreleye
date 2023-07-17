"""
Library for building documents
"""
import os
from pybuild import build_common
from pycoral import clog
from pycoral import cmd_general
from pycoral import ssh_host

# pic is not really a language, but it will be in the shared directory.
LANGUAGES = ["zh", "en", "pic"]


class CoralDocFile():
    """
    Each doc file has an object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, local_host, doc_dir, language, relative_path):
        # Local host to run commands
        self.cdf_local_host = local_host
        # Absolute path to doc dir
        self.cdf_doc_dir = doc_dir
        # The language
        self.cdf_language = language
        # The relative path under doc/$language, e.g. barreleye/README.md
        self.cdf_relative_path = relative_path
        # Line number of the doc file
        self._cdf_line_number = None
        # Absolute path
        self.cdf_fpath = "%s/%s/%s" % (doc_dir, language, relative_path)

    def cdf_line_number(self, log):
        """
        Parse the doc file to get the line number.
        """
        if self._cdf_line_number is not None:
            return self._cdf_line_number
        command = "cat %s | wc -l" % self.cdf_fpath
        retval = self.cdf_local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.cdf_local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        line_number_str = retval.cr_stdout.strip()
        try:
            line_number = int(line_number_str)
        except:
            log.cl_error("invalid output [%s] of command [%s] on host [%s]",
                         retval.cr_stdout,
                         command,
                         self.cdf_local_host.sh_hostname)
            return -1
        self._cdf_line_number = line_number
        return self._cdf_line_number


def check_doc(log, doc_dir):
    """
    Check the doc
    """
    local_host = ssh_host.get_local_host(ssh=False)
    languages = local_host.sh_get_dir_fnames(log, doc_dir)
    if languages is None:
        log.cl_error("failed to get fnames under [%s] of host [%s]",
                     doc_dir, local_host.sh_hostname)
        return -1
    # Key is the relative file path under language dir. The value is the
    # line number of the file. If the line numbers are different between
    # different languages, that means inconsistency.
    line_number_dict = {}
    for language in languages:
        if language not in LANGUAGES:
            log.cl_error("unexpected language [%s] under [%s] of host [%s]",
                         language, doc_dir, local_host.sh_hostname)
            return -1

        lang_dir = doc_dir + "/" + language
        command = "cd %s && find . -type f" % lang_dir
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        relative_fpaths = retval.cr_stdout.splitlines()
        for relative_fpath in relative_fpaths:
            doc_file = CoralDocFile(local_host, doc_dir, language,
                                    relative_fpath)
            log.cl_info("checking doc [%s]", doc_file.cdf_fpath)
            line_number = doc_file.cdf_line_number(log)
            if relative_fpath in line_number_dict:
                old_line_number = line_number_dict[relative_fpath]
                if old_line_number != line_number:
                    log.cl_error("inconsistent line number of doc [%s] "
                                 "between languages, %s vs. %s",
                                 relative_fpath, old_line_number,
                                 line_number)
                    return -1
            else:
                line_number_dict[relative_fpath] = line_number
    return 0


class CoralDocCommand():
    """
    Commands for building doc.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._cdc_log_to_file = log_to_file

    def check(self):
        """
        check whether the documents.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        doc_dir = os.getcwd() + "/doc"
        rc = check_doc(log, doc_dir)
        cmd_general.cmd_exit(log, rc)


build_common.coral_command_register("doc", CoralDocCommand())
