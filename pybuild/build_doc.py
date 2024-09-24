"""
Library for building documents
"""
import os
from pybuild import build_common
from pycoral import clog
from pycoral import cmd_general
from pycoral import constant
from pycoral import ssh_host


def check_doc(log, doc_dir):
    """
    Check the doc
    """
    # pylint: disable=too-many-locals,too-many-branches
    local_host = ssh_host.get_local_host(ssh=False)
    languages = local_host.sh_get_dir_fnames(log, doc_dir)
    if languages is None:
        log.cl_error("failed to get fnames under [%s] of host [%s]",
                     doc_dir, local_host.sh_hostname)
        return -1

    # The doc file shall has the format of
    # ${prefix}.${language}.{suffix}, e.g. manual.zh.md or help.zh.txt
    # This script will compare the files with different language but the
    # same prefix and suffix. If the line numbers are different between
    # different languages, that means inconsistency.

    command = "cd %s && find . -type f" % doc_dir
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

    # Each item of the dict is a diction. One diction for each language.
    # The keys of each diction is file paths of ${prefix}.{suffix}
    # The values are the line number of file ${prefix}.{language}.{suffix}
    languages_dict = {}
    fpaths_without_language = []
    for language in constant.CORAL_LANGUAGES:
        languages_dict[language] = {}

    relative_fpaths = retval.cr_stdout.splitlines()
    for relative_fpath in relative_fpaths:
        fpath = doc_dir + "/" + relative_fpath
        dirname = os.path.dirname(relative_fpath)
        fname = os.path.basename(relative_fpath)
        fields = fname.split(".")
        # Not has format of a.{language}.md
        if len(fields) < 3:
            continue

        # Not supported language
        language = fields[-2]
        if language not in constant.CORAL_LANGUAGES:
            continue

        language_dict = languages_dict[language]

        suffix = fields[-1]
        prefixes = fields[:-2]
        prefix_string = ".".join(prefixes)
        fpath_without_language = (dirname + "/" + prefix_string + "." +
                                  suffix)
        fpaths_without_language.append(fpath_without_language)
        if fpath_without_language in language_dict:
            log.cl_error("duplicated fname without language for file [%s]",
                         relative_fpath)
            return -1

        line_number = local_host.sh_file_line_number(log, fpath)
        if line_number < 0:
            log.cl_error("failed to get the line number of path [%s]",
                         fpath)
            return -1

        language_dict[fpath_without_language] = line_number
        if fpath_without_language not in fpaths_without_language:
            fpaths_without_language.append(fpath_without_language)

    for fpath_without_language in fpaths_without_language:
        line_number = -1
        for language in constant.CORAL_LANGUAGES:
            language_dict = languages_dict[language]
            if fpath_without_language not in language_dict:
                continue
            if line_number == -1:
                line_number = language_dict[fpath_without_language]
                continue
            if language_dict[fpath_without_language] != line_number:
                log.cl_error("inconsistent of doc [%s], %s vs. %s",
                             fpath_without_language,
                             language_dict[fpath_without_language],
                             line_number)
                return -1
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
