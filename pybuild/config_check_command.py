"""
Implementation of "coral config_check" command
"""
import os
import sys
from pybuild import build_common
from pycoral import cmd_general


def config_check(coral_command, fpath):
    """
    Configure the current source code and generate files.
    :param fpath: the path of config file.
    """
    # pylint: disable=protected-access
    source_dir = os.getcwd()
    logdir_is_default = True
    log, _, _ = cmd_general.init_env(fpath,
                                     source_dir,
                                     coral_command._cc_log_to_file,
                                     logdir_is_default)
    cmd_general.cmd_exit(log, 0)


build_common.coral_command_register("config_check", config_check)
