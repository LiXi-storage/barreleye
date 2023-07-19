"""
Version of Coral
"""
from pycoral import ssh_host
from pycoral import cmd_general

# The Coral version file
CORAL_VERSION_FPATH = "version"
# To show this git tree is dirty and different from the "git describe"
VERSION_EXTRA_GIT_DIRTY = "dirty"


def coral_uniformed_version(version):
    """
    Char '-' is illegal for RPM. Replace it to "_". So
    in version field "-" is the same with "_".
    """
    return version.replace("-", "_")


def coral_assemble_version(version, major, minor, extra):
    """
    Assemble version string.
    """
    version_string = "%s.%s.%s" % (version, major, minor)
    if extra is not None:
        version_string += "-" + extra
    return version_string


def coral_version_from_git(log, local_host, source_dir):
    """
    Get coral version from git.
    Two possible version formats:
        version.major.minor
        version.major.minor-extra
    In the first format, return (version, major, minor, None)

    The extra could be the same with the extra field in the tag, if there is
    no commit after git tag, e.g. when the "git describe" string is e.g.
    "2.0.0-rc1".

    The extra could also include other things, if there are one or commits
    after git tag, e.g. when the "git describe" string is e.g.
    "2.0.0-rc1-1-g8f66f7e".

    Return (version, major, minor, extra)
    """
    command = "cd %s && git describe" % source_dir
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None, None, None, None

    lines = retval.cr_stdout.strip().splitlines()
    if len(lines) != 1:
        log.cl_error("unexpected output line number [%s] of command [%s] "
                     "on host [%s]",
                     len(lines), command, local_host.sh_hostname)
        return None, None, None, None

    git_version_line = lines[0]
    git_version, git_major, git_minor, git_extra = \
        cmd_general.coral_parse_version(log, git_version_line,
                                        minus_as_delimiter=True)
    if git_version is None:
        log.cl_error("invalid revision [%s] got from command [%s]",
                     git_version_line, command)
        return None, None, None, None

    rc = git_tree_is_clean(log, local_host, source_dir)
    if rc < 0:
        log.cl_error("failed to check whether git tree is clean or not")
        return None, None, None, None
    if rc == 1:
        return git_version, git_major, git_minor, git_extra

    if git_extra is None:
        git_extra = VERSION_EXTRA_GIT_DIRTY
    else:
        git_extra += "-" + VERSION_EXTRA_GIT_DIRTY
    return git_version, git_major, git_minor, git_extra


def read_version_file(log, local_host, source_dir):
    """
    Read version file
    """
    command = "cat %s/%s" % (source_dir, CORAL_VERSION_FPATH)
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None
    return retval.cr_stdout


def coral_get_version(log, source_dir):
    """
    Get coral version from git.
    Two possible version formats:
        version.major.minor
        version.major.minor-extra
    In the first format, return (version, major, minor, None)
    Return (version, major, minor, extra)
    """
    # pylint: disable=too-many-locals,too-many-branches
    local_host = ssh_host.get_local_host(ssh=False)
    version_fpath = CORAL_VERSION_FPATH

    version_data = read_version_file(log, local_host, source_dir)
    if version_data is None:
        log.cl_error("failed to read version file")
        return None, None, None, None

    version_line = None
    lines = version_data.strip().splitlines()
    for line in lines:
        if line.startswith("#"):
            continue
        if version_line is not None:
            log.cl_error("version file [%s] has multiple uncommented lines",
                         version_fpath)
            return None, None, None, None
        version_line = line

    if version_line is None:
        log.cl_error("version file [%s] has no uncommented line",
                     version_fpath)
        return None, None, None, None

    version, major, minor, extra = \
        cmd_general.coral_parse_version(log, version_line,
                                        minus_as_delimiter=True)
    if version is None:
        log.cl_error("invalid version [%s] in file [%s]",
                     version_line, version_fpath)
        return None, None, None, None

    path = source_dir + "/.git"
    has_git = local_host.sh_path_exists(log, path)
    if has_git < 0:
        log.cl_error("failed to check whether path [%s] exists",
                     path)
        return None, None, None, None

    if not has_git:
        return version, major, minor, extra

    git_version, git_major, git_minor, git_extra = \
        coral_version_from_git(log, local_host, source_dir)
    if git_version is None:
        log.cl_error("failed to get version from git")
        return None, None, None, None

    if git_version != version:
        log.cl_error("inconsistent version numbers got from file [%s] "
                     "and git, [%s] vs. [%s]", version_fpath,
                     version, git_version)
        return None, None, None, None

    if git_major != major:
        log.cl_error("inconsistent major numbers got from file [%s] "
                     "and git, [%s] vs. [%s]", version_fpath,
                     major, git_major)
        return None, None, None, None

    if git_minor != minor:
        log.cl_error("inconsistent minor numbers got from file [%s] "
                     "and git, [%s] vs. [%s]", version_fpath,
                     minor, git_minor)
        return None, None, None, None

    if extra is None or extra == git_extra:
        return version, major, minor, git_extra

    # If extra field from git has more things than extra field from file, the
    # it should be seperated by the first "-"
    minus_index = git_extra.find("-")
    if minus_index < 0:
        log.cl_error("the extra field [%s] from git is not equal to the "
                     "extra field [%s] from file, and does not have [-]",
                     git_extra, extra)
        return None, None, None, None

    git_extra_start = git_extra[:minus_index]
    if git_extra_start != extra:
        log.cl_error("the extra field [%s] from git does not start "
                     "with [%s-] which is the extra field from file",
                     git_extra, extra)
        return None, None, None, None

    return version, major, minor, git_extra


def coral_get_origin_version_string(log, source_dir):
    """
    Get the version string
    """
    version, major, minor, extra = coral_get_version(log, source_dir)
    if version is None:
        log.cl_error("failed to get version")
        return None
    version_string = coral_assemble_version(version, major, minor,
                                            extra)
    return version_string


def coral_get_version_string(log, source_dir):
    """
    Return the version that is legal as RPM name
    """
    version_string = coral_get_origin_version_string(log, source_dir)
    if version_string is None:
        return None
    return coral_uniformed_version(version_string)


def git_tree_is_clean(log, local_host, source_dir):
    """
    Check if git working tree is dirty
    """
    command = ("cd %s && git status --untracked-files=no --porcelain" %
               source_dir)
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
    if retval.cr_stdout == "":
        return 1
    return 0
