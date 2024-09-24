"""
Common library for building Coral
"""
from pycoral import cmd_general
from pycoral import constant

CORAL_ISO_FNAME_PATTERN = "coral-*.iso"
# The pattern of the ISO under Coral source code dir when builds
LOGDIR_PREFIX = "logdir_"
CORAL_BUILD_LOG_PATTERN = (cmd_general.IDENTITY_PATTERN + "/" +
                           LOGDIR_PREFIX + "*/" +
                           constant.CORAL_BUILD_LOG_DIR_BASENAME)

class GitBuildInfo():
    """
    Information for building from Git
    """
    # pylint: disable=too-many-instance-attributes,too-few-public-methods
    def __init__(self, local_host, git_repo, git_commit=None,
                 ssh_identity_file=None, copy_dir=False):
        # Git URL of Lustre source code. If branch is None, this should be
        # a directory path on local host.
        self.gbi_git_repo = git_repo
        # Git commit to checkout, e.g. tag name, branch name or anything that
        # can be understood by "git checkout". If no need to checkout,
        # Git commit should be None.
        self.gbi_git_commit = git_commit
        # SSH identity file to clone git URL
        self.gbi_git_ssh_identity = ssh_identity_file
        # Local host
        self.gbi_local_host = local_host
        # Do not use git-fetch, instead copy the git directory
        self.gbi_copy_dir = copy_dir

    def gbi_get_source(self, log, build_dir):
        """
        Get source to build_dir on local host
        """
        local_host = self.gbi_local_host
        if self.gbi_copy_dir and self.gbi_git_commit is None:
            log.cl_info("copying source codes from [%s] to [%s] on local "
                        "host [%s]", self.gbi_git_repo, build_dir,
                        local_host.sh_hostname)
            command = ("rm -fr %s && cp -a %s %s" %
                       (build_dir, self.gbi_git_repo, build_dir))
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
        else:
            commit_str = "source codes "
            if self.gbi_git_commit is not None:
                commit_str = "commit [%s] " % self.gbi_git_commit
            log.cl_info("git-fetching %sfrom [%s] to [%s] on local "
                        "host [%s]", commit_str, self.gbi_git_repo,
                        build_dir, local_host.sh_hostname)
            ret = local_host.sh_git_rmdir_and_fetch_from_url(log, build_dir,
                                                             self.gbi_git_repo,
                                                             commit=self.gbi_git_commit,
                                                             ssh_identity_file=self.gbi_git_ssh_identity)
            if ret:
                log.cl_error("failed to clone git repository [%s]",
                             self.gbi_git_repo)
                return -1
        return 0
