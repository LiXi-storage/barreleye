"""
Library for Git operations on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
class SSHHostGitMixin():
    """
    Mixin class of SSHHost for Git operations.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostGitMixin)
    """
    def sh_git_fetch_from_url(self, log, git_dir, git_url, commit=None,
                              ssh_identity_file=None):
        """
        Fetch the soure codes from Git server. Assuming the dir
        has been inited by git.
        If commit is None, fetch without checking out.
        """
        command = ("cd %s && git config remote.origin.url %s && "
                   "git fetch --tags %s +refs/heads/*:refs/remotes/origin/*" %
                   (git_dir, git_url, git_url))
        if commit is not None:
            command += " && git checkout %s -f" % commit
        if ssh_identity_file is not None:
            # Git 2.3.0+ has GIT_SSH_COMMAND, but earlier versions do not.
            command = ("ssh-agent sh -c 'ssh-add " + ssh_identity_file +
                       " && " + command + "'")

        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1
        return 0

    def sh_git_rmdir_and_fetch_from_url(self, log, git_dir, git_url, commit=None,
                                        ssh_identity_file=None):
        """
        Get the soure codes from Git server.
        """
        command = ("rm -fr %s && mkdir -p %s && git init %s" %
                   (git_dir, git_dir, git_dir))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        ret = self.sh_git_fetch_from_url(log, git_dir, git_url, commit=commit,
                                         ssh_identity_file=ssh_identity_file)
        return ret


    def sh_git_short_sha(self, log, path):
        """
        Return the short SHA of the HEAD of a git directory
        """
        command = "cd %s && git rev-parse --short HEAD" % path
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
        return retval.cr_stdout.strip()

    def sh_git_subject(self, log, path):
        """
        Return subject of the last commit.
        """
        command = ("cd " + path +
                   ' && git log --pretty=format:%s -1')
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
            log.cl_error("unexpected output lines of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        git_subject = lines[0]
        return git_subject
