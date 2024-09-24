"""
Library for remote file copy through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import os
import glob
import shutil
import socket
from pycoral import utils
from pycoral import ssh_basic

def scp_remote_escape(filename):
    """
    Escape special characters from a filename so that it can be passed
    to scp (within double quotes) as a remote file.

    Bis-quoting has to be used with scp for remote files, "bis-quoting"
    as in quoting x 2
    scp does not support a newline in the filename

    Args:
            filename: the filename string to escape.

    Returns:
            The escaped filename string. The required englobing double
            quotes are NOT added and so should be added at some point by
            the caller.
    """
    escape_chars = r' !"$&' + "'" + r'()*,:;<=>?[\]^`{|}'

    new_name = []
    for char in filename:
        if char in escape_chars:
            new_name.append("\\%s" % (char,))
        else:
            new_name.append(char)

    return ssh_basic.sh_escape("".join(new_name))


class SSHHostCopyMixin():
    """
    Mixin class of SSHHost for remote file copy.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostCopyMixin)
    """
    def sh_make_rsync_cmd(self, sources, dest, delete_dest, preserve_symlinks):
        """
        Given a list of source paths and a destination path, produces the
        appropriate rsync command for copying them. Remote paths must be
        pre-encoded.
        """
        # pylint: disable=no-self-use
        if self._sbh_inited_as_local and not self._sbh_ssh_for_local:
            ssh_option = ""
        else:
            ssh_cmd = self._get_ssh_command()
            ssh_option = " --rsh='%s'" % ssh_cmd
        if delete_dest:
            delete_flag = " --delete"
        else:
            delete_flag = ""
        if preserve_symlinks:
            symlink_flag = ""
        else:
            symlink_flag = " -L"
        command = "rsync%s%s --timeout=1800%s -az %s %s"
        return command % (symlink_flag, delete_flag, ssh_option,
                          " ".join(sources), dest)

    def sh_has_rsync(self, log):
        """
        Check whether host has rsync
        """
        return self.sh_has_command(log, "rsync")

    def sh_send_file(self, log, source, dest, delete_dest=False,
                     preserve_symlinks=False,
                     from_local=True,
                     remote_host=None,
                     timeout=None):
        """
        Send file/dir from a host to another host
        If from_local is True, the file will be sent from local host;
        Otherwise, it will be sent from this host (self).
        If remot_host is not none, the file will be sent to that host;
        Otherwise, it will be sent to this host (self).
        """
        # pylint: disable=too-many-locals
        if not self.sh_has_rsync(log):
            log.cl_error("host [%s] doesnot have rsync, trying to install",
                         self.sh_hostname)
            return -1

        if isinstance(source, str):
            source = [source]
        if remote_host is None:
            remote_host = self
        remote_dest = remote_host.sh_encode_remote_paths([dest], False)

        local_sources = [ssh_basic.sh_escape(path) for path in source]
        rsync = remote_host.sh_make_rsync_cmd(local_sources, remote_dest,
                                              delete_dest, preserve_symlinks)
        if from_local:
            ret = utils.run(rsync, timeout=timeout)
        else:
            ret = self.sh_run(log, rsync, timeout=timeout)
        if ret.cr_exit_status:
            log.cl_error("failed to send file [%s] on host [%s] to dest [%s] "
                         "on host [%s], command = [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         utils.list2string(source),
                         self.sh_hostname, dest,
                         remote_host.sh_hostname,
                         rsync, ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0


    def sh_make_rsync_compatible_globs(self, log, path, is_local):
        """
        Given an rsync-style path, returns a list of globbed paths
        that will hopefully provide equivalent behaviour for scp. Does not
        support the full range of rsync pattern matching behaviour, only that
        exposed in the get/send_file interface (trailing slashes).

        The is_local param is flag indicating if the paths should be
        interpreted as local or remote paths.
        """

        # non-trailing slash paths should just work
        if len(path) == 0 or path[-1] != "/":
            return [path]

        # make a function to test if a pattern matches any files
        if is_local:
            def glob_matches_files(log, path, pattern):
                """
                Match the files on local host
                """
                # pylint: disable=unused-argument
                return len(glob.glob(path + pattern)) > 0
        else:
            def glob_matches_files(log, path, pattern):
                """
                Match the files on remote host
                """
                result = self.sh_run(log, "ls \"%s\"%s" %
                                     (ssh_basic.sh_escape(path), pattern))
                return result.cr_exit_status == 0

        # take a set of globs that cover all files, and see which are needed
        patterns = ["*", ".[!.]*"]
        patterns = [p for p in patterns if glob_matches_files(log, path, p)]

        # convert them into a set of paths suitable for the commandline
        if is_local:
            return ["\"%s\"%s" % (ssh_basic.sh_escape(path), pattern)
                    for pattern in patterns]
        return [scp_remote_escape(path) + pattern
                for pattern in patterns]

    def sh_make_scp_cmd(self, sources, dest):
        """
        Given a list of source paths and a destination path, produces the
        appropriate scp command for encoding it. Remote paths must be
        pre-encoded.
        """
        # pylint: disable=no-self-use
        extra_option = ""
        if self.sh_identity_file is not None:
            extra_option = ("-i %s" % self.sh_identity_file)
        command = ("scp -rqp -o StrictHostKeyChecking=no %s "
                   "%s '%s'")
        return command % (extra_option, " ".join(sources), dest)

    def _sh_make_rsync_compatible_source(self, log, source, is_local):
        """
        Applies the same logic as sh_make_rsync_compatible_globs, but
        applies it to an entire list of sources, producing a new list of
        sources, properly quoted.
        """
        return sum((self.sh_make_rsync_compatible_globs(log, path, is_local)
                    for path in source), [])

    def sh_encode_remote_paths(self, paths, escape=True):
        """
        Given a list of file paths, encodes it as a single remote path, in
        the style used by rsync and scp.
        """
        if escape:
            paths = [scp_remote_escape(path) for path in paths]
        if self._sbh_inited_as_local and not self._sbh_ssh_for_local:
            return '"%s"' % (" ".join(paths))
        return 'root@%s:"%s"' % (self.sh_hostname, " ".join(paths))

    def _shcm_set_umask_perms(self, dest):
        """
        Given a destination file/dir (recursively) set the permissions on
        all the files and directories to the max allowed by running umask.

        now this looks strange but I haven't found a way in Python to _just_
        get the umask, apparently the only option is to try to set it
        """
        # pylint: disable=no-self-use
        umask = os.umask(0)
        os.umask(umask)

        max_privs = 0o777 & ~umask

        def set_file_privs(filename):
            """
            Set the privileges of a file
            """
            file_stat = os.stat(filename)

            file_privs = max_privs
            # if the original file permissions do not have at least one
            # executable bit then do not set it anywhere
            if not file_stat.st_mode & 0o111:
                file_privs &= ~0o111

            os.chmod(filename, file_privs)

        # try a bottom-up walk so changes on directory permissions won't cut
        # our access to the files/directories inside it
        for root, dirs, files in os.walk(dest, topdown=False):
            # when setting the privileges we emulate the chmod "X" behaviour
            # that sets to execute only if it is a directory or any of the
            # owner/group/other already has execute right
            for dirname in dirs:
                os.chmod(os.path.join(root, dirname), max_privs)

            for filename in files:
                set_file_privs(os.path.join(root, filename))

        # now set privs for the dest itself
        if os.path.isdir(dest):
            os.chmod(dest, max_privs)
        else:
            set_file_privs(dest)

    def sh_get_file(self, log, source, dest, delete_dest=False,
                    preserve_perm=True):
        """
        copy the file/dir from the host to local host

        Currently, get file is based on scp. For bettern scalability
        we should improve to rsync.
        scp has no equivalent to --delete, just drop the entire dest dir
        """
        if self.sh_is_localhost():
            # This will prevent problems like copy to the same file
            # Removing the original file and etc.
            log.cl_error("getting file from the local host is unsupported")
            return -1

        dest = os.path.abspath(dest)
        if delete_dest and os.path.isdir(dest):
            shutil.rmtree(dest)
            ret = utils.mkdir(dest)
            if ret:
                log.cl_error("failed to mkdir [%s] on localhost",
                             dest)
                return -1

        if isinstance(source, str):
            source = [source]

        remote_source = self._sh_make_rsync_compatible_source(log, source,
                                                              False)
        if remote_source:
            # _sh_make_rsync_compatible_source() already did the escaping
            remote_source = self.sh_encode_remote_paths(remote_source,
                                                        escape=False)
            local_dest = ssh_basic.sh_escape(dest)
            scp = self.sh_make_scp_cmd([remote_source], local_dest)
            retval = utils.run(scp)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to get file [%s] on host [%s] to "
                             "directory [%s] on local host [%s], "
                             "command = [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             source, self.sh_hostname, dest,
                             socket.gethostname(), scp,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        if not preserve_perm:
            # we have no way to tell scp to not try to preserve the
            # permissions so set them after copy instead.
            # for rsync we could use "--no-p --chmod=ugo=rwX" but those
            # options are only in very recent rsync versions
            self._shcm_set_umask_perms(dest)
        return 0
