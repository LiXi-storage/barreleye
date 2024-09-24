"""
Library for file operations on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import stat
import os

class SSHHostFileMixin():
    """
    Mixin class of SSHHost for file operations.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostFileMixin)
    """
    def sh_file_line_number(self, log, fpath):
        """
        Return the line number of a file
        """
        command = "wc -l %s" % (fpath)
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
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected number of stdout lines for command [%s] on "
                         "host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        line = lines[0]
        fields = line.split()
        if len(fields) != 2:
            log.cl_error("unexpected number of stdout fields for command [%s] on "
                         "host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        line_number_str = fields[0]
        try:
            line_number = int(line_number_str)
        except:
            log.cl_error("invalid number of stdout for command [%s] on "
                         "host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return line_number

    def sh_file_executable(self, log, fpath):
        """
        Check the file is executable
        """
        command = ("test -f %s && test -x %s " % (fpath, fpath))
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
        return 0


    def sh_create_inode(self, log, inode_path, inode_type=stat.S_IFREG,
                        major=None, minor=None, path=None):
        """
        Create inode with a specifi type
        """
        if inode_type == stat.S_IFREG:
            command = ("touch %s" % (inode_path))
        elif inode_type == stat.S_IFDIR:
            command = ("mkdir %s" % (inode_path))
        elif inode_type == stat.S_IFBLK:
            command = ("mknod %s b %s %s" % (inode_path, major, minor))
        elif inode_type == stat.S_IFCHR:
            command = ("mknod %s c %s %s" % (inode_path, major, minor))
        elif inode_type == stat.S_IFLNK:
            command = ("ln -s %s %s" % (path, inode_path))
        elif inode_type == stat.S_IFSOCK:
            command = ("python -c \"import socket;"
                       "sock = socket.socket(socket.AF_UNIX);"
                       "sock.bind('%s')\"" % (inode_path))
        elif inode_type == stat.S_IFIFO:
            command = ("mknod %s p" % (inode_path))
        else:
            log.cl_error("unknown type [%s]", inode_type)
            return -1

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
        return 0

    def sh_remove_inode(self, log, inode_path, inode_type=stat.S_IFREG):
        """
        Create inode with a specifi type
        """
        if inode_type == stat.S_IFDIR:
            command = ("rmdir %s" % (inode_path))
        else:
            command = ("rm -f %s" % (inode_path))

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
        return 0

    def sh_path_exists(self, log, path):
        """
        Path exists or not
        Return 1 if exist, 0 if not, negative if error
        """
        command = "stat %s" % path
        retval = self.sh_run(log, command)
        output = retval.cr_stderr.strip()
        if retval.cr_exit_status == 0:
            return 1
        if output.endswith("No such file or directory"):
            return 0
        log.cl_error("failed to run [%s] on host [%s], ret = [%d], "
                     "stdout = [%s], stderr = [%s]", command,
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return -1

    def sh_path_isreg(self, log, path):
        """
        Path is regular file or not
        Return 1 if exists and at the same time is a regular file, 0 if not,
        netagive on error.
        """
        stat_result = self.sh_stat(log, path)
        if stat_result is None:
            return -1
        return stat.S_ISREG(stat_result.st_mode)

    def sh_path_isdir(self, log, path):
        """
        Path is directory or not
        Return 1 if exists and at the same time is a directory, 0 if not,
        netagive on error.
        """
        stat_result = self.sh_stat(log, path)
        if stat_result is None:
            return -1
        return stat.S_ISDIR(stat_result.st_mode)

    def sh_stat(self, log, path, quiet=False):
        """
        Stat file, return os.stat_result. Return None on error
        """
        # stat_result accepts tuple of at least 10 integers with order:
        # st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size,
        # st_atime, st_mtime, st_ctime.
        #
        # 3.3: Added st_atime_ns, st_mtime_ns, st_ctime_ns
        # 3.5: Added st_file_attributes
        # 3.7: Added st_fstype
        # 3.8: Added st_reparse_tag
        command = "stat -c '%f %i %D %h %u %g %s %X %Y %Z' " + path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            if not quiet:
                log.cl_error("failed to run [%s] on host [%s], ret = [%d], "
                             "stdout = [%s], stderr = [%s]", command,
                             self.sh_hostname, retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            if not quiet:
                log.cl_error("unexpected stat result of path [%s] on host [%s], "
                             "stdout = [%s]",
                             path, self.sh_hostname, retval.cr_stdout)
            return None

        fields = lines[0].split()
        if len(fields) != 10:
            if not quiet:
                log.cl_error("unexpected field number of stat result for "
                             "path [%s] on host [%s], stdout = [%s]",
                             path, self.sh_hostname, retval.cr_stdout)
            return None

        args = []
        bases = [16, 10, 16, 10, 10, 10, 10, 10, 10, 10]
        field_number = 0
        for field in fields:
            base = bases[field_number]
            try:
                args.append(int(field, base))
            except ValueError:
                if not quiet:
                    log.cl_error("unexpected field [%d] with value [%s] for path "
                                 "[%s] on host [%s], stdout = [%s]",
                                 field_number, field, path, self.sh_hostname,
                                 retval.cr_stdout)
                return None
            field_number += 1
        return os.stat_result(tuple(args))

    def sh_get_file_space_usage(self, log, path):
        """
        Return the allocated space of a file in byte, return -1 on error
        """
        command = "stat -c '%b %B' " + path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run [%s] on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]", command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        lines = retval.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("unexpected line number in stdout of command [%s] "
                         "on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        fields = lines[0].split()
        if len(fields) != 2:
            log.cl_error("unexpected field number in stdout of command [%s] "
                         "on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        blocks = 1
        for field in fields:
            try:
                field_number = int(field, 10)
            except ValueError:
                log.cl_error("invalid field [%s] in stdout of command [%s] "
                             "on host [%s], ret = [%d], "
                             "stdout = [%s], stderr = [%s]",
                             field, command,
                             self.sh_hostname, retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
                return -1
            blocks *= field_number

        return blocks

    def sh_get_file_size(self, log, path, size=True, quiet=False):
        """
        Return the file size or blocks in byte, return -1 on error
        """
        if size:
            stat_result = self.sh_stat(log, path, quiet=quiet)
            if stat_result is None:
                if not quiet:
                    log.cl_error("failed to stat file [%s] on host [%s]",
                                 path, self.sh_hostname)
                return -1

            return stat_result.st_size
        return self.sh_get_file_space_usage(log, path)

    def sh_resolve_path(self, log, path_pattern, quiet=False):
        """
        Resolve a path pattern to a path
        Wildcard can be used in the pattern.
        """
        # Need -d otherwise directory will list its content
        command = ("ls -d %s" % (path_pattern))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 2:
            return []
        if retval.cr_exit_status < 0:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None

        fnames = retval.cr_stdout.split()
        return fnames

    def sh_real_path(self, log, path, quiet=False):
        """
        Return the real path from a path.
        """
        command = ("realpath %s" % (path))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status < 0:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None

        fpaths = retval.cr_stdout.splitlines()
        if len(fpaths) != 1:
            if not quiet:
                log.cl_error("unexpected stdout line number of command [%s] "
                             "on host [%s], ret = [%d], stdout = [%s], "
                             "stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return None
        return fpaths[0]


    def sh_files_equal(self, log, fpath0, fpath1):
        """
        Compare two files. If equal, return 1. If not equal, return 0.
        Return -1 on error.
        """
        command = "diff %s %s -1" % (fpath0, fpath1)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            return 1
        if retval.cr_exit_status == 1 and retval.cr_stdout != "":
            return 0

        log.cl_error("failed to run command [%s] on host [%s], ret = [%d], "
                     "stdout = [%s], stderr = [%s]", command,
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return -1

    def sh_sync_two_dirs(self, log, src, dest_parent):
        """
        Sync a dir to another dir. dest_parent is the target parent dir. And
        the dir with the same name under that parent will be removed.
        """
        # Stripe / otherwise it will has different meaning
        dest_parent = dest_parent.rstrip("/")
        command = ("rsync --delete --sparse -azv %s %s" %
                   (src, dest_parent))
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
        return 0

    def sh_get_dir_fnames(self, log, directory, all_fnames=False):
        """
        Return fnames under a dir
        """
        command = "ls %s" % (directory)
        if all_fnames:
            command += " --all"
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
        fnames = retval.cr_stdout.splitlines()
        return fnames

    def sh_remove_empty_dirs(self, log, dirpath):
        """
        Remove all empty dirs under the dir
        """
        while True:
            command = "find %s -empty -type d" % dirpath
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1
            lines = retval.cr_stdout.splitlines()
            if len(lines) == 0:
                return 0
            for line in lines:
                command = "rmdir %s" % line
                retval = self.sh_run(log, command)
                if retval.cr_exit_status:
                    log.cl_error("failed to run command [%s] on host [%s], "
                                 "ret = [%d], stdout = [%s], stderr = [%s]",
                                 command, self.sh_hostname,
                                 retval.cr_exit_status, retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1
        return 0
