"""
The host that localhost could use SSH to run command

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import os
import stat
import socket
import traceback
import getpass
from pycoral import utils
from pycoral import os_distro
from pycoral import ssh_basic
from pycoral import ssh_checksum
from pycoral import ssh_cpu
from pycoral import ssh_remote_copy
from pycoral import ssh_distro
from pycoral import ssh_file
from pycoral import ssh_filesystem
from pycoral import ssh_kdump
from pycoral import ssh_lvm
from pycoral import ssh_network
from pycoral import ssh_pip
from pycoral import ssh_reboot
from pycoral import ssh_rpm
from pycoral import ssh_selinux
from pycoral import ssh_service
from pycoral import ssh_virsh
from pycoral import ssh_zfs
from pycoral import ssh_git


class SSHHost(ssh_basic.SSHBasicHost,
              ssh_checksum.SSHHostChecksumMixin,
              ssh_cpu.SSHHostCPUMixin,
              ssh_remote_copy.SSHHostCopyMixin,
              ssh_distro.SSHHostDistroMixin,
              ssh_file.SSHHostFileMixin,
              ssh_filesystem.SSHHostFilesystemMixin,
              ssh_git.SSHHostGitMixin,
              ssh_kdump.SSHHostKdumpMixin,
              ssh_lvm.SSHHostLVMMixin,
              ssh_network.SSHHostNetworkMixin,
              ssh_pip.SSHHostPipMixin,
              ssh_reboot.SSHHostRebootMixin,
              ssh_rpm.SSHHostRPMMixin,
              ssh_selinux.SSHHostSelinuxMixin,
              ssh_service.SSHHostServiceMixin,
              ssh_zfs.SSHHostZFSMixin,
              ssh_virsh.SSHHostVirshMixin):
    """
    Each SSH host has an object of SSHHost
    """
    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    def __init__(self, hostname, identity_file=None, ssh_port=22,
                 local=False, ssh_for_local=True, login_name="root"):
        super().__init__(hostname, identity_file=identity_file,
                         ssh_port=ssh_port, local=local,
                         ssh_for_local=ssh_for_local, login_name=login_name)

    def sh_prepare_user(self, log, name, uid, gid):
        """
        Add an user if it doesn't exist
        """
        # pylint: disable=too-many-return-statements
        ret = self.sh_run(log, "grep '%s:%s' /etc/passwd | wc -l" % (uid, gid))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to check uid [%s] gid [%s] on host "
                           "[%s], ret = [%d], stdout = [%s], stderr = [%s]",
                           uid, gid, self.sh_hostname, ret.cr_exit_status,
                           ret.cr_stdout, ret.cr_stderr)
            return -1

        if ret.cr_stdout.strip() != "0":
            log.cl_debug("user [%s] with uid [%s] gid [%s] already exists "
                         "on host [%s], will not create it",
                         name, uid, gid, self.sh_hostname)
            return 0

        ret = self.sh_run(log, "getent group %s" % (gid))
        if ret.cr_exit_status != 0 and len(ret.cr_stdout.strip()) != 0:
            log.cl_warning("failed to check gid [%s] on host "
                           "[%s], ret = [%d], stdout = [%s], stderr = [%s]",
                           gid, self.sh_hostname, ret.cr_exit_status,
                           ret.cr_stdout, ret.cr_stderr)
            return -1

        if ret.cr_exit_status == 0 and len(ret.cr_stdout.strip()) != 0:
            log.cl_debug("group [%s] with gid [%s] already exists on "
                         "host [%s], will not create it",
                         name, gid, self.sh_hostname)
            return 0

        ret = self.sh_run(log, "groupadd -g %s %s" % (gid, name))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to add group [%s] with gid [%s] on "
                           "host [%s], ret = [%d], stdout = [%s], "
                           "stderr = [%s]",
                           name, gid, self.sh_hostname,
                           ret.cr_exit_status,
                           ret.cr_stdout,
                           ret.cr_stderr)
            return -1

        ret = self.sh_run(log, "useradd -u %s -g %s %s" % (uid, gid, name))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to add user [%s] with uid [%s] gid [%s] "
                           "on host [%s], ret = [%d], stdout = [%s], "
                           "stderr = [%s]",
                           name, uid, gid, self.sh_hostname,
                           ret.cr_exit_status, ret.cr_stdout, ret.cr_stderr)
            return -1
        return 0

    def sh_nfs_exports(self, log):
        """
        Return a list of exported directory on this host.
        """
        command = "exportfs"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return None
        lines = retval.cr_stdout.splitlines()
        line_number = len(lines)
        if line_number % 2 == 1:
            log.cl_error("unexpected line number of output for command "
                         "[%s] on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return None
        exports = []
        for export_index in range(line_number // 2):
            line_index = export_index * 2
            line = lines[line_index]
            exports.append(line)
        return exports

    def sh_get_kernel_ver(self, log):
        """
        Get the kernel version of the remote machine
        """
        ret = self.sh_run(log, "/bin/uname -r")
        if ret.cr_exit_status != 0:
            return None
        return ret.cr_stdout.rstrip()

    def sh_detect_device_fstype(self, log, device):
        """
        Return the command job on a host
        """
        command = ("blkid -o value -s TYPE %s" % device)
        ret = self.sh_run(log, command)
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return None
        if ret.cr_stdout == "":
            return None
        lines = ret.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("command [%s] on host [%s] has unexpected output "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return None

        return lines[0]

    def sh_dumpe2fs(self, log, device):
        """
        Dump ext4 super information
        Return a direction
        """
        info_dict = {}
        command = ("dumpe2fs -h %s" % (device))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        lines = retval.cr_stdout.splitlines()
        for line in lines:
            if line == "":
                continue
            name = ""
            pointer = 0
            for character in line:
                if character != ":":
                    name += character
                    pointer += 1
                else:
                    pointer += 1
                    break

            for character in line[pointer:]:
                if character != " ":
                    break
                pointer += 1

            value = line[pointer:]
            info_dict[name] = value
            log.cl_debug("dumpe2fs name: [%s], value: [%s]", name, value)
        return info_dict

    def sh_disable_ssh_key_checking(self, log):
        """
        Disable key checking of ssh
        """
        host_key_config = "StrictHostKeyChecking no"
        retval = self.sh_run(log, r"grep StrictHostKeyChecking /etc/ssh/ssh_config "
                             r"| grep -v \#")
        if retval.cr_exit_status != 0:
            retval = self.sh_run(log, "echo '%s' >> /etc/ssh/ssh_config" %
                                 host_key_config)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to change ssh config on host [%s]",
                             self.sh_hostname)
                return -1
        else:
            line = retval.cr_stdout.strip()
            if line != host_key_config:
                log.cl_error("unexpected StrictHostKeyChecking config on "
                             "host [%s], expected [%s], got [%s]",
                             self.sh_hostname, host_key_config,
                             line)
                return -1
        return 0

    def sh_kernel_set_default(self, log, kernel):
        """
        Set the default boot kernel
        Example of kernel string:
        /boot/vmlinuz-2.6.32-573.22.1.el6_lustre.2.7.15.3.x86_64
        """
        command = ("grubby --set-default=%s" % (kernel))
        ret = self.sh_run(log, command)
        if ret.cr_exit_status:
            log.cl_error("command [%s] failed on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         ret.cr_exit_status,
                         ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_target_cpu(self, log):
        """
        Return the target CPU, e.g. x86_64 or aarch64
        """
        command = "uname -i"
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

    def sh_lsmod(self, log):
        """
        Returm the inserted module names
        """
        command = "lsmod"
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
        if len(lines) <= 1:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_stdout)
            return None
        lines = lines[1:]
        modules = []
        for line in lines:
            fields = line.split()
            if len(fields) == 0:
                log.cl_error("unexpected line [%s] of command [%s] on host "
                             "[%s]",
                             line,
                             command,
                             self.sh_hostname)
                return None
            modules.append(fields[0])
        return modules

    def sh_get_device_size(self, log, device):
        """
        Get the device size (KB)
        """
        command = ("lsblk -b -d %s --output SIZE | "
                   "sed -n '2,1p' | awk '{print $1/1024}'" %
                   device)
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

        try:
            device_size = int(retval.cr_stdout.strip())
        except:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return device_size

    def sh_fuser_kill(self, log, fpath):
        """
        Run "fuser -km" to a fpath
        """
        command = ("fuser -km %s" % (fpath))
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

    def sh_epoch_seconds(self, log):
        """
        Return the seconds since 1970-01-01 00:00:00 UTC
        """
        command = "date +%s"
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
        second_string = retval.cr_stdout.strip()
        try:
            seconds = int(second_string)
        except:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "stdout = [%s]", command, self.sh_hostname,
                         retval.cr_stdout)
            return -1
        return seconds

    def sh_blockdev_size(self, log, dev):
        """
        Return the size of block device in bytes
        """
        command = "blockdev --getsize64 %s" % dev
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
        size_str = retval.cr_stdout.strip()
        try:
            size = int(size_str)
        except ValueError:
            log.cl_error("invalid size printed by command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return size

    def sh_tree_used_bytes(self, log, path):
        """
        Return the used bytes of a file/dir tree in bytes
        """
        command = "du --summarize --block-size=1 %s" % path
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
        fields = retval.cr_stdout.split()
        if len(fields) != 2:
            log.cl_error("invalid stdout of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        bytes_str = fields[0]
        try:
            used_bytes = int(bytes_str)
        except ValueError:
            log.cl_error("invalid size printed by command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return used_bytes

    def sh_shuffle_file(self, log, input_fpath, output_fpath):
        """
        Shuffle the file and generate shuffled file.
        """
        command = ("shuf %s -o %s" %
                   (input_fpath, output_fpath))
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

    def sh_tar_and_remove_dir(self, log, parent_dir, fname,
                              timeout=None):
        """
        Tar and remove the directory
        """
        origin_dir = parent_dir + "/" + fname
        fpath_tmp = (parent_dir + "/." + fname + "." +
                     utils.random_word(8) +".tar.gz")
        fpath = origin_dir + ".tar.gz"

        command = ("tar -czf %s -C %s %s" %
                   (fpath_tmp, parent_dir, fname))
        retval = self.sh_run(log, command, timeout=timeout)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        command = ("mv %s %s" % (fpath_tmp, fpath))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        command = ("rm -fr %s" % origin_dir)
        retval = self.sh_run(log, command, timeout=timeout)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_get_identity(self, log):
        """""
        Similar with cmd_general.get_identity except it
        does not depend on special python library.
        """
        command = "date +%Y-%m-%d-%H_%M_%S"
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
            log.cl_error("unexpected stdout [%s] of command [%s] on host [%s]",
                         retval.cr_stdout,
                         command,
                         self.sh_hostname)
            return None
        return lines[0] + "-" + utils.random_word(8)

    def sh_apt_install(self, log, debs):
        """
        Install debs
        """
        # pylint: disable=too-many-branches
        if len(debs) == 0:
            return 0

        command = "apt install -y"
        for deb in debs:
            command += " " + deb

        log.cl_info("installing deb packages [%s] on host [%s] through apt",
                    utils.list2string(debs), self.sh_hostname)
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
        return 0

    def sh_install_packages(self, log, packages):
        """
        Install packages though yum or apt
        """
        distro = self.sh_distro(log)
        if distro in (os_distro.DISTRO_RHEL7, os_distro.DISTRO_RHEL8,
                      os_distro.DISTRO_RHEL9):
            return self.sh_yum_install(log, packages)
        if distro in (os_distro.DISTRO_UBUNTU2004, os_distro.DISTRO_UBUNTU2204):
            return self.sh_apt_install(log, packages)
        log.cl_error("unsupported distro [%s] of host [%s] when installing packages",
                     distro, self.sh_hostname)
        return -1

    def sh_unpack_file_from_iso(self, log, iso_fpath, parent_dir, relative_fpath):
        """
        Unpack a file from ISO and return the fpath.
        """
        is_reg = self.sh_path_isreg(log, iso_fpath)
        if is_reg < 0:
            log.cl_error("failed to check whether path [%s] is regular file on host [%s]",
                         iso_fpath,
                         self.sh_hostname)
            return None
        if not is_reg:
            log.cl_error("path [%s] is not regular file on host [%s]",
                         iso_fpath, self.sh_hostname)
            return None

        command = ("mkdir -p %s" % (parent_dir))
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

        command = ("7z x %s -o%s -i'!%s' -y" %
                   (iso_fpath, parent_dir,
                    relative_fpath))
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

        fpath = parent_dir + "/" + relative_fpath
        return fpath



def get_local_host(ssh=True, host_type=SSHHost):
    """
    Return local host
    """
    local_hostname = socket.gethostname()
    login_name = getpass.getuser()
    local_host = host_type(local_hostname, local=True, ssh_for_local=ssh,
                           login_name=login_name)
    return local_host


def read_file(log, local_host, fpath, max_size=None):
    """
    Return the file content of file
    Return None if failure.
    """
    stat_result = local_host.sh_stat(log, fpath)
    if stat_result is None:
        log.cl_error("failed to get stat of path [%s] on "
                     "local host [%s]", fpath,
                     local_host.sh_hostname)
        return None

    if not stat.S_ISREG(stat_result.st_mode):
        log.cl_error("path [%s] on local host [%s] is not a regular "
                     "file", fpath,
                     local_host.sh_hostname)
        return None

    size = stat_result.st_size
    if max_size is not None and size > max_size:
        log.cl_error("size [%s] of file [%s] on local host [%s] "
                     "is too big", size, fpath,
                     local_host.sh_hostname)
        return None

    try:
        with open(fpath, "r", encoding='utf-8') as new_file:
            content = new_file.read()
    except:
        log.cl_error("failed to read from file [%s] from host [%s]: %s",
                     fpath, local_host.sh_hostname,
                     traceback.format_exc())
        return None
    return content

SSH_KEY_DIR = "ssh_keys"
SSH_KEY_SUFFIX = ".ssh_key"


def ssh_key_fpath(workspace, hostname):
    """
    Return the file path of SSH key
    """
    key_dir = workspace + "/" + SSH_KEY_DIR
    fpath = key_dir + "/" + hostname + SSH_KEY_SUFFIX
    return fpath


def send_ssh_keys(log, host, workspace):
    """
    Send the ssh key dir from local host to remote host
    """
    key_dir = workspace + "/" + SSH_KEY_DIR
    ret = host.sh_send_file(log, key_dir, workspace)
    if ret:
        log.cl_error("failed to send dir [%s] on local host [%s] to "
                     "dir [%s] on host [%s]",
                     key_dir, socket.gethostname(),
                     workspace, host.sh_hostname)
        return -1
    return 0


def write_ssh_key(log, workspace, local_host, hostname, content):
    """
    Write ssh key
    """
    fpath = ssh_key_fpath(workspace, hostname)
    key_dir = os.path.dirname(fpath)

    command = "mkdir -p %s" % key_dir
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, local_host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None

    try:
        with open(fpath, "w", encoding='utf-8') as fd:
            fd.write(content)
    except:
        log.cl_error("failed to write file [%s] on host [%s]: %s",
                     fpath, local_host.sh_hostname,
                     traceback.format_exc())
        return None

    command = "chmod 600 %s" % fpath
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, local_host.sh_hostname,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None
    return fpath


def get_or_add_host_to_dict(log, host_dict, hostname, ssh_key,
                            host_type=SSHHost):
    """
    If the hostname exists in the dict, check that the key is the same.
    If the hostname does not exist in the dict, create and add the host to
    the dict.
    """
    if hostname not in host_dict:
        host = host_type(hostname, identity_file=ssh_key)
        host_dict[hostname] = host
    else:
        host = host_dict[hostname]
        if host.sh_identity_file != ssh_key:
            log.cl_error("host [%s] was configured to use ssh key [%s], now [%s]",
                         hostname, host.sh_identity_file, ssh_key)
            return None
    return host


def check_clock_diff(log, host0, host1, max_diff=60):
    """
    Return -1 if the clock diff is too large.
    """
    seconds0 = host0.sh_epoch_seconds(log)
    if seconds0 < 0:
        log.cl_error("failed to get epoch seconds of host [%s]",
                     host0.sh_hostname)
        return -1

    seconds1 = host1.sh_epoch_seconds(log)
    if seconds1 < 0:
        log.cl_error("failed to get epoch seconds of host [%s]",
                     host1.sh_hostname)
        return -1

    if seconds0 + max_diff < seconds1 or seconds1 + max_diff < seconds0:
        log.cl_error("diff of clocks of host [%s] and [%s] is larger "
                     "than [%s] seconds",
                     host0.sh_hostname, host1.sh_hostname, max_diff)
        return -1
    return 0


def check_clocks_diff(log, hosts, max_diff=60):
    """
    Return -1 if the clocks of the hosts differ a lot.
    """
    if len(hosts) < 2:
        return 0
    host = hosts[0]
    for compare_host in hosts[1:]:
        ret = check_clock_diff(log, compare_host, host, max_diff=max_diff)
        if ret:
            log.cl_error("failed to check clock diff of hosts [%s] and [%s]",
                         host.sh_hostname,
                         compare_host.sh_hostname)
            return -1
    return 0
