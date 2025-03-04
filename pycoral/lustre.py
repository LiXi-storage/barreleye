"""
Library for Lustre file system management
"""
# pylint: disable=too-many-lines
import re
import os

# Local libs
from pycoral import utils
from pycoral import os_distro
from pycoral import ssh_host
from pycoral import constant
from pycoral import lustre_version
from pycoral import release_info

LUSTRE_SERVICE_TYPE_MGT = "MGT"
LUSTRE_SERVICE_TYPE_MDT = "MDT"
LUSTRE_SERVICE_TYPE_OST = "OST"
# Max length of Lustre fsname
LUSTRE_MAXFSNAME = 8

BACKFSTYPE_ZFS = "zfs"
BACKFSTYPE_LDISKFS = "ldiskfs"

LUSTRE_SERVICE_STATUS_CHECK_INTERVAL = 10

# The dir path of Lustre test scripts
LUSTRE_TEST_SCRIPT_DIR = "/usr/lib64/lustre/tests"


OST_SERVICE_PATTERN = (r"^(?P<fsname>\S+)-OST(?P<index_string>[0-9a-f]{4})$")
OST_SERVICE_REG = re.compile(OST_SERVICE_PATTERN)

MDT_SERVICE_PATTERN = (r"^(?P<fsname>\S+)-MDT(?P<index_string>[0-9a-f]{4})$")
MDT_SERVICE_REG = re.compile(MDT_SERVICE_PATTERN)

MGT_SERVICE_PATTERN = (r"MGS")
MGT_SERVICE_REG = re.compile(MGT_SERVICE_PATTERN)


def lustre_parse_target_name(log, target_name, quiet=False):
    """
    "MDT000a" -> "MDT", 10
    "OST000a" -> "OST", 10
    """
    if len(target_name) != 7:
        if not quiet:
            log.cl_error("invalid length of Lustre target name [%s]",
                         target_name)
        return None, None
    if ((not target_name.startswith(LUSTRE_SERVICE_TYPE_MDT)) and
            (not target_name.startswith(LUSTRE_SERVICE_TYPE_OST))):
        if not quiet:
            log.cl_error("invalid target type in Lustre target name [%s]",
                         target_name)
        return None, None
    target_type = target_name[:3]
    target_index_str = target_name[3:]
    target_index = lustre_string2index(log, target_index_str)
    if target_index is None:
        if not quiet:
            log.cl_error("invalid target index [%s] in Lustre target name [%s]",
                         target_index_str, target_name)
        return None, None
    return target_type, target_index


def lustre_parse_service_name(log, service_name, quiet=False):
    """
    "lustre0-MDT000a" -> "lustre0", "MDT", 10
    "lustre0-OST000a" -> "lustre0", "OST", 10
    """
    fields = service_name.split("-")
    if len(fields) != 2:
        if not quiet:
            log.cl_error("invalid number of [-] in service name [%s]",
                         service_name)
        return None, None, None
    fsname = fields[0]
    target_name = fields[1]

    if len(fsname) > 8:
        if not quiet:
            log.cl_error("too long fsname in service name [%s]",
                         service_name)
        return None, None, None

    target_type, target_index = \
        lustre_parse_target_name(log, target_name, quiet=quiet)
    if target_type is None or target_index is None:
        if not quiet:
            log.cl_error("invalid target name in service name [%s]",
                         service_name)
        return None, None, None
    return fsname, target_type, target_index


def lustre_uuid2service_name(service_name_uuid):
    """
    "lustre0-MDT000a_UUID" -> "lustre0-MDT000a"
    "lustre0-OST000a_UUID" -> "lustre0-MDT000a"
    """
    if not service_name_uuid.endswith("_UUID"):
        return None

    # fsname-MDT0000_UUID
    if len(service_name_uuid) < 1 + 1 + 7 + 5:
        return None

    return service_name_uuid[:-5]


def lustre_string2index(log, index_string):
    """
    Transfer string to index number, e.g.
    "000e" -> 14
    """
    try:
        index_number = int(index_string, 16)
    except:
        log.cl_error("invalid index string [%s]", index_string)
        return None
    if index_number > 0xffff:
        log.cl_error("too big index [%s]", index_string)
        return None
    return index_number


def lustre_index2string(index_number):
    """
    Transfer number to index string, e.g.
    14 -> "000e"
    """
    if index_number > 0xffff:
        return None
    index_string = "%04x" % index_number
    return index_string


def lustre_ost_index2string(index_number):
    """
    Transfer number to OST index string, e.g.
    14 -> "OST000e"
    """
    if index_number > 0xffff:
        return None
    index_string = "OST%04x" % index_number
    return index_string


def lustre_mdt_index2string(index_number):
    """
    Transfer number to MDT index string, e.g.
    14 -> "MDT000e"
    """
    if index_number > 0xffff:
        return None
    return "MDT%04x" % index_number


def check_service_name(log, service_name, is_ost=False):
    """
    Return 0 if the service name is valid
    """
    fields = service_name.split("-")
    if len(fields) != 2:
        log.cl_error("invalid service name [%s]: unexpected number of [-]",
                     service_name)
        return -1

    fsname = fields[0]
    index_string = fields[1]
    if len(fsname) > LUSTRE_MAXFSNAME:
        log.cl_error("fsname [%s] of service name [%s] is too long",
                     fsname, service_name)
        return -1
    if not fsname.isalnum():
        log.cl_error("invalid character in fsname [%s] of service name [%s]",
                     fsname, service_name)
        return -1

    if is_ost:
        prefix = LUSTRE_SERVICE_TYPE_OST
    else:
        prefix = LUSTRE_SERVICE_TYPE_MDT

    if not index_string.startswith(prefix):
        log.cl_error("index string [%s] of service name [%s] does not "
                     "start with [%s]",
                     index_string, service_name, prefix)
        return -1
    index_number_str = index_string[len(prefix):]
    if len(index_number_str) != 4:
        log.cl_error("unexpected length of index number [%s] for service "
                     "name [%s]",
                     index_number_str, service_name)
        return -1

    for char in index_number_str:
        if char in "0123456789abcdef":
            continue

        log.cl_error("unexpected character [%s] in index number [%s] "
                     "of service name [%s]",
                     char, index_number_str, service_name)
        return -1
    return 0


def version_value(major, minor, patch):
    """
    Return a numeric version code based on a version string.  The version
    code is useful for comparison two version strings to see which is newer.
    """
    value = (major << 16) | (minor << 8) | patch
    return value


class LustreServiceInstanceSort():
    """
    For sorting the instances by load
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, instance, load):
        self.lsis_instance = instance
        self.lsis_load = load


class LustreServiceInstance():
    """
    A Lustre services might has multiple instances on multiple hosts,
    which are usually for HA.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, service, host, device, mnt, nid,
                 zpool_create=None):
        # LustreService
        self.lsi_service = service
        # The host that provides the service instance.
        self.lsi_host = host
        # The path of device, e.g. /dev/sdb
        self.lsi_device = device
        # The mount point of the service, e.g. /mnt/lustre0-MDT0000
        self.lsi_mnt = mnt
        self.lsi_nid = nid
        self.lsi_zpool_create = zpool_create

    def _lsi_real_device(self, log):
        """
        Sometimes, the device could be symbol link, so return the real block
        device
        """
        if self.lsi_service.ls_backfstype == BACKFSTYPE_ZFS:
            return 0, self.lsi_device

        command = "readlink -f %s" % self.lsi_device
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.lsi_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        return 0, retval.cr_stdout.strip()

    def _lsi_zfs_pool_create(self, log, dryrun=False):
        """
        Create ZFS pool
        """
        # pylint: disable=too-many-branches
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        if self.lsi_zpool_create is None:
            log.cl_error("no zpool_create configured for service "
                         "[%s] on host [%s]", service_name, hostname)
            return -1

        fields = self.lsi_device.split("/")
        if len(fields) != 2:
            log.cl_error("unexpected device [%s] for service [%s] on "
                         "host [%s], should have the format of "
                         "[pool/dataset]",
                         self.lsi_device, service_name, hostname)
            return -1
        zfs_pool = fields[0]

        zfs_pools = host.sh_zfspool_list(log)
        if zfs_pools is None:
            log.cl_error("failed to list ZFS pools on host [%s]",
                         hostname)
            return -1

        if zfs_pool in zfs_pools:
            command = "zpool destroy %s" % (zfs_pool)
            if dryrun:
                log.cl_info("[Dry run] run command [%s] on host [%s]",
                            command, hostname)
            else:
                retval = host.sh_run(log, command, timeout=10)
                if retval.cr_exit_status:
                    log.cl_error("failed to run command [%s] on host [%s], "
                                 "ret = [%d], stdout = [%s], stderr = [%s]",
                                 command,
                                 hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1

        fields = self.lsi_zpool_create.split()
        if len(fields) <= 1:
            log.cl_error("unexpected zpool_create command [%s] for "
                         "service [%s] on host [%s]",
                         self.lsi_zpool_create, service_name, hostname)
            return -1

        # Get rid of the symbol links to avoid following error from
        # zpool_create:
        # missing link: ... was partitioned but ... is missing
        zpool_create = fields[0]
        for field in fields[1:]:
            if field.startswith("/"):
                command = "readlink -f %s" % field
                retval = host.sh_run(log, command)
                if retval.cr_exit_status:
                    log.cl_error("failed to run command [%s] on host [%s], "
                                 "ret = [%d], stdout = [%s], stderr = [%s]",
                                 command,
                                 hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1
                zpool_create += " " + retval.cr_stdout.strip()
            else:
                zpool_create += " " + field

        if dryrun:
            log.cl_info("[Dry Run] running command [%s] on host [%s]",
                        zpool_create, hostname)
        else:
            retval = host.sh_run(log, zpool_create)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             zpool_create,
                             hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def lsi_format(self, log, dryrun=False):
        """
        Format this service device
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname
        backfstype = service.ls_backfstype

        message = ("formatting service [%s] on host [%s]" %
                   (service_name, hostname))
        if dryrun:
            message = "[Dry Run] " + message
        log.cl_info(message)
        service = self.lsi_service
        mgs_nid_string = ""
        if service.ls_service_type == LUSTRE_SERVICE_TYPE_MGT:
            nids = service.ls_nids()
        else:
            nids = service.ls_lustre_fs.lf_mgs_nids()
        for mgs_nid in nids:
            if mgs_nid_string != "":
                mgs_nid_string += ":"
            mgs_nid_string += mgs_nid
        ret, service_string = service.ls_service_string()
        if ret:
            log.cl_error("failed to get the service string of service [%s]",
                         service_name)
            return -1

        if backfstype == BACKFSTYPE_ZFS:
            ret = self._lsi_zfs_pool_create(log, dryrun=dryrun)
            if ret:
                log.cl_error("failed to create ZFS pool of service [%s]",
                             service_name)
                return -1

        mkfsoptions_string = ""
        if service.ls_service_type == LUSTRE_SERVICE_TYPE_MGT:
            type_argument = "--mgs"
            index_argument = ""
            fsname_argument = ""
        elif service.ls_service_type == LUSTRE_SERVICE_TYPE_OST:
            type_argument = "--ost"
            index_argument = " --index=%s" % service.ls_index
            fsname_argument = " --fsname %s" % service.ls_lustre_fs.lf_fsname
            if backfstype == BACKFSTYPE_LDISKFS:
                mkfsoptions_string = ' --mkfsoptions="-O project"'
        elif service.ls_service_type == LUSTRE_SERVICE_TYPE_MDT:
            type_argument = "--mdt"
            if service.lmdt_is_mgs:
                type_argument += " --mgs"
            index_argument = " --index=%s" % service.ls_index
            fsname_argument = " --fsname %s" % service.ls_lustre_fs.lf_fsname
            if backfstype == BACKFSTYPE_LDISKFS:
                mkfsoptions_string = ' --mkfsoptions="-O project"'
        else:
            log.cl_error("unsupported service type [%s]",
                         service.ls_service_type)
            return -1
        command = ("mkfs.lustre%s %s %s "
                   "--reformat --backfstype=%s --mgsnode=%s%s%s" %
                   (fsname_argument, type_argument, service_string,
                    backfstype, mgs_nid_string, index_argument,
                    mkfsoptions_string))
        command += " " + self.lsi_device

        if dryrun:
            log.cl_info("[Dry Run] running command [%s] on host [%s]",
                        command, host.sh_hostname)
            return 0

        retval = host.sh_run(log, command, timeout=None)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_info("formatted service [%s] on host [%s]",
                    service_name, hostname)
        return 0

    def _lsi_zpool_import_export(self, log, export=True):
        """
        Import or export the zpool of this Lustre service device
        """
        # pylint: disable=too-many-branches
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname
        zpool_name = service.ls_zpool_name
        if export:
            operate = "export"
        else:
            operate = "import"

        if service.ls_backfstype != BACKFSTYPE_ZFS:
            log.cl_error("Lustre service [%s] is not based on ZFS, should "
                         "not %s", service_name, operate)
            return -1

        ret = self.lsi_check_zpool_imported(log)
        if ret < 0:
            log.cl_error("failed to check whether zpool of Lustre service "
                         "[%s] imported or not on host [%s]", service_name,
                         hostname)
            return -1

        if ret and not export:
            log.cl_info("zpool [%s] of Lustre service [%s] is already "
                        "imported on host [%s]",
                        zpool_name, service_name, hostname)
            return 0
        if ret == 0 and export:
            log.cl_info("zpool [%s] of Lustre service [%s] is not "
                        "import on host [%s]",
                        zpool_name, service_name, hostname)
            return 0

        force = ""
        if (not export):
            # Check whether it is possible to import
            command = ("clownf_storage mountable %s" % (service.ls_zpool_name))
            retval = host.sh_run(log, command)
            exit_status = retval.cr_exit_status
            if exit_status == constant.CSM_FORCE_REQUIRED:
                log.cl_debug("importing zpool [%s] requires -f option",
                             service.ls_zpool_name)
                force = " -f"
            elif exit_status == constant.CSM_EINVAL:
                log.cl_error("invalid command [%s]", command)
                return -1
            if exit_status == constant.CSM_UNSUPPORTED:
                log.cl_error("unsupported MMP feature on zpool [%s]",
                             service.ls_zpool_name)
                return -1
            if exit_status == constant.CSM_AGAIN:
                log.cl_error("temporary failure when checking whether zpool [%s] can be mounted",
                             service.ls_zpool_name)
                return -1
            if exit_status == constant.CSM_OCCUPIED:
                log.cl_error("wll not be able to import zpool [%s] because it is occupied",
                             service.ls_zpool_name)
                return -1
            if exit_status != constant.CSM_MOUNTABLE:
                log.cl_error("unexpected exit value of command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        log.cl_debug("%sing zpool [%s] of Lustre service [%s] on host [%s]",
                     operate, zpool_name, service_name, hostname)

        command = ("zpool %s %s%s" % (operate, service.ls_zpool_name, force))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_debug("%sed zpool [%s] of Lustre service [%s] on host [%s]",
                     operate, zpool_name, service_name, hostname)
        return 0

    def lsi_zpool_import(self, log):
        """
        Import the zpool of this Lustre service device
        """
        return self._lsi_zpool_import_export(log, export=False)

    def lsi_zpool_export(self, log):
        """
        Export the zpool of this Lustre service device
        """
        return self._lsi_zpool_import_export(log, export=True)

    def lsi_mount(self, log):
        """
        Mount this Lustre service device
        """
        # pylint: disable=too-many-branches
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        log.cl_info("mounting Lustre service [%s] on host [%s]",
                    service_name, hostname)

        if service.ls_backfstype == BACKFSTYPE_ZFS:
            ret = self.lsi_zpool_import(log)
            if ret:
                log.cl_error("failed to import zpool of Lustre service [%s] "
                             "on host [%s]", service_name,
                             hostname)
                return ret


        command = ("mkdir -p %s" % (self.lsi_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        option = ""
        command = ("mount -t lustre%s %s %s" %
                   (option, self.lsi_device, self.lsi_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_debug("mounted Lustre service [%s] on host [%s]", service_name,
                     hostname)
        return 0

    def lsi_umount(self, log):
        """
        Umount this Lustre service device
        """
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        log.cl_info("umounting Lustre service [%s] on host [%s]",
                    service_name, hostname)

        command = ("umount %s" % (self.lsi_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if service.ls_backfstype == BACKFSTYPE_ZFS:
            ret = self.lsi_zpool_export(log)
            if ret:
                log.cl_error("failed to export zpool of Lustre service [%s] "
                             "on host [%s]", service_name,
                             hostname)
                return ret

        log.cl_debug("umounted Lustre service [%s] on host [%s]",
                     service_name, hostname)
        return 0

    def lsi_check_mounted(self, log):
        """
        Return 1 when service is mounted
        Return 0 when service is not mounted
        Return negative when error
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # pylint: disable=too-many-return-statements
        host = self.lsi_host
        hostname = host.sh_hostname
        service = self.lsi_service
        service_type = service.ls_service_type
        service_name = service.ls_service_name

        server_pattern = (r"^(?P<device>\S+) (?P<mount_point>\S+) lustre (?P<options>\S+) .+$")
        server_regular = re.compile(server_pattern)

        client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
        client_regular = re.compile(client_pattern)

        if service_type == LUSTRE_SERVICE_TYPE_OST:
            service_pattern = (r"^(?P<fsname>\S+)-OST(?P<index_string>[0-9a-f]{4})$")
            service_regular = re.compile(service_pattern)
        elif service_type == LUSTRE_SERVICE_TYPE_MDT:
            service_pattern = (r"^(?P<fsname>\S+)-MDT(?P<index_string>[0-9a-f]{4})$")
            service_regular = re.compile(service_pattern)

        mgs_pattern = (r"MGS")
        mgs_regular = re.compile(mgs_pattern)

        # Detect Lustre services
        command = ("cat /proc/mounts")
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret, real_device = self._lsi_real_device(log)
        if ret:
            log.cl_error("failed to get the real service device of service "
                         "[%s] on host [%s]",
                         service_name, hostname)
            return -1

        ret = 0
        for line in retval.cr_stdout.splitlines():
            # log.cl_debug("checking line [%s]", line)
            match = server_regular.match(line)
            if not match:
                continue

            device = match.group("device")
            mount_point = match.group("mount_point")

            # Skip the Clients
            match = client_regular.match(line)
            if match:
                continue

            if device in (real_device, self.lsi_device):
                if mount_point != self.lsi_mnt:
                    log.cl_error("Lustre service device [%s] is mounted on "
                                 "host [%s], but on mount point [%s], not "
                                 "on [%s]",
                                 device, hostname,
                                 mount_point, self.lsi_mnt)
                    return -1
            else:
                if mount_point == self.lsi_mnt:
                    log.cl_error("A Lustre service on device [%s] is mounted on "
                                 "mount point [%s] of host [%s], but that "
                                 "is not service [%s]", device, mount_point,
                                 hostname, service_name)
                    return -1
                continue

            label = host.lsh_lustre_device_service_name(log, device,
                                                        backfstype=self.lsi_service.ls_backfstype)
            if label is None:
                log.cl_error("failed to get the label of device [%s] on "
                             "host [%s]", device, hostname)
                return -1

            if service_type == LUSTRE_SERVICE_TYPE_MGT:
                match = mgs_regular.match(label)
                if match:
                    log.cl_debug("MGS [%s] mounted on dir [%s] of host [%s]",
                                 device, mount_point, hostname)
                    ret = 1
                    break
            elif (service_type in (LUSTRE_SERVICE_TYPE_OST,
                                   LUSTRE_SERVICE_TYPE_MDT)):
                fsname = service.ls_lustre_fs.lf_fsname
                match = service_regular.match(label)
                if match:
                    device_fsname = match.group("fsname")
                    index_string = match.group("index_string")
                    device_index = lustre_string2index(log, index_string)
                    if device_index is None:
                        log.cl_error("invalid label [%s] of device [%s] on "
                                     "host [%s]", label, device, hostname)
                        return -1

                    if device_fsname != fsname:
                        log.cl_error("unexpected fsname [%s] of device [%s] on "
                                     "host [%s], expected fsname is [%s]",
                                     device_fsname, device, hostname,
                                     fsname)
                        return -1

                    if device_index != service.ls_index:
                        log.cl_error("unexpected service index [%s] of device [%s] on "
                                     "host [%s], expected index is [%s]",
                                     device_index, device, hostname,
                                     service.ls_index)
                        return -1
                    log.cl_debug("service of file system [%s] mounted on "
                                 "dir [%s] of host [%s]",
                                 fsname, mount_point, hostname)
                    ret = 1
                    break
            else:
                log.cl_error("unsupported service type [%s]",
                             service_type)

        return ret

    def lsi_check_zpool_imported(self, log):
        """
        Return 1 when zpool of the service is imported
        Return 0 when zpool of the service is not imported
        Return negative when error
        """
        host = self.lsi_host
        hostname = host.sh_hostname
        service = self.lsi_service
        zpool_name = service.ls_zpool_name
        command = "zpool list %s" % zpool_name
        retval = host.sh_run(log, command)
        output = retval.cr_stderr.strip()
        if retval.cr_exit_status and output.endswith("no such pool"):
            return 0
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 1



class LustreService():
    """
    Lustre service parent class for MDT/MGS/OST
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, lustre_fs, service_type, service_index,
                 backfstype, zpool_name=None):
        # For MGS, ls_lustre_fs is None. LustreFilesystem
        self.ls_lustre_fs = lustre_fs
        # Keys are hostname, values are LustreServiceInstance
        self.ls_instance_dict = {}
        # LUSTRE_SERVICE_TYPE_[MGT|MDT|OST]
        self.ls_service_type = service_type
        if backfstype == BACKFSTYPE_LDISKFS:
            assert zpool_name is None
        else:
            assert backfstype == BACKFSTYPE_ZFS
            assert zpool_name is not None
        self.ls_zpool_name = zpool_name
        if service_type == LUSTRE_SERVICE_TYPE_MGT:
            assert service_index == 0
            self.ls_index_string = "MGS"
        elif service_type == LUSTRE_SERVICE_TYPE_MDT:
            index_string = lustre_mdt_index2string(service_index)
            assert index_string is not None
            # Example: "OST0001"
            self.ls_index_string = index_string
        else:
            assert service_type == LUSTRE_SERVICE_TYPE_OST
            index_string = lustre_ost_index2string(service_index)
            assert index_string is not None
            # Example: "MDT000e"
            self.ls_index_string = index_string
        self.ls_index = service_index
        # BACKFSTYPE_ZFS or BACKFSTYPE_LDISKFS
        self.ls_backfstype = backfstype
        if lustre_fs is None:
            # MGS
            self.ls_service_name = None
        else:
            self.ls_service_name = lustre_fs.lf_fsname + "-" + self.ls_index_string

    def ls_service_string(self):
        """
        Return the service string used by mkfs.lustre or tunefs.lustre
        """
        service_string = ""
        for instance in self.ls_instance_dict.values():
            if instance.lsi_nid is None:
                return -1, None
            if service_string != "":
                service_string += " "
            service_string += "--servicenode=%s" % instance.lsi_nid
        return 0, service_string

    def ls_nids(self):
        """
        Return the nid array of the service
        """
        nids = []
        for instance in self.ls_instance_dict.values():
            assert instance.lsi_nid is not None
            nids.append(instance.lsi_nid)
        return nids

    def ls_hosts(self):
        """
        Return the host array of the service
        """
        hosts = []
        for instance in self.ls_instance_dict.values():
            host = instance.lsi_host
            if host not in hosts:
                hosts.append(instance.lsi_host)
        return hosts

    def ls_instance_add(self, log, instance):
        """
        Add instance of this service
        """
        hostname = instance.lsi_host.sh_hostname
        if hostname in self.ls_instance_dict:
            log.cl_error("instance of service [%s] already exists "
                         "for host [%s]",
                         self.ls_service_name,
                         hostname)
            return -1
        self.ls_instance_dict[hostname] = instance
        return 0

    def ls_filter_instances(self, log, only_hostnames, exclude_dict,
                            not_select_reason="not selected"):
        """
        Return hostnames that can mount the service. Return None on error.
        exclude_dict will be updated if some hosts are excluded.
        :param exclude_dict: key is hostname, value is string of reason.
        :param not_select_reason: why host is not in only_hostnames.
        """
        selected_hostnames = list(self.ls_instance_dict.keys())

        if only_hostnames is not None:
            for only in only_hostnames:
                if only not in self.ls_instance_dict:
                    log.cl_error("host [%s] doesnot have any instance of "
                                 "service [%s]", only, self.ls_service_name)
                    return None

            for hostname in selected_hostnames[:]:
                if hostname not in only_hostnames:
                    selected_hostnames.remove(hostname)
                    if hostname not in exclude_dict:
                        exclude_dict[hostname] = not_select_reason

        for exclude in exclude_dict:
            if exclude not in self.ls_instance_dict:
                log.cl_warning("exclude host [%s] doesnot have any "
                               "instance of service [%s], ignoring",
                               exclude, self.ls_service_name)
                continue
            for hostname in selected_hostnames[:]:
                if hostname == exclude:
                    selected_hostnames.remove(hostname)
        return selected_hostnames

    def _ls_instances_sort(self, log, instances, prefered_hostnames=None):
        """
        Sort the instances by their loads. If an instance has problem, remove
        the instance from the list. The instances on prefered hosts will be
        moved to the head of the list in the oder of load. The prefered_hostnames
        list itself do not have any order.
        """
        sort_instances = []
        for instance in instances:
            host = instance.lsi_host
            client_dict = {}
            service_instance_dict = {}
            ret = host.lsh_lustre_detect_services(log, client_dict,
                                                  service_instance_dict)
            if ret:
                log.cl_warning("failed to detect mounted services on "
                               "host [%s], removing the instance from "
                               "the candidate list of service [%s]",
                               host.sh_hostname, self.ls_service_name)
                continue
            load = len(service_instance_dict)
            sort_instance = LustreServiceInstanceSort(instance, load)
            sort_instances.append(sort_instance)

        sort_instances.sort(key=lambda instance: instance.lsis_load)
        sorted_instances = []
        if prefered_hostnames:
            for sort_instance in sort_instances[:]:
                service_instance = sort_instance.lsis_instance
                if service_instance.lsi_host.sh_hostname not in prefered_hostnames:
                    continue
                sorted_instances.append(service_instance)
                sort_instances.remove(sort_instance)
        for sort_instance in sort_instances:
            sorted_instances.append(sort_instance.lsis_instance)
        instances[:] = sorted_instances

    def _ls_complain_no_mountable(self, log, exclude_dict):
        """
        Compain the reason of why no intance mountable
        """
        log.cl_error("no instance of service [%s] is mountable",
                     self.ls_service_name)

        while len(exclude_dict) > 0:
            current_reason = None
            hostname_string = ""
            iter_dict = dict(exclude_dict)
            for hostname, reason in iter_dict.items():
                if current_reason is None:
                    current_reason = reason
                    hostname_string = hostname
                    del exclude_dict[hostname]
                    continue
                if current_reason == reason:
                    hostname_string += ", " + hostname
                    del exclude_dict[hostname]
                    continue
            log.cl_error("%s: %s", current_reason, hostname_string)

    def ls_mount(self, log, selected_hostnames=None, exclude_dict=None,
                 prefered_hostnames=None, quiet=False):
        """
        Mount this service
        """
        # pylint: disable=too-many-branches,too-many-locals
        # pylint: disable=too-many-statements
        if len(self.ls_instance_dict) == 0:
            log.cl_error("no instance of service [%s] is configured",
                         self.ls_service_name)
            return -1

        if exclude_dict is None:
            exclude_dict = {}

        if selected_hostnames is None:
            instances = list(self.ls_instance_dict.values())
        else:
            instances = []
            for hostname, instance in self.ls_instance_dict.items():
                if hostname in selected_hostnames:
                    instances.append(instance)

        down_hostnames = []
        for instance in instances[:]:
            host = instance.lsi_host
            if not host.sh_is_up(log):
                instances.remove(instance)
                hostname = host.sh_hostname
                if hostname not in down_hostnames:
                    down_hostnames.append(host.sh_hostname)
                if hostname not in exclude_dict:
                    exclude_dict[hostname] = "host down"

        if len(instances) == 0:
            self._ls_complain_no_mountable(log, exclude_dict)
            return -1

        self._ls_instances_sort(log, instances,
                                prefered_hostnames=prefered_hostnames)

        instance = self._ls_mounted_instance(log,
                                             down_hostnames=down_hostnames)
        if instance is not None:
            hostname = instance.lsi_host.sh_hostname
            if instance in instances:
                log.cl_info("service [%s] is already mounted on host [%s]",
                            self.ls_service_name, hostname)
                if not quiet:
                    log.cl_stdout(constant.MSG_ALREADY_MOUNTED_NO_NEWLINE)
                return 0

            ret = instance.lsi_umount(log)
            if ret == 0:
                log.cl_debug("umounted service [%s] on host [%s]",
                             self.ls_service_name, hostname)
            else:
                log.cl_error("failed to umount service [%s] on host [%s]",
                             self.ls_service_name, hostname)
                return -1

        if len(instances) == 0:
            self._ls_complain_no_mountable(log, exclude_dict)
            return -1

        for instance in instances:
            hostname = instance.lsi_host.sh_hostname
            log.cl_debug("mounting service [%s] on host [%s]",
                         self.ls_service_name, hostname)

            if self.ls_backfstype == BACKFSTYPE_ZFS:
                imported_instance = self.ls_zpool_imported_instance(log)
                if ((imported_instance is not None) and
                        (instance != imported_instance)):
                    ret = imported_instance.lsi_zpool_export(log)
                    if ret:
                        log.cl_error("failed to exported zpool of service "
                                     "[%s] on host [%s]",
                                     self.ls_service_name,
                                     imported_instance.lsi_host.sh_hostname)
                        return -1

            ret = instance.lsi_mount(log)
            if ret == 0:
                if not quiet:
                    log.cl_stdout("Mounted")
                return 0

        log.cl_error("failed to mount service [%s]", self.ls_service_name)
        return -1

    def _ls_mounted_instance(self, log, down_hostnames=None):
        """
        Return the instance that has been mounted
        If no instance is mounted, return None
        """
        mounted_instances = []
        for instance in self.ls_instance_dict.values():
            if (down_hostnames is not None and
                    instance.lsi_host.sh_hostname in down_hostnames):
                log.cl_debug("skip checking whether service [%s] is "
                             "mounted on host [%s] because the host is down",
                             self.ls_service_name,
                             instance.lsi_host.sh_hostname)
                continue
            ret = instance.lsi_check_mounted(log)
            if ret < 0:
                log.cl_error("failed to check whether service "
                             "[%s] is mounted on host [%s]",
                             self.ls_service_name,
                             instance.lsi_host.sh_hostname)
            elif ret > 0:
                log.cl_debug("service [%s] is mounted on host "
                             "[%s]", self.ls_service_name,
                             instance.lsi_host.sh_hostname)
                mounted_instances.append(instance)

        if len(mounted_instances) == 0:
            return None
        assert len(mounted_instances) == 1
        return mounted_instances[0]

    def ls_mounted_instance(self, log):
        """
        Return the instance that has been mounted
        If no instance is mounted, return None
        """
        down_hostnames = []
        for instance in self.ls_instance_dict.values():
            host = instance.lsi_host
            if ((not host.sh_is_up(log)) and
                    host.sh_hostname not in down_hostnames):
                down_hostnames.append(host.sh_hostname)
        return self._ls_mounted_instance(log, down_hostnames=down_hostnames)

    def ls_zpool_imported_instance(self, log):
        """
        If the device is ZFS, return the instance that has been imported
        If no instance is imported or not ZFS, return None
        """
        if self.ls_backfstype != BACKFSTYPE_ZFS:
            return None

        if len(self.ls_instance_dict) == 0:
            return None

        imported_instances = []
        for instance in self.ls_instance_dict.values():
            ret = instance.lsi_check_zpool_imported(log)
            if ret < 0:
                log.cl_error("failed to check whether ZFS of service "
                             "[%s] is imported on host [%s]",
                             self.ls_service_name,
                             instance.lsi_host.sh_hostname)
            elif ret > 0:
                log.cl_debug("ZFS of service [%s] is imported on host "
                             "[%s]", self.ls_service_name,
                             instance.lsi_host.sh_hostname)
                imported_instances.append(instance)

        if len(imported_instances) == 0:
            return None
        assert len(imported_instances) == 1
        return imported_instances[0]

    def ls_umount(self, log):
        """
        Umount this service
        """
        service_name = self.ls_service_name

        log.cl_debug("umounting service [%s]", service_name)
        instance = self.ls_mounted_instance(log)
        if instance is not None:
            ret = instance.lsi_umount(log)
            if ret:
                log.cl_error("failed to umount service [%s]", service_name)
                return ret
            log.cl_debug("umounted service [%s]", service_name)
            return 0
        log.cl_info("service [%s] is not mounted on any host",
                    self.ls_service_name)

        if self.ls_backfstype == BACKFSTYPE_ZFS:
            instance = self.ls_zpool_imported_instance(log)
            if instance is None:
                log.cl_debug("zpool of service [%s] is not imported on any "
                             "host",
                             self.ls_service_name)
                return 0

            ret = instance.lsi_zpool_export(log)
            if ret == 0:
                log.cl_debug("exported zpool of service [%s]", service_name)
            else:
                log.cl_error("failed to export zpool of service [%s]",
                             service_name)
                return ret
        return 0

    def ls_format(self, log, dryrun=False):
        """
        Format this service.
        Service should have been umounted.
        """
        if len(self.ls_instance_dict) == 0:
            return -1

        for instance in self.ls_instance_dict.values():
            ret = instance.lsi_format(log, dryrun=dryrun)
            if ret == 0:
                log.cl_debug("formatted service [%s]", self.ls_service_name)
                return 0
        log.cl_error("failed to format service [%s]",
                     self.ls_service_name)
        return -1



class LustreMGS(LustreService):
    """
    Lustre MGS service not combined to MDT
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, mgs_id, backfstype, zpool_name=None):
        super().__init__(None, LUSTRE_SERVICE_TYPE_MGT, 0,
                         backfstype, zpool_name=zpool_name)
        # Key is file system name, value is LustreFilesystem
        self.lmgs_filesystems = {}
        self.ls_service_name = mgs_id

    def lmgs_add_fs(self, log, lustrefs):
        """
        Add file system to this MGS
        """
        fsname = lustrefs.lf_fsname
        if fsname in self.lmgs_filesystems:
            log.cl_error("file system [%s] is already in MGS [%s]",
                         fsname, self.ls_service_name)
            return -1


        self.lmgs_filesystems[fsname] = lustrefs

        ret = lustrefs.lf_mgs_init(log, self)
        if ret:
            log.cl_error("failed to init MGS for file system [%s]",
                         lustrefs.lf_fsname)
            return ret
        return 0


class LustreMGSInstance(LustreServiceInstance):
    """
    A Lustre MGS might has multiple instances on multiple hosts,
    which are usually for HA
    """
    def __init__(self, mgs, host, device, mnt, nid,
                 zpool_create=None):
        super().__init__(mgs, host, device,
                         mnt, nid, zpool_create=zpool_create)



def init_mgs_instance(log, mgs, host, device, mnt, nid,
                      add_to_host=False, zpool_create=None):
    """
    Return LustreMGSInstance
    """
    mgsi = LustreMGSInstance(mgs, host, device, mnt, nid,
                             zpool_create=zpool_create)
    ret = mgs.ls_instance_add(log, mgsi)
    if ret:
        log.cl_error("failed to add instance of MGS [%s]",
                     mgs.ls_service_name)
        return None

    if add_to_host:
        ret = host.lsh_mgsi_add(log, mgsi)
        if ret:
            log.cl_error("MGS instance [%s] already configured for host [%s]",
                         mgs.ls_service_name,
                         host.sh_hostname)
            return None
    return mgsi


class LustreFilesystem():
    """
    Each Lustre file system has an object of this type.
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, fsname
                 ):
        self.lf_fsname = fsname
        # Key is the service name, value is LustreOST
        self.lf_ost_dict = {}
        # Key is the service name, value is LustreMDT
        self.lf_mdt_dict = {}
        # Key is $HOSTNAME:$MNT, value is LustreClient
        self.lf_client_dict = {}
        self.lf_mgs = None
        # Key is the service name, combined MGT/MDT is not duplicated
        self.lf_service_dict = {}
        self.lf_mgs_mdt = None
        self._lf_host_dict = None

    def lf_host_dict(self):
        """
        Return the host dict
        """
        if self._lf_host_dict is not None:
            return self._lf_host_dict
        host_dict = {}
        for service in self.lf_service_dict.values():
            for host in service.ls_hosts():
                if host.sh_hostname not in host_dict:
                    host_dict[host.sh_hostname] = host
        for client in self.lf_client_dict.values():
            host = client.lc_host
            if host.sh_hostname not in host_dict:
                host_dict[host.sh_hostname] = host
        self._lf_host_dict = host_dict
        return host_dict

    def lf_mgs_init(self, log, mgs, combined=False):
        """
        Init a seperate or combined MGS
        """
        if self.lf_mgs is not None:
            log.cl_error("file system [%s] alreay has a MGS", self.lf_fsname)
            return -1

        if self.lf_mgs_mdt is not None:
            log.cl_error("file system [%s] alreay has a combined MGS",
                         self.lf_fsname)
            return -1

        if combined:
            self.lf_mgs_mdt = mgs
        else:
            service_name = mgs.ls_service_name
            self.lf_mgs = mgs
            self.lf_service_dict[service_name] = mgs
        return 0

    def lf_ost_add(self, log, service_name, ost):
        """
        Add OST into this file system
        """
        if service_name in self.lf_ost_dict:
            log.cl_error("service [%s] is already configured for file system [%s]",
                         service_name, self.lf_fsname)
            return -1
        self.lf_ost_dict[service_name] = ost
        self.lf_service_dict[service_name] = ost
        return 0

    def lf_mdt_add(self, log, service_name, mdt):
        """
        Add MDT into this file system
        """
        if service_name in self.lf_mdt_dict:
            log.cl_error("service [%s] is already configured for file system [%s]",
                         service_name, self.lf_fsname)
            return -1
        if mdt.lmdt_is_mgs:
            ret = self.lf_mgs_init(log, mdt, combined=True)
            if ret:
                log.cl_error("failed to init MGS for file system [%s]",
                             self.lf_fsname)
                return -1
        self.lf_mdt_dict[service_name] = mdt
        self.lf_service_dict[service_name] = mdt
        return 0

    def lf_mgs_nids(self):
        """
        Return the nid array of the MGS
        """
        if self.lf_mgs_mdt is None:
            assert self.lf_mgs is not None
            return self.lf_mgs.ls_nids()
        assert self.lf_mgs is None
        return self.lf_mgs_mdt.ls_nids()

    def lf_oss_list(self):
        """
        Return the host list that could run OSS service
        """
        hosts = []
        for ost in self.lf_ost_dict.values():
            ost_hosts = ost.ls_hosts()
            for ost_host in ost_hosts:
                if ost_host not in hosts:
                    hosts.append(ost_host)
        return hosts

    def lf_mds_list(self):
        """
        Return the host list that could run MDS service
        """
        hosts = []
        for mds in self.lf_mdt_dict.values():
            mds_hosts = mds.ls_hosts()
            for mds_host in mds_hosts:
                if mds_host not in hosts:
                    hosts.append(mds_host)
        return hosts

    def lf_oss_and_mds_list(self):
        """
        Return the host list that could run MDS/OSS service
        """
        hosts = []
        for mds in self.lf_mdt_dict.values():
            mds_hosts = mds.ls_hosts()
            for mds_host in mds_hosts:
                if mds_host not in hosts:
                    hosts.append(mds_host)
        for ost in self.lf_ost_dict.values():
            ost_hosts = ost.ls_hosts()
            for ost_host in ost_hosts:
                if ost_host not in hosts:
                    hosts.append(ost_host)
        return hosts

    def lf_format(self, log, format_mgt=False, dryrun=False):
        """
        Format the whole file system.
        :param format_mgt: format the standalone MGT.
        """
        message = "formatting file system [%s]" % self.lf_fsname
        if dryrun:
            message = "[Dry Run] " + message
        log.cl_info(message)
        if len(self.lf_mgs_nids()) == 0:
            log.cl_error("the MGS nid of Lustre file system [%s] is not "
                         "configured, not able to format", self.lf_fsname)
            return -1

        for service_name, mdt in self.lf_mdt_dict.items():
            ret = mdt.ls_format(log, dryrun=dryrun)
            if ret:
                log.cl_error("failed to format MDT [%s]", service_name)
                return -1

        for service_name, ost in self.lf_ost_dict.items():
            ret = ost.ls_format(log, dryrun=dryrun)
            if ret:
                log.cl_error("failed to format OST [%s]", service_name)
                return -1

        if self.lf_mgs is not None and format_mgt:
            service_name = self.lf_mgs.ls_service_name
            ret = self.lf_mgs.ls_format(log, dryrun=dryrun)
            if ret:
                log.cl_error("failed to format MGT [%s]", service_name)
                return -1
        log.cl_debug("formatted file system [%s]", self.lf_fsname)
        return 0


    def lf_mount(self, log):
        """
        Mount the whole file system
        """
        # pylint: disable=too-many-branches
        log.cl_info("mounting file system [%s]", self.lf_fsname)
        if self.lf_mgs is not None:
            ret = self.lf_mgs.ls_mount(log, quiet=True)
            if ret:
                log.cl_error("failed to mount MGS of Lustre file "
                             "system [%s]", self.lf_fsname)
                return -1

        for service_name, mdt in self.lf_mdt_dict.items():
            ret = mdt.ls_mount(log, quiet=True)
            if ret:
                log.cl_error("failed to mount MDT [%s] of Lustre file "
                             "system [%s]", service_name, self.lf_fsname)
                return -1

        for service_name, ost in self.lf_ost_dict.items():
            ret = ost.ls_mount(log, quiet=True)
            if ret:
                log.cl_error("failed to mount OST [%s] of Lustre file "
                             "system [%s]", service_name, self.lf_fsname)
                return -1

        for client_index, client in self.lf_client_dict.items():
            ret = client.lc_mount(log, quiet=True)
            if ret:
                log.cl_error("failed to mount client [%s] of Lustre file "
                             "system [%s]", client_index, self.lf_fsname)
                return -1

        log.cl_debug("mounted file system [%s]", self.lf_fsname)
        return 0

    def lf_umount(self, log, force_umount_mgt=False):
        """
        Umount the whole file system
        :param force_umount_mgt: umount the standalone MGT.
        """
        # pylint: disable=too-many-branches
        for client_index, client in self.lf_client_dict.items():
            ret = client.lc_umount(log)
            if ret:
                log.cl_error("failed to umount client [%s]", client_index)
                return -1

        for service_name, ost in self.lf_ost_dict.items():
            ret = ost.ls_umount(log)
            if ret:
                log.cl_error("failed to umount OST [%s]", service_name)
                return -1

        for service_name, mdt in self.lf_mdt_dict.items():
            ret = mdt.ls_umount(log)
            if ret:
                log.cl_error("failed to umount MDT [%s]", service_name)
                return -1

        if self.lf_mgs is not None:
            service_name = self.lf_mgs.ls_service_name
            if ((not force_umount_mgt) and
                    len(self.lf_mgs.lmgs_filesystems) > 1):
                log.cl_info("skip umounting MGS [%s] it is shared by "
                            "another file system", service_name)
                return 0
            ret = self.lf_mgs.ls_umount(log)
            if ret:
                log.cl_error("failed to umount MGS [%s]", service_name)
                return -1
        return 0


class LustreMDTInstance(LustreServiceInstance):
    """
    A Lustre MDT might has multiple instances on multiple hosts,
    which are usually for HA
    """
    def __init__(self, mdt, host, device, mnt, nid,
                 zpool_create=None):
        super().__init__(mdt, host, device, mnt, nid,
                         zpool_create=zpool_create)
        self.mdti_nid = nid

    def mdti_has_unsynced_ost_changes(self, log):
        """
        Return 1 if there are still unfinished changes saved in
        LLOG records or in-progess changes.
        """
        service = self.lsi_service
        lustre_fs = service.ls_lustre_fs
        fsname = lustre_fs.lf_fsname
        host = self.lsi_host

        command = ("lctl get_param -n osp.%s-OST*-osc-%s.sync_*" %
                   (fsname, service.ls_index_string))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            if line != "0":
                return 1
        return 0


def init_mdt_instance(log, mdt, host, device, mnt, nid,
                      add_to_host=False, zpool_create=None,
                      mdt_instance_type=LustreMDTInstance):
    """
    Return LustreMDTInstance
    """
    mdti = mdt_instance_type(mdt, host, device, mnt, nid,
                             zpool_create=zpool_create)
    ret = mdt.ls_instance_add(log, mdti)
    if ret:
        log.cl_error("failed to add instance of service [%s]",
                     mdt.ls_service_name)
        return None
    if add_to_host:
        ret = host.lsh_mdti_add(mdti)
        if ret:
            log.cl_error("MDT instance [%s] already configured for host [%s]",
                         mdt.ls_service_name,
                         host.sh_hostname)
            return None
    return mdti


class LustreMDT(LustreService):
    """
    Lustre MDT service
    """
    # pylint: disable=too-few-public-methods
    # index: 0, 1, etc.
    def __init__(self, lustre_fs, service_index, backfstype, is_mgs=False,
                 zpool_name=None):
        super().__init__(lustre_fs, LUSTRE_SERVICE_TYPE_MDT,
                         service_index, backfstype, zpool_name=zpool_name)
        self.lmdt_is_mgs = is_mgs


def init_mdt(log, lustre_fs, service_index, backfstype, is_mgs=False,
             zpool_name=None):
    """
    Return LustreMDT
    """
    if service_index > 0xffff:
        log.cl_error("MDT index [%s] is too big", service_index)
        return None
    mdt = LustreMDT(lustre_fs, service_index, backfstype,
                    is_mgs=is_mgs, zpool_name=zpool_name)
    ret = lustre_fs.lf_mdt_add(log, mdt.ls_service_name, mdt)
    if ret:
        log.cl_error("failed to add MDT [%s] into file system [%s]",
                     mdt.ls_service_name, lustre_fs.lf_fsname)
        return None
    return mdt


class LustreOSTInstance(LustreServiceInstance):
    """
    A Lustre OST might has multiple instances on multiple hosts,
    which are usually for HA
    """
    def __init__(self, ost, host, device, mnt, nid,
                 zpool_create=None):
        super().__init__(ost, host, device, mnt, nid,
                         zpool_create=zpool_create)


def init_ost_instance(log, ost, host, device, mnt, nid,
                      add_to_host=False, zpool_create=None):
    """
    Return LustreOSTInstance
    """
    osti = LustreOSTInstance(ost, host, device, mnt, nid,
                             zpool_create=zpool_create)
    ret = ost.ls_instance_add(log, osti)
    if ret:
        log.cl_error("failed to add instance of service [%s]",
                     ost.ls_service_name)
        return None

    if add_to_host:
        ret = host.lsh_osti_add(osti)
        if ret:
            log.cl_error("OST instance [%s] already configured for host [%s]",
                         ost.ls_service_name,
                         host.sh_hostname)
            return None
    return osti


class LustreOST(LustreService):
    """
    Lustre OST service
    """
    # pylint: disable=too-few-public-methods
    # index: 0, 1, etc.
    def __init__(self, lustre_fs, service_index, backfstype,
                 zpool_name=None):
        super().__init__(lustre_fs, LUSTRE_SERVICE_TYPE_OST,
                         service_index, backfstype, zpool_name=zpool_name)


def init_ost(log, lustre_fs, service_index, backfstype, zpool_name=None):
    """
    Return LustreOST
    """
    if service_index > 0xffff:
        log.cl_error("OST index [%s] is too big", service_index)
        return None
    ost = LustreOST(lustre_fs, service_index, backfstype,
                    zpool_name=zpool_name)
    ret = lustre_fs.lf_ost_add(log, ost.ls_service_name, ost)
    if ret:
        log.cl_error("failed to add OST [%s] into file system [%s]",
                     ost.ls_service_name, lustre_fs.lf_fsname)
        return None
    return ost


class LustreClient():
    """
    Lustre client
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, lustre_fs, host, mnt):
        # Type: LustreFilesystem
        self.lc_lustre_fs = lustre_fs
        self.lc_host = host
        self.lc_mnt = mnt
        # Name of the client.
        self.lc_client_name = ("%s:%s" % (host.sh_hostname, mnt))

    def lc_get_uuid(self, log):
        """
        Return the uuid of this client
        """
        return self.lc_host.lsh_get_client_uuid(log, self.lc_mnt)

    def lc_check_mounted(self, log):
        """
        Return 1 when client is mounted
        Return 0 when client is not mounted
        Return negative when error
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # pylint: disable=too-many-return-statements
        host = self.lc_host
        hostname = host.sh_hostname
        fsname = self.lc_lustre_fs.lf_fsname
        mount_point = self.lc_mnt

        client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
        client_regular = re.compile(client_pattern)

        # Detect Lustre services
        command = ("cat /proc/mounts")
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret = 0
        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            # Skip the Clients
            match = client_regular.match(line)
            if not match:
                continue

            mounted_fsname = match.group("fsname")
            mounted_mnt = match.group("mount_point")

            if mounted_fsname == fsname:
                if mount_point != mounted_mnt:
                    log.cl_debug("Lustre client [%s] is mounted on "
                                 "host [%s], but on mount point [%s], not "
                                 "on [%s]", fsname, hostname, mounted_mnt,
                                 mount_point)
                    continue
            else:
                if mount_point == mounted_mnt:
                    log.cl_error("one Lustre client is mounted on mount "
                                 "point [%s] of host [%s], but file system "
                                 "name is [%s], expected [%s]",
                                 mount_point, hostname, mounted_fsname,
                                 fsname)
                    return -1
                continue

            log.cl_debug("Lustre client of file system [%s] is already "
                         "mounted on dir [%s] of host [%s]",
                         fsname, mount_point, hostname)
            ret = 1
            break
        return ret

    def lc_mount(self, log, quiet=False):
        """
        Mount this client
        """
        host = self.lc_host
        hostname = host.sh_hostname
        fsname = self.lc_lustre_fs.lf_fsname

        ret = self.lc_check_mounted(log)
        if ret < 0:
            log.cl_error("failed to check whether Lustre client "
                         "[%s] is mounted on host [%s]",
                         fsname, hostname)
        elif ret > 0:
            log.cl_info("Lustre client [%s] is already mounted on "
                        "directory [%s] of host [%s]", fsname, self.lc_mnt,
                        hostname)
            if not quiet:
                log.cl_stdout(constant.MSG_ALREADY_MOUNTED_NO_NEWLINE)
            return 0

        command = ("mkdir -p %s" % (self.lc_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        log.cl_info("mounting client of file system [%s] to directory [%s] "
                    "of host [%s]", fsname, self.lc_mnt, hostname)
        nid_string = ""
        for mgs_nid in self.lc_lustre_fs.lf_mgs_nids():
            if nid_string != "":
                nid_string += ":"
            nid_string += mgs_nid
        option = ""
        command = ("mount -t lustre%s %s:/%s %s" %
                   (option, nid_string,
                    fsname, self.lc_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        log.cl_debug("mounted Lustre client [%s] to mount point [%s] of "
                     "host [%s]", fsname, self.lc_mnt,
                     hostname)
        if not quiet:
            log.cl_stdout("Newly mounted")
        return 0

    def lc_umount(self, log):
        """
        Umount this client
        """
        host = self.lc_host
        hostname = host.sh_hostname
        fsname = self.lc_lustre_fs.lf_fsname
        log.cl_info("umounting client file system [%s] from mount point "
                    "[%s] on host [%s]",
                    fsname, self.lc_mnt, hostname)

        ret = self.lc_check_mounted(log)
        if ret < 0:
            log.cl_error("failed to check whether Lustre client "
                         "[%s] is mounted on host [%s]",
                         fsname, hostname)
        elif ret == 0:
            log.cl_info("Lustre client [%s] is not mounted on dir [%s] of "
                        "host [%s]", fsname, self.lc_mnt, hostname)
            return 0

        command = ("umount %s" % (self.lc_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1
        log.cl_debug("umounted client file system [%s] from mount point [%s]",
                     fsname, self.lc_mnt)
        return 0

    def _lc_ost_mdt_names(self, log, mdt=False):
        """
        Return OST/MDT service names connected to this client.
        Example of "lfs osts":
OBDS:
0: lustre0-OST0000_UUID ACTIVE
1: lustre0-OST0001_UUID ACTIVE
2: lustre0-OST0002_UUID ACTIVE
3: lustre0-OST0003_UUID INACTIVE

        Example of "lfs mdts":
MDTS:
0: lustre0-MDT0000_UUID ACTIVE
1: lustre0-MDT0001_UUID ACTIVE

        The prefix line is determined by setup_obd_uuid().
        The format of each line is determined by lov_tgt_seq_show()/lmv_tgt_seq_show().
        """
        host = self.lc_host
        hostname = host.sh_hostname

        if mdt:
            command = "lfs mdts %s" % self.lc_mnt
            expected_prefix = "MDTS:"
        else:
            command = "lfs osts %s" % self.lc_mnt
            expected_prefix = "OBDS:"

        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return None
        lines = retval.cr_stdout.splitlines()
        if len(lines) == 0:
            return []
        prefix_line = lines[0]
        if prefix_line != expected_prefix:
            log.cl_error("unexpected prefix stdout of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command, hostname, retval.cr_stdout)
            return None

        service_names = []
        for line in lines[1:]:
            fields = line.split()
            if len(fields) != 3:
                log.cl_error("unexpected stdout line [%s] of command [%s] on host [%s], "
                             "stdout = [%s]",
                             command, hostname, retval.cr_stdout)
                return None
            service_name_uuid = fields[1]
            service_name = lustre_uuid2service_name(service_name_uuid)
            if service_name is None:
                log.cl_error("invalid service name UUID [%s] in stdout of "
                             "command [%s] on host [%s], stdout = [%s]",
                             service_name_uuid,
                             command, hostname,
                             retval.cr_stdout)
                return None
            service_names.append(service_name)
        return service_names

    def lc_mdt_names(self, log):
        """
        Return MDT service names connected to this client.
        """
        return self._lc_ost_mdt_names(log, mdt=True)

    def lc_ost_names(self, log):
        """
        Return OST service names connected to this client.
        """
        return self._lc_ost_mdt_names(log, mdt=False)


def init_client(log, lustre_fs, host, mnt, add_to_host=False):
    """
    Init LustreClient
    """
    lustre_client = LustreClient(lustre_fs, host, mnt)
    client_name = lustre_client.lc_client_name
    if client_name in lustre_fs.lf_client_dict:
        log.cl_error("client [%s] already exists in file system [%s]",
                     client_name, lustre_fs.lf_fsname)
        return None
    lustre_fs.lf_client_dict[client_name] = lustre_client
    if add_to_host:
        ret = host.lsh_client_add(lustre_fs.lf_fsname, mnt, lustre_client)
        if ret:
            log.cl_error("client [%s] already exists on host [%s]",
                         client_name, host.sh_hostname)
            return None
    return lustre_client


def lustre_read_release_info(log, local_host, workspace, host, release_dir):
    """
    Read Lustre release info.
    """
    rinfo = release_info.rinfo_read_from_dir(log, local_host, workspace,
                                             host, release_dir,
                                             constant.LUSTRE_RELEASE_INFO_FNAME)
    if rinfo is None:
        return None
    rinfo.rli_extra_relative_fpaths.append(constant.LUSTRE_RELEASE_INFO_FNAME)
    return rinfo


def lustre_read_release_info_local(log, local_host, release_dir):
    """
    Read Lustre release info on local host.
    """
    return lustre_read_release_info(log, local_host,
                                    None,  # workspace
                                    local_host,  # host
                                    release_dir)


def e2fsprogs_read_release_info(log, local_host, workspace, host, release_dir):
    """
    Read E2fsprogs release info.
    """
    rinfo = release_info.rinfo_read_from_dir(log, local_host, workspace,
                                             host, release_dir,
                                             constant.E2FSPROGS_RELEASE_INFO_FNAME)
    if rinfo is None:
        return None
    rinfo.rli_extra_relative_fpaths.append(constant.E2FSPROGS_RELEASE_INFO_FNAME)
    return rinfo



def e2fsprogs_read_release_info_local(log, local_host, release_dir):
    """
    Read E2fsprogs release info on local host.
    """
    return e2fsprogs_read_release_info(log, local_host,
                                       None,  # workspace
                                       local_host,  # host
                                       release_dir)

def lustre_get_relative_rpm_dir(target_cpu):
    """
    Return the relative RPM dir of Lustre
    """
    return constant.RPMS_DIR_BASENAME + "/" + target_cpu


def lustre_get_rpm_dir(release_dir, target_cpu):
    """
    Return the RPM dir of Lustre
    """
    return (release_dir + "/" + lustre_get_relative_rpm_dir(target_cpu))


class LustreDistribution():
    """
    Lustre Distribution, including Lustre, kernel, e2fsprogs RPMs
    """
    # pylint: disable=too-many-instance-attributes,too-few-public-methods
    def __init__(self, host, target_cpu, lustre_dir, e2fsprogs_dir,
                 nondist_lustre_relative_fpaths=None,
                 nondist_lustre_relative_dir_paths=None):
        # Dir of Lustre release
        self.ldis_lustre_dir = lustre_dir
        # Dir that contains Lustre RPM files
        self.ldis_lustre_rpm_dir = lustre_get_rpm_dir(lustre_dir, target_cpu)
        # Key is "RPM_*", value is the fname of the RPM
        self.ldis_lustre_rpm_dict = {}
        # LustreVersion
        self.ldis_lustre_version = None
        # Kernel version of Lustre
        self.ldis_kernel_version = None
        # ZFS is supported in the Lustre RPMs
        self.ldis_zfs_support = True
        # Ldiskfs is supported in the Lustre RPMs
        self.ldis_ldiskfs_support = True
        # Dir that contains Lustre RPM files
        self.ldis_e2fsprogs_dir = e2fsprogs_dir
        # Target CPU
        self.ldis_target_cpu = target_cpu
        # E2fsprogs dir that contains RPM files
        self.ldis_e2fsprogs_rpm_dir = (e2fsprogs_dir + "/" + constant.RPMS_DIR_BASENAME +
                                       "/" + target_cpu)
        # The RPMs are checked and has no problem
        self.ldis_checked = False
        # Host that this distribution is on.
        self.ldis_host = host
        # e2fsprogs version, e.g. "1.45.2.wc1"
        self.ldis_e2fsprogs_version = None
        # The release info of Lustre. Type: ReleaseInfo.
        self.ldis_lustre_rinfo = None
        # The release info of E2fsprogs. Type: ReleaseInfo.
        self.ldis_e2fsprogs_rinfo = None
        # Fpaths ignored when copying/packing the Lustre release dir.
        if nondist_lustre_relative_fpaths is None:
            nondist_lustre_relative_fpaths = []
        self.ldis_nondist_lustre_relative_fpaths = nondist_lustre_relative_fpaths
        # Dir paths ignored when copying/packing the Lustre release dir.
        if nondist_lustre_relative_dir_paths is None:
            nondist_lustre_relative_dir_paths = []
        self.ldis_nondist_lustre_relative_dir_paths = nondist_lustre_relative_dir_paths

    def _ldis_install_kernel(self, log):
        """
        Install the kernel in this release.
        """
        # pylint: disable=too-many-branches,too-many-statements
        host = self.ldis_host
        distro = host.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get distro of host [%s]",
                         host.sh_hostname)
            return -1
        if distro not in [os_distro.DISTRO_RHEL7,
                          os_distro.DISTRO_RHEL8]:
            log.cl_error("unsupported distro [%s] on host [%s]",
                         distro, host.sh_hostname)
            return -1

        log.cl_info("installing Lustre kernel RPM on host [%s]", host.sh_hostname)
        if lustre_version.LVD_RPM_KERNEL_FIRMWARE in self.ldis_lustre_rpm_dict:
            rpm_name = self.ldis_lustre_rpm_dict[lustre_version.LVD_RPM_KERNEL_FIRMWARE]
            command = ("rpm -ivh --force %s/%s" %
                       (self.ldis_lustre_rpm_dir, rpm_name))
            retval = host.sh_run(log, command,
                                 timeout=constant.LONGEST_TIME_RPM_INSTALL)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run commnad [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname, retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
                return -1

        rpm_name = self.ldis_lustre_rpm_dict[lustre_version.LVD_RPM_KERNEL]
        command = ("rpm -ivh --force %s/%s" %
                   (self.ldis_lustre_rpm_dir, rpm_name))
        if lustre_version.LVD_RPM_KERNEL_CORE in self.ldis_lustre_rpm_dict:
            rpm_name = self.ldis_lustre_rpm_dict[lustre_version.LVD_RPM_KERNEL_CORE]
            command += " " + self.ldis_lustre_rpm_dir + "/" + rpm_name
        if lustre_version.LVD_RPM_KERNEL_MODULES in self.ldis_lustre_rpm_dict:
            rpm_name = self.ldis_lustre_rpm_dict[lustre_version.LVD_RPM_KERNEL_MODULES]
            command += " " + self.ldis_lustre_rpm_dir + "/" + rpm_name
        retval = host.sh_run(log, command,
                             timeout=constant.LONGEST_TIME_RPM_INSTALL)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run commnad [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        # Somehow crashkernel=auto doen't work for RHEL7 sometimes
        log.cl_info("changing boot argument of crashkernel on host [%s]",
                    host.sh_hostname)

        exist_config_fpath = None
        config_fpaths = ["/boot/grub2/grub.cfg",
                         "/boot/efi/EFI/redhat/grub.cfg",
                         "/boot/efi/EFI/centos/grub.cfg",
                         "/boot/efi/EFI/rocky/grub.cfg"]
        for config_fpath in config_fpaths:
            ret = host.sh_path_exists(log, config_fpath)
            if ret < 0:
                log.cl_error("failed to check whether [%s] exists on host [%s]",
                             config_fpath, host.sh_hostname)
                return -1
            if ret:
                if exist_config_fpath is not None:
                    log.cl_error("both [%s] and [%s] exist on host [%s]",
                                 config_fpath, exist_config_fpath,
                                 host.sh_hostname)
                exist_config_fpath = config_fpath
        if exist_config_fpath is None:
            log.cl_error("failed to find any file of [%s] on host [%s]",
                         utils.list2string(config_fpaths), host.sh_hostname)
            return -1

        command = ("sed -i 's/crashkernel=auto/crashkernel=128M/g' %s" %
                   exist_config_fpath)
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1
        return 0

    def _ldis_install_ofed(self, log):
        """
        Install OFED in this release.
        """
        host = self.ldis_host
        ofed_rpms = get_mofed_rpm_fnames(log, host, self.ldis_lustre_rpm_dir)
        if ofed_rpms is None:
            log.cl_info("no OFED RPM found, skipping OFED installation on host [%s]",
                        host.sh_hostname)
            return 0

        log.cl_info("installing OFED RPM on host [%s]", host.sh_hostname)

        # New version of mlnx-tools might block the installation of old
        # version mlnx-ofa_kernel, but please do not add it to this list.
        # The reason is, new mlnx-ofa_kernel like 5.8 depends on mlnx-tools.
        # Keeping existing mlnx-tools at least gives an chance for manual
        # fixing of missing mlnx-tools.
        mlnx_key_rpms = ["mlnx-ofa_kernel",
                         "mlnx-ofa_kernel-devel",
                         "mlnx-ofa_kernel-debuginfo",
                         "kmod-mlnx-ofa_kernel"]

        rpm_string = ""
        for rpm_name in mlnx_key_rpms:
            if host.sh_has_rpm(log, rpm_name):
                if rpm_string != "":
                    rpm_string += " "
                rpm_string += rpm_name

        if rpm_string != "":
            command = "rpm -e --nodeps "+ rpm_string
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        command = "rpm -ivh --nodeps"
        for ofed_rpm in ofed_rpms:
            command += " " + self.ldis_lustre_rpm_dir + "/" + ofed_rpm
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def ldis_send_from_local(self, log, host, lustre_dir, e2fsprogs_dir):
        """
        Send this local distribution to a remote host.
        """
        if not self.ldis_host.sh_is_localhost():
            log.cl_error("sending Lustre release from remote host is not supported")
            return None
        local_host = self.ldis_host
        target_cpu = host.sh_target_cpu(log)
        if target_cpu is None:
            log.cl_error("failed to get cpu type of host [%s]",
                         host.sh_hostname)
            return None
        if target_cpu != self.ldis_target_cpu:
            log.cl_error("cpu type [%s] of host [%s] is different with [%s] "
                         "of host [%s]", target_cpu,
                         target_cpu, host.sh_hostname, self.ldis_target_cpu,
                         local_host.sh_hostname)
            return None

        remote_distr = LustreDistribution(host, target_cpu, lustre_dir, e2fsprogs_dir)
        remote_distr.ldis_zfs_support = self.ldis_zfs_support
        remote_distr.ldis_ldiskfs_support = self.ldis_ldiskfs_support
        remote_distr.ldis_lustre_rpm_dict = self.ldis_lustre_rpm_dict.copy()
        remote_distr.ldis_lustre_version = self.ldis_lustre_version
        remote_distr.ldis_kernel_version = self.ldis_kernel_version
        remote_distr.ldis_checked = self.ldis_checked
        remote_distr.ldis_e2fsprogs_version = self.ldis_e2fsprogs_version
        remote_distr.ldis_lustre_rinfo = self.ldis_lustre_rinfo
        remote_distr.ldis_e2fsprogs_rinfo = self.ldis_e2fsprogs_rinfo
        remote_distr.ldis_nondist_lustre_relative_fpaths = \
            list(self.ldis_nondist_lustre_relative_fpaths)
        remote_distr.ldis_nondist_lustre_relative_dir_paths = \
            list(self.ldis_nondist_lustre_relative_dir_paths)

        lustre_rinfo = self.ldis_lustre_rinfo
        ret = lustre_rinfo.rli_files_send(log, local_host,
                                          self.ldis_lustre_dir,
                                          host,
                                          remote_distr.ldis_lustre_dir)
        if ret:
            log.cl_error("failed to send Lustre release [%s] on local host "
                         "[%s] to dir [%s] on host [%s]",
                         self.ldis_lustre_dir,
                         local_host.sh_hostname,
                         remote_distr.ldis_lustre_dir,
                         host.sh_hostname)
            return None

        e2fsprogs_rinfo = self.ldis_e2fsprogs_rinfo
        ret = e2fsprogs_rinfo.rli_files_send(log, local_host,
                                             self.ldis_e2fsprogs_dir,
                                             host,
                                             remote_distr.ldis_e2fsprogs_dir)
        if ret:
            log.cl_error("failed to send e2fsprogs release [%s] on local host "
                         "[%s] to dir [%s] on host [%s]",
                         self.ldis_e2fsprogs_dir,
                         local_host.sh_hostname,
                         remote_distr.ldis_e2fsprogs_dir,
                         host.sh_hostname)
            return None
        return remote_distr

    def ldis_needed_zfs_rpm_fnames(self, log):
        """
        Return ZFS RPM file names needed to install.
        """
        # pylint: disable=too-many-branches
        host = self.ldis_host
        fnames = host.sh_get_dir_fnames(log, self.ldis_lustre_rpm_dir)
        if fnames is None:
            log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                         self.ldis_lustre_rpm_dir, host.sh_hostname)
            return None

        needed_fnames = []
        for fname in fnames:
            if fname.startswith("libnvpair"):
                needed_fnames.append(fname)
                continue
            if fname.startswith("libuutil"):
                needed_fnames.append(fname)
                continue

            if fname.startswith("libzfs"):
                if "devel" in fname:
                    continue
                needed_fnames.append(fname)
                continue

            if fname.startswith("libzpool"):
                needed_fnames.append(fname)
                continue

            if fname.startswith("kmod-zfs"):
                if "devel" in fname:
                    continue
                needed_fnames.append(fname)
                continue

            if fname.startswith("zfs-debuginfo"):
                continue
            if fname.startswith("zfs-dkms"):
                continue
            if fname.startswith("zfs-dracut"):
                continue
            if fname.startswith("zfs-kmod-debuginfo"):
                continue
            if fname.startswith("zfs-test"):
                continue
            if fname.startswith("zfs"):
                needed_fnames.append(fname)
                continue
        return needed_fnames

    def _ldis_install_zfs(self, log):
        """
        Install ZFS in this release.
        """
        host = self.ldis_host
        log.cl_info("installing ZFS RPMs on host [%s]", host.sh_hostname)
        install_timeout = constant.LONGEST_SIMPLE_COMMAND_TIME * 2
        rpm_fnames = self.ldis_needed_zfs_rpm_fnames(log)
        if rpm_fnames is None:
            log.cl_error("failed to get ZFS rpms of Lustre release")
            return -1
        if len(rpm_fnames) == 0:
            log.cl_error("no ZFS rpms is found")
            return -1
        command = "cd %s && rpm -ivh" % self.ldis_lustre_rpm_dir
        for rpm_fname in rpm_fnames:
            command += " " + rpm_fname
        retval = host.sh_run(log, command, timeout=install_timeout)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _ldis_install_lustre_rpms(self, log):
        """
        Install Lustre in this release.
        """
        host = self.ldis_host
        install_timeout = constant.LONGEST_SIMPLE_COMMAND_TIME * 2
        log.cl_info("installing Lustre RPMs on host [%s]", host.sh_hostname)
        for rpm_type in lustre_version.LUSTRE_RPM_TYPES:
            if rpm_type not in self.ldis_lustre_rpm_dict:
                continue

            command = ("rpm -ivh --force --nodeps %s/%s" %
                       (self.ldis_lustre_rpm_dir,
                        self.ldis_lustre_rpm_dict[rpm_type]))
            retval = host.sh_run(log, command,
                                 timeout=install_timeout)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def _ldis_check_e2fsprogs(self, log, local_host, workspace):
        """
        Check whether e2fsprogs dir contain expected RPMs
        """
        # pylint: disable=too-many-branches
        host = self.ldis_host

        fnames = host.sh_get_dir_fnames(log, self.ldis_e2fsprogs_rpm_dir)
        if fnames is None:
            log.cl_error("failed to get the file names under [%s] of "
                         "host [%s]",
                         self.ldis_e2fsprogs_rpm_dir,
                         host.sh_hostname)
            return -1

        for fname in fnames:
            if not fname.endswith(".rpm"):
                continue

            if not fname.startswith("e2fsprogs-1"):
                continue

            command = ("rpm -qp %s/%s --queryformat '%%{version}-%%{release} %%{url}'" %
                       (self.ldis_e2fsprogs_rpm_dir, fname))
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, local_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            lines = retval.cr_stdout.splitlines()
            if len(lines) != 1:
                log.cl_error("unexpected output of command [%s] on host [%s], "
                             "stdout = [%s]",
                             command, host.sh_hostname,
                             retval.cr_stdout)
                return -1
            line = lines[0]
            fields = line.split()
            if len(fields) != 2:
                log.cl_error("unexpected output of command [%s] on host [%s], "
                             "stdout = [%s]",
                             command, host.sh_hostname,
                             retval.cr_stdout)
                return -1

            if ("wc" not in fields[0]) or ("whamcloud" not in fields[1]):
                log.cl_error("improper e2fsprogs rpms under dir [%s] of host [%s]",
                             self.ldis_e2fsprogs_rpm_dir,
                             host.sh_hostname)
                return -1
            self.ldis_e2fsprogs_version = fields[0]
        if self.ldis_e2fsprogs_version is None:
            log.cl_error("failed to find e2fsprogs under dir [%s] of host [%s]",
                         self.ldis_e2fsprogs_rpm_dir, host.sh_hostname)
            return -1

        rinfo = e2fsprogs_read_release_info(log, local_host, workspace, host,
                                            self.ldis_e2fsprogs_dir)
        if rinfo is None:
            log.cl_error("failed to read E2fsprogs release info from dir [%s]",
                         self.ldis_e2fsprogs_dir)
            return -1
        if rinfo.rli_file_dict is None:
            log.cl_error("E2fsprogs release [%s] on host [%s] does not contain file information",
                         self.ldis_e2fsprogs_dir,
                         local_host.sh_hostname)
            return -1
        complete = rinfo.rli_files_are_complete(log, host, self.ldis_e2fsprogs_dir,
                                                ignore_unncessary_files=True)
        if complete < 0:
            return -1
        if not complete:
            log.cl_error("E2fsprogs release is not complete")
            return -1

        self.ldis_e2fsprogs_rinfo = rinfo
        return 0

    def _ldis_check_lustre(self, log, local_host, workspace,
                           definition_dir=constant.LUSTRE_VERSION_DEFINITION_DIR):
        """
        Check the Lustre RPMs
        """
        # pylint: disable=too-many-branches,too-many-statements
        # pylint: disable=too-many-locals
        host = self.ldis_host
        rpm_files = host.sh_get_dir_fnames(log, self.ldis_lustre_rpm_dir)
        if rpm_files is None:
            log.cl_error("failed to get fnames under dir [%s] on host [%s]",
                         self.ldis_lustre_rpm_dir, host.sh_hostname)
            return -1

        version_db = lustre_version.load_lustre_version_database(log, local_host,
                                                                 definition_dir)
        if version_db is None:
            log.cl_error("failed to load version database [%s]",
                         definition_dir)
            return -1

        self.ldis_lustre_version, self.ldis_lustre_rpm_dict = \
            version_db.lvd_match_version_from_rpms(log, rpm_files)
        if self.ldis_lustre_version is None:
            log.cl_error("failed to detect Lustre version from RPMs under "
                         "dir [%s] on host [%s]",
                         self.ldis_lustre_rpm_dir,
                         host.sh_hostname)
            return -1
        log.cl_info("Lustre version [%s] detected from RPM files",
                    self.ldis_lustre_version.lv_version_name)

        ldiskfs_disable_reasons = ""
        zfs_disable_reasons = ""
        missing_rpms = ""
        for rpm_name in self.ldis_lustre_version.lv_rpm_pattern_dict:
            if rpm_name in lustre_version.LUSTRE_CLIENT_REQUIRED_RPM_TYPES:
                continue
            if rpm_name not in self.ldis_lustre_rpm_dict:
                if rpm_name in (lustre_version.LVD_LUSTRE_OSD_LDISKFS,
                                lustre_version.LVD_LUSTRE_OSD_LDISKFS_MOUNT):
                    if ldiskfs_disable_reasons != "":
                        ldiskfs_disable_reasons += ","
                    ldiskfs_disable_reasons += rpm_name
                elif rpm_name in (lustre_version.LVD_LUSTRE_OSD_ZFS,
                                  lustre_version.LVD_LUSTRE_OSD_ZFS_MOUNT):
                    if zfs_disable_reasons != "":
                        zfs_disable_reasons += ","
                    zfs_disable_reasons += rpm_name
                elif rpm_name in (lustre_version.LVD_RPM_KERNEL_CORE,
                                  lustre_version.LVD_RPM_KERNEL_MODULES):
                    # kernel-core and kernel-modules do not exist for RHEL7
                    # kernels.
                    pass
                else:
                    if missing_rpms != "":
                        missing_rpms += ","
                    missing_rpms += rpm_name

        if missing_rpms != "":
            log.cl_error("failed to find RPMs [%s] with version [%s] under "
                         "dir [%s]",
                         missing_rpms, self.ldis_lustre_version.lv_version_name,
                         self.ldis_lustre_rpm_dir)
            return -1

        if ldiskfs_disable_reasons != "":
            log.cl_info("ldiskfs support due to missing rpms [%s]",
                        ldiskfs_disable_reasons)
            self.ldis_ldiskfs_support = False

        if zfs_disable_reasons != "":
            log.cl_info("zfs support disabled due to missing rpms [%s]",
                        zfs_disable_reasons)
            self.ldis_zfs_support = False

        if lustre_version.LVD_RPM_KERNEL_CORE in self.ldis_lustre_rpm_dict:
            kernel_rpm_name = self.ldis_lustre_rpm_dict[lustre_version.LVD_RPM_KERNEL_CORE]
        else:
            kernel_rpm_name = self.ldis_lustre_rpm_dict[lustre_version.LVD_RPM_KERNEL]

        kernel_rpm_path = (self.ldis_lustre_rpm_dir + '/' + kernel_rpm_name)
        command = ("rpm -qpl %s | grep /lib/modules/ |"
                   "sed 1q | awk -F '/' '{print $4}'" %
                   kernel_rpm_path)
        retval = utils.run(command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        self.ldis_kernel_version = retval.cr_stdout.strip()

        rinfo = lustre_read_release_info(log, local_host, workspace,
                                         host, self.ldis_lustre_dir)
        if rinfo is None:
            log.cl_error("failed to read Lustre release info from dir [%s] on host [%s]",
                         self.ldis_lustre_dir, host.sh_hostname)
            return -1
        rinfo.rli_nondist_relative_fpaths = self.ldis_nondist_lustre_relative_fpaths
        rinfo.rli_nondist_relative_dir_paths = self.ldis_nondist_lustre_relative_dir_paths
        if rinfo.rli_file_dict is None:
            log.cl_error("Lustre release [%s] on host [%s] does not contain file information",
                         self.ldis_lustre_dir,
                         host.sh_hostname)
            return -1
        complete = rinfo.rli_files_are_complete(log, host, self.ldis_lustre_dir,
                                                ignore_unncessary_files=True)
        if complete < 0:
            return -1
        if not complete:
            log.cl_error("Lustre release [%s] is not complete", self.ldis_lustre_dir)
            return -1

        self.ldis_lustre_rinfo = rinfo
        self.ldis_checked = True
        return 0

    def ldis_check(self, log, local_host, workspace,
                   definition_dir=constant.LUSTRE_VERSION_DEFINITION_DIR):
        """
        Prepare the RPMs
        """
        rc = self._ldis_check_e2fsprogs(log, local_host, workspace)
        if rc:
            log.cl_error("failed to check e2fsprogs RPMs")
            return -1

        rc = self._ldis_check_lustre(log, local_host, workspace, definition_dir=definition_dir)
        if rc:
            log.cl_error("failed to check Lustre RPMs")
            return -1
        self.ldis_checked = True
        return 0

    def _ldis_missing_devel_types(self, log, local_host):
        """
        Return the RPM fnames that need to be installed.
        """
        rpm_dict = {}
        rpm_name_dict = {}
        rpm_name_dict[lustre_version.LVD_LUSTRE] = "lustre"
        rpm_dict[lustre_version.LVD_LUSTRE] = self.ldis_lustre_rpm_dict[lustre_version.LVD_LUSTRE]
        if lustre_version.LVD_LUSTRE_DEVEL in self.ldis_lustre_rpm_dict:
            rpm_name_dict[lustre_version.LVD_LUSTRE_DEVEL] = "lustre-devel"
            rpm_dict[lustre_version.LVD_LUSTRE_DEVEL] = self.ldis_lustre_rpm_dict[lustre_version.LVD_LUSTRE_DEVEL]

        missing_rpm_types = []
        for rpm_type in rpm_dict:
            rpm_name = rpm_name_dict[rpm_type]
            command = "rpm -qi %s" % rpm_name

            retval = local_host.sh_run(log, command)
            if retval.cr_exit_status == 0:
                continue
            missing_rpm_types.append(rpm_type)
        return missing_rpm_types

    def ldis_install_lustre_devel_rpms(self, log, host):
        """
        Install Lustre until on local host
        """
        missing_rpm_types = self._ldis_missing_devel_types(log, host)
        if len(missing_rpm_types) == 0:
            return 0

        log.cl_info("installing RPM [%s] for build on host [%s]",
                    utils.list2string(missing_rpm_types), host.sh_hostname)
        command = "rpm -ivh --nodeps"
        for rpm_type in missing_rpm_types:
            rpm_name = self.ldis_lustre_rpm_dict[rpm_type]
            rpm_fpath = self.ldis_lustre_rpm_dir + "/" + rpm_name
            command += " " + rpm_fpath
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        missing_rpm_types = self._ldis_missing_devel_types(log, host)

        if len(missing_rpm_types) != 0:
            log.cl_error("Lustre rpms [%s] are still not installed",
                         utils.list2string(missing_rpm_types))
            return -1
        return 0

    def ldis_install_e2fsprogs_rpms(self, log):
        """
        Install E2fsprogs until on host
        """
        host = self.ldis_host
        need_install = False
        retval = host.sh_run(log, "rpm -q e2fsprogs --queryformat '%{version}-%{release}'")
        if retval.cr_exit_status:
            log.cl_info("installing e2fsprogs [%s] on host [%s]",
                        self.ldis_e2fsprogs_version, host.sh_hostname)
            need_install = True
        else:
            current_version = retval.cr_stdout
            if self.ldis_e2fsprogs_version != current_version:
                log.cl_info("upgrading e2fsprogs from [%s] to [%s] on host [%s]",
                            current_version, self.ldis_e2fsprogs_version,
                            host.sh_hostname)
                need_install = True

        if not need_install:
            log.cl_info("e2fsprogs [%s] already installed on host [%s]",
                        self.ldis_e2fsprogs_version,
                        host.sh_hostname)
            return 0

        command = "rpm -Uvh %s/*.rpm" % self.ldis_e2fsprogs_rpm_dir
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def ldis_can_skip_install_lustre(self, log, host):
        """
        Check whether the install of Lustre RPMs could be skipped
        """
        for rpm_fname in self.ldis_lustre_rpm_dict.values():
            log.cl_debug("checking whether RPM [%s] is installed on "
                         "host [%s]", rpm_fname, host.sh_hostname)
            rpm_name, _ = os.path.splitext(rpm_fname)
            if not host.sh_has_rpm(log, rpm_name):
                log.cl_debug("RPM [%s] is not installed on host [%s], "
                             "will not skip install",
                             rpm_name, host.sh_hostname)
                return False
        return True

    def ldis_install(self, log):
        """
        Install this release.
        """
        ret = self._ldis_install_kernel(log)
        if ret:
            log.cl_error("failed to install Lustre kernel")
            return -1

        ret = self._ldis_install_ofed(log)
        if ret:
            log.cl_error("failed to install OFED")
            return -1

        ret = self.ldis_install_e2fsprogs_rpms(log)
        if ret:
            log.cl_error("failed to install e2fsprogs")
            return -1

        if self.ldis_zfs_support:
            ret = self._ldis_install_zfs(log)
            if ret:
                log.cl_error("failed to install ZFS")
                return -1

        ret = self._ldis_install_lustre_rpms(log)
        if ret:
            log.cl_error("failed to install Lustre RPMs")
            return -1
        return 0


def lustre_client_id(fsname, mnt):
    """
    Return the Lustre client ID
    """
    return "%s:%s" % (fsname, mnt)


class LustreOBDDevice():
    """
    The device status got from "lctl devices"
    """
    # pylint: disable=too-many-instance-attributes,too-few-public-methods
    def __init__(self, device_index, status, type_name,
                 obd_name, obd_uuid, refcount):
        # String of device index
        self.lobd_device_index = device_index
        # ST: stopping, IN: inactive, UP: set_up, AT: attached, __: else.
        self.lobd_device_status = status
        # Device type, e.g. mds, osd-ldiskfs, mgc, lod, mdt, mdd, qmt, osp, lwp.
        self.lobd_type_name = type_name
        # OBD name, e.g. MDS, lustre0-MDT0000-osd, MGS, MGC10.0.1.1@tcp,
        # lustre0-MDT0000-mdtlov, lustre0-MDT0000, lustre0-QMT0000,
        # lustre0-MDT0001-osp-MDT0000, lustre0-OST0000-osc-MDT0000,
        # lustre0-MDT0000-lwp-MDT0000
        self.lobd_obd_name = obd_name
        # UUID, e.g. MDS_uuid, lustre0-MDT0000-osd_UUID, MGS,
        # c00a4a1f-5e08-4054-9b81-f4b055923152, lustre0-MDT0000-mdtlov_UUID,
        # lustre0-MDT0000_UUID, lustre0-MDD0000_UUID, lustre0-QMT0000_UUID,
        # lustre0-MDT0000-mdtlov_UUID, lustre0-MDT0000-lwp-MDT0000_UUID
        self.lobd_uuid = obd_uuid
        # String of reference count
        self.lobd_refcount = refcount


def obd_line2device(log, line):
    """
    See obd_device_list_seq_show() for the outout format.
    """
    fields = line.split()
    if len(fields) != 6:
        log.cl_error("unexpected field number of line [%s]", line)
        return None
    device_index = fields[0]
    status = fields[1]
    type_name = fields[2]
    obd_name = fields[3]
    obd_uuid = fields[4]
    refcount = fields[5]
    return LustreOBDDevice(device_index, status, type_name,
                           obd_name, obd_uuid, refcount)


class LustreHost(ssh_host.SSHHost):
    # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """
    Each host being used to run Lustre tests has an object of this
    """
    def __init__(self, hostname, identity_file=None,
                 ssh_port=22, local=False, is_server=False, is_client=False,
                 ssh_for_local=True, login_name="root"):
        super().__init__(hostname, identity_file=identity_file,
                         ssh_port=ssh_port, local=local,
                         ssh_for_local=ssh_for_local, login_name=login_name)
        # key: $fsname:$mnt, value: LustreClient object
        self.lsh_clients = {}
        # Key: ls_service_name, value: LustreOSTInstance object
        self.lsh_ost_instances = {}
        # Key: ls_service_name, value: LustreMDTInstance object
        self.lsh_mdt_instances = {}
        self.lsh_cached_has_fuser = None
        self.lsh_fuser_install_failed = False
        self.lsh_mgsi = None
        # Key: ls_service_name, value instance object
        self.lsh_instance_dict = {}
        self.lsh_lustre_version_major = None
        self.lsh_lustre_version_minor = None
        self.lsh_lustre_version_patch = None
        self.lsh_version_value = None
        self.lsh_is_server = is_server
        self.lsh_is_client = is_client

    def lsh_detect_lustre_version(self, log):
        """
        Detect the Lustre version
        """
        command = ("lctl lustre_build_version")
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        lustre_version_string = retval.cr_stdout.strip()
        version_pattern = (r"^.+(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+).+$")
        version_regular = re.compile(version_pattern)
        match = version_regular.match(lustre_version_string)
        if match:
            self.lsh_lustre_version_major = int(match.group("major"))
            self.lsh_lustre_version_minor = int(match.group("minor"))
            self.lsh_lustre_version_patch = int(match.group("patch"))
        else:
            log.cl_error("unexpected version string format: [%s]",
                         lustre_version_string)
            return -1

        self.lsh_version_value = version_value(self.lsh_lustre_version_major,
                                               self.lsh_lustre_version_minor,
                                               self.lsh_lustre_version_patch)
        log.cl_debug("version_string: %s %d", lustre_version_string,
                     self.lsh_version_value)
        return 0

    def lsh_osti_add(self, osti):
        """
        Add OST into this host
        """
        service_name = osti.lsi_service.ls_service_name
        if service_name in self.lsh_ost_instances:
            return -1
        self.lsh_ost_instances[service_name] = osti
        self.lsh_instance_dict[service_name] = osti
        return 0

    def lsh_mgsi_add(self, log, mgsi):
        """
        Add MGSI into this host
        """
        if self.lsh_mgsi is not None:
            log.cl_error("MGS already exits on host [%s]", self.sh_hostname)
            return -1
        self.lsh_mgsi = mgsi
        service_name = mgsi.lsi_service.ls_service_name
        self.lsh_instance_dict[service_name] = mgsi
        return 0

    def lsh_mdti_add(self, mdti):
        """
        Add MDT into this host
        """
        service_name = mdti.lsi_service.ls_service_name
        if service_name in self.lsh_mdt_instances:
            return -1
        self.lsh_mdt_instances[service_name] = mdti
        self.lsh_instance_dict[service_name] = mdti
        return 0

    def lsh_client_add(self, fsname, mnt, client):
        """
        Add MDT into this host
        """
        client_id = lustre_client_id(fsname, mnt)
        if client_id in self.lsh_clients:
            return -1
        self.lsh_clients[client_id] = client
        return 0

    def lsh_lustre_device_label(self, log, device,
                                backfstype=BACKFSTYPE_LDISKFS):
        """
        Run e2label on a Lustre device
        """
        if backfstype == BACKFSTYPE_LDISKFS:
            command = ("e2label %s" % device)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return None
        else:
            command = ("zfs get -H lustre:svname %s | awk {'print $3'}" % device)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return None
        return retval.cr_stdout.strip()

    def lsh_lustre_device_service_name(self, log, device,
                                       backfstype=BACKFSTYPE_LDISKFS):
        """
        Return the service name from device label.
        Note that the label could either be "MGS", "fsname-OST0000",
        or "fsname:OST0000". This function will replace ":" to "-".
        """
        label = self.lsh_lustre_device_label(log, device,
                                             backfstype=backfstype)
        if label is None:
            log.cl_error("failed to get the lable of device [%s]",
                         device)
            return None
        return label.replace(":", "-")

    def lsh_lustre_detect_services(self, log, client_dict,
                                   service_instance_dict,
                                   mdt_instance_type=LustreMDTInstance,
                                   add_found=False):
        """
        Detect mounted Lustre services (MGS/MDT/OST/clients) from the host
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        server_pattern = (r"^(?P<device>\S+) (?P<mount_point>\S+) lustre (?P<options>\S+) .+$")
        server_regular = re.compile(server_pattern)

        client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
        client_regular = re.compile(client_pattern)

        # Detect Lustre services
        command = ("cat /proc/mounts")
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            match = server_regular.match(line)
            if not match:
                continue

            device = match.group("device")
            mount_point = match.group("mount_point")
            option_string = match.group("options")
            option_pairs = option_string.split(",")

            match = client_regular.match(line)
            if match:
                fsname = match.group("fsname")
                client_id = lustre_client_id(fsname, mount_point)
                if client_id in self.lsh_clients:
                    client = self.lsh_clients[client_id]
                else:
                    lustre_fs = LustreFilesystem(fsname)
                    client = init_client(log, lustre_fs, self, mount_point,
                                         add_to_host=add_found)
                    if client is None:
                        return -1
                client_dict[client_id] = client
                log.cl_debug("client [%s] mounted on dir [%s] of host [%s]",
                             fsname, mount_point, self.sh_hostname)
                continue
            # Format determined by server_show_options()
            is_mgs = bool("mgs" in option_pairs)

            if "osd=osd-zfs" in option_pairs:
                backfstype = BACKFSTYPE_ZFS
                fields = device.split("/")
                if len(fields) != 2:
                    log.cl_error("invalid device [%s] for Lustre service on ZFS",
                                 device)
                    return -1
                zpool_name = fields[0]
            elif "osd=osd-ldiskfs" in option_pairs:
                backfstype = BACKFSTYPE_LDISKFS
                zpool_name = None
            else:
                log.cl_error("not able to get the backfstype for device [%s] on "
                             "host [%s]", device, self.sh_hostname)
                return -1

            label = self.lsh_lustre_device_service_name(log, device,
                                                        backfstype=backfstype)
            if label is None:
                log.cl_error("failed to get the label of device [%s] on "
                             "host [%s]", device, self.sh_hostname)
                return -1

            match = OST_SERVICE_REG.match(label)
            if match:
                fsname = match.group("fsname")
                index_string = match.group("index_string")
                ost_index = lustre_string2index(log, index_string)
                if ost_index is None:
                    log.cl_error("invalid label [%s] of device [%s] on "
                                 "host [%s]", label, device, self.sh_hostname)
                    return -1
                service_name = fsname + "-OST" + index_string

                if service_name in self.lsh_ost_instances:
                    osti = self.lsh_ost_instances[service_name]
                else:
                    lustre_fs = LustreFilesystem(fsname)
                    ost = init_ost(log, lustre_fs, ost_index, backfstype,
                                   zpool_name=zpool_name)
                    if ost is None:
                        return -1
                    osti = init_ost_instance(log, ost, self, device, mount_point,
                                             None, add_to_host=add_found)
                    if osti is None:
                        return -1
                service_instance_dict[service_name] = osti
                log.cl_debug("OST [%s] mounted on dir [%s] of host [%s]",
                             service_name, mount_point, self.sh_hostname)
                continue

            match = MDT_SERVICE_REG.match(label)
            if match:
                fsname = match.group("fsname")
                index_string = match.group("index_string")
                mdt_index = lustre_string2index(log, index_string)
                if mdt_index is None:
                    log.cl_error("invalid label [%s] of device [%s] on "
                                 "host [%s]", label, device, self.sh_hostname)
                    return -1
                service_name = fsname + "-MDT" + index_string

                if service_name in self.lsh_mdt_instances:
                    mdti = self.lsh_mdt_instances[service_name]
                else:
                    lustre_fs = LustreFilesystem(fsname)
                    mdt = init_mdt(log, lustre_fs, mdt_index, backfstype,
                                   is_mgs=is_mgs, zpool_name=zpool_name)
                    if mdt is None:
                        return -1
                    mdti = init_mdt_instance(log, mdt, self, device,
                                             mount_point, None,
                                             add_to_host=add_found,
                                             mdt_instance_type=mdt_instance_type)
                    if mdti is None:
                        return -1
                service_instance_dict[service_name] = mdti
                log.cl_debug("MDT [%s] mounted on dir [%s] of host [%s]",
                             fsname, mount_point, self.sh_hostname)
                continue

            match = MGT_SERVICE_REG.match(label)
            if match:
                log.cl_debug("MGT mounted on dir [%s] of host [%s]",
                             mount_point, self.sh_hostname)
                # There is no reliable way to check whether the MGS mounted
                # is exactly the one configured. But since only one MGS is
                # allowed on a host. If the mount point is the same, consider
                # it is the configured MGS.
                if (self.lsh_mgsi is not None and
                        mount_point == self.lsh_mgsi.lsi_mnt):
                    mgsi = self.lsh_mgsi
                    service_name = mgsi.lsi_service.ls_service_name
                else:
                    service_name = "MGS-" + utils.random_word(8)
                    mgs = LustreMGS(service_name, backfstype,
                                    zpool_name=zpool_name)
                    mgsi = init_mgs_instance(log, mgs, self, device,
                                             mount_point, None,
                                             add_to_host=add_found)
                    if mgsi:
                        return -1
                service_instance_dict[service_name] = mgsi
                continue

            log.cl_error("unable to detect service mounted on dir [%s] of "
                         "host [%s]", mount_point, self.sh_hostname)
            return -1

        return 0

    def lsh_lustre_umount_services(self, log, client_only=False):
        """
        Umount Lustre OSTs/MDTs/clients on the host
        """
        # pylint: disable=too-many-return-statements
        client_dict = {}
        service_instance_dict = {}
        ret = self.lsh_lustre_detect_services(log, client_dict,
                                              service_instance_dict)
        if ret:
            log.cl_error("failed to detect Lustre services on host [%s]",
                         self.sh_hostname)
            return -1

        for client in client_dict.values():
            command = ("umount -f %s" % client.lc_mnt)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_debug("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            else:
                continue

            # Kill the user of Lustre client to umount
            ret = self.sh_fuser_kill(log, client.lc_mnt)
            if ret:
                log.cl_error("failed to kill processes using [%s]",
                             client.lc_mnt)
                return -1

            command = ("umount -f %s" % client.lc_mnt)
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

        if client_only:
            return 0

        for service_type in [LUSTRE_SERVICE_TYPE_MDT, LUSTRE_SERVICE_TYPE_OST,
                             LUSTRE_SERVICE_TYPE_MGT]:
            for instance in service_instance_dict.values():
                if instance.lsi_service.ls_service_type != service_type:
                    continue
                command = ("umount %s" % instance.lsi_mnt)
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

    def _lsh_yum_cleanup(self, log):
        """
        Cleanup YUM repository.
        """
        distro = self.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get distro of host [%s]",
                         self.sh_hostname)
            return -1
        if distro != os_distro.DISTRO_RHEL7:
            return 0
        command = "rpm --rebuilddb"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        log.cl_info("cleaning up yum processes on host [%s]",
                    self.sh_hostname)
        ret = self.sh_run(log, "ps aux | grep -v grep | grep yum | "
                          "awk '{print $2}'")
        if ret.cr_exit_status != 0:
            log.cl_error("failed to kill yum processes on host "
                         "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                         self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout, ret.cr_stderr)
            return -1

        for pid in ret.cr_stdout.splitlines():
            log.cl_debug("killing pid [%s] on host [%s]",
                         pid, self.sh_hostname)
            ret = self.sh_run(log, "kill -9 %s" % pid)
            if ret.cr_exit_status != 0:
                log.cl_error("failed to kill pid [%s] on host "
                             "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                             pid, self.sh_hostname, ret.cr_exit_status,
                             ret.cr_stdout, ret.cr_stderr)
                return -1

        log.cl_info("running yum-complete-transaction on host [%s]",
                    self.sh_hostname)
        ret = self.sh_run(log, "which yum-complete-transaction")
        if ret.cr_exit_status != 0:
            ret = self.sh_yum_install(log, ["yum-utils"])
            if ret:
                log.cl_error("failed to install yum-utils on host [%s]",
                             self.sh_hostname)
                return -1

        ret = self.sh_run(log, "yum-complete-transaction")
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run yum-complete-transaction on host "
                         "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                         self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout, ret.cr_stderr)
            return -1
        return 0

    def _lsh_lustre_uninstall(self, log):
        # pylint: disable=too-many-return-statements,too-many-branches
        # pylint: disable=too-many-statements
        """
        Uninstall Lustre RPMs
        """
        log.cl_info("uninstalling existing Lustre RPMs on host [%s]",
                    self.sh_hostname)
        names = ["lustre", "libnvpair", "libuutil", "libzfs",
                 "libzpool", "kmod-spl", "kmod-zfs", "spl", "zfs"]
        for name in names:
            grep_command = "grep " + name
            ret = self.sh_rpm_find_and_uninstall(log, grep_command)
            if ret != 0:
                log.cl_error("failed to uninstall RPMs with name [%s] on host "
                             "[%s]", name, self.sh_hostname)
                return -1
        return 0

    def lsh_lustre_install(self, log, workspace, local_lustre_distr):
        """
        Install Lustre RPMs on a host.
        """
        local_host = local_lustre_distr.ldis_host
        if not local_lustre_distr.ldis_checked:
            log.cl_error("Lustre release [%s] on local host [%s] has not been checked",
                         local_lustre_distr.ldis_lustre_dir,
                         local_host.sh_hostname)
            return -1

        log.cl_info("updating ssh config of host [%s]", self.sh_hostname)
        ret = self.sh_disable_ssh_key_checking(log)
        if ret:
            log.cl_error("failed to update SSH config on host [%s]",
                         self.sh_hostname)
            return -1

        log.cl_info("installing Lustre release [%s] of local host [%s] on host [%s]",
                    local_lustre_distr.ldis_lustre_dir,
                    local_host.sh_hostname,
                    self.sh_hostname)

        host_lustre_dir = workspace + "/" + constant.CORAL_LUSTRE_RELEASE_BASENAME
        host_e2fsprogs_dir = workspace + "/" + constant.CORAL_E2FSPROGS_RELEASE_BASENAME
        remote_dist = local_lustre_distr.ldis_send_from_local(log, self,
                                                              host_lustre_dir,
                                                              host_e2fsprogs_dir)
        if remote_dist is None:
            log.cl_error("failed to send Lustre release to host [%s]",
                         self.sh_hostname)
            return -1
        ret = remote_dist.ldis_install(log)
        if ret:
            log.cl_error("failed to install Lustre release [%s] on host [%s]",
                         host_lustre_dir, self.sh_hostname)
            return -1
        return 0

    def _lsh_lustre_reinstall(self, log, workspace, local_lustre_distr):
        """
        Reinstall Lustre RPMs
        """
        ret = self._lsh_yum_cleanup(log)
        if ret:
            log.cl_error("failed to cleanup YUM status on host [%s]",
                         self.sh_hostname)
            return -1

        ret = self._lsh_lustre_uninstall(log)
        if ret:
            log.cl_error("failed to uninstall Lustre RPMs on host [%s]",
                         self.sh_hostname)
            return -1

        ret = self.lsh_lustre_install(log, workspace, local_lustre_distr)
        if ret != 0:
            log.cl_error("failed to install RPMs on host [%s]",
                         self.sh_hostname)
            return -1
        return 0

    def _lsh_lustre_check_clean(self, log, local_lustre_distr, quiet=True):
        """
        Check whether the host is clean for running Lustre
        """
        if quiet:
            func = log.cl_debug
        else:
            func = log.cl_error
            log.cl_info("checking whether host [%s] is clean to run Lustre",
                        self.sh_hostname)
        if local_lustre_distr is None:
            func("Lustre release is not configured for host [%s]",
                 self.sh_hostname)
            return -1
        if not local_lustre_distr.ldis_checked:
            func("Lustre release [%s] is not prepared for host [%s]",
                 local_lustre_distr.ldis_lustre_dir,
                 self.sh_hostname)
            return -1
        kernel_version = local_lustre_distr.ldis_kernel_version
        # Check whether kernel is installed kernel
        if not self.sh_is_up(log):
            func("host [%s] is not up", self.sh_hostname)
            return -1

        if kernel_version != self.sh_get_kernel_ver(log):
            func("host [%s] has a wrong kernel version, expected "
                 "[%s], got [%s]", self.sh_hostname, kernel_version,
                 self.sh_get_kernel_ver(log))
            return -1

        # Run some fundamental command to check Lustre is installed correctly
        check_commands = ["lustre_rmmod", "depmod -a", "modprobe lustre"]
        for command in check_commands:
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                func("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
                return -1
        log.cl_info("host [%s] is clean to run Lustre",
                    self.sh_hostname)
        return 0

    def lsh_lustre_prepare(self, log, workspace, local_lustre_distr,
                           lazy_prepare=False, skip_reboot=False):
        """
        Prepare the host for running Lustre
        """
        # pylint: disable=too-many-branches,too-many-statements
        if (not self.lsh_is_server) and (not self.lsh_is_client):
            # Just cleanup the Lustre module to make the health_check happy
            ret = self.sh_run(log, "which lustre_rmmod")
            if ret.cr_exit_status != 0:
                return 0

            command = "lustre_rmmod"
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_info("failed to run command [%s] on host [%s], "
                            "ret = [%d], stdout = [%s], stderr = [%s]",
                            command,
                            self.sh_hostname,
                            retval.cr_exit_status,
                            retval.cr_stdout,
                            retval.cr_stderr)
                return -1
            return 0

        if local_lustre_distr is None:
            if self.lsh_is_server:
                log.cl_error("failed to prepare Lustre server [%s] because Lustre RPMs is not configured",
                             self.sh_hostname)
                return -1
            log.cl_info("Lustre RPMs is not configured for host [%s], do not prepare Lustre",
                        self.sh_hostname)
            return 0
        if not local_lustre_distr.ldis_checked:
            log.cl_error("Lustre release [%s] has not been checked",
                         local_lustre_distr.ldis_lustre_dir,
                         self.sh_hostname)
            return -1
        if lazy_prepare and local_lustre_distr.ldis_can_skip_install_lustre(log, self):
            log.cl_info("skipping installation of Lustre RPMs on host [%s]",
                        self.sh_hostname)
        else:
            ret = self._lsh_lustre_reinstall(log, workspace,
                                             local_lustre_distr)
            if ret:
                log.cl_error("failed to reinstall Lustre RPMs on host [%s]",
                             self.sh_hostname)
                return -1

        # Generate the /etc/hostid so that mkfs.lustre with ZFS won't complain
        command = "genhostid"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        need_reboot = False
        ret = self.lsh_lustre_umount_services(log)
        if ret:
            log.cl_info("failed to umount Lustre clients and servers on "
                        "host [%s], reboot is needed",
                        self.sh_hostname)
            need_reboot = True

        if lazy_prepare and not need_reboot:
            ret = self._lsh_lustre_check_clean(log, local_lustre_distr)
            if ret:
                log.cl_debug("host [%s] need a reboot to change the kernel "
                             "or cleanup the status of Lustre",
                             self.sh_hostname)
                need_reboot = True
            else:
                ret = self.sh_service_start_enable(log, "kdump")
                if ret:
                    log.cl_error("failed to start and enable kdump service")
                    return -1

        if not lazy_prepare:
            need_reboot = True

        if need_reboot:
            default_kernel = "/boot/vmlinuz-" + local_lustre_distr.ldis_kernel_version
            ret = self.sh_kernel_set_default(log, default_kernel)
            if ret:
                log.cl_error("failed to set default kernel of host [%s] to [%s]",
                             self.sh_hostname,
                             default_kernel)
                return -1

            if skip_reboot:
                log.cl_info("need to reboot host [%s] later",
                            self.sh_hostname)
                return 0

            ret = self.sh_reboot(log)
            if ret:
                log.cl_error("failed to reboot host [%s]", self.sh_hostname)
                return -1

            ret = self._lsh_lustre_check_clean(log, local_lustre_distr,
                                               quiet=False)
            if ret:
                log.cl_error("host [%s] is not clean to run Lustre",
                             self.sh_hostname)
                return -1

            ret = self.sh_service_start_enable(log, "kdump")
            if ret:
                log.cl_error("failed to start and enable kdump service")
                return -1

        return 0

    def lsh_get_fsname(self, log, path):
        """
        Return the fsname of a Lustre path.
        """
        client_name = self.lsh_getname(log, path)
        if client_name is None:
            return None
        fields = client_name.split("-")
        if len(fields) != 2:
            log.cl_error("invalid client name [%s] of Lustre mnt [%s] "
                         "on host [%s]",
                         client_name, path, self.sh_hostname)
            return None
        return fields[0]

    def lsh_getname(self, log, path):
        """
        Return the Lustre client name on a path.
        If error, return None.
        """
        command = ("lfs getname %s" % (path))
        log.cl_debug("start to run command [%s] on host [%s]", command,
                     self.sh_hostname)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        output = retval.cr_stdout.strip()
        name_pattern = (r"^(?P<client_name>\S+) %s$" % path)
        name_regular = re.compile(name_pattern)
        match = name_regular.match(output)
        client_name = None
        if match:
            client_name = match.group("client_name")
        else:
            log.cl_error("failed to parse output [%s] to get name",
                         output)
            return None
        return client_name

    def lsh_get_client_uuid(self, log, path):
        """
        Return the UUID of a mnt of Lustre client.
        If error, return None.
        """
        client_name = self.lsh_getname(log, path)
        if client_name is None:
            log.cl_error("failed to get the client name of path [%s] "
                         "on host [%s]", path, self.sh_hostname)
            return None

        command = ("lctl get_param -n llite.%s.uuid" % (client_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout

    def lsh_detect_mgs_filesystems(self, log):
        """
        Detect Lustre file system managed by a MGS
        """
        command = ("cat /proc/fs/lustre/mgs/MGS/filesystems")
        filesystems = []
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return filesystems
        for line in retval.cr_stdout.splitlines():
            filesystem = line.strip()
            filesystems.append(filesystem)
        return filesystems

    def lsh_get_balanced_load(self):
        """
        Return the (load_per_host, has_remainder) when balanced.
        """
        service_number = len(self.lsh_instance_dict)
        host_number = None
        for instance in self.lsh_instance_dict.values():
            service = instance.lsi_service
            if host_number is None:
                host_number = len(service.ls_instance_dict)
                break

        if host_number is None:
            return 0, 0

        load_per_host = service_number // host_number
        has_remainder = (load_per_host * host_number) != service_number
        return load_per_host, has_remainder

    def lsh_get_load_status(self, log):
        """
        Return 0 if the host is load balanced.
        Return 1 if overloaded.
        Return -1 if underloaded.
        Return None on error.

        This function assumes that the cluster is symmetric.

        A symmetric cluster is balanced iff:

        Every host provides the mean load in the symmetric host group.

        A symmetric host group in a symmetric cluster includes all the hosts
        that can provide the same services.
        """
        hostname = self.sh_hostname
        load_per_host, has_remainder = self.lsh_get_balanced_load()
        # Host without any service is considered as load balanced
        if load_per_host == 0 and has_remainder == 0:
            return 0

        client_dict = {}
        service_instance_dict = {}
        ret = self.lsh_lustre_detect_services(log, client_dict,
                                              service_instance_dict)
        if ret:
            log.cl_error("failed to check the load on host [%s]",
                         hostname)
            return None

        load = len(service_instance_dict)

        if has_remainder:
            if load > load_per_host + 1:
                log.cl_debug("host [%s] is overloaded, expecting [%s] or [%s], "
                             "got [%s]",
                             hostname, load_per_host, load_per_host + 1, load)
                return 1
            if load < load_per_host:
                log.cl_debug("host [%s] is underloaded, expecting [%s] or [%s], "
                             "got [%s]",
                             hostname, load_per_host, load_per_host + 1, load)
                return -1
        else:
            if load > load_per_host:
                log.cl_debug("host [%s] is overloaded, expecting [%s], "
                             "got [%s]",
                             hostname, load_per_host, load)
                return 1
            if load < load_per_host:
                log.cl_debug("host [%s] is underloaded, expecting [%s], "
                             "got [%s]",
                             hostname, load_per_host, load)
                return -1
        return 0

    def lsh_healty_check(self, log):
        """
        Return the healthy check result. Return LUSTRE_STR_ERROR on failure
        """
        if (not self.lsh_is_server) and (not self.lsh_is_client):
            return constant.LUSTRE_STR_HEALTHY

        modules = self.sh_lsmod(log)
        if modules is None:
            return constant.LUSTRE_STR_ERROR

        if "lustre" not in modules:
            return constant.LUSTRE_STR_HEALTHY

        command = 'lctl get_param -n health_check'
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return constant.LUSTRE_STR_ERROR
        healthy = retval.cr_stdout.strip()
        if healthy == "healthy":
            return constant.LUSTRE_STR_HEALTHY
        if healthy == "NOT HEALTHY":
            return constant.LUSTRE_STR_UNHEALTHY
        if healthy == "LBUG":
            return constant.LUSTRE_STR_LBUG
        log.cl_error("unexpected output of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return constant.LUSTRE_STR_ERROR

    def lsh_panic_on_lbug_disable(self, log):
        """
        Disable libcfs_panic_on_lbug
        """
        command = 'echo -n 0 > /sys/module/libcfs/parameters/libcfs_panic_on_lbug'
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_panic_on_lbug_enable(self, log):
        """
        Enable libcfs_panic_on_lbug
        """
        command = 'echo -n 1 > /sys/module/libcfs/parameters/libcfs_panic_on_lbug'
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_force_lbug(self, log):
        """
        Force the host to LBUG
        """
        self.sh_run(log, "lctl set_param force_lbug=1", timeout=1)

        # Don't check the return value or output. The command will eitehr
        # trigger a kernel panic or stuck the ssh process
        return 0

    def lsh_is_active_mgs(self, log, fsname):
        """
        Check whether the host is active.
        """
        command = "lctl get_param mgs.MGS.live.%s" % fsname
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            return 0
        return 1

    def lsh_clear_changelog_for_user(self, log,
                                     mdt_service_name, changelog_user):
        """
        Clear changelog for a specific user.
        """
        hostname = self.sh_hostname
        command = ("lfs changelog_clear %s %s 0" %
                   (mdt_service_name, changelog_user))
        log.cl_info("running command [%s] on host [%s]",
                    command, hostname)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_check_device_is_ost(self, log, ost_device,
                                expected_fsname=None):
        """
        Chech whether the device is OST. Return service_name if valid.
        Return None if not.
        """
        label = self.lsh_lustre_device_service_name(log, ost_device,
                                                    backfstype=BACKFSTYPE_LDISKFS)
        if label is None:
            log.cl_error("failed to get the label of device [%s] on "
                         "host [%s]", ost_device,
                         self.sh_hostname)
            return None
        match = OST_SERVICE_REG.match(label)
        if not match:
            log.cl_error("label [%s] of device [%s] on host [%s] does not "
                         "look like a Lustre OST",
                         label, ost_device,
                         self.sh_hostname)
            return None

        if expected_fsname is not None:
            fsname = match.group("fsname")
            if fsname != expected_fsname:
                log.cl_error("unexpected fsname [%s] for device [%s] on "
                             "host [%s], expected [%s]",
                             fsname, ost_device, self.sh_hostname,
                             expected_fsname)
                return None

        index_string = match.group("index_string")
        ost_index = lustre_string2index(log, index_string)
        if ost_index is None:
            log.cl_error("invalid index of label [%s] for device [%s] on "
                         "host [%s]", label, ost_device,
                         self.sh_hostname)
            return None
        return label

    def lsh_check_device_is_mdt(self, log, mdt_device,
                                expected_fsname=None):
        """
        Chech whether the device is MDT. Return service_name if valid.
        Return None if not.
        """
        label = self.lsh_lustre_device_service_name(log, mdt_device,
                                                    backfstype=BACKFSTYPE_LDISKFS)
        if label is None:
            log.cl_error("failed to get the label of device [%s] on "
                         "host [%s]", mdt_device,
                         self.sh_hostname)
            return None
        match = MDT_SERVICE_REG.match(label)
        if not match:
            log.cl_error("label [%s] of device [%s] on host [%s] does not "
                         "look like a Lustre MDT",
                         label, mdt_device,
                         self.sh_hostname)
            return None

        if expected_fsname is not None:
            fsname = match.group("fsname")
            if fsname != expected_fsname:
                log.cl_error("unexpected fsname [%s] for device [%s] on "
                             "host [%s], expected [%s]",
                             fsname, mdt_device, self.sh_hostname,
                             expected_fsname)
                return None

        index_string = match.group("index_string")
        mdt_index = lustre_string2index(log, index_string)
        if mdt_index is None:
            log.cl_error("invalid index of label [%s] for device [%s] on "
                         "host [%s]", label, mdt_device,
                         self.sh_hostname)
            return None
        return label

    def lsh_get_obd_device_dict(self, log):
        """
        Return a dict of LustreOBDDevice(). Key is obd name.

        See obd_device_list_seq_show() for the output format.
        Example output of "lctl device_list":
  0 UP osd-ldiskfs lustre0-MDT0000-osd lustre0-MDT0000-osd_UUID 12
  1 UP mgs MGS MGS 16
  2 UP mgc MGC10.0.2.97@tcp 8854c555-7e53-4f14-8d69-5b854f71dd1e 4
  3 UP mds MDS MDS_uuid 2
  4 UP lod lustre0-MDT0000-mdtlov lustre0-MDT0000-mdtlov_UUID 3
  5 UP mdt lustre0-MDT0000 lustre0-MDT0000_UUID 22
  6 UP mdd lustre0-MDD0000 lustre0-MDD0000_UUID 3
  7 UP qmt lustre0-QMT0000 lustre0-QMT0000_UUID 3
  8 UP lwp lustre0-MDT0000-lwp-MDT0000 lustre0-MDT0000-lwp-MDT0000_UUID 4
  9 UP osd-ldiskfs lustre0-OST0001-osd lustre0-OST0001-osd_UUID 4
 10 UP ost OSS OSS_uuid 2
 11 UP osp lustre0-OST0000-osc-MDT0000 lustre0-MDT0000-mdtlov_UUID 4
 12 UP obdfilter lustre0-OST0001 lustre0-OST0001_UUID 4
 13 UP lwp lustre0-MDT0000-lwp-OST0001 lustre0-MDT0000-lwp-OST0001_UUID 4
 14 UP osp lustre0-OST0001-osc-MDT0000 lustre0-MDT0000-mdtlov_UUID 4
 15 UP osp lustre0-OST0002-osc-MDT0000 lustre0-MDT0000-mdtlov_UUID 4
 16 UP osp lustre0-OST0003-osc-MDT0000 lustre0-MDT0000-mdtlov_UUID 4
 17 UP osp lustre0-OST0004-osc-MDT0000 lustre0-MDT0000-mdtlov_UUID 4
 18 UP lov lustre0-clilov-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 3
 19 UP lmv lustre0-clilmv-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 4
 20 UP mdc lustre0-MDT0000-mdc-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 4
 21 UP osc lustre0-OST0000-osc-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 4
 22 UP osc lustre0-OST0001-osc-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 4
 23 UP osc lustre0-OST0002-osc-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 4
 24 UP osc lustre0-OST0003-osc-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 4
 25 UP osc lustre0-OST0004-osc-ffff95f70170b800 0bf8d41f-fc2b-4dc0-ae72-0127d8ef83c5 4
        """
        obd_device_dict = {}
        command = "lctl device_list"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return obd_device_dict

        lines = retval.cr_stdout.splitlines()
        for line in lines:
            obd_device = obd_line2device(log, line)
            if obd_device is None:
                return None

            if obd_device.lobd_obd_name in obd_device_dict:
                log.cl_error("multiple OBD device with same name [%s] on host [%s]",
                             obd_device.lobd_obd_name, self.sh_hostname)
                return None
            obd_device_dict[obd_device.lobd_obd_name] = obd_device
        return obd_device_dict

    def lsh_mgs_running(self, log):
        """
        Return 1 if MGS is running on this host. Return 0 if not. Return -1 on error.
        """
        obd_device_dict = self.lsh_get_obd_device_dict(log)
        if obd_device_dict is None:
            log.cl_error("failed to get OBD devices")
            return -1
        if "MGS" in obd_device_dict:
            return 1
        return 0

    def lsh_mdt_running(self, log):
        """
        Return 1 if MDT is running on this host. Return 0 if not. Return -1 on error.
        """
        obd_device_dict = self.lsh_get_obd_device_dict(log)
        if obd_device_dict is None:
            log.cl_error("failed to get OBD devices")
            return -1
        for obd_device in obd_device_dict.values():
            if obd_device.lobd_type_name == "mdt":
                return 1
        return 0

def get_fsname_from_service_name(service_name):
    """
    Extract fsname from the volume name
    """
    fields = service_name.split('-')
    if len(fields) != 2:
        return None
    return fields[0]


def detect_lustre_clients(log, host):
    """
    Detect Lustre clients from the host
    """
    client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
    client_regular = re.compile(client_pattern)

    # Detect Lustre client
    command = ("cat /proc/mounts | grep lustre")
    retval = host.sh_run(log, command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return []

    clients = []
    for line in retval.cr_stdout.splitlines():
        log.cl_debug("checking line [%s]", line)
        match = client_regular.match(line)
        if match:
            mount_point = match.group("mount_point")
            fsname = match.group("fsname")
            lustre_fs = LustreFilesystem(fsname)
            client = init_client(log, lustre_fs, host, mount_point)
            if client is None:
                return None
            clients.append(client)
            log.cl_debug("client [%s] mounted on dir [%s] of host [%s]",
                         fsname, mount_point, host.sh_hostname)
    return clients


def get_fsname_from_device(log, host, device_path):
    """
    Get file system name either from ldiskfs or ZFS.
    """
    info_dict = host.sh_dumpe2fs(log, device_path)
    if info_dict is None:
        srv_name = host.sh_zfs_get_srvname(log, device_path)
        if srv_name is None:
            log.cl_error("failed to get service name from device [%s] "
                         "on host [%s] either as ZFS or as ldiskfs",
                         device_path, host.sh_hostname)
            return -1, None
    else:
        srv_name = info_dict["Filesystem volume name"]
    fsname = get_fsname_from_service_name(srv_name)
    if fsname is None:
        log.cl_error("failed to get fsname from service name [%s] of "
                     "device [%s] on host [%s]", srv_name,
                     device_path, host.sh_hostname)
        return -1, None
    return 0, fsname


def get_lustre_dist(log, local_host, workspace, lustre_dir, e2fsprogs_dir,
                    distr_type=LustreDistribution,
                    definition_dir=constant.LUSTRE_VERSION_DEFINITION_DIR):
    """
    Return Lustre distribution
    """
    target_cpu = local_host.sh_target_cpu(log)
    if target_cpu is None:
        log.cl_error("failed to get target cpu of host [%s]",
                     local_host.sh_hostname)
        return None
    lustre_dist = distr_type(local_host, target_cpu, lustre_dir,
                             e2fsprogs_dir)
    ret = lustre_dist.ldis_check(log, local_host, workspace,
                                 definition_dir=definition_dir)
    if ret:
        log.cl_error("failed to check Lustre release")
        return None
    return lustre_dist


def check_mgs_id(log, mgs_id):
    """
    Check whether MGS ID is valid
    """
    for char in mgs_id:
        if not char.isalnum() and char != "-" and char != "_":
            log.cl_error("MGS ID [%s] has invalid character [%s]",
                         mgs_id, char)
            return -1

    fsname, target_type, target_index = \
        lustre_parse_service_name(log, mgs_id, quiet=True)
    if fsname is not None or target_type is not None or target_index is not None:
        log.cl_error("MGS ID [%s] can be mixed up with service name",
                     mgs_id)
        return -1
    return 0


def filter_mofed_rpm_fnames(log, fnames, ignore_missing=False):
    """
    Return the fnames of mlnx-ofa_kernel RPMs, e.g.
    mlnx-ofa_kernel-5.8-OFED.5.8.3.0.7.1.x86_64.rpm
    kmod-mlnx-ofa_kernel-5.8-OFED.5.8.3.0.7.1.x86_64.rpm
    """
    prefixes = ["mlnx-ofa_kernel-",
                "kmod-mlnx-ofa_kernel-"]
    # Key is prefix, value is the RPM name.
    fname_dict = {}
    for fname in fnames:
        for prefix in prefixes:
            if not fname.startswith(prefix):
                continue
            length = len(prefix)
            first_char = fname[length:length + 1]
            if not first_char.isdigit():
                continue
            if prefix in fname_dict:
                log.cl_error("multiple rpms found with prefix [%s], "
                             "[%s] and [%s]",
                             prefix,
                             fname_dict[prefix],
                             fname)
                return None
            fname_dict[prefix] = fname
    if not ignore_missing:
        for prefix in prefixes:
            if prefix not in fname_dict:
                log.cl_error("missing rpm with prefix [%s]",
                             prefix)
                return None
    return list(fname_dict.values())



def get_mofed_rpm_fnames(log, host, rpm_dir):
    """
    Return the fnames of mlnx-ofa_kernel RPMs, e.g.
    mlnx-ofa_kernel-5.8-OFED.5.8.3.0.7.1.x86_64.rpm
    kmod-mlnx-ofa_kernel-5.8-OFED.5.8.3.0.7.1.x86_64.rpm
    """
    fnames = host.sh_get_dir_fnames(log, rpm_dir)
    if fnames is None:
        log.cl_error("failed to get the file names under [%s] of "
                     "host [%s]", rpm_dir, host.sh_hostname)
        return None
    return filter_mofed_rpm_fnames(log, fnames)
