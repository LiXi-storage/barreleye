"""
Library for Lustre file system management
"""
# pylint: disable=too-many-lines
import os
import re
import time
import socket
import traceback

# Local libs
from pycoral import utils
from pycoral import ssh_host
from pycoral import constant
from pycoral import clog

# The directory path that has Lustre test script
LUSTRE_TEST_SCRIPT_DIR = "/usr/lib64/lustre/tests"

LUSTRE_SERVICE_TYPE_MGT = "MGT"
LUSTRE_SERVICE_TYPE_MDT = "MDT"
LUSTRE_SERVICE_TYPE_OST = "OST"

JOBID_VAR_PROCNAME_UID = "procname_uid"

PARAM_PATH_OST_IO = "ost.OSS.ost_io"
PARAM_PATH_MDT = "mds.MDS.mdt"
PARAM_PATH_MDT_READPAGE = "mds.MDS.mdt_readpage"
PARAM_PATH_MDT_SETATTR = "mds.MDS.mdt_setattr"
TBF_TYPE_GENERAL = "general"
TBF_TYPE_UID = "uid"
TBF_TYPE_GID = "gid"
TBF_TYPE_JOBID = "jobid"
TBF_TYPE_OPCODE = "opcode"
TBF_TYPE_NID = "nid"

BACKFSTYPE_ZFS = "zfs"
BACKFSTYPE_LDISKFS = "ldiskfs"

LUSTRE_SERVICE_STATUS_CHECK_INTERVAL = 10

# The dir path of Lustre test scripts
LUSTRE_TEST_SCRIPT_DIR = "/usr/lib64/lustre/tests"
# The path of regression file
LUSTRE_REGRESSION_FPATH = LUSTRE_TEST_SCRIPT_DIR + "/test-groups/regression"
# Pattern to match the subtest name
LUSTRE_PATTERN_SUBTEST = "[a-zA-Z0-9_]*"
# This is the value hard coded in Lustre.
JOB_ID_PROCNAME_UID = "procname_uid"


def lustre_string2index(index_string):
    """
    Transfer string to index number, e.g.
    "000e" -> 14
    """
    index_number = int(index_string, 16)
    if index_number > 0xffff:
        return -1, ""
    return 0, index_number


def lustre_index2string(index_number):
    """
    Transfer number to index string, e.g.
    14 -> "000e"
    """
    if index_number > 0xffff:
        return -1, ""
    index_string = "%04x" % index_number
    return 0, index_string


def lustre_ost_index2string(index_number):
    """
    Transfer number to OST index string, e.g.
    14 -> "OST000e"
    """
    if index_number > 0xffff:
        return -1, ""
    index_string = "OST%04x" % index_number
    return 0, index_string


def lustre_mdt_index2string(index_number):
    """
    Transfer number to MDT index string, e.g.
    14 -> "MDT000e"
    """
    if index_number > 0xffff:
        return -1, ""
    index_string = "MDT%04x" % index_number
    return 0, index_string


def version_value(major, minor, patch):
    """
    Return a numeric version code based on a version string.  The version
    code is useful for comparison two version strings to see which is newer.
    """
    value = (major << 16) | (minor << 8) | patch
    return value


class LustreServiceInstanceSort(object):
    """
    For sorting the instances by load
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, instance, load):
        self.lsis_instance = instance
        self.lsis_load = load


class LustreServiceInstance(object):
    """
    A Lustre services might has multiple instances on multiple hosts,
    which are usually for HA
    """
    # pylint: disable=too-many-arguments,too-many-instance-attributes
    def __init__(self, log, service, host, device, mnt, nid,
                 zpool_create=None):
        self.lsi_service = service
        self.lsi_host = host
        self.lsi_device = device
        self.lsi_mnt = mnt
        self.lsi_nid = nid
        self.lsi_hostname = host.sh_hostname
        self.lsi_zpool_create = zpool_create
        ret = service.ls_instance_add(log, self)
        if ret:
            reason = ("failed to add instance of service")
            log.cl_error(reason)
            raise Exception(reason)

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

    def lsi_format(self, log):
        """
        Format this service device
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname
        backfstype = service.ls_backfstype

        log.cl_info("formatting service [%s] on host [%s]",
                    service_name, hostname)
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
        elif ret == 0 and export:
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
            elif exit_status == constant.CSM_UNSUPPORTED:
                log.cl_error("unsupported MMP feature on zpool [%s]",
                             service.ls_zpool_name)
                return -1
            elif exit_status == constant.CSM_AGAIN:
                log.cl_error("temporary failure when checking whether zpool [%s] can be mounted",
                             service.ls_zpool_name)
                return -1
            elif exit_status == constant.CSM_OCCUPIED:
                log.cl_error("wll not be able to import zpool [%s] because it is occupied",
                             service.ls_zpool_name)
                return -1
            elif exit_status != constant.CSM_MOUNTABLE:
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

        command = ("mkdir -p %s && mount -t lustre %s %s" %
                   (self.lsi_mnt, self.lsi_device, self.lsi_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to mount Lustre service [%s] "
                         "using command [%s] on host [%s], ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         service_name, command, hostname,
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

            if device == real_device or device == self.lsi_device:
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

            ret, label = host.lsh_lustre_device_label(log, device)
            if ret:
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
            elif (service_type == LUSTRE_SERVICE_TYPE_OST or
                  service_type == LUSTRE_SERVICE_TYPE_MDT):
                fsname = service.ls_lustre_fs.lf_fsname
                match = service_regular.match(label)
                if match:
                    device_fsname = match.group("fsname")
                    index_string = match.group("index_string")
                    ret, device_index = lustre_string2index(index_string)
                    if ret:
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
                        log.cl_error("unexpected index [%s] of device [%s] on "
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
        elif retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 1


class LustreService(object):
    """
    Lustre service parent class for MDT/MGS/OST
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, log, lustre_fs, service_type, index,
                 backfstype, zpool_name=None):
        # pylint: disable=too-many-arguments
        self.ls_lustre_fs = lustre_fs
        # Keys are hostname, values are LustreServiceInstance
        self.ls_instance_dict = {}
        self.ls_service_type = service_type
        if backfstype == BACKFSTYPE_LDISKFS:
            assert zpool_name is None
        elif backfstype == BACKFSTYPE_ZFS:
            assert zpool_name is not None
        else:
            reason = ("invalid backfstype [%s]" % (backfstype))
            log.cl_error(reason)
            raise Exception(reason)
        self.ls_zpool_name = zpool_name
        if service_type == LUSTRE_SERVICE_TYPE_MGT:
            assert index == 0
            self.ls_index_string = "MGS"
        elif service_type == LUSTRE_SERVICE_TYPE_MDT:
            ret, index_string = lustre_mdt_index2string(index)
            if ret:
                reason = ("invalid MDT index [%s]" % (index))
                log.cl_error(reason)
                raise Exception(reason)
            self.ls_index_string = index_string
        elif service_type == LUSTRE_SERVICE_TYPE_OST:
            ret, index_string = lustre_ost_index2string(index)
            if ret:
                reason = ("invalid OST index [%s]" % (index))
                log.cl_error(reason)
                raise Exception(reason)
            self.ls_index_string = index_string
        else:
            reason = ("unsupported service type [%s]" % service_type)
            log.cl_error(reason)
            raise Exception(reason)
        self.ls_index = index
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
        hostname = instance.lsi_hostname
        if hostname in self.ls_instance_dict:
            log.cl_error("an instance of service [%s] is already "
                         "added to host [%s]",
                         self.ls_service_name, hostname)
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
                                 "service [%s]", self.ls_service_name)
                    return None

            for hostname in selected_hostnames[:]:
                if hostname not in only_hostnames:
                    selected_hostnames.remove(hostname)
                    if hostname not in exclude_dict:
                        exclude_dict[hostname] = not_select_reason

        for exclude in exclude_dict:
            if exclude not in self.ls_instance_dict:
                log.cl_warning("exclude host [%s] doesnot have any "
                               "instance of service [%s], ignoring",
                               self.ls_service_name)
                continue
            for hostname in selected_hostnames[:]:
                if hostname == exclude:
                    selected_hostnames.remove(hostname)
        return selected_hostnames

    def _ls_instance_dict_sort_by_load(self, log, instances):
        """
        Sort the instances by their loads. If an instance has problem, remove
        the instance from the list.
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
                 quiet=False):
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

        self._ls_instance_dict_sort_by_load(log, instances)

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
            else:
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
        else:
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
        else:
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
        else:
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

    def ls_format(self, log):
        """
        Format this service.
        Service should have been umounted.
        """
        if len(self.ls_instance_dict) == 0:
            return -1

        log.cl_info("formatting service [%s]", self.ls_service_name)
        for instance in self.ls_instance_dict.values():
            ret = instance.lsi_format(log)
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
    def __init__(self, log, mgs_id, backfstype, zpool_name=None):
        super(LustreMGS, self).__init__(log, None, LUSTRE_SERVICE_TYPE_MGT, 0,
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
    # pylint: disable=too-many-arguments
    def __init__(self, log, mgs, host, device, mnt, nid,
                 add_to_host=False, zpool_create=None):
        super(LustreMGSInstance, self).__init__(log, mgs, host, device,
                                                mnt, nid, zpool_create=zpool_create)
        if add_to_host:
            ret = host.lsh_mgsi_add(log, self)
            if ret:
                reason = ("failed to add MGS instance of file system [%s] to "
                          "host [%s]" %
                          (mgs.ls_lustre_fs.lf_fsname, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)


class LustreFilesystem(object):
    """
    Information about Lustre file system
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, fsname):
        self.lf_fsname = fsname
        # Key is the service name, value is LustreOST
        self.lf_osts = {}
        # Key is the service name, value is LustreMDT
        self.lf_mdts = {}
        # Key is $HOSTNAME:$MNT, value is LustreClient
        self.lf_client_dict = {}
        self.lf_mgs = None
        # Key is the service name, combined MGT/MDT is not duplicated
        self.lf_service_dict = {}
        self.lf_mgs_mdt = None
        self.lf_qos = None
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

    def lf_qos_add(self, qos):
        """
        Add QoS control into this file system
        """
        if self.lf_qos is not None:
            return -1
        self.lf_qos = qos
        return 0

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
        if service_name in self.lf_osts:
            log.cl_error("service [%s] is already configured for file system [%s]",
                         service_name, self.lf_fsname)
            return -1
        self.lf_osts[service_name] = ost
        self.lf_service_dict[service_name] = ost
        return 0

    def lf_mdt_add(self, log, service_name, mdt):
        """
        Add MDT into this file system
        """
        if service_name in self.lf_mdts:
            log.cl_error("service [%s] is already configured for file system [%s]",
                         service_name, self.lf_fsname)
            return -1
        if mdt.lmdt_is_mgs:
            ret = self.lf_mgs_init(log, mdt, combined=True)
            if ret:
                log.cl_error("failed to init MGS for file system [%s]",
                             self.lf_fsname)
                return -1
        self.lf_mdts[service_name] = mdt
        self.lf_service_dict[service_name] = mdt
        return 0

    def lf_mgs_nids(self):
        """
        Return the nid array of the MGS
        """
        if self.lf_mgs_mdt is None:
            assert self.lf_mgs is not None
            return self.lf_mgs.ls_nids()
        else:
            assert self.lf_mgs is None
            return self.lf_mgs_mdt.ls_nids()

    def lf_set_jobid_var(self, log, jobid_var):
        """
        Set the job ID var
        """
        fsname = self.lf_fsname

        if self.lf_mgs is not None:
            service = self.lf_mgs
        else:
            service = self.lf_mgs_mdt

        for instance in service.ls_instance_dict.values():
            host = instance.lsi_host
            ret = host.lsh_set_jobid_var(log, fsname, jobid_var)
            if ret == 0:
                log.cl_info("set jobid var of file system [%s] to [%s]",
                            fsname, jobid_var)
                break
        if ret:
            log.cl_error("failed to set jobid var of file system [%s] to [%s]",
                         fsname, jobid_var)
        return ret

    def lf_oss_list(self):
        """
        Return the host list that could run OSS service
        """
        hosts = []
        for ost in self.lf_osts.values():
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
        for mds in self.lf_mdts.values():
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
        for mds in self.lf_mdts.values():
            mds_hosts = mds.ls_hosts()
            for mds_host in mds_hosts:
                if mds_host not in hosts:
                    hosts.append(mds_host)
        for ost in self.lf_osts.values():
            ost_hosts = ost.ls_hosts()
            for ost_host in ost_hosts:
                if ost_host not in hosts:
                    hosts.append(ost_host)
        return hosts

    def lf_format(self, log, format_mgt=False):
        """
        Format the whole file system.
        :param format_mgt: format the standalone MGT.
        """
        log.cl_info("formatting file system [%s]", self.lf_fsname)
        if len(self.lf_mgs_nids()) == 0:
            log.cl_error("the MGS nid of Lustre file system [%s] is not "
                         "configured, not able to format", self.lf_fsname)
            return -1

        for service_name, mdt in self.lf_mdts.items():
            ret = mdt.ls_format(log)
            if ret:
                log.cl_error("failed to format MDT [%s]", service_name)
                return -1

        for service_name, ost in self.lf_osts.items():
            ret = ost.ls_format(log)
            if ret:
                log.cl_error("failed to format OST [%s]", service_name)
                return -1

        if self.lf_mgs is not None and format_mgt:
            service_name = self.lf_mgs.ls_service_name
            ret = self.lf_mgs.ls_format(log)
            if ret:
                log.cl_error("failed to format MGT [%s]", service_name)
                return -1
        log.cl_debug("formatted file system [%s]", self.lf_fsname)
        return 0

    def lf_mount(self, log):
        """
        Mount the whole file system
        """
        log.cl_info("mounting file system [%s]", self.lf_fsname)
        if self.lf_mgs is not None:
            ret = self.lf_mgs.ls_mount(log, quiet=True)
            if ret:
                log.cl_error("failed to mount MGS of Lustre file "
                             "system [%s]", self.lf_fsname)
                return -1

        for service_name, mdt in self.lf_mdts.items():
            ret = mdt.ls_mount(log, quiet=True)
            if ret:
                log.cl_error("failed to mount MDT [%s] of Lustre file "
                             "system [%s]", service_name, self.lf_fsname)
                return -1

        for service_name, ost in self.lf_osts.items():
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

        for service_name, ost in self.lf_osts.items():
            ret = ost.ls_umount(log)
            if ret:
                log.cl_error("failed to umount OST [%s]", service_name)
                return -1

        for service_name, mdt in self.lf_mdts.items():
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
    # pylint: disable=too-many-arguments
    def __init__(self, log, mdt, host, device, mnt, nid,
                 add_to_host=False, zpool_create=None):
        super(LustreMDTInstance, self).__init__(log, mdt, host, device,
                                                mnt, nid,
                                                zpool_create=zpool_create)
        self.mdti_nid = nid
        if add_to_host:
            ret = host.lsh_mdti_add(self)
            if ret:
                reason = ("MDT instance [%s] of file system [%s] already "
                          "exists on host [%s]" %
                          (mdt.ls_lustre_fs.lf_fsname,
                           mdt.ls_index, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)

    def mdti_enable_hsm_control(self, log):
        """
        Enable HSM control
        """
        mdt = self.lsi_service

        # Improve: first disable hsm_control to cleanup actions/agents
        command = ("lctl get_param mdt.%s-%s.hsm_control" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        expected_output = ("mdt.%s-%s.hsm_control=enabled\n" %
                           (mdt.ls_lustre_fs.lf_fsname,
                            mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to get HSM control status for file system "
                         "[%s] on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         self.lsi_host.sh_hostname,
                         retval.cr_stderr)
            return -1
        elif retval.cr_stdout == expected_output:
            return 0

        command = ("lctl set_param mdt.%s-%s.hsm_control=enabled" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to enable HSM control for file system [%s] "
                         "on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         self.lsi_host.sh_hostname, retval.cr_stderr)
            return -1
        return 0

    def mdti_enable_raolu(self, log):
        """
        Enable remove_archive_on_last_unlink
        """
        mdt = self.lsi_service

        command = ("lctl get_param mdt.%s-%s.hsm."
                   "remove_archive_on_last_unlink" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        expected_output = ("mdt.%s-%s.hsm."
                           "remove_archive_on_last_unlink=1\n" %
                           (mdt.ls_lustre_fs.lf_fsname,
                            mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_info("no remove_archive_on_last_unlink support in file "
                        "system [%s] on host [%s], %s",
                        mdt.ls_lustre_fs.lf_fsname,
                        self.lsi_host.sh_hostname, retval.cr_stderr)
            return 1
        elif retval.cr_stdout == expected_output:
            return 0

        command = ("lctl set_param mdt.%s-%s.hsm."
                   "remove_archive_on_last_unlink=1" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to enable remove_archive_on_last_unlink[%s] "
                         "on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         self.lsi_host.sh_hostname, retval.cr_stderr)
            return -1
        return 0

    def mdti_changelog_register(self, log):
        """
        Register changelog user
        """
        mdt = self.lsi_service

        command = ("lctl --device %s-%s changelog_register -n" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to register changelog user of [%s-%s] "
                         "on host [%s] with command [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         mdt.ls_index_string,
                         self.lsi_host.sh_hostname, command,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def mdti_changelog_deregister(self, log, user_id):
        """
        Deregister changelog user
        """
        mdt = self.lsi_service

        command = ("lctl --device %s-%s changelog_deregister %s" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string, user_id))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to deregister changelog user of [%s-%s] "
                         "on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         mdt.ls_index_string,
                         self.lsi_host.sh_hostname,
                         retval.cr_stderr)
            return -1
        return 0

    def mdti_prevent_congestion_by_tbf(self, log, rpc_limit):
        """
        Prevent congestion by defining TBF rules to the server
        """

        name = "congestion"
        host = self.lsi_host

        ret = host.lsh_stop_mdt_tbf_rule(log, name)
        if ret:
            log.cl_debug("failed to stop rule [%s]", name)

        expression = "uid={0} warning=1"
        ret = host.lsh_start_mdt_tbf_rule(log, name, expression,
                                          rpc_limit)
        if ret:
            return -1

        # Make sure ls -l won't be affected
        name = "ldlm_enqueue"
        ret = host.lsh_stop_mdt_tbf_rule(log, name)
        if ret:
            log.cl_debug("failed to stop rule [%s]", name)

        expression = "opcode={ldlm_enqueue}"
        ret = host.lsh_start_mdt_tbf_rule(log, name, expression,
                                          10000)
        if ret:
            return -1

        return 0


class LustreMDT(LustreService):
    """
    Lustre MDT service
    """
    # pylint: disable=too-few-public-methods
    # index: 0, 1, etc.
    def __init__(self, log, lustre_fs, index, backfstype, is_mgs=False,
                 zpool_name=None):
        # pylint: disable=too-many-arguments
        super(LustreMDT, self).__init__(log, lustre_fs, LUSTRE_SERVICE_TYPE_MDT,
                                        index, backfstype, zpool_name=zpool_name)
        self.lmdt_is_mgs = is_mgs

        ret = lustre_fs.lf_mdt_add(log, self.ls_service_name, self)
        if ret:
            reason = ("failed to add MDT [%s] into file system [%s]" %
                      (self.ls_service_name, lustre_fs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)

    def lmt_prevent_congestion_by_tbf(self, log, rpc_limit):
        """
        Prevent congestion by defining TBF rules to the server
        """
        if len(self.ls_instance_dict) == 0:
            return -1

        instance = self.ls_mounted_instance(log)
        if instance is None:
            log.cl_info("service [%s] is not mounted on any host, "
                        "not able to prevent congestion",
                        self.ls_service_name)
            return -1

        return instance.mdti_prevent_congestion_by_tbf(log, rpc_limit)


class LustreOSTInstance(LustreServiceInstance):
    """
    A Lustre OST might has multiple instances on multiple hosts,
    which are usually for HA
    """
    # pylint: disable=too-many-arguments
    def __init__(self, log, ost, host, device, mnt, nid,
                 add_to_host=False, zpool_create=None):
        super(LustreOSTInstance, self).__init__(log, ost, host, device,
                                                mnt, nid, zpool_create=zpool_create)
        if add_to_host:
            ret = host.lsh_osti_add(self)
            if ret:
                reason = ("OST instance [%s] of file system [%s] already "
                          "exists on host [%s]" %
                          (ost.ls_lustre_fs.lf_fsname,
                           ost.ls_index, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)


class LustreOST(LustreService):
    """
    Lustre OST service
    """
    # pylint: disable=too-few-public-methods
    # index: 0, 1, etc.
    def __init__(self, log, lustre_fs, index, backfstype, zpool_name=None):
        # pylint: disable=too-many-arguments
        super(LustreOST, self).__init__(log, lustre_fs, LUSTRE_SERVICE_TYPE_OST,
                                        index, backfstype, zpool_name=zpool_name)
        ret = lustre_fs.lf_ost_add(log, self.ls_service_name, self)
        if ret:
            reason = ("OST [%s] already exists in file system [%s]" %
                      (self.ls_index_string, lustre_fs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)


class LustreClient(object):
    """
    Lustre client
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, log, lustre_fs, host, mnt, add_to_host=False):
        # pylint: disable=too-many-arguments
        self.lc_lustre_fs = lustre_fs
        self.lc_host = host
        self.lc_mnt = mnt
        index = ("%s:%s" % (host.sh_hostname, mnt))
        if index in lustre_fs.lf_client_dict:
            reason = ("client [%s] already exists in file system [%s]" %
                      (index, lustre_fs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)
        lustre_fs.lf_client_dict[index] = self
        self.lc_client_name = index
        if add_to_host:
            ret = host.lsh_client_add(lustre_fs.lf_fsname, mnt, self)
            if ret:
                reason = ("client [%s] already exists on host [%s]" %
                          (index, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)

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

        log.cl_info("mounting client of file system [%s] to mount point [%s] "
                    "of host [%s]", fsname, self.lc_mnt, hostname)

        ret = self.lc_check_mounted(log)
        if ret < 0:
            log.cl_error("failed to check whether Lustre client "
                         "[%s] is mounted on host [%s]",
                         fsname, hostname)
        elif ret > 0:
            log.cl_info("Lustre client [%s] is already mounted on host "
                        "[%s]", fsname, hostname)
            if not quiet:
                log.cl_stdout(constant.MSG_ALREADY_MOUNTED_NO_NEWLINE)
            return 0

        nid_string = ""
        for mgs_nid in self.lc_lustre_fs.lf_mgs_nids():
            if nid_string != "":
                nid_string += ":"
            nid_string += mgs_nid
        command = ("mkdir -p %s && mount -t lustre %s:/%s %s" %
                   (self.lc_mnt, nid_string,
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


RPM_KERNEL = "kernel"
RPM_KERNEL_FIRMWARE = "kernel-firmware"
RPM_LUSTRE = "lustre"
RPM_IOKIT = "iokit"
RPM_KMOD = "kmod"
RPM_OSD_LDISKFS = "osd_ldiskfs"
RPM_OSD_LDISKFS_MOUNT = "osd_ldiskfs_mount"
RPM_OSD_ZFS = "osd_zfs"
RPM_OSD_ZFS_MOUNT = "osd_zfs_mount"
RPM_TESTS = "tests"
RPM_TESTS_KMOD = "tests_kmod"
RPM_MLNX_OFA = "mlnx_ofa"
RPM_MLNX_KMOD = "mlnx_ofa_modules"

# The order should be proper for the dependency of RPMs
LUSTRE_RPM_TYPES = [RPM_KMOD, RPM_OSD_LDISKFS_MOUNT, RPM_OSD_LDISKFS,
                    RPM_OSD_ZFS_MOUNT, RPM_OSD_ZFS,
                    RPM_LUSTRE, RPM_IOKIT, RPM_TESTS_KMOD, RPM_TESTS]
# A Lustre version need to have these patterns set, otherwise tools would
# fail.
LUSTRE_REQUIRED_RPM_TYPES = [RPM_KERNEL, RPM_KMOD, RPM_OSD_LDISKFS_MOUNT,
                             RPM_OSD_LDISKFS, RPM_LUSTRE, RPM_IOKIT,
                             RPM_TESTS_KMOD, RPM_TESTS]


class LustreVersion(object):
    """
    RPM version of Lustre
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, name, rpm_patterns, priority):
        # pylint: disable=too-few-public-methods,too-many-arguments
        self.lv_name = name
        # Key: RPM_*, value: regular expression to match the RPM fname.
        self.lv_rpm_pattern_dict = rpm_patterns
        # If two versions are matched, the one with higher priority will
        # be used.
        self.lv_priority = priority
        assert priority >= 0
        for required_rpm in LUSTRE_REQUIRED_RPM_TYPES:
            if required_rpm not in rpm_patterns:
                reason = "no RPM pattern for [%s]" % required_rpm
                raise Exception(reason)


# Key is version name, value is LustreVersion
LUSTRE_VERSION_DICT = {}

B2_12_PATTERNS = {
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.12.+\.rpm)$",
    RPM_IOKIT: r"^(lustre-iokit-2.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.12.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.12.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.12.+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.12.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.12.+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
}
LUSTRE_VERSION_NAME_2_12 = "2.12"
LUSTRE_VERSION_2_12 = LustreVersion(LUSTRE_VERSION_NAME_2_12,
                                    B2_12_PATTERNS,
                                    0)
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_2_12] = LUSTRE_VERSION_2_12


ES5_1_PATTERNS = {
    RPM_IOKIT: r"^(lustre-iokit-2\.12\.3.+\.rpm)$",
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.12\.3_ddn.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.12\.3_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.12\.3_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.12\.3.+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.12\.3_ddn.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.12\.3.+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
}
LUSTRE_VERSION_NAME_ES5_1 = "es5.1"
LUSTRE_VERSION_ES5_1 = LustreVersion(LUSTRE_VERSION_NAME_ES5_1,
                                     ES5_1_PATTERNS,
                                     1)
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_ES5_1] = LUSTRE_VERSION_ES5_1


ES5_2_PATTERNS = {
    RPM_IOKIT: r"^(lustre-iokit-2\.12\.[56].+\.rpm)$",
    RPM_KERNEL: r"^(kernel-3.+\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2\.12\.[56]_ddn.+\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.12\.[56]_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2\.12\.[56]_ddn.+\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.12\.[56].+\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2\.12\.[56]_ddn.+\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.12\.[56].+\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.rpm)$",
}
LUSTRE_VERSION_NAME_ES5_2 = "es5.2"
LUSTRE_VERSION_ES5_2 = LustreVersion(LUSTRE_VERSION_NAME_2_12,
                                     ES5_2_PATTERNS,
                                     1)
LUSTRE_VERSION_DICT[LUSTRE_VERSION_NAME_ES5_2] = LUSTRE_VERSION_ES5_2


def match_lustre_version_from_rpms(log, rpm_fnames, skip_kernel=False):
    """
    Match the Lustre version from RPM names
    """
    # pylint: disable=too-many-locals,too-many-branches
    # Key is version name, type is matched_rpm_type_dict
    matched_version_dict = {}
    for version in LUSTRE_VERSION_DICT.values():
        # Key is RPM type, value is RPM fname
        matched_rpm_type_dict = {}
        # Key is RPM fname, value is RPM type
        used_rpm_fname_dict = {}
        version_matched = True
        for rpm_type in LUSTRE_REQUIRED_RPM_TYPES:
            if rpm_type == RPM_KERNEL and skip_kernel:
                continue
            if rpm_type not in version.lv_rpm_pattern_dict:
                log.cl_error("Lustre version [%s] does not have required RPM"
                             "pattern for type [%s]",
                             version.lv_name, rpm_type)
                return None, None
            pattern = version.lv_rpm_pattern_dict[rpm_type]

            matched = False
            for rpm_fname in rpm_fnames:
                match = re.search(pattern, rpm_fname)
                if match is None:
                    continue

                matched = True
                if rpm_type in matched_rpm_type_dict:
                    log.cl_error("both RPM [%s] and [%s] can be matched to "
                                 "type [%s] of Lustre version [%s]",
                                 rpm_fname,
                                 matched_rpm_type_dict[rpm_type],
                                 rpm_type,
                                 version.lv_name)
                    return None, None

                if rpm_fname in used_rpm_fname_dict:
                    log.cl_error("RPM [%s] can be matched to both type [%s] "
                                 "and [%s] of Lustre version [%s]",
                                 rpm_fname,
                                 used_rpm_fname_dict[rpm_fname],
                                 rpm_type,
                                 version.lv_name)
                    return None, None
                used_rpm_fname_dict[rpm_fname] = rpm_type
                matched_rpm_type_dict[rpm_type] = rpm_fname

            if not matched:
                log.cl_debug("not able to match to Lustre version "
                             "[%s] because of missing RPM type [%s]",
                             version.lv_name, rpm_type)
                version_matched = False
                break
        if version_matched:
            matched_version_dict[version.lv_name] = matched_rpm_type_dict

    if len(matched_version_dict) == 0:
        log.cl_debug("no Lustre version is matched by RPMs %s",
                     rpm_fnames)
        return None, None

    highest_priority = 0
    for version_name in matched_version_dict:
        version = LUSTRE_VERSION_DICT[version_name]
        if version.lv_priority > highest_priority:
            highest_priority = version.lv_priority

    matched_versions = []
    matched_rpm_type_dicts = []
    version_string = ""
    for version_name, rpm_type_dict in matched_version_dict.items():
        version = LUSTRE_VERSION_DICT[version_name]
        if version.lv_priority == highest_priority:
            matched_versions.append(version)
            matched_rpm_type_dicts.append(rpm_type_dict)
            if version_string == "":
                version_string = version.lv_name
            else:
                version_string += ", " + version.lv_name

    if len(matched_versions) > 1:
        log.cl_error("multiple Lustre versions [%s] are matched by RPMs",
                     version_string)
        return None, None
    return matched_versions[0], matched_rpm_type_dicts[0]


class LustreTest(object):
    """
    Test of Lustre
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, test_name, test_fpath):
        # Name of test
        self.lt_test_name = test_name
        # List of LustreSubtest
        self.lt_subtests = []
        # Key is name of subtest, value is LustreSubtest
        self.lt_subtest_dict = {}
        # File path of test script
        self.lt_test_fpath = test_fpath
        # Global failures hit when running tests
        self.lt_test_errors = []
        # Fatal error number, if > 1, do not test any more.
        self.lt_fatal_number = 0

    def lt_error(self, log, fatal, msg, *args):
        """
        Add global error message
        """
        message = clog.get_message(msg, args)
        self.lt_test_errors.append(message)
        log.cl_error(msg, *args)
        if fatal:
            self.lt_fatal_number += 1

    def lt_duplicate(self, log):
        """
        Duplicate myself and return a new LustreTest.
        Do not copy the results.
        """
        lustre_test = LustreTest(self.lt_test_name, self.lt_test_fpath)
        for subtest in self.lt_subtests:
            new_subtest = subtest.lst_duplicate(lustre_test)
            ret = lustre_test.lt_add_subtest(log, new_subtest)
            if ret:
                log.cl_error("failed to add subtest [%s] to test [%s]",
                             subtest.lst_subtest_name, self.lt_test_name)
                return None
        return lustre_test

    def lt_status_merge(self, log, lustre_test):
        """
        Merge the status in lustre_test to myself. Ignore the subtest results
        that have not finished.
        """
        for subtest in lustre_test.lt_subtests:
            if subtest.lst_check_status_unfinished():
                continue
            subtest_name = subtest.lst_subtest_name
            if subtest_name not in self.lt_subtest_dict:
                log.cl_error("merging status of unknown subtest [%s/%s]",
                             self.lt_test_name, subtest_name)
                return -1
            summary_subtest = self.lt_subtest_dict[subtest_name]
            summary_subtest.lst_status_merge(subtest)
        return 0

    def lt_parse_only(self, log, only, selected_subtest_names):
        """
        Parse the only string
        """
        subtest_names = only.split(",")
        only_subtest_names = []
        for subtest_name in subtest_names:
            if subtest_name not in self.lt_subtest_dict:
                log.cl_error("subtest [%s] does not exist for test [%s]",
                             subtest_name, self.lt_test_name)
                return -1

            if subtest_name in only_subtest_names:
                log.cl_error("multiple subtest [%s] specified in --only",
                             subtest_name)
                return -1
            only_subtest_names.append(subtest_name)

        for subtest_name in selected_subtest_names[:]:
            if subtest_name not in only_subtest_names:
                selected_subtest_names.remove(subtest_name)
        return 0

    def lt_parse_exclude(self, log, except_string, selected_subtest_names):
        """
        Parse the only string and init the result of subtests
        """
        subtest_names = except_string.split(",")
        for subtest_name in subtest_names:
            if subtest_name not in self.lt_subtest_dict:
                log.cl_error("subtest [%s] does not exist for test [%s]",
                             subtest_name, self.lt_test_name)
                return -1

            if subtest_name in selected_subtest_names:
                selected_subtest_names.remove(subtest_name)

        return 0

    def lt_failed_subtests(self):
        """
        Return the list of failed subtests
        """
        selected = []
        for subtest in self.lt_subtests:
            if subtest.lst_check_status_failed():
                selected.append(subtest)
        return selected

    def lt_passed_subtests(self):
        """
        Return the list of passed subtests
        """
        selected = []
        for subtest in self.lt_subtests:
            if subtest.lst_check_status_passed():
                selected.append(subtest)
        return selected

    def lt_skipped_subtests(self):
        """
        Return the list of skipped subtests
        """
        selected = []
        for subtest in self.lt_subtests:
            if subtest.lst_check_status_skipped():
                selected.append(subtest)
        return selected

    def lt_excluded_subtests(self):
        """
        Return the list of subtests that are not included/selected
        """
        selected = []
        for subtest in self.lt_subtests:
            if subtest.lst_check_status_excluded():
                selected.append(subtest)
        return selected

    def lt_unfinished_subtests(self):
        """
        Return the list of subtests that are not finished
        """
        selected = []
        for subtest in self.lt_subtests:
            if subtest.lst_check_status_unfinished():
                selected.append(subtest)
        return selected

    def lt_add_subtest(self, log, subtest):
        """
        Add a subtest into the list
        """
        if subtest.lst_subtest_name in self.lt_subtest_dict:
            log.cl_error("subtest [%s] is already in test [%s]",
                         subtest.lst_subtest_name, self.lt_test_name)
            return -1
        self.lt_subtest_dict[subtest.lst_subtest_name] = subtest
        subtest.lst_subtest_index = len(self.lt_subtests)
        self.lt_subtests.append(subtest)
        return 0

    def lt_next_subtest_to_run(self, log):
        """
        Return the next subtest that has not be run before. This is for
        the purpose of continue running the remaning subtests.
        If all subtest have been run, return None.
        """
        next_subtest = None
        for subtest in self.lt_subtests:
            if (subtest.lst_check_status_failed() or
                    subtest.lst_check_status_passed() or
                    subtest.lst_check_status_skipped()):
                if next_subtest is not None:
                    log.cl_error("subtest [%s/%s] finished before [%s/%s]",
                                 self.lt_test_name,
                                 subtest.lst_subtest_name,
                                 self.lt_test_name,
                                 next_subtest.lst_subtest_name)
                    return -1, None
                continue
            if next_subtest is None:
                next_subtest = subtest
        return 0, next_subtest


class LustreSubtestResult(object):
    """
    Result of subtest
    """
    STATUS_INITIAL = "initial"
    STATUS_STARTED = "started"
    STATUS_FAILED = "failed"
    STATUS_SKIPPED = "skipped"
    STATUS_PASSED = "passed"

    def __init__(self, subtest):
        # Status of STATUS_*
        self.lstr_subtest_status = LustreSubtestResult.STATUS_INITIAL
        # Status of the subtest
        # Subtest this result belongs to
        self.lstr_subtest = subtest
        # Duration of this subtest from the output of the test script.
        self.lstr_stdout_duration = None
        # Why this subtest has been skipped
        self.lstr_subtest_skip_reason = None
        # Why this subtest has failed
        self.lstr_subtest_fail_reason = None
        # The time that the subtest started
        self.lstr_start_time = None
        # The time that the subtest finished
        self.lstr_end_time = None

    def lstr_status_started(self, log, timestamp=None):
        """
        Set the status of the subtest to STATUS_STARTED
        """
        subtest = self.lstr_subtest
        lustre_test = subtest.lst_lustre_test
        if self.lstr_subtest_status != LustreSubtestResult.STATUS_INITIAL:
            log.cl_error("unexpected status [%s] of subtest [%s] from test "
                         "[%s], expected [%s]",
                         self.lstr_subtest_status,
                         subtest.lst_subtest_name,
                         lustre_test.lt_test_name,
                         LustreSubtestResult.STATUS_INITIAL)
            return -1
        if timestamp is None:
            self.lstr_start_time = time.time()
        elif timestamp > 0:
            self.lstr_start_time = timestamp
        self.lstr_subtest_status = LustreSubtestResult.STATUS_STARTED
        return 0

    def lstr_status_failed(self, log, duration=None, fail_reason=None,
                           timestamp=None):
        """
        Set the status of the subtest to STATUS_FAILED
        """
        # pylint: disable=too-many-arguments
        subtest = self.lstr_subtest
        lustre_test = subtest.lst_lustre_test
        if (self.lstr_subtest_status != LustreSubtestResult.STATUS_STARTED and
                self.lstr_subtest_status != LustreSubtestResult.STATUS_FAILED):
            log.cl_error("unexpected status [%s] of subtest [%s] from test "
                         "[%s], expected [%s/%s]",
                         self.lstr_subtest_status,
                         subtest.lst_subtest_name,
                         lustre_test.lt_test_name,
                         LustreSubtestResult.STATUS_STARTED,
                         LustreSubtestResult.STATUS_FAILED)
            return -1
        if timestamp is None:
            self.lstr_end_time = time.time()
        elif timestamp > 0:
            self.lstr_end_time = timestamp
        self.lstr_subtest_status = LustreSubtestResult.STATUS_FAILED

        if duration is not None:
            if self.lstr_stdout_duration is not None:
                log.cl_error("multiple duration messages of subtest [%s] "
                             "from test [%s], was [%s], now [%s]",
                             subtest.lst_subtest_name,
                             lustre_test.lt_test_name,
                             self.lstr_stdout_duration, duration)
                return -1
            self.lstr_stdout_duration = duration

        if fail_reason is not None:
            # Fail reason could matched for multiple times. For example,
            # following files might be printed together:
            #  sanity test_100: @@@@@@ FAIL: privileged port not found
            #  sanity test_100: @@@@@@ FAIL: test_100 failed with 1
            if self.lstr_subtest_fail_reason is None:
                self.lstr_subtest_fail_reason = fail_reason
            else:
                self.lstr_subtest_fail_reason += ", " + fail_reason
        return 0

    def lstr_status_passed(self, log, subtest_duration, timestamp=None):
        """
        Set the status of the subtest to STATUS_PASSED
        """
        subtest = self.lstr_subtest
        lustre_test = subtest.lst_lustre_test
        if self.lstr_subtest_status != LustreSubtestResult.STATUS_STARTED:
            log.cl_error("unexpected status [%s] of subtest [%s] from "
                         "test [%s], expected [%s]",
                         self.lstr_subtest_status,
                         subtest.lst_subtest_name,
                         lustre_test.lt_test_name,
                         LustreSubtestResult.STATUS_STARTED)
            return -1
        if timestamp is None:
            self.lstr_end_time = time.time()
        elif timestamp > 0:
            self.lstr_end_time = timestamp
        self.lstr_subtest_status = LustreSubtestResult.STATUS_PASSED
        self.lstr_stdout_duration = subtest_duration
        return 0

    def lstr_status_skipped(self, log, duration=None, skip_reason=None,
                            timestamp=None):
        """
        Set the status of the subtest to STATUS_SKIPPED
        """
        subtest = self.lstr_subtest
        lustre_test = subtest.lst_lustre_test
        # The subtest could be skipped without even started
        if (self.lstr_subtest_status != LustreSubtestResult.STATUS_STARTED and
                self.lstr_subtest_status != LustreSubtestResult.STATUS_INITIAL and
                self.lstr_subtest_status != LustreSubtestResult.STATUS_SKIPPED):
            log.cl_error("unexpected status [%s] of subtest [%s] from "
                         "test [%s], expected [%s/%s/%s]",
                         self.lstr_subtest_status,
                         subtest.lst_subtest_name,
                         lustre_test.lt_test_name,
                         LustreSubtestResult.STATUS_STARTED,
                         LustreSubtestResult.STATUS_INITIAL,
                         LustreSubtestResult.STATUS_SKIPPED)
            return -1
        if timestamp is None:
            self.lstr_end_time = time.time()
        elif timestamp > 0:
            self.lstr_end_time = timestamp
        self.lstr_subtest_status = LustreSubtestResult.STATUS_SKIPPED
        if duration is not None:
            if self.lstr_stdout_duration is not None:
                log.cl_error("multiple duration messages of subtest [%s] "
                             "from test [%s], was [%s], now [%s]",
                             subtest.lst_subtest_name,
                             lustre_test.lt_test_name,
                             self.lstr_stdout_duration,
                             duration)
                return -1
            self.lstr_stdout_duration = duration

        if skip_reason is not None:
            if self.lstr_subtest_skip_reason is not None:
                log.cl_error("multiple skip reason messages of subtest "
                             "[%s] from test [%s], was [%s], now [%s]",
                             subtest.lst_subtest_name,
                             lustre_test.lt_test_name,
                             self.lstr_subtest_skip_reason,
                             skip_reason)
                return -1
            self.lstr_subtest_skip_reason = skip_reason
        return 0

    def lstr_duration(self):
        """
        lstr_stdout_duration isn't valid all the time, so calculate by
        myself.
        If the subtest has not finished (not started, still running or
        quited), return -1
        """
        if self.lstr_start_time is None:
            return -1

        if self.lstr_end_time is None:
            return -1

        return self.lstr_end_time - self.lstr_start_time


def lustre_subtest_title_format(subtest_title):
    """
    Cleanup the subtest title:
    1) remove leading/tailing spaces
    2) replace newlines with spaces
    """
    new_title = subtest_title.strip()
    new_title.replace("\n", " ")
    return new_title


class LustreSubtest(object):
    """
    Subtest in test script
    """
    # pylint: disable=too-few-public-methods

    def __init__(self, lustre_test, subtest_name, subtest_title):
        # LustreTest
        self.lst_lustre_test = lustre_test
        # Name of this subtest
        self.lst_subtest_name = subtest_name
        # Title of this subtest
        self.lst_subtest_title = lustre_subtest_title_format(subtest_title)
        # Index in lt_subtests of LustreTest
        self.lst_subtest_index = None
        # Results of this subtest
        self.lst_subtest_results = []

    def lst_duration(self):
        """
        Return the average duration of all results. If no result or no valid
        duration, return -1
        """
        duration_sum = 0
        number = 0
        for subtest_result in self.lst_subtest_results:
            duration = subtest_result.lstr_duration()
            if duration < 0:
                continue
            duration_sum += duration
            number += 1
        if number == 0:
            return -1
        return duration_sum / number

    def lst_skip_reason(self):
        """
        Return skip reason message seperated by comma.
        """
        reason = None
        for subtest_result in self.lst_subtest_results:
            skip_reason = subtest_result.lstr_subtest_skip_reason
            if skip_reason is None:
                continue
            if reason is None:
                reason = skip_reason
            else:
                reason += ", " + skip_reason
        return reason

    def lst_fail_reason(self):
        """
        Return fail reason message seperated by comma.
        """
        reason = None
        for subtest_result in self.lst_subtest_results:
            fail_reason = subtest_result.lstr_subtest_fail_reason
            if fail_reason is None:
                continue
            if reason is None:
                reason = fail_reason
            else:
                reason += ", " + fail_reason
        return reason

    def lst_duplicate(self, lustre_test):
        """
        Duplicate myself and return a new LustreSubtest.
        Do not copy the results.
        """
        return LustreSubtest(lustre_test, self.lst_subtest_name,
                             self.lst_subtest_title)

    def lst_init_result(self, log):
        """
        Init the result of this subtest
        """
        lustre_test = self.lst_lustre_test
        if len(self.lst_subtest_results) > 0:
            log.cl_error("multiple results for subtest [%s] of test [%s]",
                         self.lst_subtest_name,
                         lustre_test.lt_test_name)
            return -1
        subtest_result = LustreSubtestResult(self)
        self.lst_subtest_results.append(subtest_result)
        return 0

    def lst_status_merge(self, subtest):
        """
        Merge a status from another subtest.
        """
        self.lst_subtest_results += subtest.lst_subtest_results

    def lst_set_status(self, log, status, timestamp=None,
                       fail_reason=None, skip_reason=None):
        """
        Set the status of the subtest
        """
        # pylint: disable=too-many-arguments
        if (fail_reason is not None and
                status != LustreSubtestResult.STATUS_FAILED):
            log.cl_error("not able to set fail reason [%s] with status "
                         "[%s] to subtest [%s/%s]", fail_reason, status,
                         self.lst_lustre_test.lt_test_name,
                         self.lst_subtest_name)
            return -1
        if (skip_reason is not None and
                status != LustreSubtestResult.STATUS_SKIPPED):
            log.cl_error("not able to set skip reason [%s] with status "
                         "[%s] to subtest [%s/%s]", skip_reason, status,
                         self.lst_lustre_test.lt_test_name,
                         self.lst_subtest_name)
            return -1
        if status == LustreSubtestResult.STATUS_STARTED:
            return self.lst_set_status_started(log, timestamp=timestamp)
        elif status == LustreSubtestResult.STATUS_FAILED:
            return self.lst_set_status_failed(log, fail_reason=fail_reason,
                                              timestamp=timestamp)
        elif status == LustreSubtestResult.STATUS_SKIPPED:
            return self.lst_set_status_skipped(log, skip_reason=skip_reason,
                                               timestamp=timestamp)
        elif status == LustreSubtestResult.STATUS_PASSED:
            return self.lst_set_status_passed(log, timestamp=timestamp)

        log.cl_error("not able to set invalid status [%s] of subtest [%s/%s]",
                     status, self.lst_lustre_test.lt_test_name,
                     self.lst_subtest_name)
        return -1

    def lst_set_status_started(self, log, timestamp=None):
        """
        Set the status of the subtest to STATUS_STARTED
        timestamp < 0 means no timestamp
        """
        lustre_test = self.lst_lustre_test
        if len(self.lst_subtest_results) != 1:
            log.cl_error("unexpected result number [%s] for subtest [%s] "
                         "of test [%s], expected [1]",
                         len(self.lst_subtest_results),
                         self.lst_subtest_name,
                         lustre_test.lt_test_name)
            return -1

        subtest_result = self.lst_subtest_results[0]
        return subtest_result.lstr_status_started(log, timestamp=timestamp)

    def lst_set_status_failed(self, log, duration=None, fail_reason=None,
                              timestamp=None):
        """
        Set the status of the subtest to STATUS_FAILED
        """
        # pylint: disable=too-many-arguments
        lustre_test = self.lst_lustre_test
        if len(self.lst_subtest_results) != 1:
            log.cl_error("unexpected result number [%s] for subtest [%s] "
                         "of test [%s], expected [1]",
                         len(self.lst_subtest_results),
                         self.lst_subtest_name,
                         lustre_test.lt_test_name)
            return -1

        subtest_result = self.lst_subtest_results[0]
        return subtest_result.lstr_status_failed(log, duration=duration,
                                                 fail_reason=fail_reason,
                                                 timestamp=timestamp)

    def lst_check_status_failed(self):
        """
        Return true if the status is failed
        """
        for subtest_result in self.lst_subtest_results:
            if subtest_result.lstr_subtest_status == LustreSubtestResult.STATUS_FAILED:
                return True
        return False

    def lst_set_status_passed(self, log, subtest_duration=None,
                              timestamp=None):
        """
        Set the status of the subtest to STATUS_PASSED
        """
        lustre_test = self.lst_lustre_test
        if len(self.lst_subtest_results) != 1:
            log.cl_error("unexpected result number [%s] for subtest [%s] "
                         "of test [%s], expected [1]",
                         len(self.lst_subtest_results),
                         self.lst_subtest_name,
                         lustre_test.lt_test_name)
            return -1

        subtest_result = self.lst_subtest_results[0]
        return subtest_result.lstr_status_passed(log, subtest_duration,
                                                 timestamp=timestamp)

    def lst_check_status_passed(self):
        """
        Return true if the status is all passed.
        If any failure, return False.
        If no passed (e.g. all skipped/started), return False
        """
        has_passed = False
        for subtest_result in self.lst_subtest_results:
            if subtest_result.lstr_subtest_status == LustreSubtestResult.STATUS_FAILED:
                return False
            elif subtest_result.lstr_subtest_status == LustreSubtestResult.STATUS_PASSED:
                has_passed = True
        return has_passed

    def lst_check_status_skipped(self):
        """
        Return true if the status is skipped
        """
        for subtest_result in self.lst_subtest_results:
            if subtest_result.lstr_subtest_status == LustreSubtestResult.STATUS_SKIPPED:
                return True
        return False

    def lst_set_status_skipped(self, log, duration=None, skip_reason=None,
                               timestamp=None):
        """
        Set the status of the subtest to STATUS_SKIPPED
        """
        lustre_test = self.lst_lustre_test
        result_number = len(self.lst_subtest_results)
        if result_number == 0:
            # This subtest might be skipped because it is excluded, ignore
            return 0

        if result_number != 1:
            log.cl_error("unexpected result number [%s] for subtest [%s] "
                         "of test [%s], expected [1]",
                         len(self.lst_subtest_results),
                         self.lst_subtest_name,
                         lustre_test.lt_test_name)
            return -1

        subtest_result = self.lst_subtest_results[0]
        return subtest_result.lstr_status_skipped(log, duration=duration,
                                                  skip_reason=skip_reason,
                                                  timestamp=timestamp)

    def lst_check_status_excluded(self):
        """
        Return true if the status is not included/selected
        """
        if len(self.lst_subtest_results) == 0:
            return True
        return False

    def lst_check_status_unfinished(self):
        """
        Return true if the status is not finished (not started or still running)
        """
        if len(self.lst_subtest_results) == 0:
            return False
        for subtest_result in self.lst_subtest_results:
            if (subtest_result.lstr_subtest_status not in
                    [LustreSubtestResult.STATUS_INITIAL,
                     LustreSubtestResult.STATUS_STARTED]):
                return False
        return True

    def lst_failed_results(self):
        """
        Return failed results
        """
        results = []
        for subtest_result in self.lst_subtest_results:
            if subtest_result.lstr_subtest_status == LustreSubtestResult.STATUS_FAILED:
                results.append(subtest_result)
        return results

    def lst_passed_results(self):
        """
        Return passed results
        """
        results = []
        for subtest_result in self.lst_subtest_results:
            if subtest_result.lstr_subtest_status == LustreSubtestResult.STATUS_PASSED:
                results.append(subtest_result)
        return results

    def lst_skipped_results(self):
        """
        Return number of passed results
        """
        results = []
        for subtest_result in self.lst_subtest_results:
            if subtest_result.lstr_subtest_status == LustreSubtestResult.STATUS_SKIPPED:
                results.append(subtest_result)
        return results


def parse_test_script(log, local_host, logdir, test_name):
    """
    Parse a test script, return LustreTest
    """
    # pylint: disable=too-many-locals,too-many-statements
    # pylint: disable=too-many-branches
    # Only runtests has no .`sh subfix.
    if test_name == "runtests":
        fpath = LUSTRE_TEST_SCRIPT_DIR + "/" + test_name
    else:
        fpath = LUSTRE_TEST_SCRIPT_DIR + "/" + test_name + ".sh"
    test_script_fpath = logdir + "/" + fpath
    try:
        with open(test_script_fpath, "r") as test_file:
            test_script = test_file.read()
    except:
        log.cl_error("failed to read file [%s] on host [%s]: %s",
                     test_script_fpath, local_host.sh_hostname,
                     traceback.format_exc())
        return None

    # Some subtest has multiple lines of title, e.g. conf-sanity 83
    # Thus we need to combile multiple line to a single title
    multiple_line = False
    lines = test_script.splitlines()
    lustre_test = LustreTest(test_name, fpath)
    subtest_title = ""
    subtest_name = ""
    for line in lines:
        if multiple_line:
            if len(line) < 1 or (line[-1] != "\\" and line[-1] != "\""):
                multiple_line = False
                log.cl_error("unfinished multiple line [%s] of "
                             "subtest title in test [%s]",
                             line, test_name)
                return None
            elif line[-1] == "\\":
                subtest_title += line[:-1]
            else:
                subtest_title += line[:-1]
                multiple_line = False
                log.cl_debug("found subtest [%s] with title [%s] for "
                             "test [%s]",
                             subtest_name, subtest_title, test_name)
                if "\n" in subtest_title:
                    log.cl_error("invalid title [%s] of subtest [%s] "
                                 "for test [%s]",
                                 subtest_title, subtest_name, test_name)
                    return None
                subtest = LustreSubtest(lustre_test, subtest_name,
                                        subtest_title)
                ret = lustre_test.lt_add_subtest(log, subtest)
                if ret:
                    log.cl_error("failed to add subtest [%s] to test [%s]",
                                 subtest_name, test_name)
                    return None
            continue

        # Add .* in the end of the pattern because recovery-small:20a has
        # a space after "
        pattern_run_test = ('^run_test +(%s) +"(.+)".*$' %
                            (LUSTRE_PATTERN_SUBTEST))
        # This is not good, but pylint-1.6.5-7.el7 does not support
        # Python 3.6 properly. And following error will printed:
        # Module 're' has no 'M' member (no-member).
        # Skip it here.
        #
        # pylint: disable=no-member
        matched = re.match(pattern_run_test, line, re.M)
        if matched:
            subtest_name = matched.group(1)
            subtest_title = matched.group(2)
            log.cl_debug("found subtest [%s] with title [%s] for "
                         "test [%s]",
                         subtest_name, subtest_title, test_name)
            if "\n" in subtest_title:
                log.cl_error("invalid title [%s] of subtest [%s] "
                             "for test [%s]",
                             subtest_title, subtest_name, test_name)
                return None
            subtest = LustreSubtest(lustre_test, subtest_name,
                                    subtest_title)
            ret = lustre_test.lt_add_subtest(log, subtest)
            if ret:
                log.cl_error("failed to add subtest [%s] to test [%s]",
                             subtest_name, test_name)
                return None
            continue

        pattern_multiple_line = ('^run_test +(%s) +"(.+)\\\\$' %
                                 (LUSTRE_PATTERN_SUBTEST))
        matched = re.match(pattern_multiple_line, line, re.M)
        if matched:
            subtest_name = matched.group(1)
            subtest_title = matched.group(2)
            multiple_line = True
    if multiple_line:
        log.cl_error("unfinished multiple line of subtest title test "
                     "[%s]", subtest_name)
        return None
    return lustre_test


class LustreDistribution(object):
    """
    Lustre Distribution, including Lustre, kernel, e2fsprogs RPMs
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, local_host, lustre_rpm_dir, e2fsprogs_rpm_dir):
        # Dir that contains Lustre RPM files
        self.ldis_lustre_rpm_dir = lustre_rpm_dir
        # Key is "RPM_*", value is the name of the RPM
        self.ldis_lustre_rpm_dict = {}
        # LustreVersion
        self.ldis_lustre_version = None
        # Kernel version of Lustre
        self.ldis_kernel_version = None
        # ZFS is supported in the Lustre RPMs
        self.ldis_zfs_support = True
        # Ldiskfs is supported in the Lustre RPMs
        self.ldis_ldiskfs_support = True
        # E2fsprogs dir that contains RPM files
        self.ldis_e2fsprogs_rpm_dir = e2fsprogs_rpm_dir
        # The RPMs are checked and has no problem
        self.ldis_prepared = False
        # Local host to run command
        self.ldis_local_host = local_host
        # e2fsprogs version, e.g. "1.45.2.wc1"
        self.ldis_e2fsprogs_version = None
        # Key is test name, value a list of LustreTest
        self.ldis_test_dict = {}

    def _ldis_check_e2fsprogs(self, log):
        """
        Check whether e2fsprogs dir contain expected RPMs
        """
        local_host = self.ldis_local_host

        command = "ls %s" % self.ldis_e2fsprogs_rpm_dir
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        fnames = retval.cr_stdout.splitlines()
        for fname in fnames:
            if not fname.endswith(".rpm"):
                continue

            if not fname.startswith("e2fsprogs-1"):
                continue

            command = ("rpm -qp %s/%s --queryformat '%%{version} %%{url}'" %
                       (self.ldis_e2fsprogs_rpm_dir, fname))
            retval = local_host.sh_run(log, command)
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
                             command, local_host.sh_hostname,
                             retval.cr_stdout)
                return -1
            line = lines[0]
            fields = line.split()
            if len(fields) != 2:
                log.cl_error("unexpected output of command [%s] on host [%s], "
                             "stdout = [%s]",
                             command, local_host.sh_hostname,
                             retval.cr_stdout)
                return -1

            if ("wc" not in fields[0]) or ("whamcloud" not in fields[1]):
                log.cl_error("improper e2fsprogs rpms under dir [%s] of host [%s]",
                             self.ldis_e2fsprogs_rpm_dir,
                             local_host.sh_hostname)
                return -1
            self.ldis_e2fsprogs_version = fields[0]
        if self.ldis_e2fsprogs_version is None:
            log.cl_error("no e2fsprogs under dir [%s] of host [%s]",
                         self.ldis_e2fsprogs_rpm_dir, local_host.sh_hostname)
            return -1
        return 0

    def _ldis_check_lustre(self, log):
        """
        Prepare the RPMs
        """
        # pylint: disable=too-many-branches
        local_hostname = socket.gethostname()
        try:
            rpm_files = os.listdir(self.ldis_lustre_rpm_dir)
        except:
            log.cl_error("failed to list dir [%s] on local host [%s]",
                         self.ldis_lustre_rpm_dir, local_hostname)
            return -1

        self.ldis_lustre_version, self.ldis_lustre_rpm_dict = \
            match_lustre_version_from_rpms(log, rpm_files)
        if self.ldis_lustre_version is None:
            log.cl_error("failed to detect Lustre version from RPMs under "
                         "dir [%s] on local host [%s]",
                         self.ldis_lustre_rpm_dir, local_hostname)
            return -1
        log.cl_info("detected Lustre version [%s] under dir [%s]",
                    self.ldis_lustre_version.lv_name,
                    self.ldis_lustre_rpm_dir)

        ldiskfs_disable_reasons = []
        zfs_disable_reasons = []
        missing_rpms = []
        for rpm_name in self.ldis_lustre_version.lv_rpm_pattern_dict:
            if rpm_name not in self.ldis_lustre_rpm_dict:
                if rpm_name == RPM_OSD_LDISKFS or rpm_name == RPM_OSD_LDISKFS_MOUNT:
                    ldiskfs_disable_reasons.append(rpm_name)
                elif rpm_name == RPM_OSD_ZFS or rpm_name == RPM_OSD_ZFS_MOUNT:
                    zfs_disable_reasons.append(rpm_name)
                else:
                    missing_rpms.append(rpm_name)

        if len(missing_rpms) > 0:
            log.cl_error("failed to find RPM for %s as version [%s] under "
                         "dir [%s]",
                         missing_rpms,
                         self.ldis_lustre_version.lv_name,
                         self.ldis_lustre_rpm_dir)
            return -1

        if len(ldiskfs_disable_reasons) > 0:
            log.cl_info("disabling ldiskfs support because no RPM found "
                        "for %s",
                        ldiskfs_disable_reasons)
            self.ldis_ldiskfs_support = False

        if len(zfs_disable_reasons) > 0:
            log.cl_info("disabling zfs support because no RPM found for %s",
                        zfs_disable_reasons)
            self.ldis_zfs_support = False

        kernel_rpm_name = self.ldis_lustre_rpm_dict[RPM_KERNEL]
        kernel_rpm_path = (self.ldis_lustre_rpm_dir + '/' + kernel_rpm_name)
        command = ("rpm -qpl %s | grep /lib/modules |"
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
        self.ldis_prepared = True
        return 0

    def ldis_prepare(self, log):
        """
        Prepare the RPMs
        """
        rc = self._ldis_check_e2fsprogs(log)
        if rc:
            log.cl_error("failed to check e2fsprogs RPMs")
            return -1

        rc = self._ldis_check_lustre(log)
        if rc:
            log.cl_error("failed to check Lustre RPMs")
            return -1
        self.ldis_prepared = True
        return 0

    def _ldis_parse_test_script(self, log, local_host, logdir, test_name):
        """
        Parse a test script
        """
        lustre_test = parse_test_script(log, local_host, logdir, test_name)
        if lustre_test is None:
            log.cl_error("failed to parse test script of test [%s]",
                         test_name)
            return -1
        self.ldis_test_dict[test_name] = lustre_test
        return 0

    def ldis_parse_test_scripts(self, log, local_host, logdir):
        """
        Parse the test scripts
        """
        # pylint: disable=bare-except
        lustre_test_rpm_path = (self.ldis_lustre_rpm_dir + "/" +
                                self.ldis_lustre_rpm_dict[RPM_TESTS])
        ret = local_host.sh_unpack_rpm(log, lustre_test_rpm_path, logdir)
        if ret:
            log.cl_error("failed to unpack RPM [%s] to dir [%s] on host [%s]",
                         lustre_test_rpm_path, logdir,
                         local_host.sh_hostname)
            return -1

        command = "cat %s" % (logdir + LUSTRE_REGRESSION_FPATH)
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, local_host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        test_names = retval.cr_stdout.splitlines()
        for test_name in test_names:
            ret = self._ldis_parse_test_script(log, local_host, logdir,
                                               test_name)
            if ret:
                log.cl_error("failed to parse script of test [%s]",
                             test_name)
                return -1

        command = ("rm -fr %s" % logdir)
        retval = local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, local_host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        return 0


def lustre_client_id(fsname, mnt):
    """
    Return the Lustre client ID
    """
    return "%s:%s" % (fsname, mnt)


class LustreHost(ssh_host.SSHHost):
    # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """
    Each host being used to run Lustre tests has an object of this
    """
    LSH_ERROR = "error"
    LSH_HEALTHY = "healthy"
    LSH_UNHEALTHY = "UNHEALTHY"
    LSH_LBUG = "LBUG"

    def __init__(self, hostname, lustre_dist=None, identity_file=None,
                 local=False, is_server=False, is_client=True):
        # pylint: disable=too-many-arguments
        super(LustreHost, self).__init__(hostname,
                                         identity_file=identity_file,
                                         local=local)
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
        # LustreDistribution. If not want to install Lustre, set this to None
        self.lsh_lustre_dist = lustre_dist
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

    def _lsh_enable_tbf(self, log, param_path, tbf_type):
        """
        Change the NRS policy to TBF
        param_path example: ost.OSS.ost_io
        """
        if tbf_type == TBF_TYPE_GENERAL:
            command = ('lctl set_param %s.nrs_policies="tbf"' %
                       (param_path))
        else:
            command = ('lctl set_param %s.nrs_policies="tbf %s"' %
                       (param_path, tbf_type))
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

    def lsh_enable_ost_io_tbf(self, log, tbf_type):
        """
        Change the OST IO NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_OST_IO, tbf_type)

    def lsh_enable_mdt_tbf(self, log, tbf_type):
        """
        Change the mdt NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_MDT, tbf_type)

    def lsh_enable_mdt_readpage_tbf(self, log, tbf_type):
        """
        Change the mdt_readpage NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_MDT_READPAGE, tbf_type)

    def lsh_enable_mdt_setattr_tbf(self, log, tbf_type):
        """
        Change the mdt_setattr NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_MDT_SETATTR, tbf_type)

    def lsh_enable_mdt_all_tbf(self, log, tbf_type):
        """
        Change the all MDT related NRS policies to TBF
        """
        ret = self.lsh_enable_mdt_tbf(log, tbf_type)
        if ret:
            log.cl_error("failed to enable TBF policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT, self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_readpage_tbf(log, tbf_type)
        if ret:
            log.cl_error("failed to enable TBF policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_READPAGE,
                         self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_setattr_tbf(log, tbf_type)
        if ret:
            log.cl_error("failed to enable TBF policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_SETATTR,
                         self.sh_hostname)
            return ret
        return 0

    def _lsh_enable_fifo(self, log, param_path):
        """
        Change the policy to FIFO
        param_path example: ost.OSS.ost_io
        """
        command = ('lctl set_param %s.nrs_policies="fifo"' % param_path)
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

    def lsh_enable_ost_io_fifo(self, log):
        """
        Change the OST IO NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_OST_IO)

    def lsh_enable_mdt_fifo(self, log):
        """
        Change the mdt NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_MDT)

    def lsh_enable_mdt_readpage_fifo(self, log):
        """
        Change the mdt_readpage NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_MDT_READPAGE)

    def lsh_enable_mdt_setattr_fifo(self, log):
        """
        Change the mdt_setattr NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_MDT_SETATTR)

    def lsh_enable_mdt_all_fifo(self, log):
        """
        Change the all MDT related NRS policies to FIFO
        """
        ret = self.lsh_enable_mdt_fifo(log)
        if ret:
            log.cl_error("failed to enable FIFO policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT, self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_readpage_fifo(log)
        if ret:
            log.cl_error("failed to enable FIFO policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_READPAGE,
                         self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_setattr_fifo(log)
        if ret:
            log.cl_error("failed to enable FIFO policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_SETATTR,
                         self.sh_hostname)
            return ret
        return 0

    def lsh_set_jobid_var(self, log, fsname, jobid_var):
        """
        Set the job ID variable
        """
        command = ("lctl conf_param %s.sys.jobid_var=%s" %
                   (fsname, jobid_var))
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

    def _lsh_get_tbf_rule_list(self, log, param_path):
        """
        Get the rule list
        param_path example: ost.OSS.ost_io
        """
        rule_list = []
        command = "lctl get_param -n %s.nrs_tbf_rule" % param_path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, rule_list

        cpt_pattern = (r"^CPT \d+:$")
        cpt_regular = re.compile(cpt_pattern)

        rule_pattern = (r"^(?P<name>\S+) .+$")
        rule_regular = re.compile(rule_pattern)

        lines = retval.cr_stdout.splitlines()
        for line in lines:
            if line == "regular_requests:":
                continue
            if line == "high_priority_requests:":
                continue
            match = cpt_regular.match(line)
            if match:
                continue
            match = rule_regular.match(line)
            if not match:
                log.cl_error("failed to parse line [%s]", line)
                return -1, rule_list
            name = match.group("name")
            if name == "default":
                continue
            if name not in rule_list:
                rule_list.append(name)

        return 0, rule_list

    def lsh_get_ost_io_tbf_rule_list(self, log):
        """
        Get the rule list on ost_io
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_OST_IO)

    def lsh_get_mdt_tbf_rule_list(self, log):
        """
        Get the rule list on mdt
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_MDT)

    def lsh_get_mdt_readpage_tbf_rule_list(self, log):
        """
        Get the rule list on mdt_readpage
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_MDT_READPAGE)

    def lsh_get_mdt_setattr_tbf_rule_list(self, log):
        """
        Get the rule list on mdt_setattr
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_MDT_SETATTR)

    def _lsh_start_tbf_rule(self, log, param_path, name, expression, rate):
        # pylint: disable=too-many-arguments
        """
        Start a TBF rule
        param_path example: ost.OSS.ost_io
        name: rule name
        """
        if self.lsh_version_value is None:
            ret = self.lsh_detect_lustre_version(log)
            if ret:
                log.cl_error("failed to detect Lustre version on host [%s]",
                             self.sh_hostname)
                return -1

        if self.lsh_version_value >= version_value(2, 8, 54):
            command = ('lctl set_param %s.nrs_tbf_rule='
                       '"start %s %s rate=%d"' %
                       (param_path, name, expression, rate))
        else:
            log.cl_error("TBF is not supported properly in this Lustre "
                         "version")
            return -1
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

    def lsh_start_ost_io_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on ost.OSS.ost_io
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_OST_IO, name,
                                        expression, rate)

    def lsh_start_mdt_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on MDT service
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_MDT, name,
                                        expression, rate)

    def lsh_start_mdt_readpage_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on MDT readpage service
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_MDT_READPAGE, name,
                                        expression, rate)

    def lsh_start_mdt_setattr_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on MDT readpage service
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_MDT_SETATTR, name,
                                        expression, rate)

    def _lsh_stop_tbf_rule(self, log, param_path, name):
        """
        Start a TBF rule
        """
        command = ('lctl set_param %s.nrs_tbf_rule='
                   '"stop %s"' % (param_path, name))
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

    def lsh_stop_ost_io_tbf_rule(self, log, name):
        """
        Stop a TBF rule on ost.OSS.ost_io
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_OST_IO, name)

    def lsh_stop_mdt_tbf_rule(self, log, name):
        """
        Stop a TBF rule on MDS service
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_MDT, name)

    def lsh_stop_mdt_readpage_tbf_rule(self, log, name):
        """
        Stop a TBF rule on MDS readpage service
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_MDT_READPAGE, name)

    def lsh_stop_mdt_setattr_tbf_rule(self, log, name):
        """
        Stop a TBF rule on MDS readpage service
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_MDT_SETATTR, name)

    def lsh_change_tbf_rate(self, log, name, rate):
        """
        Change the TBF rate of a rule
        """
        if self.lsh_version_value is None:
            ret = self.lsh_detect_lustre_version(log)
            if ret:
                log.cl_error("failed to detect Lustre version on host [%s]",
                             self.sh_hostname)
                return -1

        if self.lsh_version_value >= version_value(2, 8, 54):
            command = ('lctl set_param ost.OSS.ost_io.nrs_tbf_rule='
                       '"change %s rate=%d"' % (name, rate))
        else:
            command = ('lctl set_param ost.OSS.ost_io.nrs_tbf_rule='
                       '"change %s %d"' % (name, rate))
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

    def lsh_lustre_device_label(self, log, device):
        """
        Run e2label on a lustre device
        """
        # try to handle as ldiskfs first
        command = ("e2label %s" % device)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            return 0, retval.cr_stdout.strip()

        # fall back to handle as zfs then
        command = ("zfs get -H lustre:svname %s | awk {'print $3'}" % device)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        return 0, retval.cr_stdout.strip()

    def lsh_lustre_detect_services(self, log, client_dict,
                                   service_instance_dict, add_found=False):
        """
        Detect mounted Lustre services (MGS/MDT/OST/clients) from the host
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # pylint: disable=too-many-arguments
        server_pattern = (r"^(?P<device>\S+) (?P<mount_point>\S+) lustre (?P<options>\S+) .+$")
        server_regular = re.compile(server_pattern)

        client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
        client_regular = re.compile(client_pattern)

        ost_pattern = (r"^(?P<fsname>\S+)-OST(?P<index_string>[0-9a-f]{4})$")
        ost_regular = re.compile(ost_pattern)

        mdt_pattern = (r"^(?P<fsname>\S+)-MDT(?P<index_string>[0-9a-f]{4})$")
        mdt_regular = re.compile(mdt_pattern)

        mgt_pattern = (r"MGS")
        mgt_regular = re.compile(mgt_pattern)

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
                    client = LustreClient(log, lustre_fs, self, mount_point,
                                          add_to_host=add_found)
                client_dict[client_id] = client
                log.cl_debug("client [%s] mounted on dir [%s] of host [%s]",
                             fsname, mount_point, self.sh_hostname)
                continue

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

            ret, label = self.lsh_lustre_device_label(log, device)
            if ret:
                log.cl_error("failed to get the label of device [%s] on "
                             "host [%s]", device, self.sh_hostname)
                return -1

            match = ost_regular.match(label)
            if match:
                fsname = match.group("fsname")
                index_string = match.group("index_string")
                ret, ost_index = lustre_string2index(index_string)
                if ret:
                    log.cl_error("invalid label [%s] of device [%s] on "
                                 "host [%s]", label, device, self.sh_hostname)
                    return -1
                service_name = fsname + "-OST" + index_string

                if service_name in self.lsh_ost_instances:
                    osti = self.lsh_ost_instances[service_name]
                else:
                    lustre_fs = LustreFilesystem(fsname)
                    ost = LustreOST(log, lustre_fs, ost_index, backfstype,
                                    zpool_name=zpool_name)
                    osti = LustreOSTInstance(log, ost, self, device, mount_point,
                                             None, add_to_host=add_found)
                service_instance_dict[service_name] = osti
                log.cl_debug("OST [%s] mounted on dir [%s] of host [%s]",
                             service_name, mount_point, self.sh_hostname)
                continue

            match = mdt_regular.match(label)
            if match:
                fsname = match.group("fsname")
                index_string = match.group("index_string")
                ret, mdt_index = lustre_string2index(index_string)
                if ret:
                    log.cl_error("invalid label [%s] of device [%s] on "
                                 "host [%s]", label, device, self.sh_hostname)
                    return -1
                service_name = fsname + "-MDT" + index_string

                if service_name in self.lsh_mdt_instances:
                    mdti = self.lsh_mdt_instances[service_name]
                else:
                    lustre_fs = LustreFilesystem(fsname)
                    mdt = LustreMDT(log, lustre_fs, mdt_index, backfstype,
                                    zpool_name=zpool_name)
                    mdti = LustreMDTInstance(log, mdt, self, device, mount_point,
                                             None, add_to_host=add_found)
                service_instance_dict[service_name] = mdti
                log.cl_debug("MDT [%s] mounted on dir [%s] of host [%s]",
                             fsname, mount_point, self.sh_hostname)
                continue

            match = mgt_regular.match(label)
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
                    mgs = LustreMGS(log, service_name, backfstype,
                                    zpool_name=zpool_name)
                    mgsi = LustreMGSInstance(log, mgs, self, device, mount_point,
                                             None, add_to_host=add_found)
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

    def _lsh_lustre_uninstall(self, log):
        # pylint: disable=too-many-return-statements,too-many-branches
        # pylint: disable=too-many-statements
        """
        Uninstall Lustre RPMs
        """
        ret = self.sh_run(log, "rpm --rebuilddb")
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run 'rpm --rebuilddb' on host "
                         "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                         self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout, ret.cr_stderr)
            return -1

        log.cl_info("killing all yum processes on host [%s]",
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
            ret = self.sh_run(log, "yum install yum-utils -y")
            if ret.cr_exit_status != 0:
                log.cl_error("failed to install yum-utils on host "
                             "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                             self.sh_hostname, ret.cr_exit_status,
                             ret.cr_stdout, ret.cr_stderr)
                return -1

        ret = self.sh_run(log, "yum-complete-transaction")
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run yum-complete-transaction on host "
                         "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                         self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout, ret.cr_stderr)
            return -1

        log.cl_info("uninstalling Lustre RPMs on host [%s]",
                    self.sh_hostname)
        ret = self.sh_rpm_find_and_uninstall(log, "grep lustre")
        if ret != 0:
            log.cl_error("failed to uninstall Lustre RPMs on host "
                         "[%s]", self.sh_hostname)
            return -1

        zfs_rpms = ["libnvpair1", "libuutil1", "libzfs2", "libzpool2",
                    "kmod-spl", "kmod-zfs", "spl", "zfs"]
        rpm_string = ""
        for zfs_rpm in zfs_rpms:
            retval = self.sh_run(log, "rpm -qi %s" % zfs_rpm)
            if retval.cr_exit_status == 0:
                if rpm_string != "":
                    rpm_string += " "
                rpm_string += zfs_rpm

        if rpm_string != "":
            retval = self.sh_run(log, "rpm -e --nodeps %s" % rpm_string)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to uninstall ZFS RPMs on host "
                             "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        return 0

    def _lsh_install_e2fsprogs(self, log, workspace):
        """
        Install e2fsprogs RPMs for Lustre
        """
        # pylint: disable=too-many-return-statements,too-many-locals
        lustre_dist = self.lsh_lustre_dist
        e2fsprogs_dir = lustre_dist.ldis_e2fsprogs_rpm_dir
        command = ("mkdir -p %s" % workspace)
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

        basename = os.path.basename(e2fsprogs_dir)
        host_copying_rpm_dir = workspace + "/" + basename
        host_e2fsprogs_rpm_dir = workspace + "/" + "e2fsprogs_rpms"

        ret = self.sh_send_file(log, e2fsprogs_dir, workspace)
        if ret:
            log.cl_error("failed to send e2fsprogs RPMs [%s] on local host to "
                         "directory [%s] on host [%s]",
                         e2fsprogs_dir, workspace,
                         self.sh_hostname)
            return -1

        if host_copying_rpm_dir != host_e2fsprogs_rpm_dir:
            command = ("mv %s %s" % (host_copying_rpm_dir, host_e2fsprogs_rpm_dir))
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

        need_install = False
        retval = self.sh_run(log,
                             "rpm -q e2fsprogs --queryformat '%{version}'")
        if retval.cr_exit_status != 0:
            need_install = True
        else:
            current_version = retval.cr_stdout
            if lustre_dist.ldis_e2fsprogs_version != current_version:
                need_install = True

        if not need_install:
            log.cl_info("the same e2fsprogs RPMs are already installed on "
                        "host [%s]", self.sh_hostname)
            return 0

        log.cl_info("installing e2fsprogs RPMs on host [%s]",
                    self.sh_hostname)
        retval = self.sh_run(log, "rpm -Uvh %s/*.rpm" % host_e2fsprogs_rpm_dir)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to install RPMs under [%s] of e2fsprogs on "
                         "host [%s], ret = %d, stdout = [%s], stderr = [%s]",
                         host_e2fsprogs_rpm_dir, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        return 0

    def _lsh_update_ssh_config(self, log):
        """
        Update SSH config
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
        elif retval.cr_stdout != host_key_config + "\n":
            log.cl_error("unexpected StrictHostKeyChecking config on host "
                         "[%s], expected [%s], got [%s]",
                         self.sh_hostname, host_key_config, retval.cr_stdout)
            return -1
        return 0

    def _lsh_lustre_install(self, log, workspace):
        """
        Install Lustre RPMs on a host
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        # pylint: disable=too-many-statements,too-many-locals
        lustre_dist = self.lsh_lustre_dist
        if not lustre_dist.ldis_prepared:
            log.cl_error("Lustre RPMs [%s] is not prepared for host [%s]",
                         lustre_dist.lr_distribution_id,
                         self.sh_hostname)
            return -1
        command = ("mkdir -p %s" % workspace)
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

        basename = os.path.basename(lustre_dist.ldis_lustre_rpm_dir)
        host_copying_rpm_dir = workspace + "/" + basename
        host_lustre_rpm_dir = workspace + "/" + "lustre_dist"

        ret = self.sh_send_file(log, lustre_dist.ldis_lustre_rpm_dir,
                                workspace)
        if ret:
            log.cl_error("failed to send Lustre RPMs [%s] on local host to "
                         "directory [%s] on host [%s]",
                         lustre_dist.ldis_lustre_rpm_dir, workspace,
                         self.sh_hostname)
            return -1

        if host_copying_rpm_dir != host_lustre_rpm_dir:
            command = ("mv %s %s" % (host_copying_rpm_dir, host_lustre_rpm_dir))
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

        ret = self._lsh_update_ssh_config(log)
        if ret:
            log.cl_error("failed to update SSH config on host [%s]",
                         self.sh_hostname)
            return -1

        log.cl_info("installing kernel RPM on host [%s]",
                    self.sh_hostname)
        if RPM_KERNEL_FIRMWARE in lustre_dist.ldis_lustre_rpm_dict:
            rpm_name = lustre_dist.ldis_lustre_rpm_dict[RPM_KERNEL_FIRMWARE]
            retval = self.sh_run(log, "rpm -ivh --force %s/%s" %
                                 (host_lustre_rpm_dir, rpm_name),
                                 timeout=ssh_host.LONGEST_TIME_RPM_INSTALL)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to install kernel RPM on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             self.sh_hostname, retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
                return -1

        rpm_name = lustre_dist.ldis_lustre_rpm_dict[RPM_KERNEL]
        retval = self.sh_run(log, "rpm -ivh --force %s/%s" %
                             (host_lustre_rpm_dir, rpm_name),
                             timeout=ssh_host.LONGEST_TIME_RPM_INSTALL)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to install kernel RPM on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1

        distro = self.sh_distro(log)
        if distro is None:
            log.cl_error("failed to get distro of host [%s]",
                         self.sh_hostname)
            return -1
        if distro == ssh_host.DISTRO_RHEL7:
            # Somehow crashkernel=auto doen't work for RHEL7 sometimes
            log.cl_info("changing boot argument of crashkernel on host [%s]",
                        self.sh_hostname)
            bios_based_config = "/boot/grub2/grub.cfg"
            uefi_based_config = "/boot/efi/EFI/redhat/grub.cfg"
            uefi_centos_based_config = "/boot/efi/EFI/centos/grub.cfg"

            ret = self.sh_path_exists(log, bios_based_config)
            if ret < 0:
                log.cl_error("failed to check whether [%s] exists on host [%s]",
                             bios_based_config, self.sh_hostname)
                return -1
            bios_based_config_exists = bool(ret)

            ret = self.sh_path_exists(log, uefi_based_config)
            if ret < 0:
                log.cl_error("failed to check whether [%s] exists on host [%s]",
                             uefi_based_config, self.sh_hostname)
                return -1
            uefi_based_config_exists = bool(ret)

            ret = self.sh_path_exists(log, uefi_centos_based_config)
            if ret < 0:
                log.cl_error("failed to check whether [%s] exists on host [%s]",
                             uefi_centos_based_config, self.sh_hostname)
                return -1
            uefi_centos_based_config_exists = bool(ret)
            if ((not bios_based_config_exists) and
                    (not uefi_based_config_exists) and
                    (not uefi_centos_based_config_exists)):
                log.cl_error("none of boot config files [%s], [%s] and [%s] "
                             "exists on host [%s]",
                             bios_based_config, uefi_based_config,
                             uefi_centos_based_config,
                             self.sh_hostname)
                return -1
            elif bios_based_config_exists:
                config = bios_based_config
            elif uefi_based_config_exists:
                config = uefi_based_config
            elif uefi_centos_based_config:
                config = uefi_centos_based_config

            command = ("sed -i 's/crashkernel=auto/crashkernel=128M/g' %s" %
                       config)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname, retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
                return -1
        else:
            log.cl_error("unsupported distro [%s]", distro)
            return -1

        # install ofed if necessary
        log.cl_info("installing OFED RPM on host [%s]", self.sh_hostname)
        retval = self.sh_run(log, "ls %s | grep mlnx-ofa_kernel" %
                             host_lustre_rpm_dir)

        if retval.cr_exit_status == 0:
            retval = self.sh_run(log, "rpm -ivh --force "
                                 "%s/mlnx-ofa_kernel*.rpm" %
                                 host_lustre_rpm_dir)
            if retval.cr_exit_status != 0:
                retval = self.sh_run(log, "yum localinstall -y --nogpgcheck "
                                     "%s/mlnx-ofa_kernel*.rpm" %
                                     host_lustre_rpm_dir)
                if retval.cr_exit_status != 0:
                    log.cl_error("failed to install OFED RPM on host [%s], "
                                 "ret = %d, stdout = [%s], stderr = [%s]",
                                 self.sh_hostname, retval.cr_exit_status,
                                 retval.cr_stdout, retval.cr_stderr)
                    return -1

        if self._lsh_install_e2fsprogs(log, workspace):
            return -1

        # Remove any files under the test directory to avoid FID problem
        log.cl_debug("removing directory [%s] on host [%s]",
                     LUSTRE_TEST_SCRIPT_DIR, self.sh_hostname)
        retval = self.sh_run(log, "rm %s -fr" % LUSTRE_TEST_SCRIPT_DIR)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to remove [%s] on host "
                         "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                         LUSTRE_TEST_SCRIPT_DIR, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        if lustre_dist.ldis_zfs_support:
            log.cl_info("installing ZFS RPMs on host [%s]", self.sh_hostname)
            install_timeout = ssh_host.LONGEST_SIMPLE_COMMAND_TIME * 2
            retval = self.sh_run(log, "cd %s && rpm -ivh libnvpair1-* libuutil1-* "
                                 "libzfs2-0* libzpool2-0* kmod-spl-[34]* "
                                 "kmod-zfs-[34]* spl-0* zfs-0*" %
                                 (host_lustre_rpm_dir),
                                 timeout=install_timeout)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to install ZFS RPMs on host "
                             "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        log.cl_info("installing Lustre RPMs on host [%s]", self.sh_hostname)
        for rpm_type in LUSTRE_RPM_TYPES:
            if rpm_type not in lustre_dist.ldis_lustre_rpm_dict:
                continue
            install_timeout = ssh_host.LONGEST_SIMPLE_COMMAND_TIME * 2
            command = ("rpm -ivh --force --nodeps %s/%s" %
                       (host_lustre_rpm_dir,
                        lustre_dist.ldis_lustre_rpm_dict[rpm_type]))
            retval = self.sh_run(log, command,
                                 timeout=install_timeout)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = %d, stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        log.cl_info("installed RPMs under [%s] on host [%s]",
                    host_lustre_rpm_dir, self.sh_hostname)

        return 0

    def _lsh_lustre_reinstall(self, log, workspace):
        """
        Reinstall Lustre RPMs
        """
        ret = self._lsh_lustre_uninstall(log)
        if ret:
            log.cl_error("failed to uninstall Lustre RPMs on host [%s]",
                         self.sh_hostname)
            return -1

        ret = self._lsh_lustre_install(log, workspace)
        if ret != 0:
            log.cl_error("failed to install RPMs on host [%s]",
                         self.sh_hostname)
            return -1
        return 0

    def _lsh_can_skip_install(self, log):
        """
        Check whether the install of Lustre RPMs could be skipped
        """
        lustre_dist = self.lsh_lustre_dist
        for rpm_name in lustre_dist.ldis_lustre_rpm_dict.values():
            log.cl_debug("checking whether RPM [%s] is installed on "
                         "host [%s]", rpm_name, self.sh_hostname)
            name, ext = os.path.splitext(rpm_name)
            if ext != ".rpm":
                log.cl_debug("RPM [%s] does not have .rpm subfix,"
                             "go on anyway", rpm_name)
            retval = self.sh_run(log, "rpm -qi %s" % name)
            if retval.cr_exit_status != 0:
                log.cl_debug("RPM [%s] is not installed on host [%s], "
                             "will not skip install",
                             rpm_name, self.sh_hostname)
                return False
        return True

    def _lsh_lustre_check_clean(self, log, quiet=True):
        """
        Check whether the host is clean for running Lustre
        """
        if quiet:
            func = log.cl_debug
        else:
            func = log.cl_error
            log.cl_info("checking whether host [%s] is clean to run Lustre",
                        self.sh_hostname)
        if self.lsh_lustre_dist is None:
            func("Lustre RPMs is not configured for host [%s]",
                 self.sh_hostname)
            return -1
        if not self.lsh_lustre_dist.ldis_prepared:
            func("Lustre RPMs [%s] is not prepared for host [%s]",
                 self.lsh_lustre_dist.lr_distribution_id,
                 self.sh_hostname)
            return -1
        kernel_version = self.lsh_lustre_dist.ldis_kernel_version
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

    def lsh_lustre_prepare(self, log, workspace, lazy_prepare=False):
        """
        Prepare the host for running Lustre
        """
        # pylint: disable=too-many-branches
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

        lustre_dist = self.lsh_lustre_dist
        if lustre_dist is None:
            if self.lsh_is_server:
                log.cl_error("failed to prepare Lustre server [%s] because Lustre RPMs is not configured",
                             self.sh_hostname)
                return -1
            log.cl_info("Lustre RPMs is not configured for host [%s], do not prepare Lustre",
                        self.sh_hostname)
            return 0
        if not lustre_dist.ldis_prepared:
            log.cl_error("failed to prepare host [%s] because Lustre RPMs [%s] is not prepared",
                         lustre_dist.lr_distribution_id,
                         self.sh_hostname)
            return -1
        if lazy_prepare and self._lsh_can_skip_install(log):
            log.cl_info("skipping installation of Lustre RPMs on host [%s]",
                        self.sh_hostname)
        else:
            ret = self._lsh_lustre_reinstall(log, workspace)
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
            ret = self._lsh_lustre_check_clean(log)
            if ret:
                log.cl_debug("host [%s] need a reboot to change the kernel "
                             "or cleanup the status of Lustre",
                             self.sh_hostname)
                need_reboot = True

        if not lazy_prepare:
            need_reboot = True

        if need_reboot:
            ret = self.sh_kernel_set_default(log, lustre_dist.ldis_kernel_version)
            if ret:
                log.cl_error("failed to set default kernel of host [%s] to [%s]",
                             self.sh_hostname, lustre_dist.ldis_kernel_version)
                return -1

            ret = self.sh_reboot(log)
            if ret:
                log.cl_error("failed to reboot host [%s]", self.sh_hostname)
                return -1

            ret = self._lsh_lustre_check_clean(log, quiet=False)
            if ret:
                log.cl_error("host [%s] is not clean to run Lustre",
                             self.sh_hostname)
                return -1

        return 0

    def lsh_getname(self, log, path):
        """
        return the Lustre client name on a path
        If error, return None
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
            log.cl_error("failed to parse output [%s] to get name" % output)
            return None
        return client_name

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

    def lsh_load_balanced(self, log):
        """
        Return 1 if the host is load balanced. Return 0 if not. Return negative
        on error.

        This function assumes that the cluster is symmetric.

        A symmetric cluster is balanced iff:

        Every host provides the mean load in the symmetric host group.

        A symmetric host group in a symmetric cluster includes all the hosts
        that can provide the same services.
        """
        hostname = self.sh_hostname
        service_number = len(self.lsh_instance_dict)
        host_number = None
        for instance in self.lsh_instance_dict.values():
            service = instance.lsi_service
            if host_number is None:
                host_number = len(service.ls_instance_dict)
                break
        # Host without any service is considered as load balanced
        if host_number is None:
            return 1

        load_per_host = service_number // host_number
        has_remainder = (load_per_host * host_number) != service_number

        client_dict = {}
        service_instance_dict = {}
        ret = self.lsh_lustre_detect_services(log, client_dict,
                                              service_instance_dict)
        if ret:
            log.cl_error("failed to check the load on host [%s]",
                         hostname)
            return -1

        load = len(service_instance_dict)

        if has_remainder:
            if load != load_per_host and load != load_per_host + 1:
                log.cl_debug("balanced load of host [%s] should be either [%s] or [%s], got [%s]",
                             hostname, load_per_host, load_per_host + 1, load)
                return 0
        else:
            if load != load_per_host:
                log.cl_debug("balanced load of host [%s] should be [%s], got [%s]",
                             hostname, load_per_host, load)
                return 0
        return 1

    def lsh_healty_check(self, log):
        """
        Return the healthy check result. Return LSH_ERROR on failure
        """
        if (not self.lsh_is_server) and (not self.lsh_is_client):
            return LustreHost.LSH_HEALTHY

        modules = self.sh_lsmod(log)
        if modules is None:
            return LustreHost.LSH_ERROR

        if "lustre" not in modules:
            return LustreHost.LSH_HEALTHY

        command = 'lctl get_param -n health_check'
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return LustreHost.LSH_ERROR
        healthy = retval.cr_stdout.strip()
        if healthy == "healthy":
            return LustreHost.LSH_HEALTHY
        elif healthy == "NOT HEALTHY":
            return LustreHost.LSH_UNHEALTHY
        elif healthy == "LBUG":
            return LustreHost.LSH_LBUG
        log.cl_error("unexpected output of command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, self.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return LustreHost.LSH_ERROR

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
            client = LustreClient(log, lustre_fs, host, mount_point)
            clients.append(client)
            log.cl_debug("client [%s] mounted on dir [%s] of host [%s]",
                         fsname, mount_point, host.sh_hostname)
    return clients


def lfs_fid2path(log, fid, fsname_rootpath):
    """
    Transfer FID to fpath
    """
    command = ("lfs fid2path %s %s" % (fsname_rootpath, fid))
    retval = utils.run(command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None

    return retval.cr_stdout.strip()


def fid_path(fid, fsname_rootpath):
    """
    Get the fid path of a file
    """
    return "%s/.lustre/fid/%s" % (fsname_rootpath, fid)


def lfs_hsm_archive(log, fpath, archive_id, host=None):
    """
    HSM archive
    """
    command = ("lfs hsm_archive --archive %s %s" % (archive_id, fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = (" on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_restore(log, fpath, host=None):
    """
    HSM restore
    """
    command = ("lfs hsm_restore %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_release(log, fpath, host=None):
    """
    HSM release
    """
    command = ("lfs hsm_release %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_remove(log, fpath, host=None):
    """
    HSM remove
    """
    command = ("lfs hsm_remove %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_cancel(log, fpath, host=None):
    """
    HSM remove
    """
    command = ("lfs hsm_cancel %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lustre_unlink(log, path, host=None):
    """
    Basic file remove
    Only remove general files and empty dirs.
    """
    # ignore the remove if fpath is root dir
    if path == "/":
        log.cl_error("trying to remove root dir, skipping it.")
        return -1

    command = "if [ -d {0} ];then rmdir {0};else rm -f {0};fi".format(path)
    extra_string = ""

    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


class HSMState(object):
    """
    The HSM state
    """
    # pylint: disable=too-few-public-methods
    HS_NONE = 0x00000000
    HS_EXISTS = 0x00000001
    HS_DIRTY = 0x00000002
    HS_RELEASED = 0x00000004
    HS_ARCHIVED = 0x00000008
    HS_NORELEASE = 0x00000010
    HS_NOARCHIVE = 0x00000020
    HS_LOST = 0x00000040

    def __init__(self, states, archive_id=0):
        self.hs_states = states
        self.hs_archive_id = archive_id

    def __eq__(self, other):
        return (self.hs_states == other.hs_states and
                self.hs_archive_id == other.hs_archive_id)

    def hs_string(self):
        """
        return string of the status
        """
        output = hex(self.hs_states)
        if self.hs_states & HSMState.HS_RELEASED:
            output += " released"
        if self.hs_states & HSMState.HS_EXISTS:
            output += " exists"
        if self.hs_states & HSMState.HS_DIRTY:
            output += " dirty"
        if self.hs_states & HSMState.HS_ARCHIVED:
            output += " archived"
        # Display user-settable flags
        if self.hs_states & HSMState.HS_NORELEASE:
            output += " never_release"
        if self.hs_states & HSMState.HS_NOARCHIVE:
            output += " never_archive"
        if self.hs_states & HSMState.HS_LOST:
            output += " lost_from_hsm"
        if self.hs_archive_id != 0:
            output += (", archive_id:%d" % self.hs_archive_id)
        return output


HSM_STATE_PATTERN = (r"^\: \((?P<states>.+)\).*$")
HSM_STATE_REGULAR = re.compile(HSM_STATE_PATTERN)
HSM_ARCHIVE_ID_PATTERN = (r"^\: \((?P<states>.+)\).+, archive_id:(?P<archive_id>.+)$")
HSM_ARCHIVE_ID_REGULAR = re.compile(HSM_ARCHIVE_ID_PATTERN)


def lfs_hsm_state(log, fpath, host=None):
    """
    HSM state
    """
    command = ("lfs hsm_state %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None

    output = retval.cr_stdout.strip()
    if not output.startswith(fpath):
        log.cl_error("unexpected output [%s]", output)
        return None

    fpath_len = len(fpath)
    output = output[fpath_len:]
    match = HSM_STATE_REGULAR.match(output)
    if not match:
        log.cl_error("output [%s] doesn't mather pattern [%s]",
                     output, HSM_STATE_PATTERN)
        return None

    states = int(match.group("states"), 16)
    archive_id = 0
    match = HSM_ARCHIVE_ID_REGULAR.match(output)
    if match:
        archive_id = int(match.group("archive_id"))
    return HSMState(states, archive_id)


def check_hsm_state(log, fpath, states, archive_id=0, host=None):
    """
    Check the current HSM state
    """
    expected_state = HSMState(states, archive_id=archive_id)
    state = lfs_hsm_state(log, fpath, host=host)
    if state is None:
        log.cl_debug("failed to get HSM state")
        return -1
    if state == expected_state:
        log.cl_debug("successfully got expected HSM states [%s]",
                     expected_state.hs_string())
        return 0
    log.cl_debug("got HSM state [%s], expected [%s]",
                 state.hs_string(), expected_state.hs_string())
    return -1


def wait_hsm_state(log, fpath, states, archive_id=0, host=None,
                   timeout=90, sleep_interval=1):
    """
    Wait util the HSM state changes to the expected state
    """
    # pylint: disable=too-many-arguments
    waited = 0
    expected_state = HSMState(states, archive_id=archive_id)
    while True:
        state = lfs_hsm_state(log, fpath, host=host)
        if state is None:
            return -1

        if state == expected_state:
            log.cl_debug("expected HSM states [%s]", expected_state.hs_string())
            return 0

        if waited < timeout:
            waited += sleep_interval
            time.sleep(sleep_interval)
            continue
        log.cl_error("timeout when waiting the hsm state, expected [%s], "
                     "got [%s]", expected_state.hs_string(), state.hs_string())
        return -1
    return -1


def get_fsname(log, host, device_path):
    """
    Get file system name either from ldiskfs or ZFS.
    """
    ret, info_dict = host.sh_dumpe2fs(log, device_path)
    if ret:
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


def lfs_path2fid(log, host, fpath):
    """
    Transfer fpath to FID string
    """
    command = ("lfs path2fid %s" % (fpath))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None

    fid = retval.cr_stdout.strip()
    if len(fid) < 2 or fid[0] != '[' or fid[-1] != ']':
        log.cl_error("invalid fid [%s]", fid)
        return None
    fid = fid[1:-1]
    return fid


class LustreFID(object):
    """
    FID
    """
    # pylint: disable=too-few-public-methods
    # fid_string: 0x200000400:0xaa1:0x0
    def __init__(self, log, fid_string):
        self.lf_fid_string = fid_string
        fields = fid_string.split(':')
        if len(fields) != 3:
            reason = ("invalid FID %s" % fid_string)
            log.cl_error(reason)
            raise Exception(reason)

        self.lf_seq = int(fields[0], 16)
        self.lf_oid = int(fields[1], 16)
        self.lf_ver = int(fields[2], 16)

    def lf_posix_archive_path(self, archive_dir):
        """
        Get the posix archive path
        """
        return ("%s/%04x/%04x/%04x/%04x/%04x/%04x/%s" %
                (archive_dir, self.lf_oid & 0xFFFF,
                 self.lf_oid >> 16 & 0xFFFF,
                 self.lf_seq & 0xFFFF,
                 self.lf_seq >> 16 & 0xFFFF,
                 self.lf_seq >> 32 & 0xFFFF,
                 self.lf_seq >> 48 & 0xFFFF,
                 self.lf_fid_string))


def host_lustre_prepare(log, workspace, host, lazy_prepare=False):
    """
    wrapper of lsh_lustre_prepare for parrallism
    """
    return host.lsh_lustre_prepare(log, workspace,
                                   lazy_prepare=lazy_prepare)


def lustre_file_setstripe(log, host, fpath, stripe_index=-1, stripe_count=1):
    """
    use lfs_setstripe to create a file
    """
    command = ("lfs setstripe -i %s -c %s %s" %
               (stripe_index, stripe_count, fpath))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1, None
    return 0


def get_lustre_dist(log, local_host, lustre_dir, e2fsprogs_dir):
    """
    Return Lustre distribution
    """
    ret = local_host.sh_check_dir_content(log, lustre_dir, ["RPMS", "SRPMS"],
                                          cleanup=False)
    if ret:
        log.cl_error("directory [%s] does not have expected content",
                     lustre_dir)
        return None

    target_cpu = local_host.sh_target_cpu(log)
    if target_cpu is None:
        log.cl_error("failed to get target cpu of host [%s]",
                     local_host.sh_hostname)
        return None

    lustre_rpms_dir = lustre_dir + "/RPMS"
    ret = local_host.sh_check_dir_content(log, lustre_rpms_dir,
                                          ["noarch", target_cpu],
                                          cleanup=False)
    if ret:
        log.cl_error("directory [%s] does not have expected content",
                     lustre_rpms_dir)
        return None

    ret = local_host.sh_check_dir_content(log, e2fsprogs_dir,
                                          ["RPMS", "SRPMS"],
                                          cleanup=False)
    if ret:
        log.cl_error("directory [%s] does not have expected content",
                     e2fsprogs_dir)
        return None

    e2fsprogs_rpms_dir = e2fsprogs_dir + "/RPMS"
    ret = local_host.sh_check_dir_content(log, e2fsprogs_rpms_dir,
                                          [target_cpu],
                                          cleanup=False)
    if ret:
        log.cl_error("directory [%s] does not have expected content",
                     e2fsprogs_rpms_dir)
        return None

    lustre_x64_rpms_dir = lustre_rpms_dir + "/" + target_cpu
    e2fsprogs_x64_rpms_dir = e2fsprogs_rpms_dir + "/" + target_cpu
    lustre_dist = LustreDistribution(local_host,
                                     lustre_x64_rpms_dir,
                                     e2fsprogs_x64_rpms_dir)
    ret = lustre_dist.ldis_prepare(log)
    if ret:
        log.cl_error("Lustre/E2fsprogs RPMs are not valid")
        return None
    return lustre_dist


def check_lustre_source(log, host, source_path):
    """
    Check the Lustre source codes are valid
    """
    subdirs = ["lustre", "lnet", "ldiskfs", "build"]
    for subdir in subdirs:
        path = source_path + "/" + subdir
        ret = host.sh_path_exists(log, path)
        if ret < 0:
            log.cl_error("failed to check whether [%s] exists on host [%s]",
                         path, host.sh_hostname)
            return -1
        elif ret == 0:
            log.cl_error("path [%s] does not exist on host [%s]",
                         path, host.sh_hostname)
            return -1
    return 0
