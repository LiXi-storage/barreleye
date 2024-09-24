"""
Library for LVM management on host through SSH.

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
class SSHHostLVMMixin():
    """
    Mixin class of SSHHost for LVM management.
    To mixin, define a class like:
    class SomeHost(SSHBasicHost, SSHHostLVMMixin)
    """
    def sh_lvm_volumes(self, log):
        """
        Return volume names with the format of vg_name/lv_version_name
        """
        command = "lvs"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, self.sh_hostname)
            return None
        lines = retval.cr_stdout.splitlines()
        if len(lines) == 0:
            return []
        lines = lines[1:]
        volume_names = []
        for line in lines:
            fields = line.split()
            if len(fields) < 2:
                log.cl_error("unexpected stdout [%s] of command [%s] on "
                             "host [%s]",
                             command, self.sh_hostname)
                return None
            lv_version_name = fields[0]
            vg_name = fields[1]
            volume_names.append(vg_name + "/" + lv_version_name)
        return volume_names

    def sh_lvm_volume_groups(self, log):
        """
        Return volume groups
        """
        command = "vgs"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, self.sh_hostname)
            return None
        stdout = retval.cr_stdout
        lines = stdout.splitlines()
        if len(lines) == 0:
            return []
        lines = lines[1:]
        vg_names = []
        for line in lines:
            fields = line.split()
            if len(fields) < 1:
                log.cl_error("unexpected stdout [%s] of command [%s] on "
                             "host [%s]",
                             stdout, command, self.sh_hostname)
                return None
            vg_name = fields[0]
            vg_names.append(vg_name)
        return vg_names

    def sh_lvm_vg_or_lv_dict(self, log, vg_or_lv_version_name, vg=True):
        """
        Return a dict of VG or LV from command "vgdisplay" or "lvdisplay"
        """
        # pylint: disable=too-many-locals,too-many-branches
        if vg:
            command = "vgdisplay %s" % vg_or_lv_version_name
        else:
            command = "lvdisplay %s" % vg_or_lv_version_name

        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s]",
                         command, self.sh_hostname)
            return None

        stdout = retval.cr_stdout
        lines = stdout.splitlines()
        if len(lines) < 2:
            log.cl_error("unexpected line number of stdout [%s] from "
                         "command [%s] on host [%s]",
                         stdout, command, self.sh_hostname)
            return None
        striped_lines = []
        for line in lines:
            line = line.lstrip()
            if len(line) == 0:
                continue
            striped_lines.append(line)
        if vg:
            leading = "--- Volume group ---"
        else:
            leading = "--- Logical volume ---"
        if striped_lines[0] != leading:
            log.cl_error("unexpected first line of stdout [%s] from "
                         "command [%s] on host [%s]",
                         stdout, command, self.sh_hostname)
            return None

        lv_or_vg_dict = {}
        lines = striped_lines[1:]
        for line in lines:
            # It is assume that key and value are seperated with two or
            # more spaces.
            #
            # But "LV Creation host, time " does not keep this assumption.
            lv_special = "LV Creation host, time"
            if not vg and line.startswith(lv_special + " "):
                key = lv_special
                value = line[len(lv_special) + 1:]
            else:
                seperate_index = line.find("  ")
                if seperate_index < 0:
                    log.cl_error("unexpected missing seperator of line [%s] in "
                                 "stdout [%s] from command [%s] on host [%s]",
                                 line, stdout, command, self.sh_hostname)
                    return None
                key = line[:seperate_index]
                value = line[seperate_index + 2:].lstrip()
            if key in lv_or_vg_dict:
                log.cl_error("unexpected multiple values for key [%s] in stdout "
                             "[%s] from command [%s] on host [%s]",
                             key, stdout, command, self.sh_hostname)
                return None
            lv_or_vg_dict[key] = value

        return lv_or_vg_dict

    def sh_lvm_vg_dict(self, log, vg_name):
        """
        Return a dict of VG from command "vgdisplay"
        """
        return self.sh_lvm_vg_or_lv_dict(log, vg_name, vg=True)

    def sh_lvm_vg_uuid(self, log, vg_name):
        """
        Return the UUID of VG from command "vgdisplay"
        """
        vg_dict = self.sh_lvm_vg_dict(log, vg_name)
        if vg_dict is None:
            return None
        if "VG UUID" not in vg_dict:
            log.cl_error("no field UUID in vgdisplay command")
            return None
        vg_uuid = vg_dict["VG UUID"]
        return vg_uuid

    def sh_lvm_lv_dict(self, log, lv_version_name):
        """
        Return a dict of VG from command "lvdisplay"
        """
        return self.sh_lvm_vg_or_lv_dict(log, lv_version_name, vg=False)

    def sh_lvm_lv_uuid(self, log, lv_version_name):
        """
        Return the UUID of lV from command "lvdisplay"
        """
        lv_dict = self.sh_lvm_lv_dict(log, lv_version_name)
        if lv_dict is None:
            return None
        if "LV UUID" not in lv_dict:
            log.cl_error("no field UUID in lvdisplay command")
            return None
        lv_uuid = lv_dict["LV UUID"]
        return lv_uuid
