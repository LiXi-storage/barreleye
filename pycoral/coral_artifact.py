"""
Management of the artifacts of Coral releases.
"""
# pylint: disable=too-many-lines
import os
from pycoral import constant
from pycoral import os_distro
from pycoral import release_info
from pycoral import parallel


def _generate_version_dir(group_dir, version):
    """
    Return the version dir
    """
    return group_dir + "/" + version


def _generate_distro_type_dir(group_dir, version, distro_type):
    """
    Return the path of distro type dir.
    """
    version_dir = _generate_version_dir(group_dir, version)
    return version_dir + "/" + distro_type


def _generate_distro_dir(group_dir, version, distro_type,
                         distro):
    """
    Return the path of distro dir.
    """
    distro_type_dir = _generate_distro_type_dir(group_dir,
                                                version,
                                                distro_type)
    return distro_type_dir + "/" + distro


def _generate_architecture_dir(group_dir, version, distro_type,
                               distro, target_cpu):
    """
    Generate the architecture dir.
    """
    distro_dir = _generate_distro_dir(group_dir,
                                      version,
                                      distro_type,
                                      distro)
    return distro_dir + "/" + target_cpu


def _generate_coral_iso_fname(version, distro_short, target_cpu):
    """
    Return the ISO fname of Coral
    """
    return (constant.CORAL_ISO_PREFIX + version + "." + distro_short + "." +
            target_cpu + constant.CORAL_ISO_SUFFIX)

FIELD_GROUP = "group"
FIELD_VERSION = "version"
FIELD_DISTRO_TYPE = "distro_type"
FIELD_DISTRO = "distro"
FIELD_ARCH = "arch"
FIELD_TAG = "tag"

def parse_artifact_name(log, artifact_name):
    """
    Return a dict of fields.

    artifact_name:
    :group/:version/:distro_type/:distro/:arch/:tag
    Example:
    devel/2.0.0/rhel/rhel7/x86_64/plain
    """
    fields = artifact_name.split("/")
    if len(fields) != 6:
        log.cl_error("invalid field number of artifact name [%s]" %
                     artifact_name)
        return None

    group_name = fields[0]
    version = fields[1]
    distro_type = fields[2]
    distro = fields[3]
    target_cpu = fields[4]
    tag = fields[5]

    distro_type_calculated = os_distro.distro2type(log, distro)
    if distro_type_calculated != distro_type:
        log.cl_error("inconsistent distro type [%s] and distro [%s]"
                     "in artifact name [%s], expected distro type [%s]",
                     distro_type, distro, artifact_name,
                     distro_type_calculated)
        return None
    return {
        FIELD_GROUP: group_name,
        FIELD_VERSION: version,
        FIELD_DISTRO_TYPE: distro_type,
        FIELD_DISTRO: distro,
        FIELD_ARCH: target_cpu,
        FIELD_TAG: tag,
    }




class CoralArtifactGroup():
    """
    The instance that manages a group of artifacts.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, bucket_dir, group_name):
        # The parent directory of the group directory
        self.cag_bucket_dir = bucket_dir
        # The group name
        self.cag_group_name = group_name
        # The directory that saves all versions of the group.
        self.cag_group_dir = bucket_dir + "/" + group_name

    def cag_get_versions(self, log, host):
        """
        Get the version names.
        """
        ret = host.sh_path_exists(log, self.cag_group_dir)
        if ret < 0:
            log.cl_error("failed to check whether dir [%s] exists on host [%s]",
                         self.cag_group_dir, host.sh_hostname)
            return -1
        if ret == 0:
            return []

        versions = host.sh_get_dir_fnames(log, self.cag_group_dir)
        if versions is None:
            log.cl_error("failed to get fnames under [%s] on host [%s]",
                         self.cag_group_dir, host.sh_hostname)
            return None
        return versions

    def cag_artifact_find(self, log, host, artifact_name):
        """
        Return CoralArtifact.
        """
        # pylint: disable=too-many-locals
        field_dict = parse_artifact_name(log, artifact_name)
        if field_dict is None:
            log.cl_error("invalid artifact name [%s]" %
                         artifact_name)
            return -1, None

        group_name = field_dict[FIELD_GROUP]
        if group_name != self.cag_group_name:
            log.cl_error("unexpected group name [%s] in artifact name [%s], "
                         "expecting [%s]" %
                         group_name, artifact_name, self.cag_group_name)
            return -1, None
        version = field_dict[FIELD_VERSION]
        distro = field_dict[FIELD_DISTRO]
        distro_short = os_distro.distro2short(log, distro)
        if distro_short is None:
            log.cl_error("unsupported distro [%s] in artifact name [%s]" %
                         distro, artifact_name)
            return -1, None
        target_cpu = field_dict[FIELD_ARCH]
        artifact_dir = self.cag_bucket_dir + "/" + artifact_name
        exists = host.sh_path_exists(log, artifact_dir)
        if exists < 0:
            log.cl_error("failed to check whether dir [%s] exists on host [%s]",
                         artifact_dir, host.sh_hostname)
            return -1, None
        if not exists:
            return 0, None

        iso_fname = _generate_coral_iso_fname(version,
                                              distro_short,
                                              target_cpu)
        iso_relative_fpath = (artifact_name + "/" + iso_fname)
        iso_artifact = CoralArtifact(self, self.cag_bucket_dir, artifact_name,
                                     version, iso_relative_fpath)
        return 0, iso_artifact

    def cag_artifact_remove(self, log, local_host,
                            workspace, host, artifact_name,
                            optional_contents=None):
        """
        Remove an artifact on a host.
        """
        ret, artifact = self.cag_artifact_find(log, host, artifact_name)
        if ret:
            log.cl_error("failed to find artifact [%s] on host [%s]",
                         artifact_name, host.sh_hostname)
            return -1

        if artifact is None:
            log.cl_info("artifact [%s] does not exist on host [%s]",
                        artifact_name, host.sh_hostname)
            return 0
        log.cl_info("removing artifact [%s] on host [%s]",
                    artifact_name, host.sh_hostname)
        ret = artifact.cra_artifact_remove(log, local_host,
                                           workspace, host,
                                           optional_contents=optional_contents)
        if ret:
            log.cl_error("failed to remove artifact [%s]",
                         artifact_name)
            return -1
        return 0

    def cag_artifact_remove_local(self, log, local_host, artifact_name,
                                  optional_contents=None):
        """
        Remove an artifact on local host.
        """
        return self.cag_artifact_remove(log, local_host,
                                        None, # workspace,
                                        local_host, # host
                                        artifact_name,
                                        optional_contents=optional_contents)

    def cag_artifact_spread(self, log, local_host, workspace, artifact_name, hosts):
        """
        Spread artifact to hosts.
        """
        ret, artifact = self.cag_artifact_find(log, local_host, artifact_name)
        if ret:
            log.cl_error("failed to find artifact [%s] on local host [%s]",
                         artifact_name, local_host.sh_hostname)
            return -1
        if artifact is None:
            log.cl_error("artifact [%s] does NOT exist on host [%s]",
                         artifact_name, local_host.sh_hostname)
            return -1
        ret = artifact.cra_artifact_spread(log, local_host, workspace, hosts)
        if ret:
            log.cl_error("failed to spread artifact [%s]", artifact_name)
            return -1
        return 0

    def _cag_artifact_remove_in_thread(self, log, workspace, local_host, host,
                                       artifact_name, optional_contents):
        """
        Remove the artifact in a parallel thread.
        """
        return self.cag_artifact_remove(log, local_host, workspace, host,
                                        artifact_name,
                                        optional_contents=optional_contents)

    def cag_artifact_remove_all(self, log, local_host, workspace, artifact_name, hosts,
                                optional_contents=None, parallelism=8):
        """
        Remove an artifact on hosts.
        """
        # pylint: disable=too-many-locals
        args_array = []
        thread_ids = []
        for host in hosts:
            args = (local_host, host, artifact_name, optional_contents)
            args_array.append(args)
            hostname = host.sh_hostname
            thread_id = "artifact_remove_%s" % hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(workspace,
                                                    "artifact_remove",
                                                    self._cag_artifact_remove_in_thread,
                                                    args_array,
                                                    thread_ids=thread_ids)

        ret = parallel_execute.pe_run(log, parallelism=parallelism)
        if ret:
            log.cl_error("failed to remove artifact [%s] on hosts",
                         artifact_name)
            return -1
        return 0

    def cag_artifact_add(self, log, local_host, workspace, host, iso_fpath,
                         overwrite=False,
                         tag=constant.CORAL_TAG_PLAIN
                         ):
        """
        Add a new Coral artifact.
        Return the artifact name added.
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        coral_release_info, artifact_name = \
            artifact_name_read_from_iso(log, local_host,
                                        workspace,
                                        host,
                                        iso_fpath,
                                        self.cag_group_name,
                                        tag=tag)
        if coral_release_info is None or artifact_name is None:
            log.cl_error("failed to get artifact name from ISO [%s] on host [%s]",
                         iso_fpath, host.sh_hostname)
            return None

        artifact_dir = self.cag_bucket_dir + "/" + artifact_name

        ret = host.sh_path_exists(log, artifact_dir)
        if ret < 0:
            log.cl_error("failed to check whether dir [%s] exist on host [%s]",
                         artifact_dir, host.sh_hostname)
            return None
        if ret:
            if not overwrite:
                log.cl_error("dir [%s] already exists on host [%s]",
                             artifact_dir, host.sh_hostname)
                return None
            log.cl_info("removing existing dir [%s] on host [%s]",
                        artifact_dir, host.sh_hostname)
            command = "rm -fr " + artifact_dir
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return None


        release_name = coral_release_info.rli_release_name(log)
        iso_fname = _generate_coral_iso_fname(release_name,
                                              coral_release_info.rli_distro_short,
                                              coral_release_info.rli_target_cpu)
        iso_fpath_dest = artifact_dir + "/" + iso_fname
        release_info_fpath = artifact_dir + "/" + constant.CORAL_RELEASE_INFO_FNAME

        log.cl_info("adding Coral release [%s] to dir [%s] on host [%s]",
                    release_name, artifact_dir, host.sh_hostname)
        command = "mkdir -p %s" % artifact_dir
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None

        same_fs = host.sh_same_filesystem(log, artifact_dir, iso_fpath)
        if same_fs < 0:
            return None

        if same_fs:
            command = "ln %s %s" % (iso_fpath, iso_fpath_dest)
        else:
            command = "cp %s %s" % (iso_fpath, iso_fpath_dest)
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on local host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None


        ret = coral_release_info.rli_save_to_file(log, local_host,
                                                  workspace,
                                                  host, release_info_fpath)
        if ret:
            log.cl_error("failed to save release info to file [%s] "
                         "on host [%s]",
                         release_info_fpath, host.sh_hostname)
            return None

        ret = release_info.rinfo_scan_and_save(log, local_host, workspace,
                                               host, artifact_dir,
                                               constant.CORAL_ARTIFACT_INFO_FNAME)
        if ret:
            log.cl_error("failed to save info to file [%s] "
                         "on host [%s]",
                         release_info_fpath, host.sh_hostname)
            return None
        return artifact_name

    def cag_artifact_add_local(self, log, local_host, workspace, iso_fpath,
                               overwrite=False,
                               tag=constant.CORAL_TAG_PLAIN
                               ):
        """
        Add a new Coral artifact on local host.
        """
        return self.cag_artifact_add(log, local_host, workspace,
                                     local_host,  # host
                                     iso_fpath,
                                     overwrite=overwrite,
                                     tag=tag,
                                     )

    def _cag_artifact_get(self, log, version,
                          distro, target_cpu, tag_fname):
        """
        Get the ISO path under tag dir.
        """
        # pylint: disable=too-many-locals
        distro_short = os_distro.distro2short(log, distro)
        if distro_short is None:
            log.cl_error("unsupported distro [%s]", distro)
            return None
        artifact_name = generate_artifact_name(log, self.cag_group_name,
                                               version,
                                               distro_short,
                                               target_cpu,
                                               tag=tag_fname)
        fname = (constant.CORAL_ISO_PREFIX + version + "." +
                 distro_short + "." + target_cpu +
                 constant.CORAL_ISO_SUFFIX)

        iso_relative_fpath = (artifact_name + "/" + fname)
        iso_artifact = CoralArtifact(self, self.cag_bucket_dir, artifact_name,
                                     version, iso_relative_fpath)
        return iso_artifact

    def _cag_get_architecture_artifacts(self, log, host, version, distro_type,
                                        distro, target_cpu):
        """
        Get the paths of ISOs under a distribution dir.
        """
        architecture_dir = _generate_architecture_dir(self.cag_group_dir,
                                                      version,
                                                      distro_type,
                                                      distro,
                                                      target_cpu)
        tag_fnames = host.sh_get_dir_fnames(log, architecture_dir)
        if tag_fnames is None:
            log.cl_error("failed to get fnames under [%s] on host [%s]",
                         architecture_dir, host.sh_hostname)
            return None
        iso_artifacts = []
        for tag_fname in tag_fnames:
            iso_artifact = self._cag_artifact_get(log, version,
                                                  distro, target_cpu,
                                                  tag_fname)
            if iso_artifact is None:
                log.cl_error("failed to get ISO for tag [%s]", tag_fname)
                return None
            iso_artifacts.append(iso_artifact)
        return iso_artifacts

    def _cag_get_distribution_artifacts(self, log, host, version,
                                        distro_type, distro):
        """
        Get the paths of ISOs under a distribution dir.
        """
        distro_dir = _generate_distro_dir(self.cag_group_dir,
                                          version,
                                          distro_type,
                                          distro)
        architectures = host.sh_get_dir_fnames(log, distro_dir)
        if architectures is None:
            log.cl_error("failed to get fnames under [%s] on host [%s]",
                         distro_dir, host.sh_hostname)
            return None
        iso_artifacts = []
        for architecture in architectures:
            artifacts = self._cag_get_architecture_artifacts(log, host,
                                                             version,
                                                             distro_type,
                                                             distro,
                                                             architecture)
            if artifacts is None:
                log.cl_error("failed to get artifacts of CPU architecture [%s]",
                             architecture)
                return None
            iso_artifacts += artifacts
        return iso_artifacts

    def _cag_get_distro_type_artifacts(self, log, host, version,
                                       distro_type):
        """
        Get the paths of ISOs under a distr type dir.
        """
        distro_type_dir = _generate_distro_type_dir(self.cag_group_dir,
                                                    version,
                                                    distro_type)
        distros = host.sh_get_dir_fnames(log, distro_type_dir)
        if distros is None:
            log.cl_error("failed to get fnames under [%s] on host [%s]",
                         distro_type_dir, host.sh_hostname)
            return None
        iso_artifacts = []
        for distro in distros:
            artifacts = self._cag_get_distribution_artifacts(log,
                                                             host,
                                                             version,
                                                             distro_type,
                                                             distro)
            if artifacts is None:
                log.cl_error("failed to get artifacts of distro [%s]",
                             distro)
                return None
            iso_artifacts += artifacts
        return iso_artifacts

    def cag_get_version_distro_types(self, log, host, version):
        """
        Get the distribution types under a version.
        """
        version_dir = _generate_version_dir(self.cag_group_dir, version)

        distro_types = host.sh_get_dir_fnames(log, version_dir)
        if distro_types is None:
            log.cl_error("failed to get distribution types under [%s] on "
                         "host [%s]",
                         version_dir, host.sh_hostname)
            return None
        return distro_types

    def cag_get_version_artifacts(self, log, host, version):
        """
        Get the paths of ISOs under a version dir.
        """
        distro_types = self.cag_get_version_distro_types(log, host, version)
        if distro_types is None:
            log.cl_error("failed to get distribution types of version [%s]",
                         version)
            return None
        iso_artifacts = []
        for distro_type in distro_types:
            artifacts = self._cag_get_distro_type_artifacts(log,
                                                            host,
                                                            version,
                                                            distro_type)
            if artifacts is None:
                log.cl_error("failed to get ISOs under distro type [%s] of release [%s]",
                             distro_type, version)
                return None
            iso_artifacts += artifacts
        return iso_artifacts

    def cag_get_artifacts(self, log, host):
        """
        Return Dict of CoralArtifact. Key is cra_artifact_name.
        """
        # pylint: disable=too-many-locals
        artifacts = []
        ret = host.sh_path_exists(log, self.cag_group_dir)
        if ret < 0:
            log.cl_error("failed to check whether dir [%s] exists on host [%s]",
                         self.cag_group_dir, host.sh_hostname)
            return None
        if ret == 0:
            return artifacts

        command = "cd %s && find . -type f" % self.cag_group_dir
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        for line in lines:
            if not line.endswith(".iso"):
                continue
            # Example:
            # ./2.0.744/rhel/rhel7/aarch64/plain/coral-2.0.744.el7.aarch64.iso
            fields = line.split("/")
            if len(fields) != 7:
                continue

            version = fields[1]
            distro = fields[3]
            target_cpu = fields[4]
            tag_fname = fields[5]
            iso_artifact = self._cag_artifact_get(log,
                                                  version,
                                                  distro, target_cpu,
                                                  tag_fname)
            if iso_artifact is None:
                log.cl_error("failed to get ISO of file [%s]",
                             line)
                return None
            artifacts.append(iso_artifact)
        return artifacts

    def cag_artifact_extract_local(self, log, local_host, artifact_name, directory):
        """
        Extra a local artifact of this group to an dir
        """
        return self.cag_artifact_extract(log, local_host,
                                         None,  # workspace
                                         local_host,  # host
                                         artifact_name, directory)


    def cag_artifact_extract(self, log, local_host, workspace, host, artifact_name, directory):
        """
        Extra an artifact of this group to an dir
        """
        ret, artifact = self.cag_artifact_find(log, host, artifact_name)
        if ret:
            log.cl_error("failed to find artifact [%s] on host [%s]",
                         artifact_name, host.sh_hostname)
            return -1
        if artifact is None:
            log.cl_error("artifact [%s] does NOT exist on host [%s]", artifact_name,
                         host.sh_hostname)
            return -1
        rinfo = artifact.cra_artifact_extract(log, local_host, workspace, host,
                                              directory)
        if rinfo is None:
            log.cl_error("failed to extract artifact [%s] to dir [%s] on host [%s]",
                         artifact_name, directory, host.sh_hostname)
            return -1
        return 0


class CoralArtifact():
    """
    Each artifact has an object of this type.
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, group, bucket_dir, artifact_name,
                 coral_version, iso_relative_fpath):
        # CoralArtifactGroup
        self.cra_group = group
        # The version string, e.g. 2.0.1
        self.cra_coral_version = coral_version
        # The bucket directory path
        self.cra_bucket_dir = bucket_dir
        # The name of the artifact, i.e. the relative path of dir
        # under bucket dir, including the group name.
        # Format:
        # :group/:version/:distro_type/:distro/:arch/:tag
        self.cra_artifact_name = artifact_name
        # Artifact dir that saves the artifact
        self.cra_artifact_dir = bucket_dir + "/" + self.cra_artifact_name
        # The relative path of ISO under the parent dir
        self.cra_iso_relative_fpath = iso_relative_fpath
        # The absolute path of ISO file
        self.cra_iso_fpath = bucket_dir + "/" + self.cra_iso_relative_fpath
        # The YAML fpath of release.
        self.cra_release_info_relative_fpath = (artifact_name + "/" +
                                                constant.CORAL_RELEASE_INFO_FNAME)
        # The absolute path of release info
        self.cra_release_info_fpath = (bucket_dir + "/" +
                                       self.cra_release_info_relative_fpath)
        # The ISO file name
        self.cra_iso_fname = os.path.basename(self.cra_iso_relative_fpath)
        # The artifact info fpath
        self.cra_artifact_info_fpath = (self.cra_artifact_dir + "/" +
                                        constant.CORAL_ARTIFACT_INFO_FNAME)

    def _cra_get_artifact_rinfo(self, log, local_host, workspace, host):
        """
        Return release_info.ReleaseInfo()
        """
        info_fpath = self.cra_artifact_info_fpath
        rinfo = release_info.rinfo_read_from_file(log, local_host, workspace, host,
                                                  info_fpath)
        if rinfo is None:
            log.cl_error("failed to read rinfo of artifact [%s] on host [%s]",
                         self.cra_artifact_name, host.sh_hostname)
            return None
        rinfo.rli_extra_relative_fpaths.append(constant.CORAL_ARTIFACT_INFO_FNAME)
        return rinfo

    def _cra_get_artifact_rinfo_local(self, log, local_host):
        """
        Return release_info.ReleaseInfo()
        """
        return self._cra_get_artifact_rinfo(log,
                                            local_host,
                                            None,  # workspace
                                            local_host)  # host

    def cra_artifact_is_complete(self, log, local_host, workspace,
                                 host, ignore_unncessary_files=False):
        """
        Check whether the artifact is complete on host.
        Return 1 if true, return 0 if false.
        Return -1 on error.
        """
        rinfo = self._cra_get_artifact_rinfo(log, local_host, workspace, host)
        if rinfo is None:
            return -1

        ret = rinfo.rli_files_are_complete(log, host,
                                           self.cra_artifact_dir,
                                           ignore_unncessary_files=ignore_unncessary_files)
        return ret

    def cra_artifact_is_complete_local(self, log, local_host,
                                       ignore_unncessary_files=False):
        """
        Check whether the artifact is complete on local host.
        Return 1 if true, return 0 if false.
        Return -1 on error.
        """
        return self.cra_artifact_is_complete(log, local_host,
                                             None,  # workspace
                                             local_host,  # host
                                             ignore_unncessary_files=ignore_unncessary_files)

    def cra_artifact_remove_local(self, log, local_host,
                                  optional_contents=None):
        """
        Remove this artifact on local host.
        """
        return self.cra_artifact_remove(log, local_host,
                                        None,  # workspace
                                        local_host,  # host
                                        optional_contents=optional_contents)

    def cra_artifact_remove(self, log, local_host, workspace, host,
                            optional_contents=None):
        """
        Remove this artifact and clean the parents if necessary.
        """
        # pylint: disable=too-many-locals,too-many-branches
        rinfo = self._cra_get_artifact_rinfo(log, local_host,
                                             workspace,
                                             host)
        if rinfo is None:
            return -1

        if optional_contents is not None:
            for optional_content in optional_contents:
                fpath = self.cra_artifact_dir + "/" + optional_content
                command = "rm -f " + fpath
                retval = host.sh_run(log, command)
                if retval.cr_exit_status:
                    log.cl_error("failed to run command [%s] on host [%s], "
                                 "ret = [%d], stdout = [%s], stderr = [%s]",
                                 command, host.sh_hostname,
                                 retval.cr_exit_status, retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1

        ret = rinfo.rli_release_remove(log, host, self.cra_artifact_dir)
        if ret:
            log.cl_error("failed to remove artifact [%s] on host [%s]",
                         self.cra_artifact_dir, host.sh_hostname)
            return -1

        architecture_dir = os.path.dirname(self.cra_artifact_dir)
        distribution_dir = os.path.dirname(architecture_dir)
        distro_type_dir = os.path.dirname(distribution_dir)
        version_dir = os.path.dirname(distro_type_dir)
        for cleanup_dir in (architecture_dir, distribution_dir,
                            distro_type_dir, version_dir):
            fnames = host.sh_get_dir_fnames(log, cleanup_dir,
                                            all_fnames=True)
            if fnames is None:
                return -1
            if len(fnames) != 2:
                break

            command = "rmdir %s" % (cleanup_dir)
            retval = host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, host.sh_hostname,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def cra_artifact_extract_local(self, log, local_host, directory):
        """
        Extract a local artifact to a dir.
        """
        return self.cra_artifact_extract(log, local_host,
                                         None,  # workspace
                                         local_host,  # host
                                         directory)

    def cra_artifact_extract(self, log, local_host, workspace, host, directory):
        """
        Extract the artifact to a dir.
        """
        rinfo = self._cra_get_artifact_rinfo(log, local_host,
                                             workspace,
                                             host)
        if rinfo is None:
            return None

        ret = rinfo.rli_release_extract(log, host, self.cra_artifact_dir,
                                        directory)
        if ret:
            log.cl_error("failed to extract artifact [%s] to dir [%s] on "
                         "host [%s]",
                         self.cra_artifact_dir, directory,
                         host.sh_hostname)
            return None
        return rinfo

    def _cra_artifact_send(self, log, local_host, host):
        """
        Send this artifact from local host to a remote host.
        """
        group = self.cra_group
        artifact_name = self.cra_artifact_name
        ret, artifact = group.cag_artifact_find(log, host,
                                                artifact_name)
        if ret:
            log.cl_error("failed to find artifact [%s] on host [%s]",
                         artifact_name, host.sh_hostname)
            return -1
        if artifact is not None:
            log.cl_info("artifact [%s] already exists on host [%s]",
                        artifact_name, host.sh_hostname)
            return 0

        rinfo = self._cra_get_artifact_rinfo_local(log, local_host)
        if rinfo is None:
            return -1

        ret = rinfo.rli_files_send(log, local_host, self.cra_artifact_dir,
                                   host, self.cra_artifact_dir)
        if ret:
            log.cl_error("failed to send artifact [%s] from local host [%s] "
                         "to host [%s]",
                         self.cra_artifact_dir,
                         local_host.sh_hostname,
                         host.sh_hostname)
            return -1
        return 0

    def _cra_artifact_send_in_thread(self, log, workspace, local_host, host):
        """
        Send the artifact in a parallel thread
        """
        # pylint: disable=unused-argument
        return self._cra_artifact_send(log, local_host, host)

    def cra_artifact_spread(self, log, local_host, workspace, remote_hosts,
                            parallelism=8):
        """
        Spread this artifact from local host to remote hosts.
        """
        args_array = []
        thread_ids = []
        for remote_host in remote_hosts:
            args = (local_host, remote_host)
            args_array.append(args)
            hostname = remote_host.sh_hostname
            thread_id = "artifact_spread_%s" % hostname
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(workspace,
                                                    "artifact_spread",
                                                    self._cra_artifact_send_in_thread,
                                                    args_array,
                                                    thread_ids=thread_ids)

        ret = parallel_execute.pe_run(log, parallelism=parallelism)
        if ret:
            log.cl_error("failed to spread artifact [%s] to hosts",
                         self.cra_artifact_name)
            return -1
        return 0


def generate_artifact_name(log, group_name, release_name, distro_short,
                           target_cpu, tag=constant.CORAL_TAG_PLAIN):
    """
    Return the artifact path, which is also the path of the key on Aliyun OSS
    """
    distro_type = os_distro.short_distro2type(log, distro_short)
    if distro_type is None:
        return None

    distro_standard = os_distro.short_distro2standard(log, distro_short)
    if distro_standard is None:
        return None

    artifact_name = (group_name + "/" + release_name + "/" + distro_type +
                     "/" + distro_standard + "/" + target_cpu + "/" + tag)
    return artifact_name


def artifact_name_read_from_iso(log, local_host, workspace, host,
                                iso_fpath, group_name,
                                tag=constant.CORAL_TAG_PLAIN):
    """
    Return artifact name by reading the ISO.
    """
    coral_release_info = \
        release_info.rinfo_read_from_coral_iso(log, local_host, workspace,
                                               host, iso_fpath)
    if coral_release_info is None:
        log.cl_error("failed to get version from ISO file [%s] on host [%s]",
                     iso_fpath, host.sh_hostname)
        return None, None

    release_name = coral_release_info.rli_release_name(log)
    if release_name is None:
        log.cl_error("failed to get release name from ISO file [%s] on host [%s]",
                     iso_fpath, local_host.sh_hostname)
        return None, None

    distro_short = coral_release_info.rli_distro_short
    if distro_short is None:
        log.cl_error("failed to get distro from ISO file [%s] on host [%s]",
                     iso_fpath, local_host.sh_hostname)
        return None, None

    target_cpu = coral_release_info.rli_target_cpu
    if target_cpu is None:
        log.cl_error("failed to get arch type from ISO file [%s] on host [%s]",
                     iso_fpath, local_host.sh_hostname)
        return None, None

    artifact_name = generate_artifact_name(log, group_name, release_name,
                                           distro_short,
                                           target_cpu,
                                           tag=tag)
    if artifact_name is None:
        return None, None
    return coral_release_info, artifact_name


def artifact_name_read_from_local_iso(log, local_host, workspace,
                                      iso_fpath, group_name,
                                      tag=constant.CORAL_TAG_PLAIN):
    """
    Return artifact name by reading the ISO on local host.
    """
    return artifact_name_read_from_iso(log, local_host, workspace,
                                       local_host,  # host
                                       iso_fpath, group_name,
                                       tag=tag)
