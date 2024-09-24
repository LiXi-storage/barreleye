"""
Commands to manage the build cache of Coral releases.
"""
import os
from pycoral import parallel
from pycoral import clog
from pycoral import cmd_general
from pycoral import ssh_host
from pycoral import coral_artifact
from pycoral import constant
from pybuild import build_common


CACHE_FIELD_ARTIFACT = "Artifact"
CACHE_FIELD_COMPLETE = "Complete"
CACHE_STATUE_COMPLETE = "complete"
CACHE_STATUE_INCOMPLETE = "incomplete"


class ArtifactStatusCache():
    """
    This object saves temporary status of an artifact.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, local_host, artifact):
        self.asc_local_host = local_host
        # CoralArtifact
        self.asc_artifact = artifact
        # Failure happened
        self.asc_failed = False
        # Whether the artifact is complete
        self.asc_complete = None

    def asc_field_result(self, log, field_name):
        """
        Return (0, result) to print for a field
        """
        # pylint: disable=too-many-branches
        ret = 0
        artifact_name = self.asc_artifact.cra_artifact_name
        if field_name == CACHE_FIELD_ARTIFACT:
            result = artifact_name
        elif field_name == CACHE_FIELD_COMPLETE:
            if self.asc_complete is None:
                log.cl_error("complete status of artifact [%s] is not inited",
                             artifact_name)
                ret = -1
                result = clog.ERROR_MSG
            elif self.asc_complete < 0:
                result = clog.ERROR_MSG
                ret = -1
            elif self.asc_complete:
                result = clog.colorful_message(clog.COLOR_GREEN,
                                               CACHE_STATUE_COMPLETE)
            else:
                result = clog.colorful_message(clog.COLOR_RED,
                                               CACHE_STATUE_INCOMPLETE)
        else:
            log.cl_error("unknown field [%s] of artifact", field_name)
            result = clog.ERROR_MSG
            ret = -1

        if self.asc_failed:
            ret = -1
        return ret, result

    def asc_init_fields(self, log, field_names):
        """
        Init this status cache according to required field names.
        """
        # pylint: disable=no-self-use
        local_host = self.asc_local_host
        artifact = self.asc_artifact
        artifact_name = artifact.cra_artifact_name
        for field in field_names:
            if field == CACHE_FIELD_ARTIFACT:
                continue
            if field == CACHE_FIELD_COMPLETE:
                self.asc_complete = \
                    artifact.cra_artifact_is_complete_local(log, local_host)
                if self.asc_complete < 0:
                    log.cl_error("failed to get the complete status of artifact [%s]",
                                 artifact_name)
                    self.asc_failed = True
                continue
            log.cl_error("unknown field [%s]", field)
            return -1
        return 0

    def asc_can_skip_init_fields(self, field_names):
        """
        Whether asc_init_fields can be skipped
        """
        # pylint: disable=no-self-use
        fields = field_names[:]
        fields.remove(CACHE_FIELD_ARTIFACT)
        if len(fields) == 0:
            return True
        return False


def artifact_status_init(log, workspace, artifact_status, field_names):
    """
    Init status of a ArtifactStatusCache
    """
    # pylint: disable=unused-argument
    return artifact_status.asc_init_fields(log, field_names)


def artifact_status_field(log, artifact_status, field_name):
    """
    Return (0, result) for a field of ArtifactStatusCache
    """
    return artifact_status.asc_field_result(log, field_name)


def print_artifacts(log, workspace, artifacts, status=False,
                    print_table=True, field_string=None):
    """
    Print table of CoralArtifact
    """
    # pylint: disable=too-many-locals
    local_host = ssh_host.get_local_host(ssh=False)
    quick_fields = [CACHE_FIELD_ARTIFACT]
    slow_fields = [CACHE_FIELD_COMPLETE]
    none_table_fields = []
    table_fields = quick_fields + slow_fields
    all_fields = table_fields + none_table_fields

    if isinstance(field_string, bool):
        cmd_general.print_all_fields(log, all_fields)
        return 0

    field_names = cmd_general.parse_field_string(log, field_string,
                                                 quick_fields, table_fields,
                                                 all_fields,
                                                 print_status=status,
                                                 print_table=print_table)
    if field_names is None:
        log.cl_error("invalid field string [%s] for host",
                     field_string)
        return -1

    artifact_status_list = []
    for artifact in artifacts:
        artifact_status = ArtifactStatusCache(local_host, artifact)
        artifact_status_list.append(artifact_status)

    if (len(artifact_status_list) > 0 and
            not artifact_status_list[0].asc_can_skip_init_fields(field_names)):
        args_array = []
        thread_ids = []
        thread_index = 0
        for artifact_status in artifact_status_list:
            args = (artifact_status, field_names)
            args_array.append(args)
            thread_id = "artifact_status_%s" % thread_index
            thread_ids.append(thread_id)
            thread_index += 1

        parallel_execute = parallel.ParallelExecute(workspace,
                                                    "artifact_status",
                                                    artifact_status_init,
                                                    args_array,
                                                    thread_ids=thread_ids)
        ret = parallel_execute.pe_run(log, parallelism=10)
        if ret:
            log.cl_error("failed to init fields %s for host",
                         field_names)
            return -1

    rc = cmd_general.print_list(log, artifact_status_list, quick_fields,
                                slow_fields, none_table_fields,
                                artifact_status_field,
                                print_table=print_table,
                                print_status=status,
                                field_string=field_string)
    return rc


def get_artifact_group_from_name(log, artifact_name):
    """
    Return CoralArtifactGroup.
    """
    field_dict = coral_artifact.parse_artifact_name(log, artifact_name)
    if field_dict is None:
        log.cl_error("invalid artifact name [%s]", artifact_name)
        return None
    group_name = field_dict[coral_artifact.FIELD_GROUP]
    if group_name not in constant.CORAL_BUILD_CACHE_TYPES:
        log.cl_error("invalid group name [%s] in artifact name [%s]",
                     group_name, artifact_name)
        return None
    return coral_artifact.CoralArtifactGroup(constant.CORAL_BUILD_ARTIFACTS,
                                             group_name)


def remove_cache_artifact(log, artifact_name):
    """
    Remvoe a cache artifact.
    """
    group = get_artifact_group_from_name(log, artifact_name)
    if group is None:
        return -1
    local_host = ssh_host.get_local_host(ssh=False)
    return group.cag_artifact_remove_local(log, local_host,
                                           artifact_name)


def add_cache_artifact(log, workspace, local_host, group_name, iso_fpath):
    """
    add a cache artifact. Return the artifact name added.
    """
    if group_name not in constant.CORAL_BUILD_CACHE_TYPES:
        log.cl_error("invalid group name [%s]",
                     group_name)
        return None
    group = coral_artifact.CoralArtifactGroup(constant.CORAL_BUILD_ARTIFACTS,
                                              group_name)
    artifact_name = group.cag_artifact_add_local(log, local_host, workspace, iso_fpath,
                                                 tag=constant.CORAL_TAG_PLAIN)
    return artifact_name


def get_cache_artifacts(log):
    """
    Return all artifacts.
    """
    local_host = ssh_host.get_local_host(ssh=False)
    all_artifacts = []
    for cache_type in constant.CORAL_BUILD_CACHE_TYPES:
        group = coral_artifact.CoralArtifactGroup(constant.CORAL_BUILD_ARTIFACTS,
                                                  cache_type)
        artifacts = group.cag_get_artifacts(log, local_host)
        if artifacts is None:
            log.cl_error("failed to get artifacts of group [%s]",
                         group.cag_group_name)
            return None
        all_artifacts += artifacts
    return all_artifacts


def extract_cache_artifact(log, local_host, artifact_name, directory):
    """
    Extra a cached artifact to a directory.
    """
    group = get_artifact_group_from_name(log, artifact_name)
    if group is None:
        return -1
    return group.cag_artifact_extract_local(log, local_host, artifact_name,
                                            directory)


def spread_cache_artifact(log, workspace, artifact_name, hosts):
    """
    spread a cache artifact.
    """
    group = get_artifact_group_from_name(log, artifact_name)
    if group is None:
        return -1
    local_host = ssh_host.get_local_host(ssh=False)
    return group.cag_artifact_spread(log, local_host, workspace, artifact_name, hosts)


def cache_artifact_remove_all(log, workspace, artifact_name, hosts):
    """
    Remove a cache artifact on hosts.
    """
    group = get_artifact_group_from_name(log, artifact_name)
    if group is None:
        return -1
    local_host = ssh_host.get_local_host(ssh=False)
    return group.cag_artifact_remove_all(log, local_host, workspace, artifact_name, hosts)


class CoralCacheCommand():
    """
    Commands to manage the cache of Coral releases that accelerates build.
    """
    # pylint: disable=too-few-public-methods
    def _init(self, log_to_file):
        # pylint: disable=attribute-defined-outside-init
        self._clc_log_to_file = log_to_file

    def ls(self, status=False):
        """
        Print the list of cached Coral releases.
        :param status: Print the status of the artifacts, default: False.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        cmd_general.check_argument_bool(log, "status", status)
        artifacts = get_cache_artifacts(log)
        if artifacts is None:
            log.cl_error("failed to get artifacts")
        source_dir = os.getcwd()
        rc = print_artifacts(log, source_dir, artifacts,
                             status=status)
        cmd_general.cmd_exit(log, rc)

    def remove(self, artifact):
        """
        Remove a build artifact of Coral.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        artifact = cmd_general.check_argument_str(log, "artifact", artifact)
        rc = remove_cache_artifact(log, artifact)
        cmd_general.cmd_exit(log, rc)

    def add(self, group, iso):
        """
        Remove an build artifact of Coral.
        :param group: The group name to add this artifact to.
        :param iso: The Coral ISO path.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        local_host = ssh_host.get_local_host(ssh=False)
        group = cmd_general.check_argument_str(log, "group", group)
        iso = cmd_general.check_argument_fpath(log, local_host, iso)
        source_dir = os.getcwd()
        artifact_name = add_cache_artifact(log, source_dir, local_host, group, iso)
        if artifact_name is None:
            rc = -1
        else:
            log.cl_info("artifact [%s] added to cache", artifact_name)
            rc = 0
        cmd_general.cmd_exit(log, rc)

    def extract(self, artifact, directory):
        """
        Extract an artifact to a directory.
        :param artifact: The artifact name to extract.
        :param dir: The target dir to copy this artifact to.
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        artifact = cmd_general.check_argument_str(log, "artifact", artifact)
        local_host = ssh_host.get_local_host(ssh=False)
        directory = cmd_general.check_argument_fpath(log, local_host, directory)
        rc = extract_cache_artifact(log, local_host, artifact, directory)
        cmd_general.cmd_exit(log, rc)

    def spread(self, artifact, host, ssh_key=None):
        """
        Spread the artifact to remote host(s).
        :param artifact: The artifact naem to spread.
        :param host: The name of host(s) to spread to, seperated by comma.
        :param ssh_key: SSH key to connet to host(s).
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        host = cmd_general.check_argument_list_str(log, "host", host)
        hostnames = cmd_general.parse_list_string(log, host)
        if hostnames is None:
            log.cl_error("invalid host option[%s]",
                         host)
            cmd_general.cmd_exit(log, -1)
        hosts = []
        for hostname in hostnames:
            hosts.append(ssh_host.SSHHost(hostname, identity_file=ssh_key))
        source_dir = os.getcwd()
        rc = spread_cache_artifact(log, source_dir, artifact, hosts)
        cmd_general.cmd_exit(log, rc)

    def remove_all(self, artifact, host, ssh_key=None):
        """
        Remove the artifact on multiple host(s).
        :param artifact: The artifact naem to spread.
        :param host: The name of host(s) to remove the artifact.
        :param ssh_key: SSH key to connet to host(s).
        """
        # pylint: disable=no-self-use
        log = clog.get_log(console_format=clog.FMT_NORMAL, overwrite=True)
        host = cmd_general.check_argument_list_str(log, "host", host)
        hostnames = cmd_general.parse_list_string(log, host)
        if hostnames is None:
            log.cl_error("invalid host option[%s]",
                         host)
            cmd_general.cmd_exit(log, -1)
        hosts = []
        for hostname in hostnames:
            hosts.append(ssh_host.SSHHost(hostname, identity_file=ssh_key))
        source_dir = os.getcwd()
        rc = cache_artifact_remove_all(log, source_dir, artifact, hosts)
        cmd_general.cmd_exit(log, rc)

build_common.coral_command_register("cache", CoralCacheCommand())
