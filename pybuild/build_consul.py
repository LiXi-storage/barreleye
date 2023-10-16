"""
Library for building Consul
"""
import os
from pybuild import build_common

# The URL of Consul zip for x86_64.
CONSUL_URL_X86_64 = "https://releases.hashicorp.com/consul/1.7.1/consul_1.7.1_linux_amd64.zip"
# The sha1sum of Consul zip for x86_64. Need to update together with
# CONSUL_URL_X86_64
CONSUL_SHA1SUM_X86_64 = "d0cd376d70ed37ce91819934c10d6a9f5e4ac240"
CONSUL_PACKAGE_NAME = "consul"


def download_consul(log, host, iso_cache, target_cpu):
    """
    Download Consul
    Return package_basename. Return None on error.
    """
    log.cl_info("downloading Consul")
    if target_cpu == "x86_64":
        url = CONSUL_URL_X86_64
        expected_sha1sum = CONSUL_SHA1SUM_X86_64
    else:
        log.cl_error("unsupported target CPU [%s]", target_cpu)
        return None

    package_basename = os.path.basename(url)
    fpath = iso_cache + "/" + package_basename
    ret = host.sh_download_file(log, url, fpath, expected_sha1sum)
    if ret:
        log.cl_error("failed to download Consul zip file")
        return None

    return package_basename


class CoralConsulPackageBuild(build_common.CoralPackageBuild):
    """
    How to build Consul
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        super().__init__(CONSUL_PACKAGE_NAME)

    def cpb_build(self, log, workspace, local_host, source_dir, target_cpu,
                  type_cache, iso_cache, packages_dir, extra_iso_fnames,
                  extra_package_fnames, extra_rpm_names, option_dict):
        """
        Build the package
        """
        # pylint: disable=unused-argument,no-self-use
        package_basename = download_consul(log, local_host, iso_cache,
                                           target_cpu)
        if package_basename is None:
            log.cl_error("failed to download Consul")
            return -1
        extra_iso_fnames.append(package_basename)
        return 0


build_common.coral_package_register(CoralConsulPackageBuild())
