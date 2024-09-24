"""
Library for building coral_tools
"""
from pybuild import build_common

REAF_PLUGIN_NAME = "reaf"


class CoralReafPlugin(build_common.CoralPluginType):
    """
    Tools Plugin
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        super().__init__(REAF_PLUGIN_NAME,
                         need_lustre_for_build=True,
                         is_devel=False)

    def cpt_build(self, log, workspace, local_host, source_dir, target_cpu,
                  type_cache, iso_cache, packages_dir, extra_iso_fnames,
                  extra_package_fnames, extra_package_names, option_dict):
        """
        Build the plugin
        """
        # pylint: disable=unused-argument,no-self-use
        return 0

build_common.coral_plugin_register(CoralReafPlugin())
