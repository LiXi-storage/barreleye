"""
Library for Yaml
"""
import traceback
import socket
import yaml


class YamlDumper(yaml.Dumper):
    # pylint: disable=too-many-ancestors
    """
    Provide proper indent
    """
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def write_yaml_file(log, prefix, config, config_fpath):
    """
    Write YAML file
    """
    config_string = prefix
    config_string += yaml.dump(config, Dumper=YamlDumper,
                               default_flow_style=False)
    try:
        with open(config_fpath, 'w', encoding='utf-8') as yaml_file:
            yaml_file.write(config_string)
    except:
        log.cl_error("failed to flush config to file [%s] on host [%s]: %s",
                     config_fpath, socket.gethostname(),
                     traceback.format_exc())
        return -1
    return 0


def read_yaml_file(log, fpath):
    """
    Read YAML file
    """
    try:
        with open(fpath, 'r', encoding='utf-8') as yaml_file:
            file_data = yaml_file.read()
    except:
        log.cl_error("failed to read file [%s] on host [%s]: %s",
                     fpath, socket.gethostname(),
                     traceback.format_exc())
        return None
    try:
        yaml_content = yaml.load(file_data, Loader=yaml.FullLoader)
    except:
        log.cl_error("failed to load file [%s] as YAML format on "
                     "host [%s]: %s",
                     fpath, socket.gethostname(),
                     traceback.format_exc())
        return None
    return yaml_content
