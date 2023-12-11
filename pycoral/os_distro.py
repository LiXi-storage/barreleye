"""
Library that handles OS distribution.
"""

# OS distribution RHEL6/CentOS6
DISTRO_RHEL6 = "rhel6"
# OS distribution RHEL7/CentOS7
DISTRO_RHEL7 = "rhel7"
# OS distribution RHEL8/CentOS8
DISTRO_RHEL8 = "rhel8"
# OS distribution ubuntu2004
DISTRO_UBUNTU2004 = "ubuntu2004"
# OS distribution ubuntu2204
DISTRO_UBUNTU2204 = "ubuntu2204"

DISTRO_SHORT_RHEL6 = "el6"
DISTRO_SHORT_RHEL7 = "el7"
DISTRO_SHORT_RHEL8 = "el8"
DISTRO_SHORT_UBUNTU2004 = "ubuntu2004"
DISTRO_SHORT_UBUNTU2204 = "ubuntu2204"

DISTRO_TYPE_UBUNTU = "ubuntu"
DISTRO_TYPE_RHEL = "rhel"

def distro2short(log, distro):
    """
    Return short distro from distro
    """
    if distro == DISTRO_RHEL6:
        return DISTRO_SHORT_RHEL6
    if distro == DISTRO_RHEL7:
        return DISTRO_SHORT_RHEL7
    if distro == DISTRO_RHEL8:
        return DISTRO_SHORT_RHEL8
    if distro == DISTRO_UBUNTU2004:
        return DISTRO_SHORT_UBUNTU2004
    if distro == DISTRO_UBUNTU2204:
        return DISTRO_SHORT_UBUNTU2204
    log.cl_error("unsupported distro [%s]", distro)
    return None


def short_distro2standard(log, short_distro):
    """
    Return distro from short distro
    """
    if short_distro == DISTRO_SHORT_RHEL6:
        return DISTRO_RHEL6
    if short_distro == DISTRO_SHORT_RHEL7:
        return DISTRO_RHEL7
    if short_distro == DISTRO_SHORT_RHEL8:
        return DISTRO_RHEL8
    if short_distro == DISTRO_SHORT_UBUNTU2004:
        return DISTRO_UBUNTU2004
    if short_distro == DISTRO_SHORT_UBUNTU2204:
        return DISTRO_UBUNTU2204
    log.cl_error("unsupported short_distro [%s]", short_distro)
    return None


def short_distro2type(log, short_distro):
    """
    Return type from short distro
    """
    if short_distro in [DISTRO_SHORT_RHEL6, DISTRO_SHORT_RHEL7,
                        DISTRO_SHORT_RHEL8]:
        return DISTRO_TYPE_RHEL
    if short_distro in [DISTRO_UBUNTU2004, DISTRO_UBUNTU2204]:
        return DISTRO_TYPE_UBUNTU

    log.cl_error("unsupported short_distro [%s]", short_distro)
    return None
