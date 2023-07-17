"""
Library for access Influxdb through HTTP API
"""
import traceback
import requests


class BarreleInfluxdbClient():
    """
    This object holds information necessary to connect to InfluxDB. Requests
    can be made to InfluxDB directly through the client.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, hostname, database):
        self.bic_hostname = hostname
        self.bic_database = database

        self.bic_baseurl = "http://%s:8086" % (hostname)
        self.bic_queryurl = self.bic_baseurl + "/query"
        self.bic_headers = {
            'Content-type': 'application/json',
            'Accept': 'text/plain'
        }
        self.bic_session = requests.Session()

    def bic_query(self, log, query, epoch=None):
        """
        Send a query to InfluxDB.
        :param epoch: response timestamps to be in epoch format either 'h',
            'm', 's', 'ms', 'u', or 'ns', defaults to `None` which is
            RFC3339 UTC format with nanosecond precision
        :type epoch: str
        """
        # pylint: disable=bare-except
        params = {}
        params['q'] = query
        params['db'] = self.bic_database

        if epoch is not None:
            params['epoch'] = epoch

        log.cl_debug("querying [%s] to [%s]", query, self.bic_queryurl)
        try:
            response = self.bic_session.request(method='GET',
                                                url=self.bic_queryurl,
                                                params=params,
                                                headers=self.bic_headers)
        except:
            log.cl_error("got exception with query [%s]: %s", query,
                         traceback.format_exc())
            return None

        return response


class InfluxdbContinuousQuery():
    """
    Information about a countinous query
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, measurement, groups, where=""):
        # Name of the measurement
        self.icq_measurement = measurement
        # List of group names
        self.icq_groups = groups
        # where query
        self.icq_where = where
