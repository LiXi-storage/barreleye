"""
Library for access Influxdb through HTTP API
"""
import http
import json
import traceback
import requests
from pybarrele import barrele_constant


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

    def bic_query_serie(self, log, query, quiet=False):
        """
        Query on Influxdb, return a serie dict like:
        {
            "columns": [
                "key"
            ],
            "values": [
                [
                    "aggregation.cpu-average.cpu.idle,fqdn=autotest-el7-vm311"
                ],
        }
        """
        response = self.bic_query(log, query, epoch="s")
        if quiet:
            log_func = log.cl_debug
        else:
            log_func = log.cl_error
        if response is None:
            log_func("failed to with query Influxdb with query [%s]",
                     query)
            return None

        if response.status_code != http.HTTPStatus.OK:
            log_func("got InfluxDB status [%d] with query [%s]",
                     response.status_code, query)
            return None

        data = response.json()
        json_string = json.dumps(data, indent=4, separators=(',', ': '))
        if barrele_constant.INFLUX_RESULTS not in data:
            log_func("got wrong InfluxDB data [%s], no [%s]",
                     json_string, barrele_constant.INFLUX_RESULTS)
            return None
        results = data[barrele_constant.INFLUX_RESULTS]

        if len(results) != 1:
            log_func("got wrong InfluxDB data [%s], [%s] is not a "
                     "array with only one element",
                     json_string, barrele_constant.INFLUX_RESULTS)
            return None
        result = results[0]

        if barrele_constant.INFLUX_SERIES not in result:
            log_func("got wrong InfluxDB data [%s], no [%s] in result",
                     json_string, barrele_constant.INFLUX_SERIES)
            return None

        series = result[barrele_constant.INFLUX_SERIES]
        if len(series) != 1:
            log_func("got wrong InfluxDB data [%s], [%s] is not a "
                     "array with only one element",
                     json_string, barrele_constant.INFLUX_SERIES)
            return None
        serie = series[0]

        return serie


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
