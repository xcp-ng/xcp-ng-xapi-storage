from __future__ import print_function

from xmlrpclib import Transport
from http.client import HTTPConnection
from .backend import PluginBackend

SOCKPATH = '/var/xapi/xcp-rrdd'

PLUGIN_DOMAINS = frozenset(['Local', 'Interdomain'])
READ_FREQS = {'Five_seconds': 5}


class UnixStreamHTTPConnection(HTTPConnection):
    def connect(self):
        from socket import socket, AF_UNIX, SOCK_STREAM

        self.sock = socket(AF_UNIX, SOCK_STREAM)
        self.sock.connect(SOCKPATH)


class UnixStreamTransport(Transport):
    def make_connection(self, host):
        return UnixStreamHTTPConnection(SOCKPATH)


class PluginControlError(Exception):
    def __str__(self):
        return repr(self)


class PluginControl(object):
    __plugin_proxy = None

    @staticmethod
    def __parse_response(response):
        if response['Status'] == 'Success':
            return response['Value']
        else:
            raise PluginControlError(response["ErrorDescription"])

    def __init__(self, plugin_name, plugin_domain, read_freq, time_to_reading):
        from xmlrpclib import ServerProxy

        # The proxy through which we talk to the rrd daemon is the
        # same for all plugins, so we just initialize it once and
        # share it among them
        if PluginControl.__plugin_proxy is None:
            PluginControl.__plugin_proxy = ServerProxy(
                'http://' + SOCKPATH,
                transport=UnixStreamTransport()
            ).Plugin

        if plugin_domain not in PLUGIN_DOMAINS:
            raise ValueError(
                "'plugin_domain' = '{}' not one of '{}'".format(
                    plugin_domain,
                    ', '.join(PLUGIN_DOMAINS)
                )
            )

        if read_freq not in READ_FREQS:
            raise ValueError(
                "'read_freq' = '{}' not one of '{}'".format(
                    read_freq,
                    ', '.join(READ_FREQS)
                )
            )

        self.__time_to_reading = float(time_to_reading)

        if time_to_reading >= READ_FREQS[read_freq] - 1:
            raise ValueError("'time_to_reading' too high")

        self.__mmap = PluginBackend(plugin_name)
        self.__plugin_name = plugin_name
        self.__plugin_domain = plugin_domain
        self.__read_freq = read_freq
        self.__dispatch = getattr(PluginControl.__plugin_proxy, plugin_domain)

    def __del__(self):
        try:
            self.__deregister()
        except AttributeError:
            pass

    def get_path(self):
        return PluginControl.__parse_response(
            PluginControl.__plugin_proxy.get_path(
                {'uid': self.__plugin_name}
            )
        )

    def __register(self):
        return PluginControl.__parse_response(
            self.__dispatch.register({
                'uid': self.__plugin_name,
                'info': self.__read_freq,
                'protocol': 'V2'
            })
        )

    def __deregister(self):
        return PluginControl.__parse_response(
            self.__dispatch.deregister({'uid': self.__plugin_name})
        )

    def full_update(self, datasource_list):
        self.__mmap.full_update(datasource_list)

    def fast_update(self, datasource_list):
        self.__mmap.fast_update(datasource_list)

    def wake_up_before_next_reading(self):
        """Block until next rrdd stats reading

        Args:
            time_to_reading: seconds this function returns
                             before the next reading from rrdd

        The xcp-rrdd daemon reads the files written by registered plugins
        in pre-determined time intervals. This function coordinates this
        timing with the daemon, and wakes up just before the next such
        reading occurs. This way, the plugin can provide freshly collected
        data. Note that it is up to the plugin author to choose a value
        for 'time_to_reading' that is at least as large the time it takes
        for the plugin to collect its data; however, it should also not
        be much larger, since this decreases the freshness of the data.
        """
        from time import sleep
        from socket import error as socket_error

        while True:
            try:
                wait_time = self.__register() - self.__time_to_reading

                if wait_time < 0:
                    wait_time = READ_FREQS[self.__read_freq] - wait_time

                sleep(wait_time)
                return
            except socket_error:
                # Log this thing instead of stderr
                msg = "Failed to contact xcp-rrdd. Sleeping for 5 seconds.."
                print(msg)
                sleep(5.0)
