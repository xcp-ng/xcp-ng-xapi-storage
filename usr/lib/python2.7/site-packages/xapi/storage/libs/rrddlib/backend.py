"""RRDD Plugin Protocol V2 Backend module
"""

from struct import calcsize
from ctypes import c_char, c_int32, c_int64, c_double

HEADER = 'DATASOURCES'
PLUGIN_PATH = '/dev/shm/metrics/'

FLOAT = 'float'
INT64 = 'int64'

# All of the format strings are Big-Endian
FMT_BE_INT32 = '>i'
FMT_BE_INT64 = '>q'
FMT_BE_DOUBLE = '>d'

# pylint: disable=protected-access
C_INT32_SIZE = calcsize(c_int32._type_)
C_INT64_SIZE = calcsize(c_int64._type_)
C_DOUBLE_SIZE = calcsize(c_double._type_)
# pylint: enable=protected-access

C_INT32_TYPE = c_char * C_INT32_SIZE
C_INT64_TYPE = c_char * C_INT64_SIZE
C_DOUBLE_TYPE = c_char * C_DOUBLE_SIZE

DATA_CRC32_START = len(HEADER) + 3 * C_INT32_SIZE


class PluginBackend(object):
    def __init__(self, plugin_name):
        from os import open as os_open, O_CREAT, O_TRUNC, O_RDWR

        self.__fd = os_open(
            PLUGIN_PATH + plugin_name,
            O_CREAT | O_TRUNC | O_RDWR
        )

        self.__plugin_name = plugin_name

        # All keys in this dict correspond to a specific
        # offset in the mmapped file and all writes to
        # them propagate to the file
        self.__data_map = None

        # mmapped file offset, up to which
        # the data crc32 is calculated
        self.__data_crc32_end = None

        # JSON formatted metadata string
        self.__metadata_str = None

        # object returned by mmap()
        self.__buffer = None

        # list containing the format strings
        # of the respective datasources
        self.__format_list = None

    def __del__(self):
        from os import close as os_close, unlink

        # self.__buffer.close()

        try:
            os_close(self.__fd)
        except OSError:
            pass

        try:
            unlink(PLUGIN_PATH + self.__plugin_name)
        except OSError:
            pass

    def __create_metadata_string(self, datasource_dict):
        from json import JSONEncoder

        metadata_dict = {'datasources': {}}
        for dsource in datasource_dict.values():
            metadata_dict['datasources'].update(dsource.to_dict())

        self.__metadata_str = JSONEncoder(
            separators=(',', ':')
        ).encode(metadata_dict)

    def __map_data_to_file(self, datasource_dict):
        """Maps self.__data_map's keys to self.__buffer.

        Maps offsets of the mmapped file (self.__buffer) to
        keys in 'self.__data_map'. All writes to the keys
        propagate to their respective offsets in the file

        Args:
            datasource_dict: {} of Datasource objects
        """
        self.__data_map = {}

        sz_type = c_char * len(HEADER)
        self.__data_map['header'] = sz_type.from_buffer(self.__buffer)
        offset = len(HEADER)

        self.__data_map['data_crc32'] = C_INT32_TYPE.from_buffer(
            self.__buffer,
            offset
        )
        offset += C_INT32_SIZE

        self.__data_map['metadata_crc32'] = C_INT32_TYPE.from_buffer(
            self.__buffer,
            offset
        )
        offset += C_INT32_SIZE

        self.__data_map['datasources_no'] = C_INT32_TYPE.from_buffer(
            self.__buffer,
            offset
        )
        offset += C_INT32_SIZE

        self.__data_map['timestamp'] = C_INT64_TYPE.from_buffer(
            self.__buffer,
            offset
        )
        offset += C_INT64_SIZE

        self.__data_map['values'] = []
        for dsource in datasource_dict.values():
            value_type = dsource.get_property('value_type')

            if value_type == FLOAT:
                c_type = C_DOUBLE_TYPE
                c_type_size = C_DOUBLE_SIZE

            elif value_type == INT64:
                c_type = C_INT64_TYPE
                c_type_size = C_INT64_SIZE

            self.__data_map['values'].append(
                c_type.from_buffer(self.__buffer, offset)
            )
            offset += c_type_size

        self.__data_map['metadata_len'] = C_INT32_TYPE.from_buffer(
            self.__buffer,
            offset
        )
        offset += C_INT32_SIZE

        sz_type = c_char * len(self.__metadata_str)
        self.__data_map['metadata'] = sz_type.from_buffer(
            self.__buffer,
            offset
        )

    def full_update(self, datasource_dict):
        """Writes datasources to mmapped file.

        This function MUST be called the first time the plugin submits
        data. After that, as long as the Datasources remain the same
        (everything apart from their reported values), the plugin
        should call fast_update().

        Args:
            datasource_dict: {} of Datasource objects with data
                collected from the plugin to be written to the
                mmapped file
        """
        from zlib import crc32
        from struct import pack

        self.__create_metadata_string(datasource_dict)
        self.__create_format_list(datasource_dict)
        self.__calc_data_crc32_end(datasource_dict)

        self.__reset_file()
        self.__map_data_to_file(datasource_dict)

        self.__data_map['header'].raw = HEADER
        self.__data_map['datasources_no'].raw = pack(
            FMT_BE_INT32,
            len(datasource_dict)
        )

        self.fast_update(datasource_dict)

        self.__data_map['metadata_len'].raw = pack(
            FMT_BE_INT32,
            len(self.__metadata_str)
        )

        self.__data_map['metadata'].raw = self.__metadata_str
        self.__data_map['metadata_crc32'].raw = pack(
            FMT_BE_INT32,
            crc32(self.__metadata_str)
        )

    def fast_update(self, datasource_dict):
        """Writes datasources to mmapped file (fast).

        What it says on the tin. ONLY USE if the difference between
        this and the previous submission is the datasources' values.

        Args:
            datasource_dict: {} of Datasource objects with data
                collected from the plugin to be written to the
                mmapped file
        """
        from zlib import crc32
        from time import time
        from struct import pack

        self.__data_map['timestamp'].raw = pack(FMT_BE_INT64, int(time()))

        for i, dsource in enumerate(datasource_dict.values()):
            self.__data_map['values'][i].raw = pack(
                self.__format_list[i],
                dsource.get_property('value')
            )

        self.__data_map['data_crc32'].raw = pack(
            FMT_BE_INT32,
            crc32(self.__buffer[DATA_CRC32_START:self.__data_crc32_end])
        )

    def __create_format_list(self, datasource_dict):
        self.__format_list = []

        for dsource in datasource_dict.values():
            value_type = dsource.get_property('value_type')

            if value_type == FLOAT:
                self.__format_list.append(FMT_BE_DOUBLE)
            elif value_type == INT64:
                self.__format_list.append(FMT_BE_INT64)

    def __calc_data_crc32_end(self, datasource_dict):
        dsources_size = 0

        for dsource in datasource_dict.values():
            value_type = dsource.get_property('value_type')

            if value_type == FLOAT:
                dsources_size += C_DOUBLE_SIZE
            elif value_type == INT64:
                dsources_size += C_INT64_SIZE

        self.__data_crc32_end = DATA_CRC32_START + C_INT64_SIZE + dsources_size

    def __reset_file(self):
        from os import ftruncate
        from mmap import mmap, PAGESIZE, MAP_SHARED, ACCESS_WRITE

        plugin_data_size = (self.__data_crc32_end +
                            C_INT32_SIZE +
                            len(self.__metadata_str))

        try:
            self.__buffer.close()
        except AttributeError:
            pass

        # in multiples of PAGESIZE
        file_size = ((plugin_data_size - 1) // PAGESIZE + 1) * PAGESIZE

        ftruncate(self.__fd, file_size)
        self.__buffer = mmap(
            self.__fd,
            file_size,
            flags=MAP_SHARED,
            access=ACCESS_WRITE
        )
