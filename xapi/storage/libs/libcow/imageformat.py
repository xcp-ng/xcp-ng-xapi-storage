"""
Map image formats to datapath URIs and tools
"""
from .qcow2util import QCOW2Util
from .rawutil import RawUtil
from .vhdutil import VHDUtil


class ImageFormat(object):
    """
    Image format details and lookup
    """
    IMAGE_RAW = 0
    IMAGE_VHD = 1
    IMAGE_QCOW2 = 2

    _formats = None

    def __init__(self, uri_prefix, image_utils):
        self._uri_prefix = uri_prefix
        self._image_utils = image_utils

    @property
    def uri_prefix(self):
        return self._uri_prefix

    @property
    def image_utils(self):
        return self._image_utils

    def __str__(self):
        return 'Image format prefix {}, utils {}'.format(
            self.uri_prefix, self.image_utils)

    @classmethod
    def initialize_formats(cls):
        return {
            cls.IMAGE_RAW: ImageFormat('tapdisk://', RawUtil),
            cls.IMAGE_VHD: ImageFormat('tapdisk://', VHDUtil),
            cls.IMAGE_QCOW2: ImageFormat('qdisk://', QCOW2Util)
        }

    @classmethod
    def get_format(cls, key):
        if not cls._formats:
            cls._formats = cls.initialize_formats()

        return cls._formats[key]
