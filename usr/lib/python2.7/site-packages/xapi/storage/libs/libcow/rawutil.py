import os

MEBIBYTE = 2**20


class RawUtil(object):

    @staticmethod
    def create(dbg, vol_path, size_mib):
        with open(vol_path, 'a') as vdi:
            # truncate is in bytes
            vdi.truncate(size_mib * MEBIBYTE)

    @staticmethod
    def get_vsize(dbg, vol_path):
        return os.stat(vol_path).st_size

    @staticmethod
    def getImgFormat(dbg):
        return 'raw'
