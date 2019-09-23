from xapi.storage.libs import util


MEBIBYTE = 2**20


class RawUtil(object):
    @staticmethod
    def create(dbg, vol_path, size_mib):
        if not util.is_block_device(vol_path):
            with open(vol_path, 'a') as vdi:
                # truncate is in bytes
                vdi.truncate(size_mib * MEBIBYTE)

    @staticmethod
    def get_vsize(dbg, vol_path):
        return util.get_file_size(vol_path)

    @staticmethod
    def getImgFormat(dbg):
        return 'raw'
