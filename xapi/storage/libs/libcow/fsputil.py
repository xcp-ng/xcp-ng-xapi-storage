import os

from xapi.storage.libs.libcow.cowutil import COWUtil


class FSPUtil(COWUtil):
    @staticmethod
    def get_vsize(dbg, vol_path):
        statvfs = os.statvfs(vol_path)
        return statvfs.f_blocks * statvfs.f_frsize

    @staticmethod
    def getImgFormat(dbg):
        return '9pfs'
