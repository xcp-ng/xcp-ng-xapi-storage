import sys
from xapi.storage.libs import util
from xapi.storage.libs.libcow.cowutil import COWUtil

MEBIBYTE = 2**20

class ReflinkUtil(COWUtil):
    @staticmethod
    def get_max_chain_height():
        return sys.maxsize

    @staticmethod
    def is_empty(dbg, vol_path):
        return False # FIXME but do we care

    @staticmethod
    def create(dbg, vol_path, size_mib):
        if not util.is_block_device(vol_path):
            with open(vol_path, 'a') as vdi:
                # truncate is in bytes
                vdi.truncate(size_mib * MEBIBYTE)

    @staticmethod
    def snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        # FIXME should make it immutable
        cmd = ['cp', '--reflink', parent_cow_path, new_cow_path]
        return call(dbg, cmd)

    @staticmethod
    def online_snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        return ReflinkUtil.snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link)
    @staticmethod
    def offline_snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        return ReflinkUtil.snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link)

    @staticmethod
    def coalesce(dbg, vol_path, parent_path):
        pass

    @staticmethod
    def get_vsize(dbg, vol_path):
        return util.get_file_size(vol_path)

    @staticmethod
    def getImgFormat(dbg):
        return 'reflink'
