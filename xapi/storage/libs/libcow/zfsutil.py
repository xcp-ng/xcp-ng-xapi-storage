import os
import os.path
from xapi.storage.libs.util import call
from xapi.storage.libs.libcow.cowutil import COWUtil

ZFS_UTIL_BIN = 'zfs'
ZPOOL_UTIL_BIN = 'zpool'

class ZFSUtil(COWUtil):
    MOUNT_ROOT = '/var/run/sr-mount/'

    @staticmethod
    def getImgFormat(dbg):
        return 'raw'

    @staticmethod
    def is_empty(dbg, vol_path):
        raise NotImplementedError()

    @staticmethod
    def build_snap_path(sr, child_id, vol_id):
        path = "sr-" + os.path.basename(sr) + '/' + str(child_id) + '@' + str(vol_id) # FIXME
        return path

    @staticmethod
    def create(dbg, vol_path, size_mib):
        cmd = [
            ZFS_UTIL_BIN, 'create',
            '-V', str(size_mib),
            vol_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def create_pool(dbg, sr_meta, mountpoint, mode, devs):
        cmd = [
            ZPOOL_UTIL_BIN, 'create',
            '-f', ZFSUtil.sr_name(sr_meta), '-m', mountpoint
        ]
        if not mode == None:
            cmd.append(mode)
        cmd.extend(devs)
        return call(dbg, cmd);

    @staticmethod
    def get_sr_size(dbg, sr_path):
        # size is returned in bytes
        cmd = [
            ZPOOL_UTIL_BIN, 'get',
            '-o', 'value', '-Hp', 'size',
            sr_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def get_sr_free_space(dbg, sr_path):
        # size is returned in bytes
        cmd = [
            ZPOOL_UTIL_BIN, 'get',
            '-o', 'value', '-Hp', 'free',
            sr_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def get_vsize(dbg, vol_path):
        # size is returned in bytes
        cmd = [
            ZFS_UTIL_BIN, 'get',
            '-o', 'value', '-Hp', 'volsize',
            vol_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def resize(dbg, vol_path, new_size):
        volsize = 'volsize=' + str(new_size)
        cmd = [
            ZFS_UTIL_BIN, 'set',
            volsize, vol_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def snapshot(dbg, snap_name, parent_path, force_parent_link):
        path = parent_path + '@' + snap_name
        cmd = [
            ZFS_UTIL_BIN, 'snapshot',
            path
        ]
        return call(dbg, cmd)

    @staticmethod
    def clone(dbg, snap_path, clone_path):
        cmd = [
            ZFS_UTIL_BIN, 'clone',
            snap_path, clone_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def setcompression(dbg, sr_meta):
        """Set compression on.

        Args:
            name: (str) name of the pool
        """
        cmd = [
            ZFS_UTIL_BIN, 'set', 'compression=on', ZFSUtil.sr_name(sr_meta)
        ]
        call(dbg, cmd)

    @staticmethod
    def promote(dbg, clone_path):
        """Promote clone volume. The clone parent-child dependency
           relationship is reversed. This enables to destroy the dataset that
           the clone was created from. This method is only used by the zfs driver.

        Args:
            clone_path: (str) Absolute path to a clone volume
        """
        cmd = [
            ZFS_UTIL_BIN, 'promote',
            clone_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def destroy(dbg, path):
        cmd = [
            ZFS_UTIL_BIN, 'destroy',
            path
        ]
        return call(dbg, cmd)

    @staticmethod
    def destroy_pool(dbg, sr_meta):
        cmd = [
            ZPOOL_UTIL_BIN, 'destroy',
            ZFSUtil.sr_name(sr_meta)
        ]
        return call(dbg, cmd)

    @staticmethod
    def online_snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        return ZFSUtil.snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link)

    @staticmethod
    def offline_snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link):
        return ZFSUtil.snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link)

    @staticmethod
    def sr_name(sr_meta):
        return "sr-{}".format(sr_meta['unique_id'])
