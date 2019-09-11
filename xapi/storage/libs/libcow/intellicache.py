import os
import errno

from xapi.storage import log
from xapi.storage.libs import tapdisk
from xapi.storage.libs.refcounter import RefCounter

from .cowutil import COWUtil


class IntelliCache(object):

    """The methods of this class perform the necessary operations to
    set up and tear down IntelliCache. Each method is intented to be
    called only by the same name method in the COWDatapath class,
    and only if the functionality is requested.
    """

    ROOT_PATH = '/var/run/sr-mount/<put_local_sr_here>/intellicache'

    @staticmethod
    def attach(dbg, cow_path, parent_cow_path):
        log.debug("parent_cow_path: {}".format(parent_cow_path))

        parent_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            parent_cow_path[20:]  # remove /var/run/sr-mount, but keep the rest
        )  # TODO: 20 should be 18; the path starts with 2 extra slashes!!
        log.debug("parent_cow_cache_path: {}".format(parent_cow_cache_path))

        try:
            os.makedirs(parent_cow_cache_path.rsplit('/', 1)[0])
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

        # Only snapshot parent if it doesn't exist
        if not os.path.isfile(parent_cow_cache_path):
            # ['/usr/bin/vhd-util', 'snapshot', '--debug',
            # '-n', '/var/run/sr-mount/ext3_sr/base_img.vhdcache',
            # '-p', '/var/run/sr-mount/nfs_sr/base_img.vhd']
            log.debug("parent cache does not exist; snapshotting")
            COWUtil.snapshot(dbg, parent_cow_cache_path,
                             parent_cow_path, False)
        else:
            log.debug("parent cache exists; no action")

        leaf_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            cow_path[20:]
        )
        log.debug("leaf_cow_cache_path: {}".format(leaf_cow_cache_path))

        try:
            os.makedirs(leaf_cow_cache_path.rsplit('/', 1)[0])
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

        try:
            # Maybe deleting is redundant?
            os.unlink(leaf_cow_cache_path)
            log.debug("leaf cache exists; deleting")
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                log.debug("leaf cache does not exist")
            else:
                raise

        # ['/usr/bin/vhd-util', 'snapshot', '--debug',
        # '-n', '/var/run/sr-mount/ext3_sr/leaf.vhdcache',
        # '-p', '/var/run/sr-mount/ext3_sr/base_img.vhdcache',
        # '-S', '24576', '-e']
        log.debug("snapshotting leaf cache")
        COWUtil.snapshot(dbg, leaf_cow_cache_path, parent_cow_cache_path, True)

        # HANDLE VDI RESIZING

        try:
            # See if 'parent_cow_cache_path' exists
            tapdisk.load_tapdisk_metadata('', parent_cow_cache_path)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                parent_cache_tap = tapdisk.create(
                    "create 'parent_cache_tap' tapdisk"
                )
                tapdisk.save_tapdisk_metadata(
                    '',
                    parent_cow_cache_path,
                    parent_cache_tap
                )
            else:
                raise

        try:
            # This should not exist
            tapdisk.load_tapdisk_metadata('', leaf_cow_cache_path)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                leaf_cache_tap = tapdisk.create(
                    "create 'leaf_cache_tap' tapdisk"
                )
                tapdisk.save_tapdisk_metadata(
                    '',
                    leaf_cow_cache_path,
                    leaf_cache_tap
                )
            else:
                raise
        else:
            raise RuntimeError(
                "Tapdisk metadata file for '{}' exists; Aborting".format(
                    leaf_cow_cache_path
                )
            )

        return leaf_cache_tap.block_device()

    @staticmethod
    def activate(dbg, cow_path, parent_cow_path, nonpersistent):
        parent_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            parent_cow_path[20:]  # remove /var/run/sr-mount, but keep the rest
        )

        leaf_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            cow_path[20:]
        )

        parent_cache_tap = tapdisk.load_tapdisk_metadata(
            dbg,
            parent_cow_cache_path
        )

        with RefCounter('tapdisk', parent_cache_tap.uuid) as rc:
            # ['/usr/sbin/tap-ctl', 'allocate']
            # ['/usr/sbin/tap-ctl', 'spawn']
            # ['/usr/sbin/tap-ctl', 'attach', '-p', '5382', '-m', '0']
            # ['/usr/sbin/tap-ctl', 'open',
            #  '-p', '5382',
            #  '-m', '0',
            #  '-a', 'vhd:/var/run/sr-mount/ext3_sr/base_img.vhdcache',
            #  '-r', '-D']
            rc.increment(
                leaf_cow_cache_path,
                parent_cache_tap.open_2,
                'Open parent COW cache',
                'vhd',
                parent_cow_cache_path, {
                    'o_direct': False,
                    'leaf_cache': True
                }
            )

        tapdisk.save_tapdisk_metadata(
            dbg,
            parent_cow_cache_path,
            parent_cache_tap
        )

        leaf_cache_tap = tapdisk.load_tapdisk_metadata(
            dbg,
            leaf_cow_cache_path
        )

        with RefCounter('tapdisk', leaf_cache_tap.uuid) as rc:
            if rc.get_count() > 0:
                raise RuntimeError(
                    "Leaf cache COW already open: {}".format(
                        leaf_cow_cache_path
                    )
                )

            # ['/usr/sbin/tap-ctl', 'open', '-p', '5394', '-m', '1',
            # '-a', 'vhd:/var/run/sr-mount/ext3_sr/leaf.vhdcache',
            # '-e', '0', '-2', 'vhd:/var/run/sr-mount/nfs_sr/leaf.vhd',
            # '-D']
            rc.increment(
                leaf_cow_cache_path,
                leaf_cache_tap.open_2,
                'Open leaf COW cache',
                'vhd',
                leaf_cow_cache_path, {
                    'o_direct': False,
                    'existing_parent': str(parent_cache_tap.minor),
                    'standby': nonpersistent,
                    'secondary': {
                        'type': 'vhd',
                        'file_path': cow_path
                    }
                }
            )

        tapdisk.save_tapdisk_metadata(
            dbg,
            leaf_cow_cache_path,
            leaf_cache_tap
        )

    @staticmethod
    def deactivate(dbg, cow_path, parent_cow_path):
        leaf_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            cow_path[20:]
        )

        tap = tapdisk.load_tapdisk_metadata(dbg, leaf_cow_cache_path)

        with RefCounter('tapdisk', tap.uuid) as rc:
            if rc.get_count() > 1 or not rc.will_decrease(leaf_cow_cache_path):
                raise RuntimeError(
                    "Leaf cache COW already open: {}".format(
                        leaf_cow_cache_path
                    )
                )

            rc.decrement(
                leaf_cow_cache_path,
                tap.close,
                dbg
            )

        try:
            os.unlink(leaf_cow_cache_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

        parent_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            parent_cow_path[20:]  # remove /var/run/sr-mount, but keep the rest
        )  # TODO: 20 should be 18; the path starts with 2 extra slashes!!

        tap = tapdisk.load_tapdisk_metadata(dbg, parent_cow_cache_path)

        with RefCounter('tapdisk', tap.uuid) as rc:
            rc.decrement(
                parent_cow_cache_path,
                tap.close,
                dbg
            )

    @staticmethod
    def detach(dbg, cow_path, parent_cow_path):
        leaf_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            cow_path[20:]
        )

        tap = tapdisk.load_tapdisk_metadata(dbg, leaf_cow_cache_path)

        with RefCounter('tapdisk', tap.uuid) as rc:
            if rc.get_count() == 0:
                tap.destroy(dbg)
                tapdisk.forget_tapdisk_metadata(dbg, leaf_cow_cache_path)

        parent_cow_cache_path = os.path.join(
            IntelliCache.ROOT_PATH,
            parent_cow_path[20:]
        )

        tap = tapdisk.load_tapdisk_metadata(dbg, parent_cow_cache_path)

        with RefCounter('tapdisk', tap.uuid) as rc:
            if rc.get_count() == 0:
                tap.destroy(dbg)
                tapdisk.forget_tapdisk_metadata(dbg, parent_cow_cache_path)
