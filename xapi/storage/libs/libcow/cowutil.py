"""
Abstract interface for utilities handling copy on write files
"""


class COWUtil(object):

    @staticmethod
    def get_max_chain_height():
        raise NotImplementedError()

    @staticmethod
    def is_empty(dbg, vol_path):
        raise NotImplementedError()

    @staticmethod
    def create(dbg, vol_path, size_mib):
        raise NotImplementedError()

    @staticmethod
    def destroy(dbg, vol_path):
        """Destroy a volume.

        Args:
            vol_path: (str) Absolute path to the volume to destroy
        """
        raise NotImplementedError()

    @staticmethod
    def clone(dbg, snap_path, clone_path):
        """Clone from snapshot.

        Args:
            snap_path: (str) Absolute path to the snapshot
            clone_path: (str) Absolute path to clone
        """
        raise NotImplementedError()

    @staticmethod
    def promote(dbg, clone_path):
        """Promote clone.

        Args:
            clone_path: (str) Absolute path to a clone
        """
        raise NotImplementedError()

    @staticmethod
    def resize(dbg, vol_path, size_mib):
        raise NotImplementedError()

    @staticmethod
    def reset(dbg, vol_path):
        """Zeroes out the disk."""
        raise NotImplementedError()

    @staticmethod
    def online_snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        """Perform COW snapshot.

        Args:
            new_cow_path: (str) Absolute path to the COW that will
                be created
            parent_cow_path: (str) Absolute path to the existing COW
                we wish to snapshot
            force_parent_link: (bool) If 'True', link new COW to
                the parent COW, even if the parent is empty
        """
        raise NotImplementedError()

    @staticmethod
    def offline_snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link):
        """Perform COW snapshot.

        Args:
            new_cow_path: (str) Absolute path to the COW that will
                be created
            parent_cow_path: (str) Absolute path to the existing COW
                we wish to snapshot
            force_parent_link: (bool) If 'True', link new COW to
                the parent COW, even if the parent is empty
        """
        raise NotImplementedError()

    @staticmethod
    def coalesce(dbg, vol_path, parent_path):
        """
        Coalesce/commit the changes in vol_path to its parent
        """
        raise NotImplementedError()

    @staticmethod
    def get_parent(dbg, vol_path):
        raise NotImplementedError()

    @staticmethod
    def get_vsize(dbg, vol_path):
        raise NotImplementedError()

    @staticmethod
    def set_parent(dbg, vol_path, parent_path):
        raise NotImplementedError()

    @staticmethod
    def is_parent_pointing_to_path(dbg, vol_path, parent_path):
        raise NotImplementedError()

    @staticmethod
    def getImgFormat(dbg):
        raise NotImplementedError()

    @staticmethod
    def refresh_datapath_clone(dbg, meta_path, new_path):
        raise NotImplementedError()

    @staticmethod
    def refresh_datapath_coalesce(
            dbg, meta_path, coalesced_node, parent_node):
        raise NotImplementedError()

    @staticmethod
    def pause_datapath(dbg, meta_path):
        raise NotImplementedError()

    @staticmethod
    def unpause_datapath(dbg, meta_path, new_path):
        raise NotImplementedError()
