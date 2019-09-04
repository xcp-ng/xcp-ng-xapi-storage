import errno
import os
import xapi.storage.libs.libcow.callbacks
from xapi.storage.libs import util

from database import GFS2Database
import dlm_monitor


class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):

    def _get_vol_dir(self, opq, name):
        return os.path.join(opq, name)

    def _get_volume_path(self, opq, name):
        vol_dir = self._get_vol_dir(opq, name)
        return os.path.join(vol_dir, name)

    def _create_volume_container(self, opq, name):
        vol_dir = self._get_vol_dir(opq, name)
        try:
            os.makedirs(vol_dir, mode=0755)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise
        vol_path = self._get_volume_path(opq, name)
        return vol_path

    def _remove_volume_container(self, opq, name):
        vol_dir = self._get_vol_dir(opq, name)
        try:
            os.rmdir(vol_dir)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                pass
            else:
                raise

    def getVolumeUriPrefix(self, opq):
        return "gfs2/" + opq + "|"

    def get_cluster_node_id(self):
        """
        Get the corosync cluster node id
        """
        cmd = ["corosync-cmapctl", "-g", "runtime.votequorum.this_node_id"]
        ret = util.call("GFS2", cmd).strip()
        return ret.split(" ")[3]

    def get_database(self, path):
        db_path = os.path.join(path, "sqlite3-metadata.db")
        database = GFS2Database(db_path)
        return database

    def create_database(self, path):
        database = self.get_database(path)
        database.create()
        database.close()

    def get_background_tasks(self):
        return [('monitor', dlm_monitor.monitor_dlm)]
