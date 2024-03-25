import os.path
import xapi.storage.libs.libcow.callbacks

class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):
    "ZFS-vol callbacks"

    def getVolumeUriPrefix(self, opq):
        return "zfs-vol/" + opq + "|"
