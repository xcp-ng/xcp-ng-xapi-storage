import os.path
import xapi.storage.libs.libcow.callbacks

class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):
    "ZFS-ng callbacks"

    def getVolumeUriPrefix(self, opq):
        return "zfs-ng/" + opq + "|"
