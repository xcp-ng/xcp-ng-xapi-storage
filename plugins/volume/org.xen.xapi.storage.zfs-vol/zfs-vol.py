import os.path
import xapi.storage.libs.libcow.callbacks

class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):
    "ZFS-vol callbacks"

    def getVolumeUriPrefix(self, opq):
        return "zfs-ng/" + opq + "|"

    def volumeGetPath(self, opq, name):
        return os.path.join("/dev/zvol", os.path.basename(opq), name)
