import xapi.storage.libs.libcow.callbacks


class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):

    def getVolumeUriPrefix(self, opq):
        return "zfs-ng/" + opq + "|"

    def volumeGetPath(self, opq, name):
        return "/dev/zvol/" + opq + "/" + name
