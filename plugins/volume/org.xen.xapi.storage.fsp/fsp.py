import xapi.storage.libs.libcow.callbacks


class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):

    def getVolumeUriPrefix(self, opq):
        return "fsp/" + opq + "|"

    def volumeGetPhysSize(self, opq, name):
        return 0
