import xapi.storage.libs.libcow.callbacks


class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):

    def getVolumeUriPrefix(self, opq):
        return "ext4-ng/" + opq + "|"
