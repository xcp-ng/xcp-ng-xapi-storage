import xapi.storage.libs.libcow.callbacks
from xapi.storage.libs.libcow.imageformat import ImageFormat

class Callbacks(xapi.storage.libs.libcow.callbacks.Callbacks):
    def imageFormat(self, sharable):
        return ImageFormat.IMAGE_REFLINK

    def getVolumeUriPrefix(self, opq):
        return "nfs-ng/" + opq + "|"
