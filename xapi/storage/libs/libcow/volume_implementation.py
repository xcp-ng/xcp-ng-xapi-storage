import xapi.storage.api.v5.volume
from .volume import COWVolume
from xapi.storage.libs import util


@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(xapi.storage.api.v5.volume.Volume_skeleton):
    def __init__(self, callbacks):
        self.callbacks = callbacks

    def clone(self, dbg, sr, key):
        return COWVolume.clone(dbg, sr, key, self.callbacks)

    def snapshot(self, dbg, sr, key):
        return COWVolume.snapshot(dbg, sr, key, self.callbacks)

    def create(self, dbg, sr, name, description, size, sharable):
        return COWVolume.create(
            dbg,
            sr,
            name,
            description,
            size,
            sharable,
            self.callbacks
        )

    def destroy(self, dbg, sr, key):
        return COWVolume.destroy(dbg, sr, key, self.callbacks)

    def resize(self, dbg, sr, key, new_size):
        return COWVolume.resize(
            dbg,
            sr,
            key,
            new_size,
            self.callbacks
        )

    def set(self, dbg, sr, key, k, v):
        COWVolume.set(dbg, sr, key, k, v, self.callbacks)

    def unset(self, dbg, sr, key, k):
        COWVolume.unset(dbg, sr, key, k, self.callbacks)

    def set_description(self, dbg, sr, key, new_description):
        COWVolume.set_description(
            dbg,
            sr,
            key,
            new_description,
            self.callbacks
        )

    def set_name(self, dbg, sr, key, new_name):
        COWVolume.set_name(dbg, sr, key, new_name, self.callbacks)

    def stat(self, dbg, sr, key):
        return COWVolume.stat(dbg, sr, key, self.callbacks)
