import os
import errno
import pickle
from xapi.storage.common import call

"""
Use Linux "nbd-client" to create block devices from NBD servers.

Note:
- There's no way to atomically allocate the 'next' free nbd device
  like there is with 'losetup -f'

- There's no way to look up an existing nbd server by url so we'll
  have to remember the mapping ourselves by pickling objects in
  the 'persist_root'
"""

persist_root = "/tmp/persist-nbd/"


def path_to_persist(nbd):
    return persist_root + nbd.nbd


def clear():
    call("clear", ["rm", "-rf", persist_root])


class Nbd:

    """An active nbd device"""

    def __init__(self, host, name, nbd):
        self.host = host
        self.name = name
        self.nbd = nbd
        path = path_to_persist(self)
        to_create = os.path.dirname(path)
        try:
            os.makedirs(to_create)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(to_create):
                pass
            else:
                raise
        with open(path, 'w') as f:
            pickle.dump(self, f)

    def destroy(self, dbg):
        call(dbg, ["nbd-client", "-d", "/dev/%s" % self.nbd])
        os.unlink(path_to_persist(self))

    def block_device(self):
        return self.nbd


class NoAvailableNbd(Exception):

    def __init__(self):
        Exception.__init__(self)


def find(dbg, host, name):
    """Return the active nbd device associated with the given name"""
    used = set()
    try:
        used = set(os.listdir(persist_root))
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            pass
        else:
            raise
    for filename in used:
        with open(persist_root + filename) as file:
            nbd = pickle.load(file)
            if nbd.name == name:
                return nbd
    return None


def create(dbg, host, name):
    """Return an active nbd device associated with the given name,
       creating a fresh one if one doesn't already exist."""
    existing = find(dbg, host, name)
    if existing:
        return existing
    used = set()
    try:
        used = set(os.listdir(persist_root))
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            pass
        else:
            raise
    all = set(x for x in os.listdir("/dev") if x.startswith("nbd"))
    for nbd in all.difference(used):
        # try:
        call(dbg, ["nbd-client", host, "/dev/" + nbd, "-name", name, "-b", "4096"])
        with open("/sys/block/" + nbd + "/queue/scheduler", "w") as fd:
            fd.write("none")
        return Nbd(host, name, nbd)
        # except:
        #    pass # try another one
    raise NoAvailableNbd()
