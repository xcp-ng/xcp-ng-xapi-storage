import os.path
from xapi.storage.common import call

# Use Linux "losetup" to create block devices from files


class Loop:

    """An active loop device"""

    def __init__(self, path, loop):
        self.path = path
        self.loop = loop

    def destroy(self, dbg):
        call(dbg, ["losetup", "-d", self.loop])

    def block_device(self):
        return self.loop


def find(dbg, path):
    """Return the active loop device associated with the given path"""
    # The kernel loop driver will transparently follow symlinks, so
    # we must too.
    path = os.path.realpath(path)
    for line in call(dbg, ["losetup", "-a"]).split("\n"):
        line = line.strip()
        if line != "":
            bits = line.split()
            loop = bits[0][0:-1]
            open_bracket = line.find('(')
            close_bracket = line.find(')')
            this_path = line[open_bracket + 1:close_bracket]
            if this_path == path:
                return Loop(path, loop)
    return None


def create(dbg, path):
    """Creates a new loop device backed by the given file"""
    # losetup will resolve paths and 'find' needs to use string equality
    path = os.path.realpath(path)

    call(dbg, ["losetup", "-f", path])
    return find(dbg, path)
