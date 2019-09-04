from xapi.storage import log
from xapi.storage.libs import util
import os
import mmap
import pickle
import sys

BLK_ALIGN = 4096
BLK_SIZE = 512
MAX_HOSTS = 64


class Host:
    def __init__(self, uuid, online):
        self.uuid = uuid
        self.online = online


class Master:
    def __init__(self, uuid, pid):
        self.uuid = uuid
        self.pid = pid


class Statefile:
    def __init__(self):
        with open("/etc/xensource/xhad.conf") as fd:
            for line in fd:
                line = line.strip()
                if line.startswith("<StateFile>"):
                    self.state_file_path = line[11:-12]
                    try:
                        self.open_state_file()
                    except Exception:
                        log.error("HA Statefile error", exc_info=True)
                        # statefile might be unplugged already, e.g. during
                        # shutdown
                        sys.exit(14)
                    break

    def open_state_file(self):
        self.fd = os.open(self.state_file_path, os.O_RDWR | os.O_DIRECT)
        self.fo = os.fdopen(self.fd, 'rw')
        self.mm = mmap.mmap(-1, 1024 * 4)

    def read(self, offset):
        self.mm.seek(0)
        self.fo.seek(offset * BLK_ALIGN)
        self.fo.readinto(self.mm)
        return pickle.loads(self.mm.read(BLK_SIZE))

    def write(self, offset, val):
        s = pickle.dumps(val)
        if len(s) < BLK_SIZE:
            self.mm.seek(0)
            self.mm.write(s)
        else:
            raise ValueError(
                'Tried to write more than {} bytes'.format(BLK_SIZE))
        os.lseek(self.fd, offset * BLK_ALIGN, os.SEEK_SET)
        os.write(self.fd, self.mm)

    def read_all_hosts(self):
        hosts = {}
        for i in range(1, MAX_HOSTS + 1):
            hosts[i] = self.read(i)
        return hosts

    def format_all_hosts(self):
        for i in range(1, MAX_HOSTS + 1):
            self.write(i, Host(None, False))

    def set_host(self, offset, host):
        self.write(offset, host)

    def get_host(self, offset):
        return self.read(offset)

    def set_master(self, master):
        self.write(0, master)

    def get_master(self):
        return self.read(0)

    def set_invalid(self, invalid):
        self.write(MAX_HOSTS + 1, invalid)

    def get_invalid(self):
        return self.read(MAX_HOSTS + 1)


def get_current_host_node_id():
    cmd = ["corosync-cmapctl", "-g", "runtime.votequorum.this_node_id"]
    ret = util.call("HA", cmd).strip()
    return int(ret.split(" ")[3])
