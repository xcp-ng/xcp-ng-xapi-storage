import os
import signal
import errno
import uuid
# from python-fdsend
# import fdsend

# TODO: Get rid of 'image' module
from . import image
from xapi.storage.libs.util import call
from xapi.storage.libs.util import var_run_prefix
from xapi.storage import log
import pickle

# Use Xen tapdisk to create block devices from files

blktap2_prefix = "/dev/xen/blktap-2/tapdev"

# nbdclient_prefix = "/var/run/blktap-control/nbdclient"
# nbdserver_prefix = "/var/run/blktap-control/nbdserver"

TD_PROC_METADATA_FILE = "meta.pickle"

IMAGE_TYPES = frozenset(['vhd', 'aio'])


class Tapdisk(object):

    def __init__(self, minor, pid, f):
        self.minor = minor
        self.pid = pid
        self.f = f
        self.secondary = None  # mirror destination
        self.type = None
        self.file_path = None
        self.uuid = str(uuid.uuid4())

    def __repr__(self):
        return "Tapdisk(%s, %s, %s)" % (self.minor, self.pid, self.f)

    def destroy(self, dbg):
        self.pause(dbg)
        call(dbg,
             ["tap-ctl",
              "destroy",
              "-m",
              str(self.minor),
              "-p",
              str(self.pid)])

    def close(self, dbg):
        call(dbg,
             ["tap-ctl",
              "close",
              "-m",
              str(self.minor),
              "-p",
              str(self.pid)])
        self.f = None

    def open(self, dbg, f, o_direct=True):
        assert (isinstance(f, image.Cow) or isinstance(f, image.Raw))
        args = ["tap-ctl", "open", "-m", str(self.minor),
                "-p", str(self.pid), "-a", str(f)]
        if not o_direct:
            args.append("-D")
        call(dbg, args)
        self.f = f

    # More flexible open
    def open_2(self, dbg, type_, file_path, options):
        # pid, minor, _type, _file, options
        assert type_ in IMAGE_TYPES
        self.type_file = ':'.join([type_, os.path.realpath(file_path)])

        cmd = [
            'tap-ctl', 'open',
            '-m', str(self.minor),
            '-p', str(self.pid),
            '-a', self.type_file
        ]

        if 'readonly' in options and options['readonly']:
            cmd.append('-R')

        if 'leaf_cache' in options and options['leaf_cache']:
            cmd.append('-r')

        if ('existing_parent' in options and
                options['existing_parent'] is not None):
            cmd.append('-e')
            cmd.append(str(options['existing_parent']))

        if ('secondary' in options and
                'type' in options['secondary'] and
                'file_path' in options['secondary']):
            assert options['secondary']['type'] in IMAGE_TYPES
            cmd.append('-2')
            cmd.append(
                ':'.join([
                    options['secondary']['type'],
                    os.path.realpath(options['secondary']['file_path'])
                ])
            )

        if 'standby' in options and options['standby']:
            cmd.append('-s')

        if 'timeout' in options and options['timeout'] is not None:
            cmd.append('-t')
            cmd.append(str(options['timeout']))

        if 'o_direct' in options and not options['o_direct']:
            cmd.append('-D')

        call(dbg, cmd)

    def pause(self, dbg):
        call(dbg,
             ["tap-ctl",
              "pause",
              "-m",
              str(self.minor),
              "-p",
              str(self.pid)])

    def unpause(self, dbg, f=None):
        cmd = ["tap-ctl", "unpause", "-m",
               str(self.minor), "-p", str(self.pid)]
        if f:
            cmd = cmd + ["-a", str(f)]
        if self.secondary is not None:
            cmd = cmd + ["-2 ", self.secondary]
        call(dbg, cmd)

    def block_device(self):
        return blktap2_prefix + str(self.minor)

    """
    ToDo: fdsend needs to be imported
    def start_mirror(self, dbg, fd):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(nbdclient_prefix + str(self.pid))
        token = "token"
        fdsend.sendfds(sock, token, fds=[fd])
        sock.close()
        self.secondary = "nbd:" + token
        self.pause(dbg)
        self.unpause(dbg)

    def stop_mirror(self, dbg):
        self.secondary = None
        self.pause(dbg)
        self.unpause(dbg)

    ToDo: fdsend needs to be imported
    def receive_nbd(self, dbg, fd):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect("%s%d.%d" % (nbdserver_prefix, self.pid, self.minor))
        token = "token"
        fdsend.sendfds(sock, token, fds=[fd])
        sock.close()
    """


def create(dbg):
    output = call(dbg, ["tap-ctl", "spawn"]).strip()
    pid = int(output)
    output = call(dbg, ["tap-ctl", "allocate"]).strip()
    prefix = blktap2_prefix
    minor = None
    if output.startswith(prefix):
        minor = int(output[len(prefix):])
    if minor is None:
        os.kill(pid, signal.SIGQUIT)
        # TODO: FIXME:  break link to XAPI
        # raise xapi.InternalError("tap-ctl allocate returned unexpected " +
        #                         "output: %s" % (output))
    call(dbg, ["tap-ctl", "attach", "-m", str(minor), "-p", str(pid)])
    return Tapdisk(minor, pid, None)


def find(minor=None, pid=None, type_=None, file_path=None):
    """Find tapdisks that satisfy the parameters.

    Args:
        minor: (str|int) minor number
        pid: (str|int) pid number
        type_: (str) 'vhd' or 'aio'
        file_path: (str) file path

    Return:
        (list[Tapdisk()]) list of initialized Tapdisk objects
    """
    tapdisk_object_list = []

    for tapdisk_dict in TapCtl.list(minor, pid, type_, file_path):
        tapdisk_object_list.append(
            Tapdisk(
                tapdisk_dict['minor'],
                tapdisk_dict['pid'],
                None
            )
        )

        tapdisk_object_list[-1].type = tapdisk_dict['type']
        tapdisk_object_list[-1].file_path = tapdisk_dict['file_path']

    return tapdisk_object_list


def find_by_file(dbg, meta_path):
    log.debug("%s: find_by_file f=%s" % (dbg, meta_path))
    # See whether this host has any metadata about this file
    try:
        log.debug("%s: find_by_file trying uri=%s" % (dbg, meta_path))
        tap = load_tapdisk_metadata(dbg, meta_path)
        log.debug("%s: returning td %s" % (dbg, tap))
        return tap
    # TODO: FIXME: Sort this error out
    # except xapi.storage.api.v5.volume.Volume_does_not_exist:
    except OSError:
        pass


def _metadata_dir(path):
    return os.path.join(var_run_prefix(), "nonpersistent",
                        "dp-tapdisk", os.path.realpath(path).lstrip('/'))


def save_tapdisk_metadata(dbg, path, tap):
    """ Record the tapdisk metadata for this VDI in host-local storage """
    dirname = _metadata_dir(path)
    try:
        os.makedirs(dirname, mode=0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    with open(dirname + "/" + TD_PROC_METADATA_FILE, "wb") as fd:
        pickle.dump(tap.__dict__, fd)


def load_tapdisk_metadata(dbg, path):
    """Recover the tapdisk metadata for this VDI from host-local
       storage."""
    dirname = _metadata_dir(path)
    log.debug("%s: load_tapdisk_metadata: trying '%s'" % (dbg, dirname))
    filename = dirname + "/" + TD_PROC_METADATA_FILE
    # No need to check for file existence;
    #  if file not there, IOError is raised
    # if not(os.path.exists(filename)):
    #    raise Exception('volume doesn\'t exist')
    #    #raise xapi.storage.api.v5.volume.Volume_does_not_exist(dirname)
    with open(filename, "rb") as fd:
        meta = pickle.load(fd)
        tap = Tapdisk(meta['minor'], meta['pid'], meta['f'])
        tap.secondary = meta['secondary']
        tap.type = meta['type']
        tap.file_path = meta['file_path']

    return tap


def forget_tapdisk_metadata(dbg, path):
    """Delete the tapdisk metadata for this VDI from host-local storage."""
    dirname = _metadata_dir(path)
    try:
        os.unlink(dirname + "/" + TD_PROC_METADATA_FILE)
    except OSError:
        pass


class TapCtl(object):

    @staticmethod
    def list(minor=None, pid=None, type_=None, file_path=None):
        result_list = []
        search_attributes = set()

        cmd = ['tap-ctl', 'list']

        if minor is not None:
            cmd += ['-m', str(minor)]
            search_attributes.add('minor')
        if pid is not None:
            cmd += ['-p', str(pid)]
            search_attributes.add('pid')
        if type_ is not None:
            cmd += ['-t', str(type_)]
            search_attributes.add('type')
        if file_path is not None:
            cmd += ['-f', str(file_path)]
            search_attributes.add('file_path')

        stdout = call('', cmd).rstrip().split('\n')
        # Example return:
        # 'pid=6068 minor=0 state=0
        # args=vhd:/run/sr-mount/<mount_point>\n'
        for line in stdout:
            # pid minor state args
            tap_dict = {}
            for field in line.split():
                name, value = field.split('=')

                if name in ('pid', 'minor'):
                    tap_dict[name] = int(value)
                elif name == 'state':
                    tap_dict[name] = int(value, 0x10)
                elif name == 'args':
                    args = value.split(':')
                    tap_dict['type'] = args[0]
                    tap_dict['file_path'] = args[1]

            for attr in search_attributes:
                if attr not in tap_dict:
                    break
            else:
                result_list.append(tap_dict)

        return result_list
