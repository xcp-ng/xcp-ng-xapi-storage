from contextlib import contextmanager
import errno
import fcntl
import importlib
import inspect
import json
import os
import shutil
import signal
import stat
import string
import subprocess
import sys
import tempfile
import urllib.parse

from xapi.storage import log
from xapi import Rpc_light_failure


class CommandException(Exception):
    def __init__(self, code, cmd="", reason='exec failed'):
        self.code = code
        self.cmd = cmd
        self.reason = reason
        Exception.__init__(self, os.strerror(abs(code)))


def mkdir_p(path, mode=0o777):
    try:
        os.makedirs(path, mode)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            os.chmod(path, mode)
        else:
            raise


def is_block_device(filename):
    return stat.S_ISBLK(os.stat(filename).st_mode)


def get_file_size(filename):
    """
    Get the size of a file or block device.
    """
    res = os.stat(filename)
    if stat.S_ISBLK(res.st_mode):
        with open(filename, 'rb') as f:
            f.seek(0, 2)
            return f.tell()
    else:
        return res.st_size


def get_physical_file_size(filename):
    res = os.stat(filename)
    if stat.S_ISBLK(res.st_mode):
        with open(filename, 'rb') as f:
            f.seek(0, 2)
            return f.tell()
    else:
        return res.st_blocks * 512


def lock_file(dbg, filename, mode='a+'):
    lock_handle = open(filename, mode)
    fcntl.flock(lock_handle, fcntl.LOCK_EX)
    return lock_handle


def unlock_file(dbg, filehandle):
    """
    Unlocks and closes file
    """
    fcntl.flock(filehandle, fcntl.LOCK_UN)
    filehandle.close()


def call_unlogged(dbg, cmd_args, error=True, simple=True, exp_rc=0):
    """[call dbg cmd_args] executes [cmd_args]
    if [error] and exit code != exp_rc, log and throws a BackendError
    if [simple], returns only stdout
    """
    p = subprocess.Popen(
        cmd_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True
    )

    stdout, stderr = p.communicate()

    if error and p.returncode != exp_rc:
        log.error(
            "{}: {} exitted with code {}: {}".format(
                dbg,
                ' '.join(cmd_args),
                p.returncode,
                stderr
            )
        )

        raise CommandException(p.returncode, cmd=str(cmd_args),
                               reason=stderr.strip())

    if simple:
        return stdout
    return stdout, stderr, p.returncode


def call(dbg, cmd_args, error=True, simple=True, exp_rc=0):
    """[call dbg cmd_args] executes [cmd_args]
    if [error] and exit code != exp_rc, log and throws a BackendError
    if [simple], returns only stdout
    """
    log.debug("{}: Running cmd {}".format(dbg, cmd_args))
    return call_unlogged(dbg, cmd_args, error, simple, exp_rc)


def get_host_name_from_env():
    return os.environ.get('STORAGE_TEST_HOST_NAME')


def get_current_host_uuid():
    with open("/etc/xensource-inventory") as fd:
        for line in fd:
            if line.strip().startswith("INSTALLATION_UUID"):
                return line.split("'")[1]


def get_current_host():
    """Gets the current host name.

    Tightly bound to xcp & XenAPI, mock out for Unit Tests
    """

    # in case of component testing, host_name will be passed in as an
    # environment variable
    host_name = get_host_name_from_env()
    if host_name:
        return host_name

    return get_current_host_uuid()


def index(iterable, entry, instance=1):
    """Get index of 'entry' in 'iterable'.

    Args:
        iterable (...): any object that implements the 'index'
            method
        entry (...): entry to search in 'iterable'
        instance (int): instance of 'entry' to find the index of
            If negative, start from the end.
            (Default: 1)

    Returns:
        (int) 'instance'th index of 'entry' in 'iterable'

    Raises:
        AttributeError
        ValueError
        TypeError
    """
    if instance < 0:
        entry_count = iterable.count(entry)
        tmp = entry_count + instance + 1

        if tmp < 1:
            raise ValueError(
                "|instance| = {} > {} = iterable.count(entry)".format(
                    -instance,
                    entry_count
                )
            )

        instance = tmp
    elif instance == 0:
        raise ValueError("'instance' must be different from 0")

    idx = 0
    for i in range(instance):
        try:
            idx += iterable[idx:].index(entry) + 1
        except ValueError:
            raise ValueError("'{}' appears {} times in list".format(entry, i))

    return idx - 1


def remove_path(path, force=False):
    """Removes filesystem entry.

    Args:
        path (str): path to file or directory

    Raises:
        ValueError
        OSError
    """
    try:
        os.unlink(path)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            if not force:
                raise
        elif exc.errno == errno.EISDIR:
            shutil.rmtree(path)
        else:
            raise


def remove_folder_content(dirpath):
    """Remove folder content and preserve main target folder."""
    for filename in os.listdir(dirpath):
        filepath = os.path.join(dirpath, filename)
        try:
            shutil.rmtree(filepath)
        except OSError:
            os.remove(filepath)


def sanitise_name(name):
    """Returns filesystem friendly 'name'.

    Invalid characters will be replaced with the underscore character.

    Args:
        name (str): name to sanitize

    Returns:
        (str) composed only of valid characters
    """
    allowed_chars = ''.join([string.ascii_letters, string.digits, '-._'])

    char_list = []
    for c in name:
        if c in allowed_chars:
            char_list.append(c)
        else:
            char_list.append('_')

    return ''.join(char_list)


def var_run_prefix():
    var_run = "/var/run"
    host_name = get_host_name_from_env()
    if host_name:
        var_run = os.path.join(var_run, "test_storage", host_name)
    return var_run


def decorate_all_routines(decorator):
    def _decorate_all_routines(cls):
        for name, fn in inspect.getmembers(cls, inspect.isroutine):
            if not name.startswith('_'):
                setattr(cls, name, decorator(fn))
        return cls
    return _decorate_all_routines


def log_exceptions_in_function(function):
    def _log_exceptions(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Rpc_light_failure as e:
            log.info('Reporting failure {} to caller'.format(e))
            raise
        except Exception:
            log.error('Exception in xapi.storage.plugin', exc_info=True)
            raise
    return _log_exceptions


def get_sr_metadata(dbg, sr):
    meta = None
    u = urllib.parse.urlparse(sr)
    if u.scheme != 'file':
        raise Exception('Unknown scheme')
    metapath = os.path.join(u.path, 'meta.json')
    log.debug('%s: metapath = %s' % (dbg, metapath))
    with open(metapath, 'r') as meta_fd:
        meta = json.load(meta_fd)
        return meta


def update_sr_metadata(dbg, sr, update_dict):
    u = urllib.parse.urlparse(sr)
    if u.scheme == 'file':
        # Getting meta.lock to avoid race during meta.json updates
        metalockpath = os.path.join(u.path, 'meta.lock')
        metalock = lock_file(dbg, metalockpath, mode='w+')
        try:
            meta = None
            try:
                meta = get_sr_metadata(dbg, sr)
                meta.update(update_dict)
            except IOError as ioerror:
                if ioerror.errno != errno.ENOENT:
                    raise
                # We're creating a new config file from scratch
                meta = update_dict
            # Updating meta.json via tempfile, to avoid corruption during
            # crashes or when running out of space
            tempfd = tempfile.NamedTemporaryFile(mode='w',
                                                 dir=u.path,
                                                 delete=False)
            json.dump(meta, tempfd)
            tempfd.write('\n')
            tempfd.close()
            metapath = os.path.join(u.path, 'meta.json')
            os.rename(tempfd.name, metapath)
            log.debug('%s: dumped metadata to %s: %s' % (dbg, metapath, meta))
        finally:
            unlock_file(dbg, metalock)
    else:
        raise Exception('Unknown scheme: `{}`'.format(u.scheme))


def dump_sr_metadata(dbg, sr, path):
    u = urllib.parse.urlparse(sr)
    if u.scheme == 'file':
        metalock = lock_file(dbg, os.path.join(u.path, 'meta.lock'), mode='w+')
        try:
            try:
                meta = get_sr_metadata(dbg, sr)
            except IOError as ioerror:
                if ioerror.errno != errno.ENOENT:
                    raise
            with open(path, "w") as file:
                json.dump(meta, file)
                file.write('\n')
            log.debug('{}: dumped metadata to {}'.format(dbg, path))
        finally:
            unlock_file(dbg, metalock)
    else:
        raise Exception('Unknown scheme: `{}`'.format(u.scheme))


def get_sr_callbacks(sr_type):
    sys.path.insert(
        1,
        '/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.{}'
        .format(sr_type)
    )
    mod = importlib.import_module(sr_type)
    return mod.Callbacks()


def raise_exc_if_device_in_use(_dbg, device_path):
    """
    Raises an exception if the device is already in
    use on the system
    """
    try:
        fd = os.open(device_path, os.O_EXCL)
        os.close(fd)
    except OSError as oserror:
        if oserror.errno == errno.EBUSY:
            raise create_storage_error(
                "SRInUse",
                ["The SR device is currently in use",
                 "Device %s in use, please check your existing" % device_path +
                 " SRs for an instance of this device"])
        else:
            raise


def create_storage_error(error_code, params):
    """
    Create an externally consumable error

    In most cases these will need to be equivalent to legacy
    storage errors so that external handlers remain compatible
    """
    import xapi
    return xapi.XenAPIException(error_code, params)


def daemonize():
    """
    Daemonize the process by disconnecting from stdin, out, err
    """
    for io_pipes in [0, 1, 2]:
        try:
            os.close(io_pipes)
        except OSError:
            pass


def set_cgroup(pid, cgroup):
    try:
        subprocess.check_call(['/usr/bin/cgclassify', '-g', cgroup, str(pid)])
    except subprocess.CalledProcessError as e:
        log.error('Unable to set cgroup {} of {}: {}'.format(cgroup, pid, e))


class TimeoutException(Exception):
    pass


@contextmanager
def timeout(seconds):
    def handler(signum, frame):
        raise TimeoutException

    oldHandler = signal.signal(signal.SIGALRM, handler)

    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, oldHandler)
