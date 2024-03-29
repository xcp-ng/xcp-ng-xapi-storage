from contextlib import contextmanager
import datetime
import errno
import fcntl
import json
import os
import re
import time
import urlparse

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.libs.libcow.metabase import VolumeMetabase

from .lock import Lock

import db_backup


class VolumeLock(object):
    """
    Container for data relating to a lock
    """

    LOCK_ACQUIRE_THRESHOLD = 1.0
    LOCK_HOLD_THRESHOLD = 10.0

    @staticmethod
    def get_lock_path(opq, name):
        """
        Get the lock path name for a lock
        """
        return os.path.join(opq, "lock_" + name)

    def __init__(self, opq, name):
        lock_path = self.get_lock_path(opq, name)
        self.path = lock_path
        self.lock_file = None
        self.name = name
        self.create_time = time.time()
        self.time_locked = None

    def lock(self, non_block=False):
        """
        Lock the lock file. Errors are to be handled by caller
        """
        flags = fcntl.LOCK_EX
        if non_block:
            flags = flags | fcntl.LOCK_NB
        self.lock_file = open(self.path, 'w+')
        fcntl.flock(self.lock_file, flags)
        self.time_locked = time.time()

        elapsed_time = self.time_locked - self.create_time
        if elapsed_time > VolumeLock.LOCK_ACQUIRE_THRESHOLD:
            log.debug('Lock name={} took {} second(s) to acquire'.format(
                self.name, self.time_locked - self.create_time))

    def unlock(self):
        """
        Unlock and remove the lock file
        """
        if self.lock_file:
            locked_time = time.time() - self.time_locked
            if locked_time > VolumeLock.LOCK_HOLD_THRESHOLD:
                log.debug(
                    "Lock name={} held for more than 10 second(s)".format(
                        (self.name)))
            fcntl.flock(self.lock_file, fcntl.LOCK_UN)
            self.lock_file.close()
            self.lock_file = None


class VolumeContext(object):

    def __init__(self, callbacks, sr, mode):
        self.opq = callbacks.volumeStartOperations(sr, mode)
        self.callbacks = callbacks

    def __enter__(self):
        return self.opq

    def __exit__(self, exc_type, value, traceback):
        self.callbacks.volumeStopOperations(self.opq)


class Callbacks(object):

    def _get_volume_path(self, opq, name):
        return os.path.join(opq, name)

    def _create_volume_container(self, opq, name):
        return self._get_volume_path(opq, name)

    def _remove_volume_container(self, opq, name):
        pass

    def get_trash_dir(self, opq):
        return os.path.join(opq, '.trash')

    def create_trash_dir(self, opq):
        """
        Make a trash directory to store old data like deleted volumes.
        """
        util.mkdir_p(self.get_trash_dir(opq))

    def empty_trash(self, opq):
        try:
            dir = self.get_trash_dir(opq)
            for path in os.listdir(dir):
                os.unlink(os.path.join(dir, path))
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    def _get_trash_volume_path(self, opq, name):
        return os.path.join(self.get_trash_dir(opq), name)

    def volumeCreate(self, opq, name, size):
        log.debug("volumeCreate opq=%s name=%s size=%d" % (opq, name, size))
        vol_path = self._create_volume_container(opq, name)
        try:
            open(vol_path, 'a').close()
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise
        return vol_path

    def volumeDestroy(self, opq, name):
        log.debug("volumeDestroy opq=%s name=%s" % (opq, name))
        self.create_trash_dir(opq)

        vol_path = self._get_volume_path(opq, name)
        try:
            os.rename(vol_path, self._get_trash_volume_path(opq, name))
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

        try:
            os.unlink(VolumeLock.get_lock_path(opq, name))
        except OSError:
            # Best effort to remove any associated lock files
            pass
        self._remove_volume_container(opq, name)

    def volumeGetPath(self, opq, name):
        log.debug("volumeGetPath opq=%s name=%s" % (opq, name))
        return self._get_volume_path(opq, name)

    def volumeResize(self, opq, name, new_size):
        pass

    def volumeGetPhysSize(self, opq, name):
        return util.get_physical_file_size(self._get_volume_path(opq, name))

    def volumeStartOperations(self, sr, mode):
        return urlparse.urlparse(sr).path

    def volumeStopOperations(self, opq):
        pass

    def volumeMetadataGetPath(self, opq):
        return os.path.join(opq, "sqlite3-metadata.db")

    def getUniqueIdentifier(self, opq):
        log.debug("getUniqueIdentifier opq=%s" % opq)
        meta_path = os.path.join(opq, "meta.json")
        with open(meta_path, "r") as fd:
            meta = json.load(fd)
            value = meta["unique_id"]
        return value

    def volumeLock(self, opq, name):
        lock = VolumeLock(opq, name)
        lock.lock()
        return lock

    def volumeUnlock(self, opq, lock):
        lock.unlock()

    def volumeTryLock(self, opq, name):
        lock = VolumeLock(opq, name)
        try:
            lock.lock(True)
            return lock
        except IOError, e:
            if e.errno in [errno.EACCES, errno.EAGAIN]:
                return None
            raise

    def get_data_metadata_path(self, opq, volume):
        """
        Get a path for semi-persistent storage of attachment details
        """
        return os.path.join(opq, volume)

    def getVolumeUriPrefix(self, opq):
        """
        Abstract method that must be implemented in SR code
        """
        raise NotImplementedError("getVolumeUriPrefix must be defined in SR")

    def get_current_host(self):
        """
        Return a unique identifier for the current host
        """
        return util.get_current_host()

    def get_background_tasks(self):
        path = os.path.abspath(re.sub("pyc$", "py", db_backup.__file__))
        return [('db_backup', path)]

    def get_database(self, opq):
        return VolumeMetabase(self.volumeMetadataGetPath(opq))

    def create_database(self, sr_path):
        db = self.get_database(sr_path)
        db.create()
        db.close()

    @contextmanager
    def db_context(self, opq):
        """
        Get the context manager for a write transaction
        """
        with Lock(opq, 'db', self):
            db = self.get_database(opq)
            with db._conn:
                yield db
            db.close()

    def _remove_old_backups(self, mnt_path, backups_path):
        with self.db_context(mnt_path) as db:
            max_backups = db.max_backups
        for backup in [r'^db-backup-.*\.sql$', r'^meta-backup-.*\.json$']:
            backups = [
                x for x in os.listdir(backups_path) if re.match(backup, x)
            ]
            backups.sort(reverse=True)
            while len(backups) > max_backups:
                os.remove('{}/{}'.format(backups_path, backups.pop()))

    def _backup(self, dbg, uri, mnt_path, backups_path):
        now = time.time()
        backup_suffix = datetime.datetime.fromtimestamp(now).strftime(
            '%Y-%m-%d_%H:%M:%S'
        )

        db_backup = '{}/db-backup-{}.sql'.format(backups_path, backup_suffix)
        with self.db_context(mnt_path) as db:
            db.dump(db_backup)
            db.last_backup_time = now
        log.debug('New db backup created: {}'.format(db_backup))

        meta_backup = '{}/meta-backup-{}.json'.format(
            backups_path, backup_suffix)
        util.dump_sr_metadata(dbg, uri, meta_backup)
        log.debug('New meta backup created: {}'.format(meta_backup))

        self._remove_old_backups(mnt_path, backups_path)

    def rolling_backup(self, dbg, uri, backups_path):
        """
        Backup meta database if necessary.
        """
        mnt_path = urlparse.urlparse(uri).path
        with Lock(mnt_path, 'db_backup', self):
            now = time.time()
            with self.db_context(mnt_path) as db:
                backup_interval = db.backup_interval
                last_backup_time = db.last_backup_time
            if now >= (last_backup_time + backup_interval):
                self._backup(dbg, uri, mnt_path, backups_path)
