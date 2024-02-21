from __future__ import absolute_import
import os
import errno
import fcntl
import pickle

from xapi.storage import log
from xapi.storage.libs import util


class RefCounter(object):

    """RefCounter class.

    RefCounter objects are used to keep track of actions that may be
    called more than 1 times by different actors.

    N.B.: All methods, bar reset(), will raise a 'TypeError'
          exception, unless lock() is called first or execution
          is in a 'with' statement block.
    """

    ROOT_PATH = '/var/run/nonpersistent/refcounter'
    LOCK_SFX = '.lock'

    def __init__(self, path):
        """RefCounter object init.

        Args:
            path (str): relative path to the refcounter file
                e.g. 'lvm/vg_name/lv_name'

        Raises:
            AttributeError
        """
        self.__entries = [
            util.sanitise_name(e) for e in path.strip('/').split('/')
        ]

        self.__id = os.path.join(*self.__entries)

        # Lock file objects.
        self.__locks = [None] * len(self.__entries)

        self.__refcounter_path = os.path.join(
            RefCounter.ROOT_PATH,
            self.__id
        )

        # Keeps refcounter in memory
        self.__refcount_dict = None

    def __enter__(self):
        """Get a locked RefCounter instance."""
        self.lock()
        return self

    def __exit__(self, exception_type, exception_val, trace):
        """Unlock RefCounter instance and destroy it."""
        self.unlock()

    def lock(self):
        """Lock refcount file and copy its contents in memory.

        This method is idempotent.
        """
        if self.__locks[-1] is not None:
            return

        self.__open_and_lock()

        with open(self.__refcounter_path, 'a+') as f:
            try:
                self.__refcount_dict = pickle.load(f)
            except EOFError:
                self.__refcount_dict = {}

        self.__log("Locked.")

    def unlock(self):
        """Dump in-memory refcount contents to file and unlock it.

        This method is idempotent.
        """
        if self.__locks[-1] is None:
            return

        if self.get_count() == 0:
            self.reset()
        else:
            with open(self.__refcounter_path, 'w') as f:
                pickle.dump(self.__refcount_dict, f, pickle.HIGHEST_PROTOCOL)

        self.__unlock_and_close()
        self.__refcount_dict = None
        self.__log("Unlocked.")

    def increment(self, key):
        """Add 'key' to RefCounter object; return new count.

        If 'key' has not been seen yet, add 'key'
        (increments count by 1).
        Else, increment requests for 'key'
        (does not increment refcount).

        Args:
            key (str): uniquely identifies the refcounter user

        Returns:
            (int) new count for RefCounter object

        Raises:
            TypeError
        """
        count = self.get_count()

        try:
            self.__refcount_dict[key] += 1
            self.__log("increment() - key '{}' exists; count = {}", key, count)
        except KeyError:
            self.__refcount_dict[key] = 1
            count += 1
            self.__log("increment() - key '{}' added; count = {}", key, count)

        return count

    def decrement(self, key):
        """Remove 'key' from RefCounter; return new count.

        If 'key' exists, remove it (decrements count by 1).

        Args:
            key (str): uniquely identifies the refcounter user

        Returns:
            (int) new count for RefCounter object

        Raises:
            TypeError
        """
        count = self.get_count()

        try:
            reqs = self.__refcount_dict[key]
        except KeyError:
            self.__log(
                "decrement() - key '{}' not present; count = {}",
                key,
                count
            )
            return count

        del self.__refcount_dict[key]

        count -= 1

        self.__log(
            "decrement() - key '{}' had {} increment "
            "requests before closing; count = {}",
            key,
            reqs,
            count
        )

        return count

    def reset(self, entry=None, instance=-1):
        """Resets all refcounters from 'entry' and forwards.

        WARNING:
        The 'entry' requested to be reset MUST NOT be present in any
        other locked RefCount instance in the same calling process,
        or the process will deadlock.

        Args:
            entry (str/None): If 'entry' is None, the refcounter
                is removed.
            instance (int): The instance of the 'entry' to remove.
                [Default: -1 (last entry)]
        """
        if entry is None:
            entry = self.__entries[-1]

        idx = util.index(self.__entries, entry, instance)
        entries_path = os.path.join(*self.__entries[:idx + 1])
        path = os.path.join(RefCounter.ROOT_PATH, entries_path)

        if self.__locks[-1] is None:
            self.__open_and_lock(0, idx + 1)
            util.remove_path(path, True)
            self.__unlock_and_close(idx)
        else:
            self.__unlock_and_close(len(self.__entries) - 1, idx)
            util.remove_path(path, True)
            self.__open_and_lock(idx + 1)
            self.__refcount_dict = {}

        self.__log("'{}' successfully reset.", entries_path)

    def get_count(self):
        """Returns current count for RefCounter.

        Raises:
            TypeError
        """
        return len(self.__refcount_dict)

    def __open_and_lock(self, start_i=None, stop_i=None):
        """Open '.lock' files and lock them.

        The locking is done from left to right. All locks are share
        locked, except the last one which is exclusively locked.

        Args:
            start_i (int): start index (inclusive)
            stop_i: (int) stop index (non-inclusive)

        Raises:
            OSError
        """
        if start_i is None:
            start_i = 0
        if stop_i is None:
            stop_i = len(self.__entries)

        # If we don't start from 0, we assume 'start_i - 1'
        # is ex_locked; sh_lock it and continue.
        if 0 < start_i < len(self.__entries):
            fcntl.flock(self.__locks[start_i - 1], fcntl.LOCK_SH)

        incremental_path = os.path.join(
            RefCounter.ROOT_PATH,
            *self.__entries[:start_i]
        )
        for i in xrange(start_i, stop_i):
            lock_path = os.path.join(
                incremental_path,
                self.__entries[i] + RefCounter.LOCK_SFX
            )

            try:
                os.makedirs(incremental_path, 0o644)
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    if os.path.isfile(incremental_path):
                        self.__unlock_and_close(i - 1)
                        raise OSError(
                            "Cannot create RefCounter group '{}'; RefCounter "
                            "file with the same name already exists.".format(
                                self.__entries[i]
                            )
                        )
                else:
                    raise

            self.__locks[i] = open(lock_path, 'a+')
            fcntl.flock(self.__locks[i], fcntl.LOCK_SH)

            incremental_path = os.path.join(
                incremental_path,
                self.__entries[i]
            )

        # The last entry we lock is allowed to be
        # a directory only when called by reset()
        if stop_i == len(self.__entries) and os.path.isdir(incremental_path):
            self.__unlock_and_close()
            raise OSError(
                "Cannot create RefCounter file '{}'; RefCounter group with "
                "the same name already exists.".format(
                    self.__entries[stop_i - 1]
                )
            )

        if stop_i - start_i > 0:
            fcntl.flock(self.__locks[stop_i - 1], fcntl.LOCK_EX)

    def __unlock_and_close(self, start_i=None, stop_i=None):
        """Closes open '.lock' files.

        The unlocking is done from right to left.
        'start_i' should be greater than 'stop_i'.

        Args:
            start_i: (int) start index (inclusive)
            stop_i: (int) stop index (non-inclusive)
        """
        if start_i is None:
            start_i = len(self.__entries) - 1
        if stop_i is None:
            stop_i = -1

        for i in xrange(start_i, stop_i, -1):
            self.__locks[i].close()
            self.__locks[i] = None

        # Move the ex_lock to the last
        # open lock, if there is one.
        if start_i > stop_i and -1 < stop_i < len(self.__entries) - 1:
            fcntl.flock(self.__locks[stop_i], fcntl.LOCK_EX)

    def __log(self, msg, *args):
        log.debug("RefCounter [{}]: ".format(self.__id) + msg.format(*args))
