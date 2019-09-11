"""
Python wrapper for libdlm_lt (the "light" version of libdlm that does
not use pthread functions).

Class:
    DLMLock: Creates an object associated with a named lock and
             lockspace in DLM. Provides blockng lock, try lock,
             setting persistent locks and adopting orphan locks.

Exceptions:
    DLMError: Generic module Exception class
    DLMErrno: Gets the last errno and raises an exception
              with the error and description

Functions:
    join_lockspace: Current node joins a lockspace
    leave_lockspace: Current leaves joins a lockspace

Lock Modes:
    LOCK_SH: shared lock
    LOCK_EX: exclusive lock

Not yet implemented:
    - Lock Value Block
        A lock can have a 32 byte value associated
        with it, that can be read/written

    - Conversion Deadlock Resolution
        A lock's granted mode may be set to NULL in order
        order to prevent a deadlock
"""

from __future__ import absolute_import
import os
import errno
from ctypes import (
    CDLL,
    CFUNCTYPE,
    Structure,
    get_errno,
    byref,
    c_int,
    c_ushort,
    c_uint,
    c_uint32,
    c_uint64,
    c_char,
    c_char_p,
    c_void_p
)

__all__ = [
    'DLMLock', 'DLMError', 'DLMErrno',      # Classes
    'join_lockspace', 'leave_lockspace',    # Functions
    'LOCK_SH', 'LOCK_EX',                   # Constants
]

# DLM library object
_LIBDLM_LT = CDLL('/usr/lib64/libdlm_lt.so.3.0', use_errno=True)

# Lock modes
_LOCK_NL = 0  # LKM_NLMODE (null lock)
LOCK_SH = 3   # LKM_PRMODE (protected read)
LOCK_EX = 5   # LKM_EXMODE (exclusive)

_LOCK_MODES = (LOCK_EX, LOCK_SH, _LOCK_NL)

# Locking flags - these match the ones in dlm.h
_LKF_NOQUEUE = 0x00000001
_LKF_CONVERT = 0x00000004
_LKF_PERSISTENT = 0x00000080
_LKF_EXPEDITE = 0x00000400
_LKF_ORPHAN = 0x00004000
_LKF_TIMEOUT = 0x00040000
_LKF_WAIT = 0x80000000


class _LockStatusBlock(Structure):
    """struct dlm_lksb

    sb_status -> status
    sb_lkid   -> lock_id
    sb_flags  -> flags -- (unused)
    sb_lvbptr -> val_blk -- (unused)
    """

    _fields_ = [
        ('status', c_int),
        ('lock_id', c_uint32),
        ('flags', c_char),
        ('val_blk', c_char_p)
    ]

# Extra return codes used by DLM
# (appear in '_LockStatusBlock.status')


# Unlock operation was successful
errno.EUNLOCK = 0x10002

# Lock operation is in progress
# ('_LKF_WAIT' not set when locking)
errno.EINPROG = 0x10003


class DLMError(Exception):
    """Generic DLM Exception class."""


class DLMErrno(DLMError):

    """Creates [errno] - <strerror> Exceptions."""

    def __init__(self):
        self.errno = get_errno()
        strerror = os.strerror(self.errno)

        super(DLMErrno, self).__init__(
            "[{}] - {}".format(errno.errorcode[self.errno], strerror)
        )


def _dummy_callback(obj):
    pass


_dummy_callback = CFUNCTYPE(None, c_void_p)(_dummy_callback)


def join_lockspace(name):
    """Join DLM lockspace.

    Affects current node only.

    Args:
        name: (str) lockspace name

    Raises:
        DLMErrno [EINVAL]: 'name' is longer than the
            supported length (currently 64 characters)
    """
    handle = _LIBDLM_LT.dlm_open_lockspace(c_char_p(name))

    if handle == 0:
        handle = _LIBDLM_LT.dlm_create_lockspace(
            c_char_p(name),
            c_ushort(0o600)
        )

        if handle == 0:
            raise DLMErrno()

    _LIBDLM_LT.dlm_close_lockspace(handle)


def leave_lockspace(name, force=False):
    """Leave DLM lockspace.

    Affects current node only.

    Args:
        name: (str) DLM lockspace name
        force: (int) force destroy lockspace, even if
            there are still active locks in it
            WARNING: if the process is holding any lock in the
                     lockspace to be removed and 'force' is set,
                     then it will hang and DLM will be unable
                     to function properly until the system is
                     restarted

    Raises:
        DLMErrno [ENOENT]: lockspace 'name' does not exist
        DLMErrno [EBUSY]: the lockspace could not be freed, because
            it still contains locks and 'force' was not set
    """
    handle = _LIBDLM_LT.dlm_open_lockspace(c_char_p(name))

    if handle == 0:
        raise DLMErrno()

    rv = _LIBDLM_LT.dlm_release_lockspace(
        c_char_p(name),
        c_void_p(handle),
        c_int(force)
    )

    # On success, rv = 0 and errno can be 0, ENOENT or ENODEV.
    # On failure, there are cases where we can have rv = 0
    # and errno set  (e.g. EBUSY)
    if rv == -1 or get_errno() == errno.EBUSY:
        raise DLMErrno()


class DLMLock(object):

    """Encapsulates DLM locks"""

    def __init__(self, lock_name, lockspace_name='default'):
        """Allocate lock resource in lockspace.

        Raises:
            DLMErrno [ENOENT]: lockspace 'lockspace_name'
                does not exist
            DLMErrno [EINVAL]: 'lock_name' is longer than the
                supported length (currently 64 characters)
        """

        self._lock_name = lock_name

        self._lksb = _LockStatusBlock()
        self._lksb.status = -1
        self._lksb.lock_id = 0
        self._lksb.flags = '\0'
        self._lksb.val_blk = None

        self._lockspace_handle = None
        self._lockspace_name = lockspace_name
        self._open_lockspace()

        self._allocate_lock()

    def __del__(self):
        """Close lockspace and unlock 'lock_name', if non persistent."""
        self._close_lockspace()

    def __enter__(self):
        """Get an exclusive wait lock."""
        self.lock_wait(LOCK_EX)
        return self

    def __exit__(self, exception_type, exception_val, trace):
        """Unlock and destroy lock."""
        self.unlock()

    def lock_wait(self, lock_mode, persist=False, timeout=None):
        """Block until lock is granted.

        Args:
            lock_mode: (int) locking mode; SHARED or EXUSIVE
                - LOCK_SH: to be used for concurrent read
                - LOCK_EX: to be used for exclusive read/write
            persist: (bool) if 'True', lock will become an
                orphan, if not explicitely unlocked
            timeout: (int/None) time in seconds, after which
                DLMErrno [ETIMEDOUT] will be raised, if
                the lock is not granted;
                if 'None', it waits indefinitely

        Raises:
            TypeError: 'timeout' not a positive integer or 'None'
            DLMErrno [EINVAL]: invalid 'lock_mode' argument
            DLMErrno [EINVAL]: 'persist' not of type 'bool'
            DLMErrno [ETIMEDOUT]: 'timeout' expired
        """
        if (timeout is not None and
                (not isinstance(timeout, int) or timeout < 0)):
            raise TypeError(
                "'timeout' must be either a positive integer or 'None'"
            )

        flags = _LKF_WAIT

        if timeout is not None:
            flags |= _LKF_TIMEOUT
            timeout = byref(c_uint64(timeout * 100))

        self._lock(lock_mode, flags, persist, timeout)

    def try_lock(self, lock_mode, persist=False):
        """Try to get lock.

        An exception is raised if the lock
        cannot be granted instantly.

        Args:
            lock_mode: (int) locking mode; SHARED or EXUSIVE
                - LOCK_SH: to be used for concurrent read
                - LOCK_EX: to be used for exclusive read/write
            persist: (bool) if 'True', lock will become an
                orphan, if not explicitely unlocked

        Raises:
            DLMErrno [EINVAL]: invalid 'lock_mode' argument
            DLMErrno [EINVAL]: 'persist' not of type 'bool'
            DLMErrno [EAGAIN]: lock held already and
                'lock_mode's incompatible
        """

        flags = _LKF_WAIT
        flags |= _LKF_NOQUEUE
        self._lock(lock_mode, flags, persist, None)

    def adopt_lock(self):
        """Adopt orphan lock.

        Adopted lock is persistent, unless it is explicitely locked
        again with 'persist' set to 'False' or unlocked.

        Raises:
            DLMErrno [ENOENT]: orphan lock by
                that name does not exist.
        """
        # Release the lock resource before adopting
        self._unlock()

        flags = _LKF_ORPHAN
        flags |= _LKF_NOQUEUE

        for lock_mode in _LOCK_MODES:
            try:
                self._lock(lock_mode, flags, True, None)
                break
            except DLMErrno as exc:
                # EAGAIN means that there is an orphan lock
                # with this name, but 'lock_mode' was wrong.
                # So try again!
                if exc.errno != errno.EAGAIN:
                    # Create a new lock resource, in
                    # case the exception is caught
                    self._allocate_lock()
                    raise

    def unlock(self):
        """Unlock currently held lock."""
        # Lock resource NOT released

        flags = _LKF_WAIT
        self._lock(_LOCK_NL, flags, False, None)

    def _lock(self, lock_mode, flags, persist, timeout):
        # Raises:
        #   DLMErrno [ENOENT]
        #   DLMErrno [EINVAL]
        #   DLMErrno [EAGAIN]
        #   DLMErrno [ETIMEDOUT]

        # Let libdlm_lt set the error
        if lock_mode not in _LOCK_MODES or not isinstance(persist, bool):
            lock_mode = 0xD1E

        if self._lksb.lock_id > 0:
            # Set CONVERT even from/to the same lock mode;
            # there might be a change in PERSIST flag
            flags |= _LKF_CONVERT

        if persist:
            flags |= _LKF_PERSISTENT

        # _dummy_callback() is necessary due to the following:
        # 1) orphan locks cannot be adopted with _LKF_WAIT
        #    (callback is ignored if _LKF_WAIT is set)
        # 2) if callback is None, dlm_ls_lockx() fails and sets
        #    errno to EINVAL
        # 3) any other value gets interpreted as a function pointer
        #    and gets called in dlm_ls_unlock_wait(), resulting
        #    in segfault
        rv = _LIBDLM_LT.dlm_ls_lockx(
            self._lockspace_handle,
            c_uint32(lock_mode),
            byref(self._lksb),
            c_uint32(flags),
            c_char_p(self._lock_name),
            c_uint(len(self._lock_name)),
            0,                  # parent
            _dummy_callback,
            None,               # astarg
            None,               # bastarg
            None,               # xid
            timeout
        )

        if rv == -1:
            # If this is not a CONVERT operation, where
            # the lock ID must be preserved, zero it
            if not flags & _LKF_CONVERT:
                self._lksb.lock_id = 0

            raise DLMErrno()

    def _allocate_lock(self):
        # Allocate a lock resource in the
        # lockspace by getting a NULL lock
        # Raises:
        #   DLMErrno [EINVAL]

        flags = _LKF_EXPEDITE
        flags |= _LKF_WAIT
        self._lock(_LOCK_NL, flags, False, None)

    def _unlock(self):
        # Properly releases the lock resource

        if self._lksb.lock_id == 0:
            return

        rv = _LIBDLM_LT.dlm_ls_unlock_wait(
            self._lockspace_handle,
            self._lksb.lock_id,
            0,
            byref(self._lksb)
        )

        # Should never happen, but just in case
        if rv == -1:
            raise DLMErrno()

        self._lksb.lock_id = 0

    def _open_lockspace(self):
        # Raises:
        #     DLMErrno [ENOENT]: lockspace does not exist

        tmp_handle = _LIBDLM_LT.dlm_open_lockspace(
            c_char_p(self._lockspace_name)
        )

        if tmp_handle == 0:
            raise DLMErrno()

        self._lockspace_handle = c_void_p(tmp_handle)

    def _close_lockspace(self):
        # If the lock is non persistent, the
        # resource will be released here

        # Always returns 0; does not set errno
        if self._lockspace_handle:
            _LIBDLM_LT.dlm_close_lockspace(self._lockspace_handle)
        self._lockspace_handle = None
