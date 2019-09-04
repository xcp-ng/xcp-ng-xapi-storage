#!/usr/bin/env python
import csha
import os
import signal
import xapi.storage.libs.libdlm as dlm

csha.log.debug('HA Holding master lock forever')

host_uuid = csha.util.get_current_host_uuid()
master_lock = dlm.DLMLock("master_lock", "xapi-clusterd-lockspace")

try:
    master_lock.try_lock(dlm.LOCK_EX)

    sf = csha.Statefile()
    sf.set_master(csha.Master(host_uuid, os.getpid()))

    # Hold the lock forever
    signal.pause()
except:
    csha.log.debug('HA Master lock already taken')
