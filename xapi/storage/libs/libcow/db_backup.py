#!/usr/bin/env python
"""
Daemon to backup SQL database and metadata.
"""
import sys
import time
import traceback
import urllib.parse

from xapi.storage import log
from xapi.storage.libs import util

LOG_DOMAIN = 'db_backup'


def backup(uri, callbacks):
    util.daemonize()
    mnt_path = urllib.parse.urlparse(uri).path
    backups_path = "{}/db_backups".format(mnt_path)
    util.mkdir_p(backups_path)
    while True:
        try:
            # 1. Get database configuration.
            with callbacks.db_context(mnt_path) as db:
                last_backup_time = db.last_backup_time
                backup_interval = db.backup_interval
            next_backup_time = last_backup_time + backup_interval

            # 2. Wait before next backup.
            now = time.time()
            while now < next_backup_time:
                time.sleep(next_backup_time - now)
                now = time.time()

            # 3. Backup!
            callbacks.rolling_backup(LOG_DOMAIN, uri, backups_path)
        except IOError:
            log.error(
                '{}: failed to write backup, abort!'.format(LOG_DOMAIN, uri))
            raise
        except Exception:
            log.error(
                '{}: execution error: {}'.format(LOG_DOMAIN, sys.exc_info()))
            log.error(traceback.format_exc())


if __name__ == '__main__':
    try:
        backup(
            'file://{}'.format(sys.argv[2]),
            util.get_sr_callbacks(sys.argv[1])
        )
    except RuntimeError:
        log.error(
            '{}: uncaught exception: {}'.format(LOG_DOMAIN, sys.exc_info()))
        raise
